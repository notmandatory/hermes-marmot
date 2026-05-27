"""
Marmot Protocol Gateway Adapter for Hermes Agent.

Uses mdk-python (Marmot Development Kit) for MLS-encrypted group messaging
over Nostr relays. No external daemon required.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

import mdk
from mdk import new_mdk_with_key, MdkConfig, MdkUniffiError

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform

from .nostr_relay import RelayManager

# Lazy-import nostr_sdk for NIP-59 gift wrap unwrapping
_nostr_sdk = None

def _get_nostr_sdk():
    global _nostr_sdk
    if _nostr_sdk is None:
        import nostr_sdk as sdk
        _nostr_sdk = sdk
    return _nostr_sdk

RELAY_URLS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
]
HOME = Path.home()
DEFAULT_IDENTITY_PATH = HOME / ".hermes" / "marmot-identity.sec"
DEFAULT_DB_PATH = HOME / ".hermes" / "marmot-mdk.db"
DEFAULT_DB_KEY_PATH = HOME / ".hermes" / "marmot-db.key"
DEFAULT_LAST_EVENT_TS_PATH = HOME / ".hermes" / "marmot-last-event.ts"

# How far back to look on first run (7 days)
DEFAULT_INITIAL_SINCE_SECS = 7 * 24 * 3600


def _load_or_create_identity(path: Path) -> tuple[str, bytes]:
    if path.exists():
        raw = path.read_bytes()
        if len(raw) == 32:
            pubkey_hex = _derive_pubkey_hex(raw)
            return pubkey_hex, raw
    from nostr_sdk import SecretKey
    sk = SecretKey.generate()
    raw = bytes.fromhex(sk.to_hex())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    pubkey_hex = _derive_pubkey_hex(raw)
    logger.info("Generated new Nostr identity: %s", pubkey_hex[:16])
    return pubkey_hex, raw


def _derive_pubkey_hex(private_key_bytes: bytes) -> str:
    from nostr_sdk import SecretKey, Keys
    sk = SecretKey.from_bytes(private_key_bytes)
    keys = Keys(sk)
    return keys.public_key().to_hex()


def _pubkey_to_npub(hex_pubkey: str) -> str:
    from nostr_sdk import PublicKey
    return PublicKey.parse(hex_pubkey).to_bech32()


def _npub_to_hex(npub: str) -> str:
    from nostr_sdk import Nip19
    result = Nip19.from_bech32(npub)
    return result.as_enum().npub.to_hex()


def _load_or_create_db_key(path: Path) -> bytes:
    if path.exists():
        return path.read_bytes()
    key = secrets.token_bytes(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(key)
    return key


def _bool_env(key: str, default: bool) -> bool:
    val = os.getenv(key, "").strip().lower()
    if val in ("1", "true", "yes"):
        return True
    if val:
        return False
    return default


def _int_env(key: str, default: int) -> int:
    val = os.getenv(key, "").strip()
    if val:
        try:
            return int(val)
        except ValueError:
            pass
    return default


class MarmotPlatformAdapter(BasePlatformAdapter):
    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("marmot"))
        extra = getattr(config, "extra", {}) or {}

        self.identity_path = Path(
            os.getenv("MARMOT_IDENTITY_PATH") or extra.get("identity_path", str(DEFAULT_IDENTITY_PATH))
        )
        self.db_path = Path(
            os.getenv("MARMOT_DB_PATH") or extra.get("db_path", str(DEFAULT_DB_PATH))
        )
        self.db_key_path = Path(
            os.getenv("MARMOT_DB_KEY_PATH") or extra.get("db_key_path", str(DEFAULT_DB_KEY_PATH))
        )

        relay_urls = os.getenv("MARMOT_RELAYS") or extra.get("relays", RELAY_URLS)
        if isinstance(relay_urls, str):
            self.relay_urls = [u.strip() for u in relay_urls.split(",") if u.strip()]
        elif isinstance(relay_urls, list):
            self.relay_urls = relay_urls
        else:
            self.relay_urls = list(RELAY_URLS)

        self.home_channel = os.getenv("MARMOT_HOME_CHANNEL") or extra.get("home_channel", "")

        allowed_raw = os.getenv("MARMOT_ALLOWED_USERS") or extra.get("allowed_users", "")
        if isinstance(allowed_raw, str):
            self.allowed_users: set = {u.strip() for u in allowed_raw.split(",") if u.strip()}
        elif isinstance(allowed_raw, list):
            self.allowed_users = set(allowed_raw)
        else:
            self.allowed_users = set()
        self.allow_all = _bool_env("MARMOT_ALLOW_ALL_USERS", extra.get("allow_all_users", False))

        self._mdk: Any = None
        self._relays: Optional[RelayManager] = None
        self._identity_pubkey_hex: str = ""
        self._identity_npub: str = ""
        self._known_event_ids: set[str] = set()
        self._sync_task: Optional[asyncio.Task] = None
        self._last_event_ts_path = Path(
            os.getenv("MARMOT_LAST_EVENT_TS_PATH") or DEFAULT_LAST_EVENT_TS_PATH
        )
        self._last_event_ts: int = 0

    @property
    def name(self) -> str:
        return "Marmot"

    # ── Connection lifecycle ───────────────────────────────────────────────

    async def connect(self) -> bool:
        from gateway.status import acquire_scoped_lock

        lock_key = "marmot:mdk"
        locked, _ = acquire_scoped_lock("marmot", lock_key)
        if not locked:
            logger.error("Marmot: another marmot instance is already running")
            self._set_fatal_error("lock_conflict", "Marmot already in use", retryable=False)
            return False
        self._lock_key = lock_key

        try:
            self._init_mdk()
            self._init_identity()
            await self._init_relays()
            self._start_sync()
        except Exception as e:
            logger.error("Marmot: connection failed: %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        self._mark_connected()
        logger.info(
            "Marmot: connected as %s, %d relay(s)",
            self._identity_npub[:16],
            len(self.relay_urls),
        )
        return True

    def _init_mdk(self) -> None:
        db_key = _load_or_create_db_key(self.db_key_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._mdk = new_mdk_with_key(
                db_path=str(self.db_path),
                encryption_key=db_key,
                config=None,
            )
        except MdkUniffiError as e:
            raise RuntimeError(f"MDK init failed: {e}")

    def _init_identity(self) -> None:
        pubkey_hex, self._identity_secret = _load_or_create_identity(self.identity_path)
        self._identity_pubkey_hex = pubkey_hex
        self._identity_npub = _pubkey_to_npub(pubkey_hex)

    def _load_last_event_ts(self) -> int:
        """Load the last processed event timestamp from disk."""
        if self._last_event_ts_path.exists():
            try:
                ts = int(self._last_event_ts_path.read_text().strip())
                logger.debug("Marmot: loaded last event timestamp: %d", ts)
                return ts
            except (ValueError, OSError):
                pass
        # Default: look back DEFAULT_INITIAL_SINCE_SECS on first run
        return int(time.time()) - DEFAULT_INITIAL_SINCE_SECS

    def _save_last_event_ts(self, ts: int) -> None:
        """Persist the last processed event timestamp to disk."""
        if ts > self._last_event_ts:
            self._last_event_ts = ts
            try:
                self._last_event_ts_path.write_text(str(ts))
            except OSError as e:
                logger.warning("Marmot: failed to save last event ts: %s", e)

    async def _init_relays(self) -> None:
        self._relays = RelayManager(self.relay_urls)
        self._last_event_ts = self._load_last_event_ts()
        await self._relays.connect_all()
        await self._publish_key_packages()
        asyncio.create_task(self._subscribe_to_events())

    async def _publish_key_packages(self) -> None:
        if not self._mdk or not self._relays:
            return
        try:
            kp = self._mdk.create_key_package_for_event(
                public_key=self._identity_pubkey_hex,
                relays=self.relay_urls,
            )
            event_id = await self._publish_signed_event(
                kind=30443,
                content=kp.key_package,
                tags=kp.tags,
            )
            logger.info("Published key package (event: %s)", event_id[:16])
        except Exception as e:
            logger.warning("Key package publish failed: %s", e)

    async def _publish_signed_event(
        self, kind: int, content: str, tags: list
    ) -> str:
        sdk = _get_nostr_sdk()
        raw_key = self.identity_path.read_bytes()
        sk = sdk.SecretKey.from_bytes(raw_key)
        keys = sdk.Keys(sk)

        unsigned = sdk.UnsignedEvent.from_json(json.dumps({
            "kind": kind,
            "content": content,
            "tags": tags,
            "pubkey": self._identity_pubkey_hex,
            "created_at": int(time.time()),
        }))
        event = unsigned.sign_with_keys(keys)
        event_json = event.as_json()

        if self._relays:
            await self._relays.publish_all(event_json)

        return event.id().to_hex()

    async def _subscribe_to_events(self) -> None:
        if not self._relays:
            return

        def _on_event(event: dict) -> None:
            asyncio.create_task(self._handle_relay_event(event))

        # Use since filter to catch up on missed events and avoid reprocessing old ones
        since_ts = self._last_event_ts
        filter_445 = {"kinds": [445], "since": since_ts}
        filter_1059 = {"kinds": [1059], "#p": [self._identity_pubkey_hex], "since": since_ts}
        logger.info("Marmot: subscribing with since=%d (%s)", since_ts, 
                    datetime.fromtimestamp(since_ts).isoformat() if since_ts else "none")
        for url, relay in self._relays.relays.items():
            if relay.is_connected:
                await relay.subscribe([filter_445], _on_event)
                await relay.subscribe([filter_1059], _on_event)

    async def _handle_relay_event(self, event: dict) -> None:
        event_id = event.get("id", "")
        if event_id in self._known_event_ids:
            return
        self._known_event_ids.add(event_id)

        if not self._mdk:
            return

        # Update last event timestamp for persistence
        event_ts = event.get("created_at", 0)
        if event_ts:
            self._save_last_event_ts(event_ts)

        kind = event.get("kind", 0)

        if kind == 1059:
            await self._handle_gift_wrap(event)
            return

        if kind != 445:
            return

        try:
            result = self._mdk.process_message(json.dumps(event))
        except Exception as e:
            logger.debug("Marmot: process_message error: %s", e)
            return

        if result.is_application_message():
            self._dispatch_mdk_message(result.message)
        elif result.is_commit():
            logger.debug("Marmot: processed MLS commit")
            self._handle_post_commit(event.get("pubkey", ""), event)
        elif result.is_proposal():
            logger.debug("Marmot: processing proposal commit")
            await self._publish_update_group_result(result.result)

    async def _handle_gift_wrap(self, event: dict) -> None:
        try:
            sdk = _get_nostr_sdk()
            raw_sk = self.identity_path.read_bytes()
            sk = sdk.SecretKey.from_bytes(raw_sk)
            keys = sdk.Keys(sk)
            signer = sdk.NostrSigner.keys(keys)
            sdk_event = sdk.Event.from_json(json.dumps(event))
            unwrapped = await sdk.UnwrappedGift.from_gift_wrap(signer, sdk_event)

            sender_hex = unwrapped.sender().to_hex()
            rumor = unwrapped.rumor()
            rumor_kind = rumor.kind().as_u16()

            rumor_json = rumor.as_json()

            if rumor_kind == 444:
                logger.info(
                    "Marmot: received welcome (kind 444) from %.16s",
                    sender_hex[:16],
                )
                welcome = self._mdk.process_welcome(
                    wrapper_event_id=event.get("id", ""),
                    rumor_event_json=rumor_json,
                )
                self._mdk.accept_welcome(welcome)
                logger.info("Marmot: accepted welcome from %.16s", sender_hex[:16])
            else:
                logger.info(
                    "Marmot: unwrapped gift wrap rumor kind %d from %s — processing",
                    rumor_kind, sender_hex[:16],
                )
                result = self._mdk.process_message(rumor_json)
                if result.is_application_message():
                    self._dispatch_mdk_message(result.message)
                elif result.is_commit():
                    logger.debug("Marmot: processed MLS commit from gift wrap")
                    self._handle_post_commit(sender_hex, event)
        except Exception as e:
            import traceback
            logger.warning("Marmot: gift wrap unwrap failed: %s\n%s", e, traceback.format_exc())

    def _dispatch_mdk_message(self, msg) -> None:
        if not self._message_handler:
            return
        try:
            ev = json.loads(msg.event_json)
        except Exception:
            return
        content = ev.get("content", "")
        if not content:
            return

        sender_pubkey = msg.sender_pubkey
        if sender_pubkey == self._identity_pubkey_hex:
            return

        sender_npub = _pubkey_to_npub(sender_pubkey)

        if not self.allow_all and self.allowed_users:
            allowed_hex = set()
            for u in self.allowed_users:
                if u.startswith("npub"):
                    try:
                        allowed_hex.add(_npub_to_hex(u))
                    except Exception:
                        pass
                elif len(u) == 64:
                    allowed_hex.add(u)
            if sender_pubkey not in allowed_hex:
                logger.debug("Marmot: ignoring message from unauthorized %s", sender_npub[:16])
                return

        group_id = msg.mls_group_id
        event_id = msg.event_id
        created_at = msg.created_at

        source = self.build_source(
            chat_id=group_id,
            chat_name=group_id[:16],
            chat_type="group",
            user_id=sender_npub,
            user_name=sender_npub[:16],
            message_id=event_id,
        )

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=event_id,
            timestamp=datetime.fromtimestamp(created_at),
        )

        logger.info(
            "Marmot: message from %.16s in %.16s: %.60s",
            sender_npub, group_id, content,
        )
        asyncio.create_task(self.handle_message(event))

    def _start_sync(self) -> None:
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def _sync_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30.0)
                await self._sync_groups()
                await self._check_self_update()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("Marmot: sync error: %s", e)

    async def _sync_groups(self) -> None:
        if not self._mdk:
            return

        try:
            welcomes = self._mdk.get_pending_welcomes()
        except Exception:
            welcomes = []
        for w in welcomes:
            try:
                self._mdk.accept_welcome(w)
                logger.info("Marmot: accepted pending welcome for group %s", w.group_name)
            except Exception as e:
                logger.debug("Marmot: accept pending welcome error: %s", e)

        try:
            groups = self._mdk.get_groups()
        except Exception:
            return
        for g in groups:
            try:
                messages = self._mdk.get_messages(
                    mls_group_id=g.mls_group_id,
                    limit=50,
                    offset=0,
                    sort_order="created_at_first",
                )
            except Exception:
                continue
            for msg in messages:
                if msg.event_id in self._known_event_ids:
                    continue
                self._known_event_ids.add(msg.event_id)
                if msg.sender_pubkey != self._identity_pubkey_hex:
                    self._dispatch_mdk_message(msg)

    async def _check_self_update(self) -> None:
        if not self._mdk or not self._relays:
            return
        try:
            needing = self._mdk.groups_needing_self_update(threshold_secs=3600)
        except Exception:
            return
        for group_id in needing:
            try:
                members = self._mdk.get_members(group_id)
                if len(members) <= 2:
                    continue
                result = self._mdk.self_update(group_id)
                await self._publish_update_group_result(result)
                logger.info("Marmot: self-update completed for %s", group_id[:16])
            except Exception as e:
                logger.debug("Marmot: self-update failed for %s: %s", group_id[:16], e)

    # ── Disconnect ─────────────────────────────────────────────────────────

    async def disconnect(self) -> None:
        if getattr(self, "_lock_key", None):
            try:
                from gateway.status import release_scoped_lock
                release_scoped_lock("marmot", self._lock_key)
            except Exception:
                pass

        self._mark_disconnected()

        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None

        if self._relays:
            await self._relays.disconnect_all()
            self._relays = None

        self._mdk = None

    # ── Sending ────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        if not self._mdk:
            return SendResult(success=False, error="MDK not initialized")
        try:
            event_json = self._mdk.create_message(
                mls_group_id=chat_id,
                sender_public_key=self._identity_pubkey_hex,
                content=content,
                kind=9,
                tags=None,
                event_tags=None,
            )
        except Exception as e:
            return SendResult(success=False, error=str(e))

        if self._relays:
            await self._relays.publish_all(event_json)
            await self._publish_gift_wraps(event_json, chat_id)

        return SendResult(success=True, message_id=chat_id[:16])

    async def _create_gift_wrap_with_current_ts(
        self, signer, recipient_pk, rumor
    ):
        """Create a NIP-59 gift wrap with current timestamp.
        
        The standard nostr-sdk gift_wrap() uses randomized past timestamps for privacy,
        but this can cause issues with clients that use `since` filters. This method
        creates a gift wrap with the current timestamp instead.
        """
        sdk = _get_nostr_sdk()
        
        # Step 1: Create the seal (kind 13)
        seal_builder = await sdk.EventBuilder.seal(signer, recipient_pk, rumor)
        seal = await seal_builder.sign(signer)
        
        # Step 2: Create a random ephemeral key for the outer wrapper
        ephemeral_sk = sdk.SecretKey.generate()
        ephemeral_keys = sdk.Keys(ephemeral_sk)
        ephemeral_signer = sdk.NostrSigner.keys(ephemeral_keys)
        
        # Step 3: Encrypt the seal to the recipient using NIP-44
        seal_json = seal.as_json()
        encrypted_content = await ephemeral_signer.nip44_encrypt(recipient_pk, seal_json)
        
        # Step 4: Create the gift wrap event (kind 1059) with current timestamp
        current_ts = int(time.time())
        gift_builder = sdk.EventBuilder(sdk.Kind(1059), encrypted_content)
        gift_builder = gift_builder.custom_created_at(sdk.Timestamp.from_secs(current_ts))
        gift_builder = gift_builder.tags([sdk.Tag.public_key(recipient_pk)])
        
        # Sign with ephemeral key
        gift = await gift_builder.sign(ephemeral_signer)
        
        return gift

    async def _publish_gift_wraps(self, event_json: str, chat_id: str) -> None:
        logger.info("Marmot: publishing gift wraps for group %.16s", chat_id)
        sdk = _get_nostr_sdk()
        raw_sk = self.identity_path.read_bytes()
        sk = sdk.SecretKey.from_bytes(raw_sk)
        keys = sdk.Keys(sk)
        signer = sdk.NostrSigner.keys(keys)

        event_data = json.loads(event_json)
        rumor = sdk.UnsignedEvent.from_json(json.dumps({
            "kind": event_data["kind"],
            "content": event_data["content"],
            "tags": event_data.get("tags", []),
            "pubkey": event_data["pubkey"],
            "created_at": event_data["created_at"],
        }))

        try:
            members = self._mdk.get_members(mls_group_id=chat_id)
        except Exception:
            return

        for member_hex in members:
            if member_hex == self._identity_pubkey_hex:
                continue
            try:
                recipient_pk = sdk.PublicKey.parse(member_hex)
                gift = await self._create_gift_wrap_with_current_ts(signer, recipient_pk, rumor)
                gift_json_str = gift.as_json()
                gift_data = json.loads(gift_json_str)
                
                logger.info("Marmot: gift wrap id=%s ts=%d tags=%s for %.16s", 
                           gift_data.get("id","")[:16], gift_data.get("created_at", 0),
                           gift_data.get("tags"), member_hex[:16])
                await self._relays.publish_all(gift_json_str)
            except Exception as e:
                logger.warning("Marmot: gift wrap failed for %.16s: %s", member_hex[:16], e)

    async def _publish_update_group_result(self, result) -> None:
        if not self._relays:
            return
        try:
            await self._relays.publish_all(result.evolution_event_json)
            logger.debug("Marmot: published evolution event for %s", result.mls_group_id[:16])
            if result.welcome_rumors_json:
                for rumor_json in result.welcome_rumors_json:
                    wrapper = json.loads(rumor_json)
                    await self._relays.publish_all(json.dumps(wrapper))
                    logger.debug("Marmot: published welcome rumor")
            self._mdk.merge_pending_commit(result.mls_group_id)
        except Exception as e:
            logger.debug("Marmot: publish update result error: %s", e)

    def _handle_post_commit(self, author_hex: str, event: dict) -> None:
        pass

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        pass

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ):
        return SendResult(success=False, error="Marmot does not support message editing")

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        return False

    async def get_chat_info(self, chat_id: str) -> dict:
        if not self._mdk:
            return {"name": chat_id[:16], "type": "group"}
        try:
            group = self._mdk.get_group(mls_group_id=chat_id)
            if group:
                return {"name": group.name or chat_id[:16], "type": "group"}
        except Exception:
            pass
        return {"name": chat_id[:16], "type": "group"}


# ---------------------------------------------------------------------------
# Plugin registration helpers
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    try:
        import mdk  # noqa: F401
        import nostr_sdk  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config) -> bool:
    return True


def interactive_setup() -> None:
    from hermes_cli.setup import (
        prompt, prompt_yes_no, save_env_value, get_env_value,
        print_header, print_info, print_warning, print_success,
    )

    print_header("Marmot Protocol Gateway (MDK)")
    existing = get_env_value("MARMOT_DB_PATH")
    if existing:
        print_info(f"Marmot: already configured (db: {existing})")
        if not prompt_yes_no("Reconfigure Marmot?", False):
            return

    print_info("Connect Hermes to Nostr encrypted-messaging (MLS/RFC 9420).")
    print_info("Uses mdk-python directly — no external daemon required.")
    print()

    relays = prompt(
        "Relay URLs (comma-separated)",
        default="wss://nos.lol,wss://relay.damus.io,wss://relay.primal.net",
    )
    if relays:
        save_env_value("MARMOT_RELAYS", relays)

    print()
    print_info("Access control")
    allow_all = prompt_yes_no("Allow anyone to DM the agent? (dev only)", False)
    if allow_all:
        save_env_value("MARMOT_ALLOW_ALL_USERS", "true")
        save_env_value("MARMOT_ALLOWED_USERS", "")
        print_warning("Open access — any Nostr user can message the agent")
    else:
        save_env_value("MARMOT_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed npubs (comma-separated)",
            default=get_env_value("MARMOT_ALLOWED_USERS") or "",
        )
        if allowed:
            save_env_value("MARMOT_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("Allowlist configured")
        else:
            save_env_value("MARMOT_ALLOWED_USERS", "")
            print_info("No npubs allowed — the agent will ignore all messages")

    print()
    home = prompt(
        "Home channel (group hex)",
        default=get_env_value("MARMOT_HOME_CHANNEL") or "",
    )
    if home:
        save_env_value("MARMOT_HOME_CHANNEL", home.strip())

    print()
    print_success("Marmot configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway: hermes gateway restart")


def is_connected(config) -> bool:
    return True


def _env_enablement() -> dict | None:
    return {"enabled": True}


def _register_marmot_cli(subparser):
    from .cli import register_cli
    register_cli(subparser)


def register(ctx):
    ctx.register_cli_command(
        name="marmot",
        help="Marmot protocol (identity, groups, send, status)",
        setup_fn=_register_marmot_cli,
        description="Query the marmot-cli daemon for identity, groups, and messages.",
    )
    ctx.register_platform(
        name="marmot",
        label="Marmot",
        adapter_factory=lambda cfg: MarmotPlatformAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint="Requires: pip install mdk-python nostr-sdk",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MARMOT_HOME_CHANNEL",
        allowed_users_env="MARMOT_ALLOWED_USERS",
        allow_all_env="MARMOT_ALLOW_ALL_USERS",
        max_message_length=65536,
        emoji="🦦",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Marmot (MLS-encrypted Nostr groups). "
            "Messages are end-to-end encrypted through Nostr relays. "
            "Each group ID is an encrypted chat room. Respond conversationally. "
            "Markdown formatting is supported."
        ),
    )
