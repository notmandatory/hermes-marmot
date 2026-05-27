"""
Marmot Protocol Gateway Adapter for Hermes Agent.

Connects to the marmot-cli daemon via JSON-RPC/TCP and relays Nostr
encrypted group messages (MLS / RFC 9420) to/from the agent.

Configuration via config.yaml::

    gateway:
      platforms:
        marmot:
          enabled: true
          extra:
            cli_path: marmot-cli
            identity: default
            host: 127.0.0.1
            port: 9222
            auto_start: true
            poll_interval_ms: 5000
            home_channel: npub1...
            allowed_users: [npub1..., npub2...]
            allow_all_users: false

Or via environment variables (override config.yaml):
    MARMOT_CLI_PATH, MARMOT_IDENTITY,
    MARMOT_DAEMON_HOST, MARMOT_DAEMON_PORT,
    MARMOT_AUTO_START, MARMOT_POLL_INTERVAL_MS,
    MARMOT_HOME_CHANNEL, MARMOT_ALLOWED_USERS,
    MARMOT_ALLOW_ALL_USERS
"""

import asyncio
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform

from .rpc_client import MarmotRpcClient
from .daemon import spawn_daemon, wait_for_daemon_ready, monitor_daemon


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
    """Marmot protocol adapter: polls marmot-cli daemon for messages.

    Instantiated by the adapter_factory passed to register_platform().
    Uses JSON-RPC over TCP to communicate with the marmot-cli daemon.
    """

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("marmot"))

        extra = getattr(config, "extra", {}) or {}

        self.cli_path = os.getenv("MARMOT_CLI_PATH") or extra.get("cli_path", "marmot-cli")
        self.identity = os.getenv("MARMOT_IDENTITY") or extra.get("identity", "default")
        self.daemon_host = os.getenv("MARMOT_DAEMON_HOST") or extra.get("host", "127.0.0.1")
        self.daemon_port = int(os.getenv("MARMOT_DAEMON_PORT") or extra.get("port", 9222))
        self.auto_start = _bool_env("MARMOT_AUTO_START", extra.get("auto_start", True))
        self.poll_interval_ms = _int_env("MARMOT_POLL_INTERVAL_MS", extra.get("poll_interval_ms", 5000))
        self.home_channel = os.getenv("MARMOT_HOME_CHANNEL") or extra.get("home_channel", "")

        self.allowed_users_raw = os.getenv("MARMOT_ALLOWED_USERS") or extra.get("allowed_users", "")
        if isinstance(self.allowed_users_raw, str):
            self.allowed_users: set = {u.strip() for u in self.allowed_users_raw.split(",") if u.strip()}
        elif isinstance(self.allowed_users_raw, list):
            self.allowed_users = set(self.allowed_users_raw)
        else:
            self.allowed_users = set()
        self.allow_all = _bool_env("MARMOT_ALLOW_ALL_USERS", extra.get("allow_all_users", False))

        self._client = MarmotRpcClient(host=self.daemon_host, port=self.daemon_port)
        self._daemon_proc: Any = None
        self._poll_task: Optional[asyncio.Task] = None
        self._daemon_monitor_task: Optional[asyncio.Task] = None
        self._daemon_restart_event = asyncio.Event()
        self._last_seen_ts: dict[str, int] = {}
        self._groups_cache: list = []
        self._groups_cache_ts: float = 0.0
        self._npub_cache: Optional[str] = None

    @property
    def name(self) -> str:
        return "Marmot"

    # ── helpers ────────────────────────────────────────────────────────────

    def _ensure_npub(self) -> str:
        if self._npub_cache:
            return self._npub_cache
        try:
            import json, socket as _sk
            s = _sk.socket(_sk.AF_INET, _sk.SOCK_STREAM)
            s.settimeout(5)
            s.connect((self.daemon_host, self.daemon_port))
            req = json.dumps({"jsonrpc":"2.0","id":1,"method":"identity_npub","params":{}}) + "\n"
            s.sendall(req.encode())
            data = s.recv(65536)
            s.close()
            result = json.loads(data.decode())
            npub = (result.get("result") or {}).get("npub", "")
            if npub:
                self._npub_cache = npub
            return npub
        except Exception:
            return ""

    async def _resolve_npub(self) -> str:
        if self._npub_cache:
            return self._npub_cache
        try:
            result = await self._client.identity_npub()
            npub = (result or {}).get("npub", "")
            if npub:
                self._npub_cache = npub
            return npub or ""
        except Exception:
            return ""

    async def _list_groups(self) -> list:
        now = time.monotonic()
        if self._groups_cache and now - self._groups_cache_ts < 60.0:
            return self._groups_cache
        try:
            result = await self._client.list_groups()
            groups = (result or {}).get("groups", [])
            if isinstance(groups, list):
                self._groups_cache = groups
                self._groups_cache_ts = now
            return self._groups_cache
        except Exception:
            return self._groups_cache

    # ── Connection lifecycle ───────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.cli_path:
            logger.error("Marmot: MARMOT_CLI_PATH must be configured")
            self._set_fatal_error("config_missing", "MARMOT_CLI_PATH is required", retryable=False)
            return False

        from gateway.status import acquire_scoped_lock
        lock_key = f"marmot:{self.daemon_host}:{self.daemon_port}"
        if not acquire_scoped_lock("marmot", lock_key):
            logger.error("Marmot: daemon %s:%s already in use", self.daemon_host, self.daemon_port)
            self._set_fatal_error("lock_conflict", "Marmot daemon in use by another profile", retryable=False)
            return False
        self._lock_key = lock_key

        try:
            if self.auto_start:
                try:
                    result = await self._client.ping(timeout=3.0)
                    if result.get("pong"):
                        logger.info("Marmot: daemon already running at %s:%s", self.daemon_host, self.daemon_port)
                except Exception:
                    logger.info("Marmot: starting daemon")
                    self._daemon_proc = await spawn_daemon(
                        cli_path=self.cli_path,
                        host=self.daemon_host,
                        port=self.daemon_port,
                    )
                    await wait_for_daemon_ready(self._client, timeout=30.0)
                    logger.info("Marmot: daemon ready")
            else:
                await wait_for_daemon_ready(self._client, timeout=10.0)
        except Exception as e:
            logger.error("Marmot: failed to connect to daemon: %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        self._daemon_restart_event.clear()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("Marmot: connected, identity=%s, daemon=%s:%s", self.identity, self.daemon_host, self.daemon_port)

        if self._daemon_proc:
            self._daemon_monitor_task = asyncio.create_task(
                monitor_daemon(
                    proc=self._daemon_proc,
                    client=self._client,
                    restart_fn=self._restart_daemon,
                    log_info=lambda msg: logger.info("Marmot: %s", msg),
                    log_error=lambda msg: logger.error("Marmot: %s", msg),
                )
            )

        self._mark_connected()
        return True

    async def _restart_daemon(self) -> None:
        if not self._daemon_restart_event.is_set():
            self._daemon_restart_event.set()
            try:
                self._daemon_proc = await spawn_daemon(
                    cli_path=self.cli_path,
                    host=self.daemon_host,
                    port=self.daemon_port,
                )
                await wait_for_daemon_ready(self._client, timeout=30.0)
                self._daemon_restart_event.clear()
                logger.info("Marmot: daemon restarted")
            except Exception as e:
                logger.error("Marmot: daemon restart failed: %s", e)

    async def disconnect(self) -> None:
        if getattr(self, "_lock_key", None):
            try:
                from gateway.status import release_scoped_lock
                release_scoped_lock("marmot", self._lock_key)
            except Exception:
                pass

        self._mark_disconnected()

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        if self._daemon_monitor_task and not self._daemon_monitor_task.done():
            self._daemon_monitor_task.cancel()
            try:
                await self._daemon_monitor_task
            except asyncio.CancelledError:
                pass
            self._daemon_monitor_task = None

        if self._daemon_proc:
            try:
                self._daemon_proc.terminate()
                await asyncio.wait_for(self._daemon_proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                try:
                    self._daemon_proc.kill()
                    await self._daemon_proc.wait()
                except Exception:
                    pass
            except Exception:
                pass
            self._daemon_proc = None

    # ── Polling ────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        first_poll = True
        while True:
            try:
                await asyncio.sleep(self.poll_interval_ms / 1000.0)
                if not await self._daemon_alive():
                    if self.is_connected:
                        logger.debug("Marmot: daemon unreachable during poll")
                    continue

                await self._run_cli_receive()

                groups = await self._list_groups()
                for g in groups:
                    gid = g.get("nostr_id") if isinstance(g, dict) else g
                    if not gid:
                        continue
                    await self._poll_group_messages(gid, first_poll)

                if first_poll:
                    first_poll = False
                    logger.info("Marmot: first poll complete, listening for new messages")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Marmot: poll loop error: %s", e)

    async def _daemon_alive(self) -> bool:
        try:
            result = await self._client.ping(timeout=3.0)
            return result is not None
        except Exception:
            return False

    def _cli_path(self) -> str:
        return os.getenv("MARMOT_CLI_PATH") or self.cli_path

    async def _run_cli_receive(self) -> None:
        try:
            cli = self._cli_path()
            proc = await asyncio.to_thread(
                subprocess.run,
                [cli, "receive"],
                capture_output=True, text=True,
                timeout=60,
            )
            if proc.returncode != 0:
                logger.debug("Marmot: marmot-cli receive exited %d: %s", proc.returncode, proc.stderr.strip()[:200])
        except subprocess.TimeoutExpired:
            logger.debug("Marmot: marmot-cli receive timed out")
        except FileNotFoundError:
            logger.warning("Marmot: %s not found", self._cli_path())
        except Exception as e:
            logger.debug("Marmot: receive subprocess error: %s", e)

    async def _poll_group_messages(self, group_id: str, first_poll: bool) -> None:
        after = self._last_seen_ts.get(group_id)
        try:
            cli = self._cli_path()
            argv = [cli, "groups", "messages", "--group", group_id]
            if after is not None:
                argv += ["--after", str(after)]
            argv += ["--limit", "50"]
            proc = await asyncio.to_thread(
                subprocess.run,
                argv,
                capture_output=True, text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            return
        except FileNotFoundError:
            return
        except Exception as e:
            logger.debug("Marmot: groups messages error: %s", e)
            return

        if proc.returncode != 0:
            return

        messages = self._parse_messages_output(proc.stdout)
        if not messages:
            return

        latest_ts = self._last_seen_ts.get(group_id, 0)
        for ts, eid, sender, content in messages:
            if ts > latest_ts:
                latest_ts = ts
            if not first_poll:
                if ts > self._last_seen_ts.get(group_id, 0):
                    self._dispatch_message(group_id, eid, sender, content, ts)

        self._last_seen_ts[group_id] = max(latest_ts, self._last_seen_ts.get(group_id, 0))

    def _parse_messages_output(self, stdout: str) -> list:
        messages: list = []
        for line in stdout.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") and "] evid:" in stripped:
                try:
                    ts_end = stripped.index("]")
                    ts_str = stripped[1:ts_end]
                    ts = int(ts_str)

                    rest = stripped[ts_end + 1:].strip()
                    if not rest.startswith("evid:"):
                        continue
                    rest = rest[5:].strip()
                    space_idx = rest.index(" ")
                    eid = rest[:space_idx]

                    rest = rest[space_idx + 1:].strip()
                    colon_idx = rest.index(": ")
                    sender = rest[:colon_idx]
                    content = rest[colon_idx + 2:]

                    messages.append((ts, eid, sender, content))
                except (ValueError, IndexError):
                    continue
        return messages

    def _dispatch_message(self, group_id: str, event_id: str, sender: str, content: str, ts: int) -> None:
        if not self._message_handler or not content:
            return

        self._ensure_npub()

        if sender == self._npub_cache:
            return

        if not self.allow_all and self.allowed_users and sender not in self.allowed_users:
            logger.debug("Marmot: ignoring message from unauthorized %s", sender[:16])
            return

        source = self.build_source(
            chat_id=group_id,
            chat_name=group_id,
            chat_type="group",
            user_id=sender,
            user_name=sender[:16],
            message_id=event_id,
        )

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=event_id,
            timestamp=datetime.fromtimestamp(ts),
        )

        logger.info("Marmot: message from %.16s in %.16s: %.60s", sender, group_id, content)
        asyncio.create_task(self.handle_message(event))

    # ── Sending ────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        try:
            # Sync MLS epoch before sending — CLI receive processes pending commits
            try:
                cli = self._cli_path()
                await asyncio.to_thread(
                    subprocess.run,
                    [cli, "receive"],
                    capture_output=True, text=True,
                    timeout=60,
                )
            except Exception:
                pass

            result = await self._client.send_message(
                group_id=chat_id,
                content=content,
                publish=True,
            )
            message_id = (result or {}).get("event_id", "") or (result or {}).get("id", "")
            return SendResult(success=True, message_id=message_id or str(int(time.time() * 1000)))
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        pass

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False):
        return SendResult(success=False, error="Marmot does not support message editing")

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        return False

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        groups = await self._list_groups()
        for g in groups:
            if isinstance(g, dict) and (g.get("nostr_id") == chat_id or g.get("id") == chat_id):
                return {"name": g.get("name", chat_id), "type": "group"}
        return {"name": chat_id, "type": "group"}


# ---------------------------------------------------------------------------
# Plugin registration helpers
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    return bool(os.getenv("MARMOT_CLI_PATH", ""))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(os.getenv("MARMOT_CLI_PATH") or extra.get("cli_path", ""))


def interactive_setup() -> None:
    from hermes_cli.setup import (
        prompt, prompt_yes_no, save_env_value, get_env_value,
        print_header, print_info, print_warning, print_success,
    )

    print_header("Marmot Protocol Gateway")
    existing = get_env_value("MARMOT_CLI_PATH")
    if existing:
        print_info(f"Marmot: already configured (path: {existing})")
        if not prompt_yes_no("Reconfigure Marmot?", False):
            return

    print_info("Connect Hermes to Nostr encrypted-messaging (MLS/RFC 9420).")
    print_info("Requires: marmot-cli (JSON-RPC daemon)")
    print()

    path = prompt("Path to marmot-cli binary", default=existing or "marmot-cli")
    if not path:
        print_warning("marmot-cli path is required")
        return
    save_env_value("MARMOT_CLI_PATH", path.strip())

    auto = prompt_yes_no("Auto-start the daemon?", True)
    save_env_value("MARMOT_AUTO_START", "true" if auto else "false")

    host = prompt("Daemon host", default=get_env_value("MARMOT_DAEMON_HOST") or "")
    if host:
        save_env_value("MARMOT_DAEMON_HOST", host.strip())

    port = prompt("Daemon port", default=get_env_value("MARMOT_DAEMON_PORT") or "")
    if port:
        try:
            save_env_value("MARMOT_DAEMON_PORT", str(int(port)))
        except ValueError:
            print_warning("Invalid port")

    identity = prompt("Identity name", default=get_env_value("MARMOT_IDENTITY") or "")
    if identity:
        save_env_value("MARMOT_IDENTITY", identity.strip())

    poll = prompt("Poll interval (ms)", default=get_env_value("MARMOT_POLL_INTERVAL_MS") or "")
    if poll:
        try:
            save_env_value("MARMOT_POLL_INTERVAL_MS", str(int(poll)))
        except ValueError:
            pass

    print()
    home = prompt("Home channel npub or group hex", default=get_env_value("MARMOT_HOME_CHANNEL") or "")
    if home:
        save_env_value("MARMOT_HOME_CHANNEL", home.strip())

    print()
    print_info("Access control")
    allow_all = prompt_yes_no("Allow anyone to DM the agent? (dev only)", False)
    if allow_all:
        save_env_value("MARMOT_ALLOW_ALL_USERS", "true")
        save_env_value("MARMOT_ALLOWED_USERS", "")
        print_warning("Open access — any Nostr user can message the agent")
    else:
        save_env_value("MARMOT_ALLOW_ALL_USERS", "false")
        allowed = prompt("Allowed npubs (comma-separated)", default=get_env_value("MARMOT_ALLOWED_USERS") or "")
        if allowed:
            save_env_value("MARMOT_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("Allowlist configured")
        else:
            save_env_value("MARMOT_ALLOWED_USERS", "")
            print_info("No npubs allowed — the agent will ignore all messages")

    print()
    print_success("Marmot configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway: hermes gateway restart")


def is_connected(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(os.getenv("MARMOT_CLI_PATH") or extra.get("cli_path", ""))


def _env_enablement() -> dict | None:
    cli_path = os.getenv("MARMOT_CLI_PATH", "").strip()
    if not cli_path:
        return None
    seed: dict = {"cli_path": cli_path}
    host = os.getenv("MARMOT_DAEMON_HOST", "").strip()
    if host:
        seed["host"] = host
    port = os.getenv("MARMOT_DAEMON_PORT", "").strip()
    if port:
        try:
            seed["port"] = int(port)
        except ValueError:
            pass
    identity = os.getenv("MARMOT_IDENTITY", "").strip()
    if identity:
        seed["identity"] = identity
    auto = os.getenv("MARMOT_AUTO_START", "").strip().lower()
    if auto:
        seed["auto_start"] = auto in ("1", "true", "yes")
    poll = os.getenv("MARMOT_POLL_INTERVAL_MS", "").strip()
    if poll:
        try:
            seed["poll_interval_ms"] = int(poll)
        except ValueError:
            pass
    home = os.getenv("MARMOT_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = home
    allowed = os.getenv("MARMOT_ALLOWED_USERS", "").strip()
    if allowed:
        seed["allowed_users"] = [u.strip() for u in allowed.split(",") if u.strip()]
    allow_all = os.getenv("MARMOT_ALLOW_ALL_USERS", "").strip().lower()
    if allow_all:
        seed["allow_all_users"] = allow_all in ("1", "true", "yes")
    return seed


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------

def _register_marmot_cli(subparser):
    from .cli import register_cli
    register_cli(subparser)


def register(ctx):
    ctx.register_cli_command(
        name="marmot",
        help="Marmot protocol (identity, groups, send, status)",
        setup_fn=_register_marmot_cli,
        description=(
            "Query the marmot-cli daemon for identity, groups, and "
            "send messages. Requires a running daemon."
        ),
    )
    ctx.register_platform(
        name="marmot",
        label="Marmot",
        adapter_factory=lambda cfg: MarmotPlatformAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MARMOT_CLI_PATH"],
        install_hint="Requires: marmot-cli (JSON-RPC daemon)",
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
