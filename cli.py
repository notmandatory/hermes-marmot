"""CLI commands for the marmot-platform plugin.

Wires ``hermes marmot <subcommand>``:

  identity        — print the agent's own npub and hex pubkey
  groups          — list all encrypted groups
  group create    — create a new MLS group
  group invite    — invite a member by npub
  group members   — list members of a group
  send            — send a message to a group
  status          — check MDK connectivity and identity
  receive         — check pending welcomes (debug)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

HOME = Path.home()
DEFAULT_IDENTITY_PATH = HOME / ".hermes" / "marmot-identity.sec"
DEFAULT_DB_PATH = HOME / ".hermes" / "marmot-mdk.db"
DEFAULT_DB_KEY_PATH = HOME / ".hermes" / "marmot-db.key"
RELAY_URLS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
]


def _get_identity() -> tuple[str, bytes]:
    path = Path(os.getenv("MARMOT_IDENTITY_PATH") or DEFAULT_IDENTITY_PATH)
    raw = path.read_bytes()
    from nostr_sdk import SecretKey, Keys
    sk = SecretKey.from_bytes(raw)
    keys = Keys(sk)
    pubkey_hex = keys.public_key().to_hex()
    npub = keys.public_key().to_bech32()
    return npub, pubkey_hex


def _get_mdk():
    from mdk import new_mdk_with_key
    db_path = Path(os.getenv("MARMOT_DB_PATH") or DEFAULT_DB_PATH)
    db_key_path = Path(os.getenv("MARMOT_DB_KEY_PATH") or DEFAULT_DB_KEY_PATH)
    if db_key_path.exists():
        db_key = db_key_path.read_bytes()
    else:
        import secrets
        db_key = secrets.token_bytes(32)
        db_key_path.parent.mkdir(parents=True, exist_ok=True)
        db_key_path.write_bytes(db_key)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return new_mdk_with_key(db_path=str(db_path), encryption_key=db_key, config=None)


def _get_relays():
    import sys
    from pathlib import Path
    _here = Path(__file__).parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))
    from nostr_relay import RelayManager
    urls = RELAY_URLS
    env_relays = os.getenv("MARMOT_RELAYS")
    if env_relays:
        urls = [u.strip() for u in env_relays.split(",") if u.strip()]
    return RelayManager(urls)


def _pubkey_to_npub(hex_pubkey: str) -> str:
    from nostr_sdk import PublicKey
    return PublicKey.parse(hex_pubkey).to_bech32()


# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------


def register_cli(subparser: argparse.ArgumentParser) -> None:
    subs = subparser.add_subparsers(dest="marmot_command")

    subs.add_parser("identity", help="Print the agent's npub and hex pubkey")
    subs.add_parser("groups", help="List all encrypted groups")

    groups_p = subs.add_parser("group", help="Group management subcommands")
    gsubs = groups_p.add_subparsers(dest="marmot_group_command")

    gc_p = gsubs.add_parser("create", help="Create a new MLS group")
    gc_p.add_argument("--name", "-n", required=True, help="Group name")
    gc_p.add_argument("--description", "-d", default="", help="Group description")
    gc_p.add_argument("--member", "-m", action="append", dest="members", help="Invite member on creation")
    gc_p.add_argument("--relay", action="append", dest="cli_relays", help="Relay URL (repeatable)")

    gi_p = gsubs.add_parser("invite", help="Invite a member by npub")
    gi_p.add_argument("--group", "-g", required=True, help="Group ID (hex)")
    gi_p.add_argument("--member", "-m", required=True, help="Npub of the person to invite")

    gm_p = gsubs.add_parser("members", help="List members of a group")
    gm_p.add_argument("--group", "-g", required=True, help="Group ID (hex)")

    groups_p.set_defaults(func=marmot_group_command)

    send_p = subs.add_parser("send", help="Send a message to a group")
    send_p.add_argument("group_id", help="Group ID (hex)")
    send_p.add_argument("message", help="Message text to send")

    profile_p = subs.add_parser("profile", help="Manage Nostr profile")
    profile_sub = profile_p.add_subparsers(dest="marmot_profile_command")
    set_p = profile_sub.add_parser("set", help="Set profile fields (name, about, picture)")
    set_p.add_argument("--name", help="Display name")
    set_p.add_argument("--about", help="Short bio")
    set_p.add_argument("--picture", help="Profile image URL")
    profile_p.set_defaults(func=marmot_profile_command)

    subs.add_parser("status", help="Check MDK connectivity and identity")
    subs.add_parser("receive", help="Show pending welcomes (debug)")

    subparser.set_defaults(func=marmot_command)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def marmot_command(args: argparse.Namespace) -> int:
    sub = getattr(args, "marmot_command", None)
    if not sub:
        print("usage: hermes marmot {identity,groups,group,send,status,receive}")
        print()
        print("subcommands:")
        print("  identity              print the agent's npub and hex pubkey")
        print("  groups                list all encrypted groups")
        print("  group create          create a new MLS group")
        print("  group invite          invite a member by npub")
        print("  group members         list members of a group")
        print("  send <group-id> <msg> send a message to a group")
        print("  status                check MDK connectivity")
        print("  receive               show pending welcomes (debug)")
        return 2
    if sub == "identity":
        return _cmd_identity()
    if sub == "groups":
        return _cmd_groups()
    if sub == "group":
        fn = getattr(args, "func", None)
        if fn is None or fn is marmot_command:
            print("usage: hermes marmot group {create,invite,members}")
            return 2
        return fn(args)
    if sub == "send":
        return _cmd_send(args.group_id, args.message)
    if sub == "status":
        return _cmd_status()
    if sub == "receive":
        return _cmd_receive()
    if sub == "profile":
        fn = getattr(args, "func", None)
        if fn is None or fn is marmot_command:
            print("usage: hermes marmot profile {set}")
            return 2
        return fn(args)
    print(f"unknown subcommand: {sub}")
    return 2


def marmot_profile_command(args: argparse.Namespace) -> int:
    sub = getattr(args, "marmot_profile_command", None)
    if sub == "set":
        return _cmd_profile_set(args.name, args.about, args.picture)
    print("usage: hermes marmot profile set --name <name> [--about <text>] [--picture <url>]")
    return 2


def marmot_group_command(args: argparse.Namespace) -> int:
    sub = getattr(args, "marmot_group_command", None)
    if not sub:
        print("usage: hermes marmot group {create,invite,members}")
        print()
        print("subcommands:")
        print("  create    create a new MLS group  (--name <name>)")
        print("  invite    invite a member         (--group <hex> --member <npub>)")
        print("  members   list group members      --group <hex>")
        return 2
    if sub == "create":
        relays = args.cli_relays or RELAY_URLS
        return _cmd_group_create(args.name, args.description, args.members, relays)
    if sub == "invite":
        return _cmd_group_invite(args.group, args.member)
    if sub == "members":
        return _cmd_group_members(args.group)
    print(f"unknown group subcommand: {sub}")
    return 2


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _cmd_identity() -> int:
    try:
        npub, hex_pk = _get_identity()
        print(f"npub:   {npub}")
        print(f"hex:    {hex_pk}")
        return 0
    except Exception as e:
        print(f"error: cannot load identity — {e}")
        return 1


def _cmd_groups() -> int:
    try:
        instance = _get_mdk()
    except Exception as e:
        print(f"error: cannot initialize MDK — {e}")
        return 1
    groups = instance.get_groups()
    if not groups:
        print("no groups")
        return 0
    for g in groups:
        name = g.name or ""
        desc = g.description or ""
        n = len(instance.get_members(mls_group_id=g.mls_group_id)) if hasattr(instance, 'get_members') else 0
        line = f"  {g.mls_group_id}"
        if name:
            line += f"  {name}"
        print(f"{line}  ({n} members)")
    return 0


def _cmd_group_create(name: str, description: str, members: list | None, relays: list) -> int:
    if not (name or "").strip():
        print("error: group name is required")
        return 2
    try:
        instance = _get_mdk()
    except Exception as e:
        print(f"error: cannot initialize MDK — {e}")
        return 1

    _, pubkey_hex = _get_identity()

    member_events = []
    if members:
        for npub in members:
            print(f"  need key package for {npub} to add them at creation time")
            print(f"  use 'hermes marmot group invite' after creating the group")

    try:
        result = instance.create_group(
            creator_public_key=pubkey_hex,
            member_key_package_events_json=member_events,
            name=name,
            description=description or "",
            relays=relays,
            admins=[pubkey_hex],
        )
    except Exception as e:
        print(f"error: failed to create group — {e}")
        return 1

    group = result.group
    print(f"group created: {group.mls_group_id}")
    if group.name:
        print(f"  name: {group.name}")

    if result.welcome_rumors_json:
        print(f"  welcome messages: {len(result.welcome_rumors_json)}")

    return 0


def _cmd_group_invite(group_id: str, npub: str) -> int:
    if not group_id or not npub:
        print("error: group_id and npub are required")
        return 2

    from nostr_sdk import Nip19
    try:
        result = Nip19.from_bech32(npub)
        member_hex = result.as_enum().npub.to_hex()
    except Exception as e:
        print(f"error: invalid npub — {e}")
        return 1

    try:
        instance = _get_mdk()
    except Exception as e:
        print(f"error: cannot initialize MDK — {e}")
        return 1

    print(f"  fetching key package for {npub} from relays...")
    print(f"  (key package fetching not yet implemented — use the gateway)")
    return 1


def _cmd_group_members(group_id: str) -> int:
    try:
        instance = _get_mdk()
    except Exception as e:
        print(f"error: cannot initialize MDK — {e}")
        return 1
    try:
        members = instance.get_members(mls_group_id=group_id)
    except Exception as e:
        print(f"error: cannot get members — {e}")
        return 1
    if not members:
        print("no members or group not found")
        return 0
    _, my_hex = _get_identity()
    for m in members:
        label = "(you)" if m == my_hex else ""
        print(f"  {_pubkey_to_npub(m)}  {label}")
    return 0


def _cmd_send(group_id: str, message: str) -> int:
    if not group_id or not message:
        print("error: group_id and message are required")
        return 2

    async def _work():
        try:
            instance = _get_mdk()
        except Exception as e:
            print(f"error: cannot initialize MDK — {e}")
            return 1

        _, pubkey_hex = _get_identity()

        try:
            event_json = instance.create_message(
                mls_group_id=group_id,
                sender_public_key=pubkey_hex,
                content=message,
                kind=9,
                tags=None,
                event_tags=None,
            )
        except Exception as e:
            print(f"error: failed to create message — {e}")
            return 1

        relays = _get_relays()
        await relays.connect_all()
        await relays.publish_all(event_json)
        await relays.disconnect_all()

        print(f"sent: {group_id[:16]}...")
        return 0

    try:
        return asyncio.run(_work())
    except KeyboardInterrupt:
        return 130


def _cmd_status() -> int:
    try:
        npub, hex_pk = _get_identity()
        print(f"identity:")
        print(f"  npub:   {npub}")
        print(f"  hex:    {hex_pk}")
    except Exception as e:
        print(f"identity:  NOT FOUND — {e}")
        return 1

    try:
        instance = _get_mdk()
        groups = instance.get_groups()
        print(f"\nmdk:")
        print(f"  groups:   {len(groups)}")
        print(f"  db:       {os.getenv('MARMOT_DB_PATH') or DEFAULT_DB_PATH}")
    except Exception as e:
        print(f"\nmdk:       ERROR — {e}")
        return 1

    return 0


def _cmd_receive() -> int:
    try:
        instance = _get_mdk()
    except Exception as e:
        print(f"error: cannot initialize MDK — {e}")
        return 1

    pending = instance.get_pending_welcomes()
    if pending:
        print(f"found {len(pending)} pending welcome(s):")
        for w in pending:
            print(f"  group: {w.group_name or '(unnamed)'} ({w.mls_group_id[:16]}...)")
            print(f"  from:  {_pubkey_to_npub(w.welcomer)}")
            print(f"  relays: {', '.join(w.group_relays)}")
            print()
        if input("Accept all pending welcomes? [y/N] ").strip().lower() == "y":
            for w in pending:
                try:
                    instance.accept_welcome(w)
                    print(f"  accepted: {w.group_name or w.mls_group_id[:16]}")
                except Exception as e:
                    print(f"  error accepting {w.group_name or w.mls_group_id[:16]}: {e}")
        return 0

    groups = instance.get_groups()
    if groups:
        print(f"no pending welcomes ({len(groups)} active group(s))")
    else:
        print("no pending welcomes")
    return 0


def _cmd_profile_set(name: str | None, about: str | None, picture: str | None) -> int:
    if not name and not about and not picture:
        print("error: provide at least one of --name, --about, --picture")
        return 2

    from nostr_sdk import SecretKey, Keys, UnsignedEvent
    import time

    npub, pubkey_hex = _get_identity()
    identity_path = Path(os.getenv("MARMOT_IDENTITY_PATH") or DEFAULT_IDENTITY_PATH)
    raw_sk = identity_path.read_bytes()
    sk = SecretKey.from_bytes(raw_sk)
    keys = Keys(sk)

    profile = {}
    if name:
        profile["name"] = name
    if about:
        profile["about"] = about
    if picture:
        profile["picture"] = picture

    unsigned = UnsignedEvent.from_json(json.dumps({
        "kind": 0,
        "content": json.dumps(profile),
        "tags": [],
        "pubkey": pubkey_hex,
        "created_at": int(time.time()),
    }))
    event = unsigned.sign_with_keys(keys)
    event_json = event.as_json()

    async def _publish():
        relays = _get_relays()
        await relays.connect_all()
        await relays.publish_all(event_json)
        await relays.disconnect_all()

    try:
        asyncio.run(_publish())
    except KeyboardInterrupt:
        return 130

    print(f"published kind 0 for {npub}")
    return 0
