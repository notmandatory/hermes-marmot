"""CLI commands for the marmot-platform plugin.

Wires ``hermes marmot <subcommand>``:

  identity        — print the agent's own npub and hex pubkey
  groups          — list all encrypted groups
  groups create   — create a new MLS group
  groups invite   — invite a member by npub
  groups members  — list members of a group
  dm <npub>       — create a 2-member DM group with an npub
  send            — send a message to a group
  status          — check daemon connectivity and identity
  receive         — print buffered events (for debugging)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys

from .rpc_client import MarmotRpcClient


def _get_client() -> MarmotRpcClient:
    host = os.getenv("MARMOT_DAEMON_HOST") or "127.0.0.1"
    port = int(os.getenv("MARMOT_DAEMON_PORT") or "9222")
    return MarmotRpcClient(host=host, port=port)


def _cli_path() -> str:
    return os.getenv("MARMOT_CLI_PATH") or "marmot-cli"


def _run_marmot_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_cli_path(), *args],
        capture_output=True, text=True,
    )


async def _rpc_npub() -> dict:
    return await _get_client().identity_npub()


async def _rpc_list_groups() -> dict:
    return await _get_client().list_groups()


async def _rpc_send(group_id: str, content: str) -> dict:
    return await _get_client().send_message(group_id=group_id, content=content, publish=True)


async def _rpc_ping() -> dict:
    return await _get_client().ping()


async def _rpc_receive() -> dict:
    return await _get_client().receive()


def _run(coro):
    try:
        return asyncio.run(coro)
    except KeyboardInterrupt:
        return 130


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
    gc_p.add_argument("--member", "-m", action="append", dest="members", help="Invite member on creation (repeatable)")
    gc_p.add_argument("--publish", action="store_true", default=True, help="Publish to relays")

    gi_p = gsubs.add_parser("invite", help="Invite a member by npub")
    gi_p.add_argument("--group", "-g", required=True, help="Group ID (hex)")
    gi_p.add_argument("--member", "-m", required=True, help="Npub of the person to invite")
    gi_p.add_argument("--publish", action="store_true", default=True, help="Publish to relays")

    gm_p = gsubs.add_parser("members", help="List members of a group")
    gm_p.add_argument("--group", "-g", required=True, help="Group ID (hex)")

    groups_p.set_defaults(func=marmot_group_command)

    dm_p = subs.add_parser("dm", help="Create a 2-member DM with an npub")
    dm_p.add_argument("npub", help="Npub of the recipient")

    send_p = subs.add_parser("send", help="Send a message to a group")
    send_p.add_argument("group_id", help="Group ID (hex)")
    send_p.add_argument("message", help="Message text to send")

    subs.add_parser("status", help="Check daemon connectivity and config")

    subs.add_parser("receive", help="Show buffered events from daemon (debug)")

    subparser.set_defaults(func=marmot_command)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def marmot_command(args: argparse.Namespace) -> int:
    sub = getattr(args, "marmot_command", None)
    if not sub:
        print("usage: hermes marmot {identity,groups,group,dm,send,status,receive}")
        print()
        print("subcommands:")
        print("  identity              print the agent's npub and hex pubkey")
        print("  groups                list all encrypted groups")
        print("  group create          create a new MLS group")
        print("  group invite          invite a member by npub")
        print("  group members         list members of a group")
        print("  dm <npub>             create a 2-member DM with an npub")
        print("  send <group-id> <msg> send a message to a group")
        print("  status                check daemon connectivity")
        print("  receive               view buffered events (debug)")
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
    if sub == "dm":
        return _cmd_dm(args.npub)
    if sub == "send":
        return _cmd_send(group_id=args.group_id, message=args.message)
    if sub == "status":
        return _cmd_status()
    if sub == "receive":
        return _cmd_receive()
    print(f"unknown subcommand: {sub}")
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
        return _cmd_group_create(name=args.name, description=args.description, members=args.members, publish=args.publish)
    if sub == "invite":
        return _cmd_group_invite(group_id=args.group, npub=args.member, publish=args.publish)
    if sub == "members":
        return _cmd_group_members(args.group)
    print(f"unknown group subcommand: {sub}")
    return 2


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _cmd_identity() -> int:
    async def _work():
        try:
            result = await _rpc_npub()
        except Exception as e:
            print(f"error: cannot reach marmot daemon — {e}")
            return 1
        npub = (result or {}).get("npub", "")
        pubkey = (result or {}).get("pubkey", "")
        if not npub and not pubkey:
            print("no identity returned (daemon may not have a key)")
            return 1
        if npub:
            print(f"npub:   {npub}")
        if pubkey:
            print(f"hex:    {pubkey}")
        return 0
    return _run(_work())


def _cmd_groups() -> int:
    async def _work():
        try:
            result = await _rpc_list_groups()
        except Exception as e:
            print(f"error: cannot reach marmot daemon — {e}")
            return 1
        groups = (result or {}).get("groups", [])
        if not groups:
            print("no groups")
            return 0
        for g in groups:
            if isinstance(g, dict):
                gid = g.get("id", "")
                name = g.get("name", "")
                n = g.get("members", 0)
                print(f"  {gid}  {name}  ({n} members)" if name else f"  {gid}  ({n} members)")
            else:
                print(f"  {g}")
        return 0
    return _run(_work())


def _cmd_group_create(name: str, description: str = "", members: list | None = None, publish: bool = True) -> int:
    if not (name or "").strip():
        print("error: group name is required")
        return 2
    argv = ["groups", "create", "--name", name]
    if description:
        argv += ["--description", description]
    if members:
        for m in members:
            argv += ["--member", m]
    if publish:
        argv.append("--publish")
    result = _run_marmot_cli(*argv)
    if result.returncode != 0:
        print(result.stderr.strip() or f"failed (exit {result.returncode})")
        return result.returncode
    out = result.stdout.strip()
    print(out if out else f"group '{name}' created")
    return 0


def _cmd_group_invite(group_id: str, npub: str, publish: bool = True) -> int:
    if not group_id or not npub:
        print("error: group_id and npub are required")
        return 2
    argv = ["groups", "invite", "--group", group_id, "--member", npub]
    if publish:
        argv.append("--publish")
    result = _run_marmot_cli(*argv)
    if result.returncode != 0:
        print(result.stderr.strip() or f"invite failed (exit {result.returncode})")
        return result.returncode
    print(result.stdout.strip() if result.stdout.strip() else f"invited {npub} to {group_id}")
    return 0


def _cmd_group_members(group_id: str) -> int:
    result = _run_marmot_cli("groups", "members", "--group", group_id)
    if result.returncode != 0:
        print(result.stderr.strip() or f"failed (exit {result.returncode})")
        return result.returncode
    out = result.stdout.strip()
    print(out if out else "no members or group not found")
    return 0


def _cmd_dm(npub: str) -> int:
    if not npub:
        print("error: npub is required")
        return 2
    result = _run_marmot_cli("dm", "create", "--recipient", npub, "--publish")
    if result.returncode != 0:
        print(result.stderr.strip() or f"DM failed (exit {result.returncode})")
        return result.returncode
    print(result.stdout.strip() if result.stdout.strip() else f"DM created with {npub}")
    return 0


def _cmd_send(group_id: str, message: str) -> int:
    async def _work():
        try:
            result = await _rpc_send(group_id, message)
        except Exception as e:
            print(f"error: failed to send — {e}")
            return 1
        event_id = (result or {}).get("id", "")
        print(f"sent: {event_id}" if event_id else "sent (no event id returned)")
        return 0
    return _run(_work())


def _cmd_status() -> int:
    async def _work():
        host = os.getenv("MARMOT_DAEMON_HOST") or "127.0.0.1"
        port = int(os.getenv("MARMOT_DAEMON_PORT") or "9222")
        cli_path = os.getenv("MARMOT_CLI_PATH") or "(not set)"
        identity = os.getenv("MARMOT_IDENTITY") or "default"
        print(f"cli_path:    {cli_path}")
        print(f"identity:    {identity}")
        print(f"daemon:      {host}:{port}")
        print()
        try:
            ping = await _rpc_ping()
            if ping.get("pong"):
                print("daemon:      connected")
            else:
                print("daemon:      connected but unexpected response")
        except Exception as e:
            print(f"daemon:      unreachable — {e}")
            return 1
        try:
            ident = await _rpc_npub()
            npub = (ident or {}).get("npub", "")
            if npub:
                print(f"npub:        {npub}")
        except Exception:
            pass
        return 0
    return _run(_work())


def _cmd_receive() -> int:
    async def _work():
        try:
            result = await _rpc_receive()
        except Exception as e:
            print(f"error: cannot reach marmot daemon — {e}")
            return 1
        events = (result or {}).get("events", [])
        if not events:
            print("no pending events")
            return 0
        for ev in events:
            print(json.dumps(ev, indent=2))
        return 0
    return _run(_work())
