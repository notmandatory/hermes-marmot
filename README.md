# Marmot Platform Gateway Plugin for Hermes Agent

End-to-end encrypted Nostr messaging via MLS (RFC 9420) for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

This plugin connects Hermes to the [marmot-cli](https://github.com/tkhumush/marmot-cli) daemon, which manages MLS-encrypted group chats over Nostr relays. Users can DM the Hermes agent from any Nostr client that supports kind 445 MLS messages (e.g. [Whitenoise](https://whitenoise.chat)).

## Requirements

- Python 3.10+
- Hermes Agent installed and configured
- `marmot-cli` binary (v0.1.0+) in PATH — build from [marmot-cli](https://github.com/tkhumush/marmot-cli)

## Quick Start

### 1. Install the plugin

```bash
mkdir -p ~/.hermes/plugins
ln -s /path/to/hermes-marmot ~/.hermes/plugins/marmot
```

### 2. Configure

Run the interactive setup:

```bash
hermes setup
```

Or set environment variables directly in `~/.hermes/.env`:

```env
MARMOT_CLI_PATH=marmot-cli
MARMOT_IDENTITY=default
MARMOT_DAEMON_HOST=127.0.0.1
MARMOT_DAEMON_PORT=9222
MARMOT_AUTO_START=true
MARMOT_POLL_INTERVAL_MS=5000
MARMOT_HOME_CHANNEL=npub1...
MARMOT_ALLOWED_USERS=npub1friend1,npub1friend2
MARMOT_ALLOW_ALL_USERS=false
```

Or via `config.yaml`:

```yaml
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
        allowed_users:
          - npub1...
          - npub2...
```

### 3. Set a display name (so Nostr clients show a name, not an npub)

```bash
marmot-cli profile update --display-name "Hermes" --name "hermes" --about "Hermes AI Agent"
```

### 4. Start the gateway

```bash
hermes gateway start
```

### 5. Find your agent's npub

```bash
hermes marmot identity
```

Share this npub with friends so they can start a DM with your agent.

### 6. Optional: Set home channel

The home channel is where cron jobs, notifications, and agent-initiated messages are delivered.

```bash
hermes marmot groups
# Find the group hex of your DM with the agent, then:
export MARMOT_HOME_CHANNEL=<group-hex>
```

## Usage

### CLI Commands

```bash
hermes marmot identity                 # Print agent's npub and hex pubkey
hermes marmot groups                   # List all encrypted groups
hermes marmot group create --name "My Group" --member npub1...   # Create group
hermes marmot group invite --group <hex> --member <npub>        # Invite member
hermes marmot group members --group <hex>                       # List members
hermes marmot dm <npub>                # Create a 2-member DM with someone
hermes marmot send <group-hex> "hello" # Send a message (debug)
hermes marmot status                   # Check daemon connectivity
hermes marmot receive                  # View buffered events (debug)
```

### DM the agent from Whitenoise

1. Open [Whitenoise](https://whitenoise.chat)
2. Go to **Chats** → **New Chat**
3. Enter the agent's npub (from `hermes marmot identity`)
4. Send a message — the agent will respond

## Architecture

```
┌──────────────┐     JSON-RPC/TCP     ┌──────────────┐     WebSocket      ┌──────────┐
│  Hermes      │ ◄──────────────────► │ marmot-cli   │ ◄────────────────► │ Nostr    │
│  Adapter     │   ping, send_msg,    │ Daemon        │   kind 445 events  │ Relays   │
│  (Python)    │   list_groups,       │ (Rust)        │                    │ (nos.lol,│
│              │   receive            │               │                    │  damus,  │
│              │                      │               │                    │  primal) │
└──────────────┘                      └──────────────┘                    └──────────┘
       │                                     │
       │  subprocess                          │  MLS (RFC 9420)
       │  marmot-cli groups messages          │  encryption layer
       │  marmot-cli receive                  │
       ▼                                     ▼
  Poll loop syncs                    Persistent relay
  MLS epoch and fetches              connections with
  new messages                       live subscriptions
```

### How it works

- **Inbound**: The adapter periodically runs `marmot-cli receive` to sync MLS commits, then parses `marmot-cli groups messages --group <hex> --after <ts>` for new messages. Parsed messages are dispatched to the agent's message handler.
- **Outbound**: When the agent sends a response, the adapter calls `receive` RPC first to process any pending MLS commits, then calls `send_message` RPC to encrypt and publish the reply.
- **Identity**: The daemon manages a single Nostr keypair. A kind 0 profile event can be published to set a display name.
- **Relays**: All 3 relays are configured at daemon build time (`wss://nos.lol`, `wss://relay.damus.io`, `wss://relay.primal.net`). Inbox/key-package relay lists are published on startup.

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `MARMOT_CLI_PATH` | `marmot-cli` | Path to the marmot-cli binary |
| `MARMOT_IDENTITY` | `default` | Identity name in marmot-cli |
| `MARMOT_DAEMON_HOST` | `127.0.0.1` | Daemon listen host |
| `MARMOT_DAEMON_PORT` | `9222` | Daemon JSON-RPC port |
| `MARMOT_AUTO_START` | `true` | Auto-spawn daemon subprocess |
| `MARMOT_POLL_INTERVAL_MS` | `5000` | Message poll interval |
| `MARMOT_HOME_CHANNEL` | — | Default channel for notifications/cron |
| `MARMOT_ALLOWED_USERS` | — | Comma-separated npubs allowed to DM |
| `MARMOT_ALLOW_ALL_USERS` | `false` | Allow anyone to DM (dev only) |

## Troubleshooting

**Agent doesn't respond to DMs:**
- Check `hermes marmot status` — daemon must be connected
- Verify `hermes marmot groups` shows the DM group
- Confirm the sender's npub is in `MARMOT_ALLOWED_USERS` or `MARMOT_ALLOW_ALL_USERS=true`
- Check `~/.hermes/logs/gateway.log` for polling or dispatch errors

**Outbound messages don't reach relays:**
- The adapter calls `receive` before `send_message` to sync MLS epoch
- If messages still fail, restart the daemon: `hermes marmot status` (auto-restarts on crash)

**"Wrong Epoch" errors in daemon logs:**
- MLS commits from other members must be processed before sending
- The adapter handles this automatically by calling `receive` before each send

## Files

| File | Purpose |
|---|---|
| `plugin.yaml` | Plugin metadata and env config schema |
| `__init__.py` | Plugin entry point, registers CLI + platform |
| `adapter.py` | Main adapter: poll loop, message dispatch, send |
| `daemon.py` | Daemon subprocess lifecycle and auto-restart |
| `rpc_client.py` | JSON-RPC 2.0 client (TCP) |
| `cli.py` | `hermes marmot` CLI subcommands |

## License

MIT
