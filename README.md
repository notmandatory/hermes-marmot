# Marmot Platform Gateway Plugin for Hermes Agent

End-to-end encrypted Nostr messaging via MLS (RFC 9420) for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

This plugin uses [mdk-python](https://github.com/marmot-protocol/mdk-python) (Marmot Development Kit) to manage MLS-encrypted group chats over Nostr relays directly — no external daemon required. Inbound DMs use [NIP-59](https://github.com/nostr-protocol/nips/blob/master/59.md) gift wrap unwrapping via [nostr-sdk](https://github.com/rust-nostr/nostr) (Python bindings). Users can DM the Hermes agent from any Nostr client that supports kind 445 MLS messages (e.g. [Whitenoise](https://whitenoise.chat)).

## Requirements

- Python 3.10+
- Hermes Agent installed and configured
- `pip install mdk-python nostr websockets nostr-sdk`

## Quick Start

### 1. Install dependencies

```bash
# Activate hermes venv and install
/home/hermes/.hermes/hermes-agent/venv/bin/python3 -m pip install mdk-python nostr websockets nostr-sdk
```

### 2. Install the plugin

```bash
mkdir -p ~/.hermes/plugins
ln -s /path/to/hermes-marmot ~/.hermes/plugins/marmot
```

### 3. Start the gateway

A Nostr identity and MDK database are auto-created on first start.

```bash
hermes gateway start
```

### 4. Find your agent's npub

```bash
hermes marmot identity
```

Share this npub with friends so they can start a DM with your agent.

### 5. Allow users to DM the agent

```bash
export MARMOT_ALLOWED_USERS=npub1friend1,npub1friend2
hermes gateway restart
```

Or set `MARMOT_ALLOW_ALL_USERS=true` for open access (dev only).

## Usage

### CLI Commands

```bash
hermes marmot identity                 # Print agent's npub and hex pubkey
hermes marmot groups                   # List all encrypted groups
hermes marmot group create --name "My Group"   # Create a group
hermes marmot group members --group <hex>       # List members
hermes marmot send <group-hex> "hello" # Send a message
hermes marmot status                   # Check MDK and identity
hermes marmot receive                  # View/accept pending welcomes
hermes marmot profile set --name "Bot" # Set Nostr display name
```

### DM the agent from Whitenoise

1. Open [Whitenoise](https://whitenoise.chat)
2. Go to **Chats** → **New Chat**
3. Enter the agent's npub (from `hermes marmot identity`)
4. Send a message — the agent will respond

## Architecture

```
┌──────────────┐     direct call     ┌──────────────────┐  kind 445/1059   ┌──────────┐
│  Hermes      │ ◄─────────────────► │  mdk-python      │ ◄──────────────► │ Nostr    │
│  Adapter     │   create_message,   │  (Python bindings│  WebSocket       │ Relays   │
│  (Python)    │   process_message,  │   to Rust MDK)   │                  │ (nos.lol,│
│              │   get_messages      │                  │                  │  damus,  │
│              │                     │                  │                  │  primal) │
└──────┬───────┘                     └──────────────────┘                  └──────────┘
       │
       │  Inbound — kind 445 group messages:
       │  ┌────────────────────────────────────────┐
       ├─►│ kind 445 → mdk.process_message()       │
       │  │ → decrypt → plaintext                  │
       │  └────────────────────────────────────────┘
       │
       │  Inbound — kind 1059 gift wraps (welcomes):
       │  ┌────────────────────────────────────────┐
       ├─►│ UnwrappedGift.from_gift_wrap()         │
       │  │ → kind 13 seal → decrypt → kind 444    │
       │  │ → mdk.process_welcome()                │
       │  │ → mdk.accept_welcome()                 │
       │  └────────────────────────────────────────┘
       │
       │  Outbound — kind 445 + gift-wrap copies:
       │  ┌────────────────────────────────────────┐
       ├─►│ mdk.create_message() → kind 445 event  │
       │  │ → publish to relays                    │
       │  │ → gift-wrap (kind 1059, current ts)    │
       │  │   for compatible clients               │
       │  └────────────────────────────────────────┘
       │
       │  Nostr identity            MLS (RFC 9420)
       │  file-based key            encryption layer
       ▼                           
  Secp256k1 keypair          SQLite database
  stored in ~/.hermes/       stores groups, keys,
  marmot-identity.sec        messages, MLS state
```

### How it works

- **No daemon**: MDK is a Python library wrapping the Rust MDK core via UniFFI. All MLS operations (key generation, encrypt/decrypt, group management) happen in-process.
- **Inbound — messages**: The adapter subscribes to kind 445 events with a `since` filter built from the last processed event timestamp (persisted to `~/.hermes/marmot-last-event.ts`). `mdk.process_message()` decrypts them and returns `Message` objects. A periodic sync loop catches missed messages via `mdk.get_messages()`.
- **Inbound — welcomes**: New group invitations arrive as [NIP-59](https://github.com/nostr-protocol/nips/blob/master/59.md) gift wraps (kind 1059). The adapter subscribes to kind 1059 events tagged to our pubkey and unwraps them via `nostr_sdk.UnwrappedGift.from_gift_wrap()`, extracting the kind 444 MLS welcome rumor. It then calls `mdk.process_welcome()` → `mdk.accept_welcome()` to join the group.
- **Outbound**: `mdk.create_message()` encrypts the message and returns a complete, signed Nostr event JSON (kind 445), published to all connected relays. Gift-wrap copies (kind 1059) are also published with the current timestamp (`_create_gift_wrap_with_current_ts()`) — nostr-sdk's built-in `gift_wrap()` randomizes timestamps per NIP-59, which can cause misses when recipients use `since` filters.
- **Identity**: A secp256k1 keypair is generated on first start and stored at `~/.hermes/marmot-identity.sec`. The hex pubkey is derived from it for MDK and NIP-59 operations.
- **Key packages**: On each connect, a fresh MLS key package (kind 30443, replaceable) is published to relays so other clients can find and add this agent to groups.
- **Group evolution**: For groups with 2 or fewer members, automatic self-update proposals are skipped to prevent unnecessary epoch drift. Periodic sync (`mdk.get_messages()`) handles message catching across restarts.
- **Relays**: WebSocket connections to 3 relays (nos.lol, damus.io, primal.net) for real-time event streaming.

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `MARMOT_RELAYS` | `wss://nos.lol,wss://relay.damus.io,wss://relay.primal.net` | Comma-separated relay URLs |
| `MARMOT_IDENTITY_PATH` | `~/.hermes/marmot-identity.sec` | Path to 32-byte identity private key |
| `MARMOT_DB_PATH` | `~/.hermes/marmot-mdk.db` | Path to MDK SQLite database |
| `MARMOT_DB_KEY_PATH` | `~/.hermes/marmot-db.key` | Path to 32-byte DB encryption key |
| `MARMOT_HOME_CHANNEL` | — | Default group hex for notifications/cron |
| `MARMOT_ALLOWED_USERS` | — | Comma-separated npubs allowed to DM |
| `MARMOT_ALLOW_ALL_USERS` | `false` | Allow anyone to DM (dev only) |

## Troubleshooting

**Agent doesn't respond to DMs:**
- Check `hermes marmot status` — identity and MDK must be loaded
- Verify `hermes marmot groups` shows the DM group
- Confirm the sender's npub is in `MARMOT_ALLOWED_USERS` or `MARMOT_ALLOW_ALL_USERS=true`
- Check `~/.hermes/logs/gateway.log` for polling or dispatch errors

**Messages arrive but responses don't reach Whitenoise:**
- MDK creates and signs kind 445 events internally — no external signing needed
- Check relay connectivity in logs
- Verify key packages are published (logged on connect)

## Files

| File | Purpose |
|---|---|
| `plugin.yaml` | Plugin metadata, env config, pip dependencies |
| `__init__.py` | Plugin entry point, registers CLI + platform |
| `adapter.py` | Main adapter: MDK lifecycle, relay comms, message dispatch, NIP-59 gift wrap handling |
| `nostr_relay.py` | Async WebSocket Nostr relay client (NIP-01) |
| `cli.py` | `hermes marmot` CLI subcommands (identity, groups, send, profile, receive) |
| `~/.hermes/marmot-last-event.ts` | Persisted timestamp of the last processed event (used for since filter on reconnect) |

## License

MIT
