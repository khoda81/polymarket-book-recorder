# polymarket-book-recorder

Rust recorder backend for [Polymarket Viz](https://github.com/khoda81/polymarket-book-vis).

The rewrite deliberately preserves the existing recorder contract and persisted
format so the Rust and TypeScript implementations can be tested against one
another before cutover.

## What is ported

- Polymarket CLOB REST book seeding
- Polymarket market WebSocket subscriptions, heartbeat, reconnect, and
  continuity invalidation
- exact integer price ticks
- token-local ask-book pressure tracking
- pressure frontier / frozen-band history
- SQLite schema version 5
- gzip-compressed pressure frontier checkpoints
- compact binary pressure mutation tails
- checkpoint + tail replay
- incremental writeback with a checkpoint every 512 mutations
- `GET /api/recorder/health`
- `GET /api/recorder/state`
- `POST /api/recorder/watch`
- graceful SIGINT/SIGTERM flush

The recorder owns mutable market state in one Tokio actor. WebSocket shards and
REST seed requests feed that actor through channels, so the hot book/pressure
state does not need shared locks.

## Run

By default the service uses:

    RECORDER_DB_PATH=.data/age-recorder.sqlite
    RECORDER_PORT=3001

Run a release build with:

    cargo run --release

## Side-by-side validation against the TypeScript recorder

Do not point both recorder processes at the same SQLite file. Make a copy of the
current database and run Rust on another port:

    cp ../polymarket-book-vis/.data/age-recorder.sqlite .data/rust-recorder.sqlite
    RECORDER_DB_PATH=.data/rust-recorder.sqlite RECORDER_PORT=3002 cargo run --release

Keep the TypeScript recorder on port 3001. Ask both recorders to watch the same
tokens, then compare their state responses:

    curl -X POST http://127.0.0.1:3001/api/recorder/watch \
      -H 'content-type: application/json' \
      -d '{"tokenIds":["TOKEN_ID"]}'

    curl -X POST http://127.0.0.1:3002/api/recorder/watch \
      -H 'content-type: application/json' \
      -d '{"tokenIds":["TOKEN_ID"]}'

    curl 'http://127.0.0.1:3001/api/recorder/state?tokenId=TOKEN_ID'
    curl 'http://127.0.0.1:3002/api/recorder/state?tokenId=TOKEN_ID'

The TypeScript recorder remains the differential oracle until live state and
restart behavior agree under real traffic. No database migration is required:
the Rust recorder reads and writes the same v5 format.
