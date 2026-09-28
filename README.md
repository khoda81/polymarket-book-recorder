# polymarket-book-recorder

Rust recorder backend for [Polymarket Viz](https://github.com/khoda81/polymarket-book-vis).

The recorder persists canonical v6 pressure state. Existing databases must
already be schema version 6; older database versions are rejected.

## Features

- Polymarket CLOB REST book seeding
- Polymarket market WebSocket subscriptions, heartbeat, reconnect, and
  continuity invalidation
- exact integer price ticks
- token-local ask-book pressure tracking
- v6 pressure history as exact per-price current shares + frozen cumulative upper-edge steps
- cumulative current pressure derived by prefix sum; lower historical edges are implicit
- no duplicated current frontier and no persisted lower band edges
- SQLite schema version 6
- gzip-compressed pressure checkpoints
- compact binary pressure mutation tails with integer millisecond timestamps
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

By default the service uses `.data/age-recorder.sqlite` and port `3001`.

Run a release build with:

    cargo run --release

Or choose them explicitly:

    cargo run --release -- --database .data/rust-recorder.sqlite --port 3002

See all CLI options with:

    cargo run --release -- --help
