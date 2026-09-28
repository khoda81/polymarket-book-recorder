# polymarket-book-recorder

Rust recorder backend for [Polymarket Viz](https://github.com/khoda81/polymarket-book-vis).

The recorder persists canonical v7 pressure snapshots in the existing SQLite
schema version 6. Persisted v6 pressure snapshots and mutation tails remain
readable; older database schemas are rejected.

## Features

- Polymarket market WebSocket subscriptions, heartbeat, reconnect, and
  continuity invalidation
- WebSocket-only causal book bootstrap; REST snapshots are never merged into
  an in-flight market stream
- market-local, stream-local watermark propagation from ordered market events
- terminal unbounded pressure for resolved winners, with dominated history
  removed and opposing historical pressure preserved
- exact integer price ticks
- token-local ask-book pressure tracking
- v7 pressure history as exact per-price current shares + frozen cumulative upper-edge steps
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

The recorder owns mutable market state in one Tokio actor. WebSocket shards
feed that actor through channels, so the hot book/pressure state does not need
shared locks.

## Run

By default the service uses `.data/age-recorder.sqlite` and port `3001`.

Run a release build with:

    cargo run --release

Or choose them explicitly:

    cargo run --release -- --database .data/rust-recorder.sqlite --port 3002

See all CLI options with:

    cargo run --release -- --help
