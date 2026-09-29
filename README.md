# polymarket-book-recorder

Rust recorder backend for [Polymarket Viz](https://github.com/khoda81/polymarket-book-vis).

The recorder persists canonical pressure state in SQLite schema version 8.
Older database schemas are rejected.

## Features

- demand-driven Polymarket market WebSocket subscriptions, heartbeat,
  reconnect, and continuity invalidation
- persisted watched tokens stay dormant after restart by default; use
  `--resume-watched` to resume all of them explicitly
- each requested token gets a 1 MiB upstream WebSocket payload lease; another
  state/watch request refreshes it, while exhaustion unsubscribes the token
  without deleting its persisted history
- WebSocket-only causal book bootstrap; REST snapshots are never merged into
  an in-flight market stream
- market-local, stream-local watermark propagation from ordered market events
- terminal unbounded pressure for resolved winners, with dominated history
  removed and opposing historical pressure preserved
- exact integer price ticks representing upper edges of 1e-4 price buckets
- raw Polymarket ask books retained only in transient memory
- authoritative per-market fee schedules from CLOB market metadata
- taker BUY prices projected to fee-adjusted effective collateral/share before pressure recording
- raw levels colliding in one effective-price tick are aggregated before entering pressure state
- v8 pressure history as exact per-price current shares + frozen cumulative upper-edge steps
- cumulative current pressure derived by prefix sum; lower historical edges are implicit
- no duplicated current frontier and no persisted lower band edges
- SQLite schema version 8
- gzip-compressed pressure checkpoints
- protobuf pressure mutation tails with explicit oneof semantics and integer millisecond timestamps
- persisted token→condition and condition→fee metadata
- gzip-compressed JSON checkpoints + protobuf tail replay
- incremental writeback with a checkpoint every 512 mutations
- `GET /api/recorder/health`
- `GET /api/recorder/state`
- `GET /api/recorder/traffic?limit=100` for runtime per-token WebSocket ingress,
  sorted from highest to lowest traffic
- `POST /api/recorder/watch`
- graceful SIGINT/SIGTERM flush

The recorder owns mutable market state in one Tokio actor. WebSocket shards
feed that actor through channels, so the hot book/pressure state does not need
shared locks.

## Run

By default the service uses `.data/rust-recorder.sqlite` and port `3001`.
Persisted watched tokens are loaded but are not re-subscribed until requested.

Run a release build with:

    cargo run --release

Or choose them explicitly:

    cargo run --release -- --database .data/rust-recorder.sqlite --port 3002

To restore the old continuous-recording startup behavior and immediately
re-subscribe every persisted watched token:

    cargo run --release -- --resume-watched

See all CLI options with:

    cargo run --release -- --help
