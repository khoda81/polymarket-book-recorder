# polymarket-book-recorder

Rust rewrite of the Polymarket Viz recorder.

The first compatibility milestone intentionally preserves the existing recorder contract instead of redesigning storage:

- SQLite schema version 5
- gzip-compressed pressure frontier checkpoints
- compact binary pressure mutation tails
- checkpoint + tail replay into the canonical pressure snapshot
- compatible GET /api/recorder/health and GET /api/recorder/state endpoints

Live Polymarket ingestion is the next milestone. POST /api/recorder/watch currently returns 501 rather than pretending a token is being recorded.

## Run against the existing database

Set RECORDER_DB_PATH to a copy of the current age-recorder.sqlite and run:

    cargo run --release

The server listens on port 3001 by default. RECORDER_PORT overrides it.

## Compatibility strategy

The TypeScript recorder remains the differential oracle until the Rust service can consume the same live feed and produce equivalent snapshots. The Rust port reads the existing database directly; there is no migration step.
