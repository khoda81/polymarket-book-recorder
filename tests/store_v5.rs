use std::io::Write;

use flate2::{write::GzEncoder, Compression};
use polymarket_book_recorder::{
    pressure::{FrontierLevel, PressureBand, PressureFrontierSnapshot, PressureLevelChange},
    pressure_log::{encode_pressure_mutation, RecorderPressureMutation},
    store::RecorderStore,
};
use rusqlite::{params, Connection};

#[test]
fn loads_v5_checkpoint_and_replays_binary_tail() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("recorder.sqlite");

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            r#"
            CREATE TABLE token_state (
                token_id TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('watched', 'completed')),
                recording_since_ms INTEGER,
                pressure BLOB
            ) WITHOUT ROWID;
            CREATE INDEX token_state_status_idx ON token_state(status);
            CREATE TABLE pressure_log (
                seq INTEGER PRIMARY KEY,
                token_id TEXT NOT NULL,
                payload BLOB NOT NULL,
                FOREIGN KEY(token_id) REFERENCES token_state(token_id)
                  ON DELETE CASCADE
            );
            CREATE INDEX pressure_log_token_seq_idx
                ON pressure_log(token_id, seq);
            PRAGMA user_version = 5;
            "#,
        )
        .unwrap();

    let checkpoint_json = r#"{
      "version":5,
      "current":[{"key":5000,"weight":10.0}],
      "field":{
        "maxPrice":10000,
        "currentValidThroughMs":1000.0,
        "runs":[{"price":5000,"volume":10.0,"frozenBands":[]}]
      }
    }"#;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(checkpoint_json.as_bytes()).unwrap();
    let checkpoint = encoder.finish().unwrap();

    connection
        .execute(
            "INSERT INTO token_state(token_id,status,recording_since_ms,pressure) VALUES (?1,'watched',900,?2)",
            params!["token", checkpoint],
        )
        .unwrap();

    let mutation = encode_pressure_mutation(&RecorderPressureMutation::Update {
        valid_through_ms: 2_000.0,
        changes: vec![PressureLevelChange {
            price: 5_000,
            shares: 4.0,
        }],
    })
    .unwrap();
    connection
        .execute(
            "INSERT INTO pressure_log(token_id,payload) VALUES (?1,?2)",
            params!["token", mutation],
        )
        .unwrap();
    drop(connection);

    let store = RecorderStore::open(&path).unwrap();
    let record = store.load("token").unwrap().unwrap();
    assert_eq!(record.recording_since_ms, Some(900));

    let snapshot: PressureFrontierSnapshot = record.pressure.unwrap();
    assert_eq!(
        snapshot.current,
        vec![FrontierLevel {
            key: 5_000,
            weight: 4.0
        }]
    );
    assert_eq!(snapshot.field.current_valid_through_ms, Some(2_000.0));
    assert_eq!(snapshot.field.runs[0].volume, 4.0);
    assert_eq!(
        snapshot.field.runs[0].frozen_bands,
        vec![PressureBand {
            lo_volume: 4.0,
            hi_volume: 10.0,
            valid_through_ms: 1_000.0,
        }]
    );
}
