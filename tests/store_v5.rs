use std::io::Write;

use flate2::{Compression, write::GzEncoder};
use polymarket_book_recorder::{
    pressure::{FrontierLevel, PressureFrontierMemory},
    store::RecorderStore,
};
use rusqlite::{Connection, params};

#[test]
fn migrates_v5_checkpoint_and_tail_to_canonical_v6() {
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

    // Deliberately contains the exact v5 pathology that motivated v6:
    // a representable zero-width historical band.
    let checkpoint_json = r#"{
      "version":5,
      "current":[{"key":5000,"weight":10.0}],
      "field":{
        "maxPrice":10000,
        "currentValidThroughMs":1000.0,
        "runs":[{
          "price":5000,
          "volume":10.000000000000002,
          "frozenBands":[
            {"loVolume":10.0,"hiVolume":10.0,"validThroughMs":500.0}
          ]
        }]
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

    let frozen_only_json = r#"{
      "version":5,
      "current":[],
      "field":{
        "maxPrice":10000,
        "currentValidThroughMs":null,
        "runs":[{
          "price":6000,
          "volume":0.0,
          "frozenBands":[
            {"loVolume":0.0,"hiVolume":7.0,"validThroughMs":1500.0}
          ]
        }]
      }
    }"#;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(frozen_only_json.as_bytes()).unwrap();
    let frozen_only_checkpoint = encoder.finish().unwrap();
    connection
        .execute(
            "INSERT INTO token_state(token_id,status,recording_since_ms,pressure) VALUES (?1,'completed',1200,?2)",
            params!["frozen-only", frozen_only_checkpoint],
        )
        .unwrap();

    let mutation = encode_v5_update(2_000.0, 5_000, 4.0);
    connection
        .execute(
            "INSERT INTO pressure_log(token_id,payload) VALUES (?1,?2)",
            params!["token", mutation],
        )
        .unwrap();
    drop(connection);

    // Opening the store performs the complete v5 -> v6 migration before the
    // recorder sees any state.
    let store = RecorderStore::open(&path).unwrap();
    let record = store.load("token").unwrap().unwrap();
    assert_eq!(record.recording_since_ms, Some(900));

    let snapshot = record.pressure.unwrap();
    let memory = PressureFrontierMemory::restore(snapshot.clone()).unwrap();
    assert_eq!(
        memory.current_levels(),
        vec![FrontierLevel {
            key: 5_000,
            weight: 4.0,
        }]
    );

    let json = serde_json::to_value(snapshot).unwrap();
    assert_eq!(json["version"], 6);
    assert_eq!(json["state"]["kind"], "observed");
    assert_eq!(json["state"]["validThroughMs"], 2_000);
    assert_eq!(json["state"]["runs"][0]["shares"], 4.0);
    assert_eq!(json["state"]["runs"][0]["frozenSteps"][0]["hiVolume"], 10.0);
    assert_eq!(
        json["state"]["runs"][0]["frozenSteps"][0]["validThroughMs"],
        1_000
    );
    assert!(json.to_string().find("loVolume").is_none());
    assert!(json.get("current").is_none());

    let historical = store.load("frozen-only").unwrap().unwrap();
    let historical_snapshot = historical.pressure.unwrap();
    let historical_json = serde_json::to_value(historical_snapshot).unwrap();
    assert_eq!(historical_json["version"], 6);
    assert_eq!(historical_json["state"]["kind"], "observed");
    assert_eq!(historical_json["state"]["validThroughMs"], 1_500);
    assert_eq!(historical_json["state"]["runs"][0]["shares"], 0.0);
    assert_eq!(
        historical_json["state"]["runs"][0]["frozenSteps"][0]["hiVolume"],
        7.0
    );

    drop(store);
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let tail_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM pressure_log", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 6);
    assert_eq!(tail_count, 0);
}

fn encode_v5_update(valid_through_ms: f64, price: u16, shares: f64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(21);
    bytes.push(2); // MUTATION_UPDATE
    bytes.extend_from_slice(&valid_through_ms.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&price.to_le_bytes());
    bytes.extend_from_slice(&shares.to_le_bytes());
    bytes
}
