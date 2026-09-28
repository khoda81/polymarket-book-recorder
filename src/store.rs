use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, bail, ensure};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::{
    fees::{FeeSchedule, MarketFeeInfo},
    pressure::{PressureFrontierMemory, PressureFrontierSnapshot},
    pressure_log::{
        RecorderPressureMutation, decode_pressure_mutation, encode_pressure_mutation,
        replay_pressure_mutation,
    },
};

pub const RECORDER_DATABASE_VERSION: i64 = 8;
pub const RECORDER_CHECKPOINT_MUTATIONS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RecorderTokenStatus {
    Watched,
    Completed,
}

impl RecorderTokenStatus {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "watched" => Ok(Self::Watched),
            "completed" => Ok(Self::Completed),
            other => bail!("invalid recorder token status: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Watched => "watched",
            Self::Completed => "completed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecorderStoreIndexRecord {
    pub token_id: String,
    pub status: RecorderTokenStatus,
    pub recording_since_ms: Option<i64>,
    pub has_pressure: bool,
}

#[derive(Debug, Clone)]
pub enum RecorderCheckpointWrite {
    Keep,
    Replace(Option<PressureFrontierSnapshot>),
}

#[derive(Debug, Clone)]
pub struct RecorderStoreWriteRecord {
    pub token_id: String,
    pub status: RecorderTokenStatus,
    pub recording_since_ms: Option<i64>,
    pub mutations: Vec<RecorderPressureMutation>,
    pub checkpoint: RecorderCheckpointWrite,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecorderStoreWriteStats {
    pub mutation_count: usize,
    pub mutation_bytes: usize,
    pub checkpoint_count: usize,
    pub checkpoint_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecorderStoreStats {
    pub pressure_tokens: u64,
    pub pressure_log_mutations: u64,
    pub database_path: String,
}

struct StoreInner {
    connection: Connection,
    mutation_counts: HashMap<String, usize>,
}

pub struct RecorderStore {
    path: PathBuf,
    inner: Mutex<StoreInner>,
}

impl RecorderStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating database directory {}", parent.display()))?;
        }

        let mut connection = Connection::open(&path)
            .with_context(|| format!("opening recorder database {}", path.display()))?;
        configure_connection(&connection)?;
        initialize_database(&mut connection)?;
        let mutation_counts = load_mutation_counts(&connection)?;

        Ok(Self {
            path,
            inner: Mutex::new(StoreInner {
                connection,
                mutation_counts,
            }),
        })
    }

    pub fn load_index(&self) -> Result<Vec<RecorderStoreIndexRecord>> {
        let inner = self.lock()?;
        let mut statement = inner.connection.prepare(
            r#"
            SELECT
                s.token_id,
                s.status,
                s.recording_since_ms,
                (
                    s.pressure IS NOT NULL OR
                    EXISTS (
                        SELECT 1
                        FROM pressure_log l
                        WHERE l.token_id = s.token_id
                    )
                ) AS has_pressure
            FROM token_state s
            "#,
        )?;

        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        let mut records = Vec::new();
        for row in rows {
            let (token_id, status, recording_since_ms, has_pressure) = row?;
            records.push(RecorderStoreIndexRecord {
                token_id,
                status: RecorderTokenStatus::parse(&status)?,
                recording_since_ms,
                has_pressure: has_pressure != 0,
            });
        }
        Ok(records)
    }

    pub fn load_pressure(&self, token_id: &str) -> Result<Option<PressureFrontierSnapshot>> {
        let (checkpoint, mutations) = {
            let inner = self.lock()?;
            let checkpoint = inner
                .connection
                .query_row(
                    "SELECT pressure FROM token_state WHERE token_id = ?1",
                    [token_id],
                    |row| row.get::<_, Option<Vec<u8>>>(0),
                )
                .optional()?;

            let Some(checkpoint) = checkpoint else {
                return Ok(None);
            };

            let mut statement = inner.connection.prepare(
                r#"
                SELECT seq, payload
                FROM pressure_log
                WHERE token_id = ?1
                ORDER BY seq
                "#,
            )?;
            let mutations = statement
                .query_map([token_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            (checkpoint, mutations)
        };

        let mut pressure = checkpoint
            .as_deref()
            .map(decode_checkpoint)
            .transpose()
            .with_context(|| format!("restoring pressure checkpoint for token {token_id}"))?;

        if !mutations.is_empty() {
            let mut memory = match pressure.take() {
                Some(snapshot) => PressureFrontierMemory::restore(snapshot)?,
                None => PressureFrontierMemory::default(),
            };

            for (seq, payload) in mutations {
                let mutation = decode_pressure_mutation(&payload).with_context(|| {
                    format!("decoding pressure mutation seq={seq} token={token_id}")
                })?;
                replay_pressure_mutation(&mut memory, mutation).with_context(|| {
                    format!("replaying pressure mutation seq={seq} token={token_id}")
                })?;
            }
            pressure = Some(memory.snapshot());
        }

        Ok(pressure)
    }

    pub fn load_market_fees(&self) -> Result<Vec<MarketFeeInfo>> {
        let inner = self.lock()?;
        let mut markets = HashMap::<String, (FeeSchedule, Vec<String>)>::new();

        {
            let mut statement = inner
                .connection
                .prepare("SELECT condition_id, rate, exponent FROM market_fee")?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (condition_id, rate, exponent) = row?;
                let exponent =
                    u32::try_from(exponent).context("persisted fee exponent does not fit u32")?;
                markets.insert(
                    condition_id,
                    (FeeSchedule::from_persisted(&rate, exponent)?, Vec::new()),
                );
            }
        }

        {
            let mut statement = inner
                .connection
                .prepare("SELECT token_id, condition_id FROM token_market ORDER BY token_id")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (token_id, condition_id) = row?;
                let (_, token_ids) = markets.get_mut(&condition_id).ok_or_else(|| {
                    anyhow::anyhow!("token {token_id} references missing fee market {condition_id}")
                })?;
                token_ids.push(token_id);
            }
        }

        Ok(markets
            .into_iter()
            .map(|(condition_id, (schedule, token_ids))| MarketFeeInfo {
                condition_id,
                schedule,
                token_ids,
            })
            .collect())
    }

    pub fn save_market_fee(&self, market: &MarketFeeInfo) -> Result<()> {
        let mut inner = self.lock()?;
        let transaction = inner.connection.transaction()?;
        transaction.execute(
            r#"
            INSERT INTO market_fee(condition_id, rate, exponent)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(condition_id) DO UPDATE SET
                rate = excluded.rate,
                exponent = excluded.exponent
            "#,
            params![
                market.condition_id,
                market.schedule.rate_string(),
                i64::from(market.schedule.exponent()),
            ],
        )?;
        for token_id in &market.token_ids {
            transaction.execute(
                r#"
                INSERT INTO token_market(token_id, condition_id)
                VALUES (?1, ?2)
                ON CONFLICT(token_id) DO UPDATE SET
                    condition_id = excluded.condition_id
                "#,
                params![token_id, market.condition_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mutation_count(&self, token_id: &str) -> Result<usize> {
        Ok(self
            .lock()?
            .mutation_counts
            .get(token_id)
            .copied()
            .unwrap_or(0))
    }

    pub fn should_checkpoint(&self, token_id: &str, additional_mutations: usize) -> Result<bool> {
        Ok(self.mutation_count(token_id)? + additional_mutations >= RECORDER_CHECKPOINT_MUTATIONS)
    }

    pub fn write(&self, records: &[RecorderStoreWriteRecord]) -> Result<RecorderStoreWriteStats> {
        struct EncodedRecord<'a> {
            record: &'a RecorderStoreWriteRecord,
            mutations: Vec<Vec<u8>>,
            checkpoint: Option<Option<Vec<u8>>>,
        }

        let mut stats = RecorderStoreWriteStats::default();
        let mut encoded = Vec::with_capacity(records.len());

        for record in records {
            let mutations = record
                .mutations
                .iter()
                .map(encode_pressure_mutation)
                .collect::<Result<Vec<_>>>()?;
            stats.mutation_count += mutations.len();
            stats.mutation_bytes += mutations.iter().map(Vec::len).sum::<usize>();

            let checkpoint = match &record.checkpoint {
                RecorderCheckpointWrite::Keep => None,
                RecorderCheckpointWrite::Replace(snapshot) => {
                    ensure!(
                        mutations.is_empty(),
                        "checkpoint replacement must already include its mutation tail"
                    );
                    stats.checkpoint_count += usize::from(snapshot.is_some());
                    let compressed = snapshot.as_ref().map(encode_checkpoint).transpose()?;
                    stats.checkpoint_bytes += compressed.as_ref().map_or(0, Vec::len);
                    Some(compressed)
                }
            };

            encoded.push(EncodedRecord {
                record,
                mutations,
                checkpoint,
            });
        }

        let mut inner = self.lock()?;
        let transaction = inner.connection.transaction()?;

        for item in &encoded {
            let record = item.record;
            let pressure = item
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.as_deref());

            if item.checkpoint.is_some() {
                transaction.execute(
                    r#"
                    INSERT INTO token_state(
                        token_id, status, recording_since_ms, pressure
                    )
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT(token_id) DO UPDATE SET
                        status = excluded.status,
                        recording_since_ms = excluded.recording_since_ms,
                        pressure = excluded.pressure
                    "#,
                    params![
                        record.token_id,
                        record.status.as_str(),
                        record.recording_since_ms,
                        pressure,
                    ],
                )?;
                transaction.execute(
                    "DELETE FROM pressure_log WHERE token_id = ?1",
                    [&record.token_id],
                )?;
            } else {
                transaction.execute(
                    r#"
                    INSERT INTO token_state(
                        token_id, status, recording_since_ms, pressure
                    )
                    VALUES (?1, ?2, ?3, NULL)
                    ON CONFLICT(token_id) DO UPDATE SET
                        status = excluded.status,
                        recording_since_ms = excluded.recording_since_ms
                    "#,
                    params![
                        record.token_id,
                        record.status.as_str(),
                        record.recording_since_ms,
                    ],
                )?;
            }

            for payload in &item.mutations {
                transaction.execute(
                    "INSERT INTO pressure_log(token_id, payload) VALUES (?1, ?2)",
                    params![record.token_id, payload],
                )?;
            }
        }

        transaction.commit()?;

        for item in encoded {
            if item.checkpoint.is_some() {
                inner.mutation_counts.remove(&item.record.token_id);
            } else if !item.mutations.is_empty() {
                *inner
                    .mutation_counts
                    .entry(item.record.token_id.clone())
                    .or_default() += item.mutations.len();
            }
        }

        Ok(stats)
    }

    pub fn stats(&self) -> Result<RecorderStoreStats> {
        let inner = self.lock()?;
        let pressure_tokens = inner.connection.query_row(
            r#"
            SELECT COUNT(*)
            FROM token_state s
            WHERE s.pressure IS NOT NULL
               OR EXISTS (
                   SELECT 1
                   FROM pressure_log l
                   WHERE l.token_id = s.token_id
               )
            "#,
            [],
            |row| row.get::<_, i64>(0),
        )? as u64;
        let pressure_log_mutations =
            inner
                .connection
                .query_row("SELECT COUNT(*) FROM pressure_log", [], |row| {
                    row.get::<_, i64>(0)
                })? as u64;

        Ok(RecorderStoreStats {
            pressure_tokens,
            pressure_log_mutations,
            database_path: self.path.display().to_string(),
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.lock()?
            .connection
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, StoreInner>> {
        self.inner
            .lock()
            .map_err(|_| anyhow::anyhow!("recorder database mutex poisoned"))
    }
}

pub(crate) fn configure_connection(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
        PRAGMA wal_autocheckpoint = 1000;
        PRAGMA foreign_keys = ON;
        "#,
    )?;
    Ok(())
}

fn initialize_database(connection: &mut Connection) -> Result<()> {
    let version = connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?;
    if version == RECORDER_DATABASE_VERSION {
        return Ok(());
    }

    if version == 0 {
        let existing_tables = connection.query_row(
            r#"
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table'
              AND name IN ('token_state', 'pressure_log')
            "#,
            [],
            |row| row.get::<_, i64>(0),
        )?;

        if existing_tables == 0 {
            connection.execute_batch(&format!(
                r#"
                BEGIN;
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
                CREATE TABLE market_fee (
                    condition_id TEXT PRIMARY KEY,
                    rate TEXT NOT NULL,
                    exponent INTEGER NOT NULL CHECK (exponent >= 0)
                ) WITHOUT ROWID;
                CREATE TABLE token_market (
                    token_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    FOREIGN KEY(condition_id) REFERENCES market_fee(condition_id)
                        ON DELETE CASCADE
                ) WITHOUT ROWID;
                CREATE INDEX token_market_condition_idx
                    ON token_market(condition_id);
                PRAGMA user_version = {RECORDER_DATABASE_VERSION};
                COMMIT;
                "#
            ))?;
            return Ok(());
        }
    }

    bail!(
        "Recorder database version {version} is unsupported; expected {RECORDER_DATABASE_VERSION}"
    )
}

fn load_mutation_counts(connection: &Connection) -> Result<HashMap<String, usize>> {
    let mut statement =
        connection.prepare("SELECT token_id, COUNT(*) FROM pressure_log GROUP BY token_id")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut counts = HashMap::new();
    for row in rows {
        let (token_id, count) = row?;
        counts.insert(token_id, usize::try_from(count)?);
    }
    Ok(counts)
}

fn decode_checkpoint(value: &[u8]) -> Result<PressureFrontierSnapshot> {
    let json = decode_gzip_json(value)?;
    let snapshot: PressureFrontierSnapshot =
        serde_json::from_str(&json).context("parsing pressure checkpoint JSON")?;
    PressureFrontierMemory::restore(snapshot.clone())?;
    Ok(snapshot)
}

fn decode_gzip_json(value: &[u8]) -> Result<String> {
    let mut decoder = GzDecoder::new(value);
    let mut json = String::new();
    decoder
        .read_to_string(&mut json)
        .context("decompressing pressure checkpoint")?;
    Ok(json)
}

pub(crate) fn encode_checkpoint(snapshot: &PressureFrontierSnapshot) -> Result<Vec<u8>> {
    PressureFrontierMemory::restore(snapshot.clone())?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    serde_json::to_writer(&mut encoder, snapshot).context("serializing pressure checkpoint")?;
    encoder.finish().context("compressing pressure checkpoint")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pressure::PressureLevel;
    use tempfile::tempdir;

    fn one_level_memory() -> PressureFrontierMemory {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 10.0,
                }],
                1_000,
            )
            .unwrap();
        memory
    }

    #[test]
    fn appends_protobuf_mutations_then_atomically_replaces_with_checkpoint() {
        let temp = tempdir().unwrap();
        let store = RecorderStore::open(temp.path().join("recorder.sqlite")).unwrap();

        let mutation = RecorderPressureMutation::ApplyDelta {
            valid_through_ms: 2_000,
            changes: vec![PressureLevel {
                price: 5_000,
                shares: 4.0,
            }],
        };

        store
            .write(&[RecorderStoreWriteRecord {
                token_id: "token".into(),
                status: RecorderTokenStatus::Watched,
                recording_since_ms: Some(1_000),
                mutations: vec![mutation.clone()],
                checkpoint: RecorderCheckpointWrite::Keep,
            }])
            .unwrap();
        assert_eq!(store.mutation_count("token").unwrap(), 1);

        let mut memory = one_level_memory();
        replay_pressure_mutation(&mut memory, mutation).unwrap();
        store
            .write(&[RecorderStoreWriteRecord {
                token_id: "token".into(),
                status: RecorderTokenStatus::Watched,
                recording_since_ms: Some(1_000),
                mutations: Vec::new(),
                checkpoint: RecorderCheckpointWrite::Replace(Some(memory.snapshot())),
            }])
            .unwrap();

        assert_eq!(store.mutation_count("token").unwrap(), 0);
        let restored = store.load_pressure("token").unwrap().unwrap();
        assert_eq!(PressureFrontierMemory::restore(restored).unwrap(), memory);
        assert_eq!(store.stats().unwrap().pressure_log_mutations, 0);
    }

    #[test]
    fn persists_market_fee_metadata() {
        let temp = tempdir().unwrap();
        let store = RecorderStore::open(temp.path().join("recorder.sqlite")).unwrap();
        let market = MarketFeeInfo {
            condition_id: "condition".into(),
            schedule: FeeSchedule::from_persisted("0.04", 1).unwrap(),
            token_ids: vec!["yes".into(), "no".into()],
        };

        store.save_market_fee(&market).unwrap();
        let restored = store.load_market_fees().unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].condition_id, "condition");
        assert_eq!(restored[0].schedule, market.schedule);
        assert_eq!(restored[0].token_ids, vec!["no", "yes"]);
    }

    #[test]
    fn rejects_v5_database() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("recorder.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 5).unwrap();
        drop(connection);

        let error = match RecorderStore::open(&path) {
            Ok(_) => panic!("v5 database should be rejected"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Recorder database version 5 is unsupported; expected 8")
        );
    }
}
