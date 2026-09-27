use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::{
    pressure::{PressureFrontierMemory, PressureFrontierSnapshot},
    pressure_log::{decode_pressure_mutation, replay_pressure_mutation},
};

pub const RECORDER_DATABASE_VERSION: i64 = 5;
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
}

#[derive(Debug, Clone)]
pub struct RecorderStoreIndexRecord {
    pub token_id: String,
    pub status: RecorderTokenStatus,
    pub recording_since_ms: Option<i64>,
    pub has_pressure: bool,
}

#[derive(Debug, Clone)]
pub struct RecorderStoreRecord {
    pub token_id: String,
    pub status: RecorderTokenStatus,
    pub recording_since_ms: Option<i64>,
    pub pressure: Option<PressureFrontierSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecorderStoreStats {
    pub watched_tokens: u64,
    pub completed_tokens: u64,
    pub pressure_tokens: u64,
    pub pressure_log_mutations: u64,
    pub database_path: String,
}

pub struct RecorderStore {
    path: PathBuf,
    connection: Mutex<Connection>,
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

        let connection = Connection::open(&path)
            .with_context(|| format!("opening recorder database {}", path.display()))?;
        configure_connection(&connection)?;
        initialize_database(&connection)?;

        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    pub fn load_index(&self) -> Result<Vec<RecorderStoreIndexRecord>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
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

    pub fn load(&self, token_id: &str) -> Result<Option<RecorderStoreRecord>> {
        let (status, recording_since_ms, checkpoint, mutations) = {
            let connection = self.lock()?;
            let row = connection
                .query_row(
                    r#"
                    SELECT status, recording_since_ms, pressure
                    FROM token_state
                    WHERE token_id = ?1
                    "#,
                    [token_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<Vec<u8>>>(2)?,
                        ))
                    },
                )
                .optional()?;

            let Some((status, recording_since_ms, checkpoint)) = row else {
                return Ok(None);
            };

            let mut statement = connection.prepare(
                r#"
                SELECT payload
                FROM pressure_log
                WHERE token_id = ?1
                ORDER BY seq
                "#,
            )?;
            let mutations = statement
                .query_map([token_id], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            (status, recording_since_ms, checkpoint, mutations)
        };

        let mut pressure = checkpoint.as_deref().map(decode_checkpoint).transpose()?;

        if !mutations.is_empty() {
            let mut memory = match pressure.take() {
                Some(snapshot) => PressureFrontierMemory::restore(snapshot)?,
                None => PressureFrontierMemory::default(),
            };

            for payload in mutations {
                let mutation = decode_pressure_mutation(&payload)?;
                replay_pressure_mutation(&mut memory, mutation)?;
            }
            pressure = Some(memory.snapshot());
        }

        Ok(Some(RecorderStoreRecord {
            token_id: token_id.to_owned(),
            status: RecorderTokenStatus::parse(&status)?,
            recording_since_ms,
            pressure,
        }))
    }

    pub fn stats(&self) -> Result<RecorderStoreStats> {
        let connection = self.lock()?;
        let watched_tokens = count_where(&connection, "status = 'watched'")?;
        let completed_tokens = count_where(&connection, "status = 'completed'")?;
        let pressure_tokens = connection.query_row(
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
            |row| row.get::<_, u64>(0),
        )?;
        let pressure_log_mutations =
            connection.query_row("SELECT COUNT(*) FROM pressure_log", [], |row| {
                row.get::<_, u64>(0)
            })?;

        Ok(RecorderStoreStats {
            watched_tokens,
            completed_tokens,
            pressure_tokens,
            pressure_log_mutations,
            database_path: self.path.display().to_string(),
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.lock()?
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow::anyhow!("recorder database mutex poisoned"))
    }
}

fn configure_connection(connection: &Connection) -> Result<()> {
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

fn initialize_database(connection: &Connection) -> Result<()> {
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

fn decode_checkpoint(value: &[u8]) -> Result<PressureFrontierSnapshot> {
    let mut decoder = GzDecoder::new(value);
    let mut json = String::new();
    decoder
        .read_to_string(&mut json)
        .context("decompressing pressure checkpoint")?;

    let snapshot: PressureFrontierSnapshot =
        serde_json::from_str(&json).context("parsing pressure checkpoint JSON")?;

    // Validate the exact v5 invariants while restoring. Return the canonical
    // serde representation so callers can replay the mutation tail on top.
    PressureFrontierMemory::restore(snapshot.clone())?;
    Ok(snapshot)
}

fn count_where(connection: &Connection, predicate: &str) -> Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM token_state WHERE {predicate}");
    Ok(connection.query_row(&sql, [], |row| row.get::<_, u64>(0))?)
}
