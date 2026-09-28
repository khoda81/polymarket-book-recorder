use std::{collections::HashMap, io::Read, path::Path};

use anyhow::{Context, Result, bail, ensure};
use flate2::read::GzDecoder;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use tracing::info;

use crate::{
    fees::{FeeResolver, FeeSchedule, MarketFeeInfo},
    pressure::{PressureFrontierMemory, PressureFrontierSnapshot, SNAPSHOT_VERSION},
    pressure_log::{decode_pressure_mutation, replay_pressure_mutation},
    store::{configure_connection, encode_checkpoint},
};

const LEGACY_DATABASE_VERSION: i64 = 7;

pub async fn migrate_database_v7_to_v8(
    path: impl AsRef<Path>,
    fees: &mut FeeResolver,
) -> Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(());
    }

    let mut connection = Connection::open(path)
        .with_context(|| format!("opening recorder database {}", path.display()))?;
    configure_connection(&connection)?;
    let version = database_version(&connection)?;

    if version == crate::store::RECORDER_DATABASE_VERSION || version == 0 {
        return Ok(());
    }
    ensure!(
        version == LEGACY_DATABASE_VERSION,
        "Recorder database version {version} is unsupported; expected {}",
        crate::store::RECORDER_DATABASE_VERSION
    );

    create_fee_metadata_tables(&connection)?;
    seed_persisted_fees(&connection, fees)?;

    let token_ids = {
        let mut statement =
            connection.prepare("SELECT token_id FROM token_state ORDER BY token_id")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };

    info!(
        tokens = token_ids.len(),
        "migrating recorder pressure prices from raw v7 to effective v8"
    );

    for (index, token_id) in token_ids.iter().enumerate() {
        let market = match fees.cached_for_token(token_id) {
            Some(market) => market.clone(),
            None => fees
                .resolve_token(token_id)
                .await
                .with_context(|| format!("resolving fee schedule for token {token_id}"))?,
        };
        persist_market_fee(&mut connection, &market)?;

        if let Some((mut memory, semantic_version)) =
            load_pressure_for_migration(&connection, token_id)?
        {
            match semantic_version {
                7 => {
                    memory
                        .map_prices(|price| market.schedule.effective_ask_tick(price))
                        .with_context(|| {
                            format!("mapping token {token_id} into effective taker-price space")
                        })?;
                }
                8 => {}
                other => bail!(
                    "unsupported pressure snapshot semantic version {other} for token {token_id}"
                ),
            }

            let checkpoint = encode_checkpoint(&memory.snapshot())?;
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE token_state SET pressure = ?1 WHERE token_id = ?2",
                params![checkpoint, token_id],
            )?;
            transaction.execute("DELETE FROM pressure_log WHERE token_id = ?1", [token_id])?;
            transaction.commit()?;
        }

        if (index + 1) % 100 == 0 || index + 1 == token_ids.len() {
            info!(
                migrated = index + 1,
                total = token_ids.len(),
                "effective-price migration progress"
            );
        }
    }

    connection.pragma_update(
        None,
        "user_version",
        crate::store::RECORDER_DATABASE_VERSION,
    )?;
    info!("recorder effective-price migration to schema v8 complete");
    Ok(())
}

fn database_version(connection: &Connection) -> Result<i64> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .context("reading recorder database version")
}

fn create_fee_metadata_tables(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS market_fee (
            condition_id TEXT PRIMARY KEY,
            rate TEXT NOT NULL,
            exponent INTEGER NOT NULL CHECK (exponent >= 0)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS token_market (
            token_id TEXT PRIMARY KEY,
            condition_id TEXT NOT NULL,
            FOREIGN KEY(condition_id) REFERENCES market_fee(condition_id)
                ON DELETE CASCADE
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS token_market_condition_idx
            ON token_market(condition_id);
        "#,
    )?;
    Ok(())
}

fn seed_persisted_fees(connection: &Connection, fees: &mut FeeResolver) -> Result<()> {
    let mut markets = HashMap::<String, (FeeSchedule, Vec<String>)>::new();
    {
        let mut statement =
            connection.prepare("SELECT condition_id, rate, exponent FROM market_fee")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        for row in rows {
            let (condition_id, rate, exponent) = row?;
            markets.insert(
                condition_id,
                (
                    FeeSchedule::from_persisted(
                        &rate,
                        u32::try_from(exponent)
                            .context("persisted fee exponent does not fit u32")?,
                    )?,
                    Vec::new(),
                ),
            );
        }
    }

    {
        let mut statement =
            connection.prepare("SELECT token_id, condition_id FROM token_market")?;
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

    for (condition_id, (schedule, token_ids)) in markets {
        fees.seed(MarketFeeInfo {
            condition_id,
            schedule,
            token_ids,
        });
    }
    Ok(())
}

fn persist_market_fee(connection: &mut Connection, market: &MarketFeeInfo) -> Result<()> {
    let transaction = connection.transaction()?;
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

fn load_pressure_for_migration(
    connection: &Connection,
    token_id: &str,
) -> Result<Option<(PressureFrontierMemory, u8)>> {
    let checkpoint = connection
        .query_row(
            "SELECT pressure FROM token_state WHERE token_id = ?1",
            [token_id],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()?
        .flatten();

    let mutations = {
        let mut statement = connection.prepare(
            r#"
            SELECT seq, payload
            FROM pressure_log
            WHERE token_id = ?1
            ORDER BY seq
            "#,
        )?;
        statement
            .query_map([token_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };

    if checkpoint.is_none() && mutations.is_empty() {
        return Ok(None);
    }

    let (mut memory, semantic_version) = match checkpoint {
        Some(checkpoint) => decode_migration_checkpoint(&checkpoint)?,
        None => (
            PressureFrontierMemory::default(),
            LEGACY_DATABASE_VERSION as u8,
        ),
    };

    for (seq, payload) in mutations {
        let mutation = decode_pressure_mutation(&payload)
            .with_context(|| format!("decoding pressure mutation seq={seq} token={token_id}"))?;
        replay_pressure_mutation(&mut memory, mutation)
            .with_context(|| format!("replaying pressure mutation seq={seq} token={token_id}"))?;
    }

    Ok(Some((memory, semantic_version)))
}

fn decode_migration_checkpoint(value: &[u8]) -> Result<(PressureFrontierMemory, u8)> {
    let mut decoder = GzDecoder::new(value);
    let mut json = String::new();
    decoder
        .read_to_string(&mut json)
        .context("decompressing pressure checkpoint during migration")?;

    let mut value: Value =
        serde_json::from_str(&json).context("parsing pressure checkpoint during migration")?;
    let semantic_version = value
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|version| u8::try_from(version).ok())
        .ok_or_else(|| anyhow::anyhow!("pressure checkpoint version is missing"))?;
    ensure!(
        semantic_version == 7 || semantic_version == SNAPSHOT_VERSION,
        "unsupported pressure checkpoint version during migration: {semantic_version}"
    );

    // v7 and v8 have the same structural representation. The version boundary
    // is semantic: raw venue prices vs effective taker prices.
    value["version"] = Value::from(SNAPSHOT_VERSION);
    let snapshot: PressureFrontierSnapshot =
        serde_json::from_value(value).context("normalizing pressure checkpoint to v8")?;
    Ok((PressureFrontierMemory::restore(snapshot)?, semantic_version))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        pressure::PressureLevel,
        pressure_log::{RecorderPressureMutation, encode_pressure_mutation},
        store::RecorderStore,
    };

    #[tokio::test]
    async fn migrates_raw_v7_pressure_to_effective_v8_and_persists_fee_manifest() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("recorder.sqlite");
        let connection = Connection::open(&path).unwrap();
        configure_connection(&connection).unwrap();
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
                PRAGMA user_version = 7;
                "#,
            )
            .unwrap();

        let raw_snapshot = serde_json::json!({
            "version": 7,
            "state": {
                "kind": "observed",
                "validThroughMs": 1_000,
                "runs": [{
                    "price": 5_000,
                    "shares": 10.0,
                    "frozenSteps": []
                }]
            }
        });
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        serde_json::to_writer(&mut encoder, &raw_snapshot).unwrap();
        encoder.flush().unwrap();
        let checkpoint = encoder.finish().unwrap();
        connection
            .execute(
                "INSERT INTO token_state(token_id, status, recording_since_ms, pressure)
                 VALUES ('yes', 'watched', 1000, ?1)",
                [checkpoint],
            )
            .unwrap();

        let mutation = encode_pressure_mutation(&RecorderPressureMutation::ApplyDelta {
            valid_through_ms: 2_000,
            changes: vec![PressureLevel {
                price: 5_000,
                shares: 4.0,
            }],
        })
        .unwrap();
        connection
            .execute(
                "INSERT INTO pressure_log(token_id, payload) VALUES ('yes', ?1)",
                [mutation],
            )
            .unwrap();
        drop(connection);

        let schedule = FeeSchedule::new(Decimal::new(4, 2), 1).unwrap();
        let mut fees = FeeResolver::new();
        fees.seed(MarketFeeInfo {
            condition_id: "condition".into(),
            schedule,
            token_ids: vec!["yes".into(), "no".into()],
        });

        migrate_database_v7_to_v8(&path, &mut fees).await.unwrap();

        let store = RecorderStore::open(&path).unwrap();
        let snapshot = store.load_pressure("yes").unwrap().unwrap();
        let memory = PressureFrontierMemory::restore(snapshot).unwrap();
        assert_eq!(
            memory.current_levels(),
            vec![PressureLevel {
                price: 5_100,
                shares: 4.0,
            }]
        );
        let migrated = serde_json::to_value(memory.snapshot()).unwrap();
        assert_eq!(migrated["version"], 8);
        assert_eq!(migrated["state"]["runs"][0]["price"], 5_100);
        assert_eq!(
            migrated["state"]["runs"][0]["frozenSteps"][0]["hiVolume"],
            10.0
        );
        assert_eq!(
            migrated["state"]["runs"][0]["frozenSteps"][0]["validThroughMs"],
            2_000
        );
        assert_eq!(store.stats().unwrap().pressure_log_mutations, 0);

        let persisted = store.load_market_fees().unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].condition_id, "condition");
        assert_eq!(persisted[0].schedule, schedule);
        assert_eq!(persisted[0].token_ids, vec!["no", "yes"]);

        let connection = Connection::open(&path).unwrap();
        assert_eq!(database_version(&connection).unwrap(), 8);
    }
}
