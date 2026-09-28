use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use anyhow::{Context, Result, bail, ensure};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use rusqlite::{Connection, params};
use serde_json::Value;
use tracing::info;

use crate::pressure::{PRICE_SCALE, PressureFrontierMemory, PressureFrontierSnapshot, PressureLevel};

const MUTATION_CLEAR: u8 = 0;
const MUTATION_REPLACE: u8 = 1;
const MUTATION_UPDATE_V6: u8 = 2;
const MUTATION_ADVANCE: u8 = 3;
const MUTATION_REPLACE_CONTINUOUS: u8 = 4;
const MUTATION_UPDATE_CONTINUOUS: u8 = 5;
const MUTATION_HEADER_BYTES: usize = 11;
const MUTATION_LEVEL_BYTES: usize = 10;

enum LegacyMutation {
    Clear,
    ObserveSnapshot {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
    V6Delta {
        valid_through_ms: i64,
        changes: Vec<PressureLevel>,
    },
    Advance {
        valid_through_ms: i64,
    },
    ReplaceContinuous {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
    ContinuousDelta {
        valid_through_ms: i64,
        changes: Vec<PressureLevel>,
    },
}

pub(crate) fn migrate_database_v6_to_v7(connection: &mut Connection) -> Result<()> {
    let token_ids = {
        let mut statement = connection.prepare(
            r#"
            SELECT token_id
            FROM token_state s
            WHERE s.pressure IS NOT NULL
               OR EXISTS (
                   SELECT 1
                   FROM pressure_log l
                   WHERE l.token_id = s.token_id
               )
            ORDER BY token_id
            "#,
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };

    info!(
        tokens = token_ids.len(),
        "migrating recorder pressure database v6 -> v7"
    );

    for (index, token_id) in token_ids.iter().enumerate() {
        let checkpoint = connection.query_row(
            "SELECT pressure FROM token_state WHERE token_id = ?1",
            [token_id],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )?;

        let mut memory = checkpoint
            .as_deref()
            .map(decode_legacy_checkpoint)
            .transpose()
            .with_context(|| format!("restoring legacy pressure checkpoint for token {token_id}"))?
            .unwrap_or_default();

        {
            let mut statement = connection.prepare(
                r#"
                SELECT seq, payload
                FROM pressure_log
                WHERE token_id = ?1
                ORDER BY seq
                "#,
            )?;
            let mut rows = statement.query([token_id])?;
            while let Some(row) = rows.next()? {
                let seq = row.get::<_, i64>(0)?;
                let payload = row.get::<_, Vec<u8>>(1)?;
                let mutation = decode_legacy_mutation(&payload).with_context(|| {
                    format!("decoding legacy pressure mutation seq={seq} token={token_id}")
                })?;
                replay_legacy_mutation(&mut memory, mutation).with_context(|| {
                    format!("replaying legacy pressure mutation seq={seq} token={token_id}")
                })?;
            }
        }

        let checkpoint = encode_v7_checkpoint(&memory.snapshot())?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE token_state SET pressure = ?1 WHERE token_id = ?2",
            params![checkpoint, token_id],
        )?;
        transaction.execute("DELETE FROM pressure_log WHERE token_id = ?1", [token_id])?;
        transaction.commit()?;

        if (index + 1) % 100 == 0 || index + 1 == token_ids.len() {
            info!(
                migrated = index + 1,
                total = token_ids.len(),
                "pressure migration progress"
            );
        }
    }

    connection.pragma_update(None, "user_version", 7)?;
    info!("recorder pressure database migration to v7 complete");
    Ok(())
}

fn decode_legacy_checkpoint(value: &[u8]) -> Result<PressureFrontierMemory> {
    let mut decoder = GzDecoder::new(value);
    let mut json = String::new();
    decoder
        .read_to_string(&mut json)
        .context("decompressing legacy pressure checkpoint")?;

    let mut value: Value =
        serde_json::from_str(&json).context("parsing legacy pressure checkpoint JSON")?;
    let version = value
        .get("version")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("legacy pressure checkpoint version is missing"))?;
    ensure!(
        version == 6 || version == 7,
        "unsupported legacy pressure checkpoint version: {version}"
    );

    value["version"] = Value::from(7_u64);
    if let Some(state) = value.get_mut("state").and_then(Value::as_object_mut)
        && state.get("kind").and_then(Value::as_str) == Some("resolvedUnbounded")
    {
        state.remove("resolvedAtMs");
    }

    let snapshot: PressureFrontierSnapshot =
        serde_json::from_value(value).context("normalizing legacy pressure checkpoint")?;
    PressureFrontierMemory::restore(snapshot)
}

fn encode_v7_checkpoint(snapshot: &PressureFrontierSnapshot) -> Result<Vec<u8>> {
    PressureFrontierMemory::restore(snapshot.clone())?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    serde_json::to_writer(&mut encoder, snapshot).context("serializing v7 pressure checkpoint")?;
    encoder.flush().context("flushing v7 pressure checkpoint")?;
    encoder.finish().context("compressing v7 pressure checkpoint")
}

fn decode_legacy_mutation(value: &[u8]) -> Result<LegacyMutation> {
    ensure!(!value.is_empty(), "empty legacy pressure mutation");

    let kind = value[0];
    if kind == MUTATION_CLEAR {
        ensure!(value.len() == 1, "malformed legacy clear mutation");
        return Ok(LegacyMutation::Clear);
    }

    ensure!(
        matches!(
            kind,
            MUTATION_REPLACE
                | MUTATION_UPDATE_V6
                | MUTATION_ADVANCE
                | MUTATION_REPLACE_CONTINUOUS
                | MUTATION_UPDATE_CONTINUOUS
        ),
        "unsupported legacy pressure mutation kind: {kind}"
    );

    if kind == MUTATION_ADVANCE {
        ensure!(value.len() == 9, "malformed legacy advance mutation");
        let valid_through_ms = i64::from_le_bytes(value[1..9].try_into()?);
        ensure!(
            valid_through_ms >= 0,
            "legacy pressure mutation timestamp must be non-negative"
        );
        return Ok(LegacyMutation::Advance { valid_through_ms });
    }

    ensure!(
        value.len() >= MUTATION_HEADER_BYTES,
        "truncated legacy pressure mutation"
    );
    let valid_through_ms = i64::from_le_bytes(value[1..9].try_into()?);
    ensure!(
        valid_through_ms >= 0,
        "legacy pressure mutation timestamp must be non-negative"
    );

    let count = usize::from(u16::from_le_bytes(value[9..11].try_into()?));
    let expected = MUTATION_HEADER_BYTES + count * MUTATION_LEVEL_BYTES;
    ensure!(
        value.len() == expected,
        "legacy pressure mutation has invalid length"
    );

    let mut levels = Vec::with_capacity(count);
    let mut offset = MUTATION_HEADER_BYTES;
    for _ in 0..count {
        let price = u16::from_le_bytes(value[offset..offset + 2].try_into()?);
        let shares = f64::from_le_bytes(value[offset + 2..offset + 10].try_into()?);
        ensure!(
            price <= PRICE_SCALE,
            "legacy pressure price exceeds scale"
        );
        ensure!(
            shares.is_finite()
                && shares >= 0.0
                && ((kind != MUTATION_REPLACE && kind != MUTATION_REPLACE_CONTINUOUS)
                    || shares > 0.0),
            "invalid legacy pressure shares"
        );
        levels.push(PressureLevel { price, shares });
        offset += MUTATION_LEVEL_BYTES;
    }

    Ok(match kind {
        MUTATION_REPLACE => LegacyMutation::ObserveSnapshot {
            valid_through_ms,
            levels,
        },
        MUTATION_UPDATE_V6 => LegacyMutation::V6Delta {
            valid_through_ms,
            changes: levels,
        },
        MUTATION_ADVANCE => unreachable!("advance returned above"),
        MUTATION_REPLACE_CONTINUOUS => LegacyMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        },
        MUTATION_UPDATE_CONTINUOUS => LegacyMutation::ContinuousDelta {
            valid_through_ms,
            changes: levels,
        },
        _ => unreachable!("validated legacy mutation kind above"),
    })
}

fn replay_legacy_mutation(
    memory: &mut PressureFrontierMemory,
    mutation: LegacyMutation,
) -> Result<()> {
    match mutation {
        LegacyMutation::Clear => *memory = PressureFrontierMemory::default(),
        LegacyMutation::ObserveSnapshot {
            valid_through_ms,
            levels,
        } => {
            let valid_through_ms = max_with_current(memory, valid_through_ms);
            memory.observe_levels(&levels, valid_through_ms)?;
        }
        LegacyMutation::V6Delta {
            valid_through_ms,
            changes,
        } => {
            let mut levels = memory
                .current_levels()
                .into_iter()
                .map(|level| (level.price, level.shares))
                .collect::<BTreeMap<_, _>>();
            for change in changes {
                if change.price == 0
                    || change.price > PRICE_SCALE
                    || !change.shares.is_finite()
                    || change.shares < 0.0
                {
                    continue;
                }
                if change.shares == 0.0 {
                    levels.remove(&change.price);
                } else {
                    levels.insert(change.price, change.shares);
                }
            }
            let levels = levels
                .into_iter()
                .map(|(price, shares)| PressureLevel { price, shares })
                .collect::<Vec<_>>();
            let valid_through_ms = max_with_current(memory, valid_through_ms);
            memory.observe_levels(&levels, valid_through_ms)?;
        }
        LegacyMutation::Advance { valid_through_ms } => {
            memory.observe_through(max_with_current(memory, valid_through_ms))?;
        }
        LegacyMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        } => {
            memory.replace_continuous(&levels, max_with_current(memory, valid_through_ms))?;
        }
        LegacyMutation::ContinuousDelta {
            valid_through_ms,
            changes,
        } => {
            memory.update_levels(&changes, max_with_current(memory, valid_through_ms))?;
        }
    }
    Ok(())
}

fn max_with_current(memory: &PressureFrontierMemory, timestamp_ms: i64) -> i64 {
    memory
        .valid_through_ms()
        .map_or(timestamp_ms, |current| current.max(timestamp_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_v6_delta_keeps_old_freeze_semantics_and_maxes_raw_time() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 10.0,
                }],
                1_001,
            )
            .unwrap();

        replay_legacy_mutation(
            &mut memory,
            LegacyMutation::V6Delta {
                valid_through_ms: 1_000,
                changes: vec![PressureLevel {
                    price: 5_000,
                    shares: 4.0,
                }],
            },
        )
        .unwrap();

        assert_eq!(memory.valid_through_ms(), Some(1_001));
        let snapshot = serde_json::to_value(memory.snapshot()).unwrap();
        assert_eq!(
            snapshot["state"]["runs"][0]["frozenSteps"][0]["validThroughMs"],
            1_001
        );
    }

    #[test]
    fn legacy_terminal_timestamp_is_removed_from_v7_checkpoint() {
        let json = br#"{"version":7,"state":{"kind":"resolvedUnbounded","resolvedAtMs":1234}}"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(json).unwrap();
        let encoded = encoder.finish().unwrap();

        let memory = decode_legacy_checkpoint(&encoded).unwrap();
        assert!(memory.is_resolved_unbounded());
        assert_eq!(
            serde_json::to_value(memory.snapshot()).unwrap(),
            serde_json::json!({"version":7,"state":{"kind":"resolvedUnbounded"}})
        );
    }
}
