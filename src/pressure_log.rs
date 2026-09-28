use anyhow::{Result, bail, ensure};

use crate::pressure::{PRICE_SCALE, PressureFrontierMemory, PressureLevel};

const MUTATION_CLEAR: u8 = 0;
const MUTATION_REPLACE: u8 = 1;
const MUTATION_UPDATE_V6: u8 = 2;
const MUTATION_ADVANCE: u8 = 3;
const MUTATION_REPLACE_CONTINUOUS: u8 = 4;
const MUTATION_UPDATE_CONTINUOUS: u8 = 5;
const MUTATION_HEADER_BYTES: usize = 11;
const MUTATION_LEVEL_BYTES: usize = 10;

#[derive(Debug, Clone, PartialEq)]
pub enum RecorderPressureMutation {
    /// Complete observation after a continuity gap.
    Replace {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
    /// Ordered-stream absolute level changes.
    Update {
        valid_through_ms: i64,
        changes: Vec<PressureLevel>,
    },
    /// Persisted v6 update semantics. Decode/replay only; never emit anew.
    LegacyV6Update {
        valid_through_ms: i64,
        changes: Vec<PressureLevel>,
    },
    /// Ordered market evidence that does not change this token's levels.
    Advance { valid_through_ms: i64 },
    /// Complete book replacement on a known-continuous stream.
    ReplaceContinuous {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
    /// Legacy v6 mutation emitted by older recorders on resolution.
    Clear,
}

pub fn encode_pressure_mutation(mutation: &RecorderPressureMutation) -> Result<Vec<u8>> {
    let (kind, valid_through_ms, entries): (u8, i64, Vec<(u16, f64)>) = match mutation {
        RecorderPressureMutation::Clear => return Ok(vec![MUTATION_CLEAR]),
        RecorderPressureMutation::Advance { valid_through_ms } => {
            ensure!(
                *valid_through_ms >= 0,
                "pressure mutation timestamp must be non-negative"
            );
            let mut bytes = Vec::with_capacity(9);
            bytes.push(MUTATION_ADVANCE);
            bytes.extend_from_slice(&valid_through_ms.to_le_bytes());
            return Ok(bytes);
        }
        RecorderPressureMutation::Replace {
            valid_through_ms,
            levels,
        } => (
            MUTATION_REPLACE,
            *valid_through_ms,
            levels
                .iter()
                .map(|level| (level.price, level.shares))
                .collect(),
        ),
        RecorderPressureMutation::Update {
            valid_through_ms,
            changes,
        } => (
            MUTATION_UPDATE_CONTINUOUS,
            *valid_through_ms,
            changes
                .iter()
                .map(|change| (change.price, change.shares))
                .collect(),
        ),
        RecorderPressureMutation::LegacyV6Update { .. } => {
            bail!("legacy v6 pressure updates are replay-only")
        }
        RecorderPressureMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        } => (
            MUTATION_REPLACE_CONTINUOUS,
            *valid_through_ms,
            levels
                .iter()
                .map(|level| (level.price, level.shares))
                .collect(),
        ),
    };

    ensure!(
        valid_through_ms >= 0,
        "pressure mutation timestamp must be non-negative"
    );
    ensure!(
        entries.len() <= u16::MAX as usize,
        "too many pressure levels in one mutation"
    );

    let mut bytes =
        Vec::with_capacity(MUTATION_HEADER_BYTES + entries.len() * MUTATION_LEVEL_BYTES);
    bytes.push(kind);
    bytes.extend_from_slice(&valid_through_ms.to_le_bytes());
    bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());

    for (price, shares) in entries {
        ensure!(price <= PRICE_SCALE, "price ticks must be in [0, 10000]");
        if !shares.is_finite()
            || shares < 0.0
            || ((kind == MUTATION_REPLACE || kind == MUTATION_REPLACE_CONTINUOUS) && shares <= 0.0)
        {
            bail!("invalid pressure mutation shares");
        }
        bytes.extend_from_slice(&price.to_le_bytes());
        bytes.extend_from_slice(&shares.to_le_bytes());
    }

    Ok(bytes)
}

pub fn replay_pressure_mutation(
    memory: &mut PressureFrontierMemory,
    mutation: RecorderPressureMutation,
) -> Result<()> {
    match mutation {
        RecorderPressureMutation::Clear => memory.clear_legacy(),
        RecorderPressureMutation::Replace {
            valid_through_ms,
            levels,
        } => {
            memory.replay_legacy_v6_replace(&levels, valid_through_ms)?;
        }
        RecorderPressureMutation::Update {
            valid_through_ms,
            changes,
        } => {
            memory.update_levels(&changes, valid_through_ms)?;
        }
        RecorderPressureMutation::LegacyV6Update {
            valid_through_ms,
            changes,
        } => {
            memory.replay_legacy_v6_update(&changes, valid_through_ms)?;
        }
        RecorderPressureMutation::Advance { valid_through_ms } => {
            memory.observe_through(valid_through_ms)?;
        }
        RecorderPressureMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        } => {
            memory.replace_continuous(&levels, valid_through_ms)?;
        }
    }
    Ok(())
}

pub fn decode_pressure_mutation(value: &[u8]) -> Result<RecorderPressureMutation> {
    ensure!(!value.is_empty(), "empty pressure mutation");

    let kind = value[0];
    if kind == MUTATION_CLEAR {
        ensure!(value.len() == 1, "malformed clear pressure mutation");
        return Ok(RecorderPressureMutation::Clear);
    }

    ensure!(
        kind == MUTATION_REPLACE
            || kind == MUTATION_UPDATE_V6
            || kind == MUTATION_UPDATE_CONTINUOUS
            || kind == MUTATION_ADVANCE
            || kind == MUTATION_REPLACE_CONTINUOUS,
        "unsupported pressure mutation kind: {kind}"
    );

    if kind == MUTATION_ADVANCE {
        ensure!(value.len() == 9, "malformed advance pressure mutation");
        let valid_through_ms = i64::from_le_bytes(value[1..9].try_into()?);
        ensure!(
            valid_through_ms >= 0,
            "pressure mutation timestamp must be non-negative"
        );
        return Ok(RecorderPressureMutation::Advance { valid_through_ms });
    }

    ensure!(
        value.len() >= MUTATION_HEADER_BYTES,
        "truncated pressure mutation"
    );

    let valid_through_ms = i64::from_le_bytes(value[1..9].try_into()?);
    ensure!(
        valid_through_ms >= 0,
        "pressure mutation timestamp must be non-negative"
    );

    let count = u16::from_le_bytes(value[9..11].try_into()?) as usize;
    let expected = MUTATION_HEADER_BYTES + count * MUTATION_LEVEL_BYTES;
    ensure!(
        value.len() == expected,
        "pressure mutation has invalid length"
    );

    let mut entries = Vec::with_capacity(count);
    let mut offset = MUTATION_HEADER_BYTES;
    for _ in 0..count {
        let price = u16::from_le_bytes(value[offset..offset + 2].try_into()?);
        let shares = f64::from_le_bytes(value[offset + 2..offset + 10].try_into()?);
        ensure!(price <= PRICE_SCALE, "price ticks must be in [0, 10000]");
        ensure!(
            shares.is_finite()
                && shares >= 0.0
                && ((kind != MUTATION_REPLACE && kind != MUTATION_REPLACE_CONTINUOUS)
                    || shares > 0.0),
            "invalid pressure mutation shares"
        );
        entries.push((price, shares));
        offset += MUTATION_LEVEL_BYTES;
    }

    let levels = entries
        .into_iter()
        .map(|(price, shares)| PressureLevel { price, shares })
        .collect();

    Ok(match kind {
        MUTATION_REPLACE => RecorderPressureMutation::Replace {
            valid_through_ms,
            levels,
        },
        MUTATION_UPDATE_V6 => RecorderPressureMutation::LegacyV6Update {
            valid_through_ms,
            changes: levels,
        },
        MUTATION_UPDATE_CONTINUOUS => RecorderPressureMutation::Update {
            valid_through_ms,
            changes: levels,
        },
        MUTATION_REPLACE_CONTINUOUS => RecorderPressureMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        },
        _ => unreachable!("validated mutation kind above"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_is_exactly_one_byte() {
        assert_eq!(
            encode_pressure_mutation(&RecorderPressureMutation::Clear).unwrap(),
            vec![0]
        );
        assert_eq!(
            decode_pressure_mutation(&[0]).unwrap(),
            RecorderPressureMutation::Clear
        );
        assert!(decode_pressure_mutation(&[0, 0]).is_err());
    }

    #[test]
    fn legacy_v6_replay_max_aggregates_raw_timestamp_regressions() {
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

        replay_pressure_mutation(
            &mut memory,
            RecorderPressureMutation::LegacyV6Update {
                valid_through_ms: 1_000,
                changes: vec![PressureLevel {
                    price: 5_000,
                    shares: 4.0,
                }],
            },
        )
        .unwrap();

        assert_eq!(memory.valid_through_ms(), Some(1_001));

        replay_pressure_mutation(
            &mut memory,
            RecorderPressureMutation::Replace {
                valid_through_ms: 999,
                levels: vec![PressureLevel {
                    price: 5_000,
                    shares: 3.0,
                }],
            },
        )
        .unwrap();

        assert_eq!(memory.valid_through_ms(), Some(1_001));
    }

    #[test]
    fn legacy_v6_update_replays_old_watermark_semantics() {
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

        let mut encoded = Vec::new();
        encoded.push(MUTATION_UPDATE_V6);
        encoded.extend_from_slice(&2_000_i64.to_le_bytes());
        encoded.extend_from_slice(&1_u16.to_le_bytes());
        encoded.extend_from_slice(&5_000_u16.to_le_bytes());
        encoded.extend_from_slice(&4.0_f64.to_le_bytes());

        let mutation = decode_pressure_mutation(&encoded).unwrap();
        assert!(matches!(
            mutation,
            RecorderPressureMutation::LegacyV6Update { .. }
        ));
        replay_pressure_mutation(&mut memory, mutation).unwrap();

        let snapshot = serde_json::to_value(memory.snapshot()).unwrap();
        assert_eq!(
            snapshot["state"]["runs"][0]["frozenSteps"][0]["validThroughMs"],
            1_000
        );
    }

    #[test]
    fn advance_and_continuous_replace_round_trip() {
        for mutation in [
            RecorderPressureMutation::Advance {
                valid_through_ms: 2_000,
            },
            RecorderPressureMutation::ReplaceContinuous {
                valid_through_ms: 3_000,
                levels: vec![PressureLevel {
                    price: 5_000,
                    shares: 12.0,
                }],
            },
        ] {
            let encoded = encode_pressure_mutation(&mutation).unwrap();
            assert_eq!(decode_pressure_mutation(&encoded).unwrap(), mutation);
        }
    }

    #[test]
    fn continuous_update_uses_new_mutation_kind_and_integer_timestamp() {
        let mutation = RecorderPressureMutation::Update {
            valid_through_ms: 1_234,
            changes: vec![
                PressureLevel {
                    price: 125,
                    shares: 12.5,
                },
                PressureLevel {
                    price: 9_875,
                    shares: 0.0,
                },
            ],
        };

        let encoded = encode_pressure_mutation(&mutation).unwrap();
        assert_eq!(encoded[0], MUTATION_UPDATE_CONTINUOUS);
        assert_eq!(&encoded[1..9], &1_234_i64.to_le_bytes());
        assert_eq!(&encoded[9..11], &2_u16.to_le_bytes());
        assert_eq!(&encoded[11..13], &125_u16.to_le_bytes());
        assert_eq!(&encoded[13..21], &12.5_f64.to_le_bytes());

        assert_eq!(decode_pressure_mutation(&encoded).unwrap(), mutation);
    }
}
