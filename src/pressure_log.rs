use anyhow::{Result, bail, ensure};

use crate::pressure::{FrontierLevel, PRICE_SCALE, PressureFrontierMemory, PressureLevelChange};

const MUTATION_CLEAR: u8 = 0;
const MUTATION_REPLACE: u8 = 1;
const MUTATION_UPDATE: u8 = 2;
const MUTATION_HEADER_BYTES: usize = 11;
const MUTATION_LEVEL_BYTES: usize = 10;

#[derive(Debug, Clone, PartialEq)]
pub enum RecorderPressureMutation {
    Replace {
        valid_through_ms: i64,
        levels: Vec<FrontierLevel>,
    },
    Update {
        valid_through_ms: i64,
        changes: Vec<PressureLevelChange>,
    },
    Clear,
}

pub fn encode_pressure_mutation(mutation: &RecorderPressureMutation) -> Result<Vec<u8>> {
    let (kind, valid_through_ms, entries): (u8, i64, Vec<(u16, f64)>) = match mutation {
        RecorderPressureMutation::Clear => return Ok(vec![MUTATION_CLEAR]),
        RecorderPressureMutation::Replace {
            valid_through_ms,
            levels,
        } => (
            MUTATION_REPLACE,
            *valid_through_ms,
            levels
                .iter()
                .map(|level| (level.key, level.weight))
                .collect(),
        ),
        RecorderPressureMutation::Update {
            valid_through_ms,
            changes,
        } => (
            MUTATION_UPDATE,
            *valid_through_ms,
            changes
                .iter()
                .map(|change| (change.price, change.shares))
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
        if !shares.is_finite() || shares < 0.0 || (kind == MUTATION_REPLACE && shares <= 0.0) {
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
        RecorderPressureMutation::Clear => memory.clear(),
        RecorderPressureMutation::Replace {
            valid_through_ms,
            levels,
        } => {
            memory.observe_levels(&levels, valid_through_ms)?;
        }
        RecorderPressureMutation::Update {
            valid_through_ms,
            changes,
        } => {
            memory.update_levels(&changes, valid_through_ms)?;
            memory.observe_through(valid_through_ms)?;
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
        kind == MUTATION_REPLACE || kind == MUTATION_UPDATE,
        "unsupported pressure mutation kind: {kind}"
    );
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
            shares.is_finite() && shares >= 0.0 && (kind != MUTATION_REPLACE || shares > 0.0),
            "invalid pressure mutation shares"
        );
        entries.push((price, shares));
        offset += MUTATION_LEVEL_BYTES;
    }

    Ok(if kind == MUTATION_REPLACE {
        RecorderPressureMutation::Replace {
            valid_through_ms,
            levels: entries
                .into_iter()
                .map(|(key, weight)| FrontierLevel { key, weight })
                .collect(),
        }
    } else {
        RecorderPressureMutation::Update {
            valid_through_ms,
            changes: entries
                .into_iter()
                .map(|(price, shares)| PressureLevelChange { price, shares })
                .collect(),
        }
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
    fn v6_mutation_uses_integer_timestamp() {
        let mutation = RecorderPressureMutation::Update {
            valid_through_ms: 1_234,
            changes: vec![
                PressureLevelChange {
                    price: 125,
                    shares: 12.5,
                },
                PressureLevelChange {
                    price: 9_875,
                    shares: 0.0,
                },
            ],
        };

        let encoded = encode_pressure_mutation(&mutation).unwrap();
        assert_eq!(encoded[0], MUTATION_UPDATE);
        assert_eq!(&encoded[1..9], &1_234_i64.to_le_bytes());
        assert_eq!(&encoded[9..11], &2_u16.to_le_bytes());
        assert_eq!(&encoded[11..13], &125_u16.to_le_bytes());
        assert_eq!(&encoded[13..21], &12.5_f64.to_le_bytes());

        assert_eq!(decode_pressure_mutation(&encoded).unwrap(), mutation);
    }
}
