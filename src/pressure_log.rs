use anyhow::{Context, Result, bail, ensure};
use prost::Message;

use crate::pressure::{PRICE_SCALE, PressureFrontierMemory, PressureLevel};

mod wire {
    include!(concat!(
        env!("OUT_DIR"),
        "/polymarket_book_recorder.pressure.v7.rs"
    ));
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecorderPressureMutation {
    /// Complete observation after a continuity gap.
    ObserveSnapshot {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
    /// Absolute level changes on one continuous ordered stream.
    ApplyDelta {
        valid_through_ms: i64,
        changes: Vec<PressureLevel>,
    },
    /// Ordered market evidence with no token-local geometry change.
    Advance { valid_through_ms: i64 },
    /// Complete book replacement on one continuous ordered stream.
    ReplaceContinuous {
        valid_through_ms: i64,
        levels: Vec<PressureLevel>,
    },
}

pub fn encode_pressure_mutation(mutation: &RecorderPressureMutation) -> Result<Vec<u8>> {
    use wire::pressure_mutation::Kind;

    let kind = match mutation {
        RecorderPressureMutation::ObserveSnapshot {
            valid_through_ms,
            levels,
        } => Kind::ObserveSnapshot(wire::ObserveSnapshot {
            valid_through_ms: Some(validate_timestamp(*valid_through_ms)?),
            levels: encode_levels(levels, false)?,
        }),
        RecorderPressureMutation::ApplyDelta {
            valid_through_ms,
            changes,
        } => Kind::ApplyDelta(wire::ApplyDelta {
            valid_through_ms: Some(validate_timestamp(*valid_through_ms)?),
            changes: encode_levels(changes, true)?,
        }),
        RecorderPressureMutation::Advance { valid_through_ms } => {
            Kind::Advance(wire::Advance {
                valid_through_ms: Some(validate_timestamp(*valid_through_ms)?),
            })
        }
        RecorderPressureMutation::ReplaceContinuous {
            valid_through_ms,
            levels,
        } => Kind::ReplaceContinuous(wire::ReplaceContinuous {
            valid_through_ms: Some(validate_timestamp(*valid_through_ms)?),
            levels: encode_levels(levels, false)?,
        }),
    };

    Ok(wire::PressureMutation { kind: Some(kind) }.encode_to_vec())
}

pub fn decode_pressure_mutation(value: &[u8]) -> Result<RecorderPressureMutation> {
    use wire::pressure_mutation::Kind;

    let message =
        wire::PressureMutation::decode(value).context("decoding protobuf pressure mutation")?;
    let kind = message
        .kind
        .ok_or_else(|| anyhow::anyhow!("pressure mutation kind is missing"))?;

    Ok(match kind {
        Kind::ObserveSnapshot(message) => RecorderPressureMutation::ObserveSnapshot {
            valid_through_ms: required_timestamp(
                message.valid_through_ms,
                "observe_snapshot.valid_through_ms",
            )?,
            levels: decode_levels(message.levels, false)?,
        },
        Kind::ApplyDelta(message) => RecorderPressureMutation::ApplyDelta {
            valid_through_ms: required_timestamp(
                message.valid_through_ms,
                "apply_delta.valid_through_ms",
            )?,
            changes: decode_levels(message.changes, true)?,
        },
        Kind::Advance(message) => RecorderPressureMutation::Advance {
            valid_through_ms: required_timestamp(
                message.valid_through_ms,
                "advance.valid_through_ms",
            )?,
        },
        Kind::ReplaceContinuous(message) => RecorderPressureMutation::ReplaceContinuous {
            valid_through_ms: required_timestamp(
                message.valid_through_ms,
                "replace_continuous.valid_through_ms",
            )?,
            levels: decode_levels(message.levels, false)?,
        },
    })
}

pub fn replay_pressure_mutation(
    memory: &mut PressureFrontierMemory,
    mutation: RecorderPressureMutation,
) -> Result<()> {
    match mutation {
        RecorderPressureMutation::ObserveSnapshot {
            valid_through_ms,
            levels,
        } => {
            memory.observe_levels(&levels, valid_through_ms)?;
        }
        RecorderPressureMutation::ApplyDelta {
            valid_through_ms,
            changes,
        } => {
            memory.update_levels(&changes, valid_through_ms)?;
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

fn validate_timestamp(value: i64) -> Result<i64> {
    ensure!(
        value >= 0,
        "pressure mutation timestamp must be non-negative"
    );
    Ok(value)
}

fn required_timestamp(value: Option<i64>, field: &str) -> Result<i64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{field} is missing"))?;
    validate_timestamp(value)
}

fn encode_levels(levels: &[PressureLevel], allow_zero_shares: bool) -> Result<Vec<wire::PressureLevel>> {
    levels
        .iter()
        .map(|level| {
            validate_level(level.price, level.shares, allow_zero_shares)?;
            Ok(wire::PressureLevel {
                price: u32::from(level.price),
                shares: level.shares,
            })
        })
        .collect()
}

fn decode_levels(
    levels: Vec<wire::PressureLevel>,
    allow_zero_shares: bool,
) -> Result<Vec<PressureLevel>> {
    levels
        .into_iter()
        .map(|level| {
            let price = u16::try_from(level.price).context("pressure price does not fit u16")?;
            validate_level(price, level.shares, allow_zero_shares)?;
            Ok(PressureLevel {
                price,
                shares: level.shares,
            })
        })
        .collect()
}

fn validate_level(price: u16, shares: f64, allow_zero_shares: bool) -> Result<()> {
    ensure!(
        price > 0 && price <= PRICE_SCALE,
        "pressure price must be in [1, {PRICE_SCALE}]"
    );
    if !shares.is_finite() || shares < 0.0 || (!allow_zero_shares && shares == 0.0) {
        bail!("invalid pressure shares");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protobuf_mutations_round_trip() {
        for mutation in [
            RecorderPressureMutation::ObserveSnapshot {
                valid_through_ms: 1_000,
                levels: vec![PressureLevel {
                    price: 5_000,
                    shares: 10.0,
                }],
            },
            RecorderPressureMutation::ApplyDelta {
                valid_through_ms: 2_000,
                changes: vec![PressureLevel {
                    price: 5_000,
                    shares: 0.0,
                }],
            },
            RecorderPressureMutation::Advance {
                valid_through_ms: 3_000,
            },
            RecorderPressureMutation::ReplaceContinuous {
                valid_through_ms: 4_000,
                levels: vec![PressureLevel {
                    price: 6_000,
                    shares: 12.0,
                }],
            },
        ] {
            let encoded = encode_pressure_mutation(&mutation).unwrap();
            assert_eq!(decode_pressure_mutation(&encoded).unwrap(), mutation);
        }
    }

    #[test]
    fn missing_timestamp_is_rejected() {
        let encoded = wire::PressureMutation {
            kind: Some(wire::pressure_mutation::Kind::Advance(wire::Advance {
                valid_through_ms: None,
            })),
        }
        .encode_to_vec();

        assert!(decode_pressure_mutation(&encoded).is_err());
    }

    #[test]
    fn impossible_levels_are_rejected() {
        assert!(
            encode_pressure_mutation(&RecorderPressureMutation::ObserveSnapshot {
                valid_through_ms: 1,
                levels: vec![PressureLevel {
                    price: 0,
                    shares: 1.0,
                }],
            })
            .is_err()
        );
        assert!(
            encode_pressure_mutation(&RecorderPressureMutation::ObserveSnapshot {
                valid_through_ms: 1,
                levels: vec![PressureLevel {
                    price: 1,
                    shares: 0.0,
                }],
            })
            .is_err()
        );
    }
}
