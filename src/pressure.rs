use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

pub const PRICE_SCALE: u16 = 10_000;
pub const SNAPSHOT_VERSION: u8 = 6;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrontierLevel {
    pub key: u16,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PressureLevelChange {
    pub price: u16,
    pub shares: f64,
}

/// Opaque v6 persisted pressure state.
///
/// There is deliberately no separately persisted current frontier. The current
/// aggregate level at a price is the difference between adjacent cumulative
/// run volumes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PressureFrontierSnapshot {
    version: u8,
    state: SnapshotState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum SnapshotState {
    Unobserved,
    Observed {
        #[serde(rename = "validThroughMs")]
        valid_through_ms: i64,
        runs: Vec<PressureRun>,
    },
}

/// One price interval in the pressure surface.
///
/// volume is current cumulative resting volume through this price. Historical
/// pressure is a stack of upper edges, high-to-low. The lower edge of a frozen
/// step is therefore derived from the next step, or from volume for the last
/// step; it is intentionally impossible to store a gap, overlap, or zero-width
/// band.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PressureRun {
    price: u16,
    volume: f64,
    frozen_steps: Vec<FrozenStep>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FrozenStep {
    hi_volume: f64,
    valid_through_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
enum MemoryState {
    Unobserved,
    Observed {
        valid_through_ms: i64,
        runs: Vec<PressureRun>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PressureFrontierMemory {
    state: MemoryState,
}

impl Default for PressureFrontierMemory {
    fn default() -> Self {
        Self {
            state: MemoryState::Unobserved,
        }
    }
}

impl PressureFrontierMemory {
    pub fn restore(snapshot: PressureFrontierSnapshot) -> Result<Self> {
        ensure!(
            snapshot.version == SNAPSHOT_VERSION,
            "unsupported pressure frontier snapshot version: {}",
            snapshot.version
        );

        let state = match snapshot.state {
            SnapshotState::Unobserved => MemoryState::Unobserved,
            SnapshotState::Observed {
                valid_through_ms,
                runs,
            } => {
                validate_runs(&runs, valid_through_ms)?;
                MemoryState::Observed {
                    valid_through_ms,
                    runs,
                }
            }
        };

        Ok(Self { state })
    }

    pub fn snapshot(&self) -> PressureFrontierSnapshot {
        let state = match &self.state {
            MemoryState::Unobserved => SnapshotState::Unobserved,
            MemoryState::Observed {
                valid_through_ms,
                runs,
            } => SnapshotState::Observed {
                valid_through_ms: *valid_through_ms,
                runs: runs.clone(),
            },
        };
        PressureFrontierSnapshot {
            version: SNAPSHOT_VERSION,
            state,
        }
    }

    pub fn clear(&mut self) {
        self.state = MemoryState::Unobserved;
    }

    pub fn observe_levels(
        &mut self,
        levels: &[FrontierLevel],
        valid_through_ms: i64,
    ) -> Result<bool> {
        let valid_through_ms = self.normalize_time(valid_through_ms)?;
        let previous_time = self.valid_through_ms();
        let previous_levels = self.current_levels_map()?;
        let next_levels = normalize_levels(levels);

        let deltas = level_deltas(&previous_levels, &next_levels);
        let geometry_changed = !deltas.is_empty();
        let mut runs = self.take_runs();
        apply_deltas(&mut runs, &deltas, previous_time)?;
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };

        Ok(geometry_changed || previous_time != Some(valid_through_ms))
    }

    pub fn update_levels(
        &mut self,
        changes: &[PressureLevelChange],
        valid_through_ms: i64,
    ) -> Result<bool> {
        let valid_through_ms = self.normalize_time(valid_through_ms)?;

        let mut final_by_price = BTreeMap::<u16, f64>::new();
        for change in changes {
            if change.price == 0
                || change.price > PRICE_SCALE
                || !change.shares.is_finite()
                || change.shares < 0.0
            {
                continue;
            }
            final_by_price.insert(change.price, change.shares);
        }

        if final_by_price.is_empty() {
            return Ok(false);
        }

        let previous_time = self.valid_through_ms();
        let previous_levels = self.current_levels_map()?;
        let mut deltas = Vec::new();

        for (price, shares) in final_by_price {
            let previous = previous_levels.get(&price).copied().unwrap_or(0.0);
            let delta = shares - previous;
            if delta != 0.0 {
                deltas.push(PressureLevelDelta { price, delta });
            }
        }

        let geometry_changed = !deltas.is_empty();
        let mut runs = self.take_runs();
        apply_deltas(&mut runs, &deltas, previous_time)?;
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };

        Ok(geometry_changed || previous_time != Some(valid_through_ms))
    }

    pub fn observe_through(&mut self, valid_through_ms: i64) -> Result<bool> {
        let valid_through_ms = self.normalize_time(valid_through_ms)?;
        let previous_time = self.valid_through_ms();
        let runs = self.take_runs();
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };
        Ok(previous_time != Some(valid_through_ms))
    }

    pub fn current_levels(&self) -> Result<Vec<FrontierLevel>> {
        Ok(self
            .current_levels_map()?
            .into_iter()
            .map(|(key, weight)| FrontierLevel { key, weight })
            .collect())
    }

    fn valid_through_ms(&self) -> Option<i64> {
        match &self.state {
            MemoryState::Unobserved => None,
            MemoryState::Observed {
                valid_through_ms, ..
            } => Some(*valid_through_ms),
        }
    }

    fn normalize_time(&self, value: i64) -> Result<i64> {
        ensure!(
            value >= 0,
            "pressure frontier timestamp must be non-negative"
        );
        Ok(self
            .valid_through_ms()
            .map_or(value, |last| value.max(last)))
    }

    fn take_runs(&mut self) -> Vec<PressureRun> {
        match std::mem::replace(&mut self.state, MemoryState::Unobserved) {
            MemoryState::Unobserved => Vec::new(),
            MemoryState::Observed { runs, .. } => runs,
        }
    }

    fn current_levels_map(&self) -> Result<BTreeMap<u16, f64>> {
        let runs = match &self.state {
            MemoryState::Unobserved => return Ok(BTreeMap::new()),
            MemoryState::Observed { runs, .. } => runs,
        };

        current_levels_from_runs(runs)
    }
}

#[derive(Debug, Clone, Copy)]
struct PressureLevelDelta {
    price: u16,
    delta: f64,
}

fn normalize_levels(levels: &[FrontierLevel]) -> BTreeMap<u16, f64> {
    let mut by_price = BTreeMap::<u16, f64>::new();
    for level in levels {
        if level.key == 0
            || level.key > PRICE_SCALE
            || !level.weight.is_finite()
            || level.weight <= 0.0
        {
            continue;
        }
        *by_price.entry(level.key).or_default() += level.weight;
    }
    by_price
}

fn current_levels_from_runs(runs: &[PressureRun]) -> Result<BTreeMap<u16, f64>> {
    let mut result = BTreeMap::new();
    let mut previous_volume = 0.0;

    for run in runs {
        let weight = run.volume - previous_volume;
        if weight < 0.0 && !same_volume(weight, 0.0) {
            bail!(
                "pressure run volume decreased at price {}: previous={} current={}",
                run.price,
                previous_volume,
                run.volume
            );
        }
        if !same_volume(weight, 0.0) {
            result.insert(run.price, weight);
        }
        previous_volume = run.volume;
    }

    Ok(result)
}

fn level_deltas(
    previous: &BTreeMap<u16, f64>,
    next: &BTreeMap<u16, f64>,
) -> Vec<PressureLevelDelta> {
    previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|price| {
            let delta = next.get(&price).copied().unwrap_or(0.0)
                - previous.get(&price).copied().unwrap_or(0.0);
            (delta != 0.0).then_some(PressureLevelDelta { price, delta })
        })
        .collect()
}

fn apply_deltas(
    runs: &mut Vec<PressureRun>,
    deltas: &[PressureLevelDelta],
    previous_valid_through_ms: Option<i64>,
) -> Result<()> {
    let mut actual = deltas
        .iter()
        .copied()
        .filter(|delta| {
            delta.price > 0
                && delta.price <= PRICE_SCALE
                && delta.delta.is_finite()
                && delta.delta != 0.0
        })
        .collect::<Vec<_>>();
    actual.sort_by_key(|delta| delta.price);

    if actual.is_empty() {
        return Ok(());
    }

    for delta in &actual {
        split_at(runs, delta.price);
    }

    let first_changed = actual[0].price;
    let first_run = lower_bound_run(runs, first_changed);
    let mut delta_index = 0;
    let mut cumulative_delta = 0.0;

    for run in runs.iter_mut().skip(first_run) {
        while delta_index < actual.len() && actual[delta_index].price <= run.price {
            cumulative_delta += actual[delta_index].delta;
            delta_index += 1;
        }

        if cumulative_delta == 0.0 {
            continue;
        }

        let next_volume = run.volume + cumulative_delta;
        ensure!(
            next_volume >= 0.0 || same_volume(next_volume, 0.0),
            "pressure delta would make cumulative volume negative at price {}",
            run.price
        );
        let next_volume = if same_volume(next_volume, 0.0) {
            0.0
        } else {
            next_volume
        };

        if same_volume(next_volume, run.volume) {
            continue;
        }

        transition_run(run, next_volume, previous_valid_through_ms)?;
    }

    merge_adjacent_runs(runs);
    Ok(())
}

fn transition_run(
    run: &mut PressureRun,
    next_volume: f64,
    previous_valid_through_ms: Option<i64>,
) -> Result<()> {
    ensure!(
        next_volume.is_finite() && next_volume >= 0.0,
        "pressure volume must be finite and non-negative"
    );

    let old_volume = run.volume;
    if same_volume(old_volume, next_volume) {
        return Ok(());
    }

    if next_volume < old_volume {
        let valid_through_ms = previous_valid_through_ms.ok_or_else(|| {
            anyhow::anyhow!("cannot freeze current pressure before it has a validity timestamp")
        })?;

        let extends_last = run
            .frozen_steps
            .last()
            .is_some_and(|step| step.valid_through_ms == valid_through_ms);

        if !extends_last {
            run.frozen_steps.push(FrozenStep {
                hi_volume: old_volume,
                valid_through_ms,
            });
        }
    } else {
        while run.frozen_steps.last().is_some_and(|step| {
            step.hi_volume <= next_volume || same_volume(step.hi_volume, next_volume)
        }) {
            run.frozen_steps.pop();
        }
    }

    run.volume = next_volume;
    Ok(())
}

fn split_at(runs: &mut Vec<PressureRun>, price: u16) {
    if price == 0 || price > PRICE_SCALE {
        return;
    }

    let index = lower_bound_run(runs, price);
    if runs.get(index).is_some_and(|run| run.price == price) {
        return;
    }

    let source = index.checked_sub(1).and_then(|i| runs.get(i)).cloned();
    runs.insert(
        index,
        PressureRun {
            price,
            volume: source.as_ref().map_or(0.0, |run| run.volume),
            frozen_steps: source.map_or_else(Vec::new, |run| run.frozen_steps),
        },
    );
}

fn merge_adjacent_runs(runs: &mut Vec<PressureRun>) {
    if runs.is_empty() {
        return;
    }

    let mut merged = Vec::with_capacity(runs.len());
    for run in runs.drain(..) {
        if merged.is_empty() && empty_state(&run) {
            continue;
        }
        if merged
            .last()
            .is_some_and(|previous| states_equal(previous, &run))
        {
            continue;
        }
        merged.push(run);
    }
    *runs = merged;
}

fn lower_bound_run(runs: &[PressureRun], price: u16) -> usize {
    runs.partition_point(|run| run.price < price)
}

fn states_equal(a: &PressureRun, b: &PressureRun) -> bool {
    same_volume(a.volume, b.volume)
        && a.frozen_steps.len() == b.frozen_steps.len()
        && a.frozen_steps.iter().zip(&b.frozen_steps).all(|(a, b)| {
            same_volume(a.hi_volume, b.hi_volume) && a.valid_through_ms == b.valid_through_ms
        })
}

fn empty_state(run: &PressureRun) -> bool {
    same_volume(run.volume, 0.0) && run.frozen_steps.is_empty()
}

fn validate_runs(runs: &[PressureRun], current_valid_through_ms: i64) -> Result<()> {
    ensure!(
        current_valid_through_ms >= 0,
        "pressure current valid-through must be non-negative"
    );

    let mut previous_price = 0;
    let mut previous_volume = 0.0;

    for (run_index, run) in runs.iter().enumerate() {
        ensure!(
            run.price > previous_price && run.price <= PRICE_SCALE,
            "pressure run prices must be strictly increasing non-zero boundaries"
        );
        ensure!(
            run.volume.is_finite() && run.volume >= 0.0,
            "run[{run_index}].volume must be finite and non-negative"
        );
        ensure!(
            run.volume >= previous_volume || same_volume(run.volume, previous_volume),
            "edge pressure must be non-decreasing in price"
        );

        let mut previous_hi: Option<f64> = None;
        let mut previous_time: Option<i64> = None;
        for (step_index, step) in run.frozen_steps.iter().enumerate() {
            ensure!(
                step.hi_volume.is_finite() && step.hi_volume >= 0.0,
                "run[{run_index}].frozenSteps[{step_index}].hiVolume must be finite and non-negative"
            );
            ensure!(
                step.valid_through_ms >= 0 && step.valid_through_ms <= current_valid_through_ms,
                "run[{run_index}].frozenSteps[{step_index}] has invalid valid-through timestamp"
            );
            ensure!(
                step.hi_volume > run.volume && !same_volume(step.hi_volume, run.volume),
                "run[{run_index}].frozenSteps[{step_index}] must sit above current volume"
            );
            if let Some(previous_hi) = previous_hi {
                ensure!(
                    step.hi_volume < previous_hi && !same_volume(step.hi_volume, previous_hi),
                    "run[{run_index}].frozenSteps must be strictly high-to-low"
                );
            }
            ensure!(
                previous_time != Some(step.valid_through_ms),
                "run[{run_index}].adjacent frozen steps with the same timestamp must be merged"
            );

            previous_hi = Some(step.hi_volume);
            previous_time = Some(step.valid_through_ms);
        }

        previous_price = run.price;
        previous_volume = run.volume;
    }

    ensure!(
        !runs.first().is_some_and(empty_state),
        "pressure field must not store a leading empty run"
    );
    Ok(())
}

fn same_volume(a: f64, b: f64) -> bool {
    (a - b).abs() <= volume_tolerance(a, b)
}

fn volume_tolerance(a: f64, b: f64) -> f64 {
    32.0 * f64::EPSILON * 1.0_f64.max(a.abs()).max(b.abs())
}

pub(crate) fn migrate_v5_checkpoint_json(json: &str) -> Result<PressureFrontierMemory> {
    let legacy: LegacySnapshotV5 =
        serde_json::from_str(json).map_err(|error| anyhow::anyhow!(error))?;
    ensure!(legacy.version == 5, "expected pressure snapshot version 5");
    ensure!(
        legacy.field.max_price == PRICE_SCALE,
        "unsupported v5 pressure max price: {}",
        legacy.field.max_price
    );

    let current = normalize_legacy_levels(&legacy.current)?;
    let mut level_iter = current.iter().peekable();
    let mut cumulative = 0.0;
    let mut previous_price = 0;
    let mut runs = Vec::with_capacity(legacy.field.runs.len());

    for (run_index, run) in legacy.field.runs.into_iter().enumerate() {
        ensure!(
            run.price > previous_price && run.price <= PRICE_SCALE,
            "v5 run[{run_index}] has invalid price boundary {}",
            run.price
        );

        while let Some((&price, &weight)) = level_iter.peek().copied() {
            if price > run.price {
                break;
            }
            cumulative += weight;
            level_iter.next();
        }

        let mut frozen_steps = Vec::<FrozenStep>::new();
        let mut previous_hi: Option<f64> = None;

        for (step_index, band) in run.frozen_bands.into_iter().enumerate() {
            ensure!(
                band.lo_volume.is_finite()
                    && band.hi_volume.is_finite()
                    && band.valid_through_ms.is_finite()
                    && band.lo_volume >= 0.0
                    && band.hi_volume >= band.lo_volume,
                "v5 run[{run_index}].frozenBands[{step_index}] is malformed"
            );

            if same_volume(band.lo_volume, band.hi_volume) {
                continue;
            }

            if let Some(previous_hi) = previous_hi {
                ensure!(
                    band.hi_volume < previous_hi && !same_volume(band.hi_volume, previous_hi),
                    "v5 run[{run_index}] frozen bands are not high-to-low"
                );
            }

            let valid_through_ms = legacy_timestamp_ms(band.valid_through_ms)?;
            if frozen_steps
                .last()
                .is_some_and(|step| step.valid_through_ms == valid_through_ms)
            {
                continue;
            }

            frozen_steps.push(FrozenStep {
                hi_volume: band.hi_volume,
                valid_through_ms,
            });
            previous_hi = Some(band.hi_volume);
        }

        while frozen_steps.last().is_some_and(|step| {
            step.hi_volume <= cumulative || same_volume(step.hi_volume, cumulative)
        }) {
            frozen_steps.pop();
        }

        runs.push(PressureRun {
            price: run.price,
            volume: cumulative,
            frozen_steps,
        });
        previous_price = run.price;
    }

    if let Some((&last_level_price, _)) = level_iter.peek().copied() {
        bail!("v5 pressure field is missing current frontier boundary at price {last_level_price}");
    }

    merge_adjacent_runs(&mut runs);

    let state = match legacy.field.current_valid_through_ms {
        Some(value) => {
            let valid_through_ms = legacy_timestamp_ms(value)?;
            validate_runs(&runs, valid_through_ms)?;
            MemoryState::Observed {
                valid_through_ms,
                runs,
            }
        }
        None => {
            ensure!(
                runs.is_empty() && current.is_empty(),
                "v5 unobserved pressure snapshot contains state"
            );
            MemoryState::Unobserved
        }
    };

    Ok(PressureFrontierMemory { state })
}

fn normalize_legacy_levels(levels: &[LegacyFrontierLevelV5]) -> Result<BTreeMap<u16, f64>> {
    let mut by_price = BTreeMap::new();
    for (index, level) in levels.iter().enumerate() {
        ensure!(
            level.key > 0 && level.key <= PRICE_SCALE,
            "v5 current[{index}] has invalid price {}",
            level.key
        );
        ensure!(
            level.weight.is_finite() && level.weight > 0.0,
            "v5 current[{index}] has invalid weight {}",
            level.weight
        );
        ensure!(
            !by_price.contains_key(&level.key),
            "v5 current contains duplicate price {}",
            level.key
        );
        by_price.insert(level.key, level.weight);
    }
    Ok(by_price)
}

fn legacy_timestamp_ms(value: f64) -> Result<i64> {
    ensure!(
        value.is_finite() && value >= 0.0 && value <= i64::MAX as f64,
        "v5 pressure timestamp is invalid: {value}"
    );
    Ok(value.trunc() as i64)
}

#[derive(Debug, Deserialize)]
struct LegacySnapshotV5 {
    version: u8,
    current: Vec<LegacyFrontierLevelV5>,
    field: LegacyFieldV5,
}

#[derive(Debug, Deserialize)]
struct LegacyFrontierLevelV5 {
    key: u16,
    weight: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyFieldV5 {
    max_price: u16,
    current_valid_through_ms: Option<f64>,
    runs: Vec<LegacyRunV5>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyRunV5 {
    price: u16,
    #[allow(dead_code)]
    volume: f64,
    frozen_bands: Vec<LegacyBandV5>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyBandV5 {
    lo_volume: f64,
    hi_volume: f64,
    valid_through_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falling_and_rising_frontier_is_a_step_stack() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[FrontierLevel {
                    key: 5_000,
                    weight: 10.0,
                }],
                1_000,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 4.0,
                }],
                2_000,
            )
            .unwrap();

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(runs[0].volume, 4.0);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 1_000,
            }]
        );

        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 8.0,
                }],
                3_000,
            )
            .unwrap();

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(runs[0].volume, 8.0);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 1_000,
            }]
        );
    }

    #[test]
    fn successive_decreases_preserve_each_observation_time() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[FrontierLevel {
                    key: 5_000,
                    weight: 10.0,
                }],
                1_000,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 8.0,
                }],
                2_000,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 6.0,
                }],
                2_000,
            )
            .unwrap();

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(
            runs[0].frozen_steps,
            vec![
                FrozenStep {
                    hi_volume: 10.0,
                    valid_through_ms: 1_000,
                },
                FrozenStep {
                    hi_volume: 8.0,
                    valid_through_ms: 2_000,
                },
            ]
        );
        assert_eq!(runs[0].volume, 6.0);
    }

    #[test]
    fn current_levels_are_derived_from_run_volume_differences() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[
                    FrontierLevel {
                        key: 1_000,
                        weight: 20.0,
                    },
                    FrontierLevel {
                        key: 3_000,
                        weight: 35.0,
                    },
                ],
                1_000,
            )
            .unwrap();

        assert_eq!(
            memory.current_levels().unwrap(),
            vec![
                FrontierLevel {
                    key: 1_000,
                    weight: 20.0,
                },
                FrontierLevel {
                    key: 3_000,
                    weight: 35.0,
                },
            ]
        );
    }

    #[test]
    fn v5_migration_drops_zero_width_band_and_recomputes_current_volume() {
        let json = r#"{
          "version": 5,
          "current": [{"key": 5000, "weight": 4.0}],
          "field": {
            "maxPrice": 10000,
            "currentValidThroughMs": 3000,
            "runs": [{
              "price": 5000,
              "volume": 4.000000000000001,
              "frozenBands": [
                {"loVolume": 10.0, "hiVolume": 10.0, "validThroughMs": 1000},
                {"loVolume": 4.0, "hiVolume": 10.0, "validThroughMs": 2000}
              ]
            }]
          }
        }"#;

        let memory = migrate_v5_checkpoint_json(json).unwrap();
        assert_eq!(
            memory.current_levels().unwrap(),
            vec![FrontierLevel {
                key: 5_000,
                weight: 4.0,
            }]
        );

        let SnapshotState::Observed {
            valid_through_ms,
            runs,
        } = memory.snapshot().state
        else {
            panic!("expected observed pressure");
        };
        assert_eq!(valid_through_ms, 3_000);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 2_000,
            }]
        );
    }

    #[test]
    fn snapshot_round_trip_cannot_encode_band_lower_edges() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[FrontierLevel {
                    key: 5_000,
                    weight: 10.0,
                }],
                1_000,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 4.0,
                }],
                2_000,
            )
            .unwrap();

        let value = serde_json::to_value(memory.snapshot()).unwrap();
        let text = value.to_string();
        assert!(!text.contains("loVolume"));
        assert!(!text.contains("\"current\""));

        let snapshot: PressureFrontierSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(PressureFrontierMemory::restore(snapshot).unwrap(), memory);
    }
}
