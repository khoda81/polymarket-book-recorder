use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub const PRICE_SCALE: u16 = 10_000;
pub const SNAPSHOT_VERSION: u8 = 7;
const LEGACY_SNAPSHOT_VERSION: u8 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PressureLevel {
    pub price: u16,
    pub shares: f64,
}

/// Opaque v6 persisted pressure state.
///
/// Current order-book levels live exactly once, as per-price shares in the
/// runs. Cumulative current pressure is derived by prefix-summing those shares.
/// Historical lower edges are likewise derived from the current cumulative
/// pressure / next frozen step and are never persisted.
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
    ResolvedUnbounded {
        #[serde(rename = "resolvedAtMs")]
        resolved_at_ms: i64,
    },
}

/// One explicit price boundary in the pressure surface.
///
/// shares is the exact current aggregate resting level at this price. A
/// history-only price boundary therefore has shares == 0. Current cumulative
/// pressure through a run is the prefix sum of all run shares.
///
/// frozen_steps stores historical upper edges, high-to-low. Its interval lower
/// edges are implied by the next step, or by current cumulative pressure for
/// the final step. Gaps, overlaps, and zero-width stored bands are therefore
/// not representable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PressureRun {
    price: u16,
    shares: f64,
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
    ResolvedUnbounded {
        resolved_at_ms: i64,
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
            snapshot.version == LEGACY_SNAPSHOT_VERSION || snapshot.version == SNAPSHOT_VERSION,
            "unsupported pressure frontier snapshot version: {}",
            snapshot.version
        );
        let snapshot_version = snapshot.version;

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
            SnapshotState::ResolvedUnbounded { resolved_at_ms } => {
                ensure!(
                    snapshot_version == SNAPSHOT_VERSION,
                    "resolved-unbounded pressure requires snapshot version {SNAPSHOT_VERSION}"
                );
                ensure!(
                    resolved_at_ms >= 0,
                    "pressure unbounded resolution timestamp must be non-negative"
                );
                MemoryState::ResolvedUnbounded { resolved_at_ms }
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
            MemoryState::ResolvedUnbounded { resolved_at_ms } => {
                SnapshotState::ResolvedUnbounded {
                    resolved_at_ms: *resolved_at_ms,
                }
            }
        };
        PressureFrontierSnapshot {
            version: SNAPSHOT_VERSION,
            state,
        }
    }

    pub(crate) fn clear_legacy(&mut self) {
        self.state = MemoryState::Unobserved;
    }

    /// Install a complete observation without claiming continuity from the
    /// previous current frontier to this observation.
    pub fn observe_levels(
        &mut self,
        levels: &[PressureLevel],
        valid_through_ms: i64,
    ) -> Result<bool> {
        self.ensure_mutable()?;
        let valid_through_ms = self.require_monotonic_time(valid_through_ms)?;
        let previous_time = self.valid_through_ms();
        let previous = self.current_levels_map();
        let next = normalize_levels(levels);
        let changes = changed_levels(&previous, &next);
        let geometry_changed = !changes.is_empty();

        let mut runs = self.take_runs();
        apply_level_changes(&mut runs, &changes, previous_time)?;
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };

        Ok(geometry_changed || previous_time != Some(valid_through_ms))
    }

    /// Apply an ordered-stream delta. The previous frontier is proven valid
    /// through the event watermark before the mutation takes effect.
    pub fn update_levels(
        &mut self,
        changes: &[PressureLevel],
        valid_through_ms: i64,
    ) -> Result<bool> {
        self.ensure_mutable()?;
        let valid_through_ms = self.require_monotonic_time(valid_through_ms)?;

        let previous = self.current_levels_map();
        let mut changed = BTreeMap::<u16, f64>::new();

        for change in changes {
            if change.price == 0
                || change.price > PRICE_SCALE
                || !change.shares.is_finite()
                || change.shares < 0.0
            {
                continue;
            }

            let old_shares = previous.get(&change.price).copied().unwrap_or(0.0);
            if same_volume(change.shares, old_shares) {
                changed.remove(&change.price);
            } else {
                changed.insert(change.price, change.shares);
            }
        }

        if changed.is_empty() {
            return self.observe_through(valid_through_ms);
        }

        let mut runs = self.take_runs();
        apply_level_changes(&mut runs, &changed, Some(valid_through_ms))?;
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };

        Ok(true)
    }

    /// Replace the complete frontier on a continuous ordered stream.
    pub fn replace_continuous(
        &mut self,
        levels: &[PressureLevel],
        valid_through_ms: i64,
    ) -> Result<bool> {
        let advanced = self.observe_through(valid_through_ms)?;
        let replaced = self.observe_levels(levels, valid_through_ms)?;
        Ok(advanced || replaced)
    }

    pub fn observe_through(&mut self, valid_through_ms: i64) -> Result<bool> {
        if matches!(self.state, MemoryState::ResolvedUnbounded { .. }) {
            return Ok(false);
        }
        let valid_through_ms = self.require_monotonic_time(valid_through_ms)?;
        let previous_time = self.valid_through_ms();
        let runs = self.take_runs();
        self.state = MemoryState::Observed {
            valid_through_ms,
            runs,
        };
        Ok(previous_time != Some(valid_through_ms))
    }

    /// Resolution to this token dominates every finite historical offer.
    pub fn resolve_unbounded(&mut self, resolved_at_ms: i64) -> Result<bool> {
        if matches!(self.state, MemoryState::ResolvedUnbounded { .. }) {
            return Ok(false);
        }
        let resolved_at_ms = self.require_monotonic_time(resolved_at_ms)?;
        self.state = MemoryState::ResolvedUnbounded { resolved_at_ms };
        Ok(true)
    }

    /// Resolution away from this token removes only future/current liquidity.
    /// Historical maxima stay frozen at the resolution watermark.
    pub fn resolve_zero_future(&mut self, resolved_at_ms: i64) -> Result<bool> {
        if matches!(self.state, MemoryState::ResolvedUnbounded { .. }) {
            return Ok(false);
        }
        self.replace_continuous(&[], resolved_at_ms)
    }

    pub fn current_levels(&self) -> Vec<PressureLevel> {
        self.current_levels_map()
            .into_iter()
            .map(|(price, shares)| PressureLevel { price, shares })
            .collect()
    }

    fn current_levels_map(&self) -> BTreeMap<u16, f64> {
        let runs = match &self.state {
            MemoryState::Unobserved | MemoryState::ResolvedUnbounded { .. } => {
                return BTreeMap::new();
            }
            MemoryState::Observed { runs, .. } => runs,
        };

        runs.iter()
            .filter(|run| run.shares > 0.0)
            .map(|run| (run.price, run.shares))
            .collect()
    }

    pub fn valid_through_ms(&self) -> Option<i64> {
        match &self.state {
            MemoryState::Unobserved => None,
            MemoryState::Observed {
                valid_through_ms, ..
            } => Some(*valid_through_ms),
            MemoryState::ResolvedUnbounded { resolved_at_ms } => Some(*resolved_at_ms),
        }
    }

    pub fn is_resolved_unbounded(&self) -> bool {
        matches!(self.state, MemoryState::ResolvedUnbounded { .. })
    }

    fn ensure_mutable(&self) -> Result<()> {
        ensure!(
            !matches!(self.state, MemoryState::ResolvedUnbounded { .. }),
            "resolved unbounded pressure is terminal"
        );
        Ok(())
    }

    fn require_monotonic_time(&self, value: i64) -> Result<i64> {
        ensure!(
            value >= 0,
            "pressure frontier timestamp must be non-negative"
        );
        if let Some(last) = self.valid_through_ms() {
            ensure!(
                value >= last,
                "pressure frontier timestamp moved backward: {value} < {last}"
            );
        }
        Ok(value)
    }

    fn take_runs(&mut self) -> Vec<PressureRun> {
        match std::mem::replace(&mut self.state, MemoryState::Unobserved) {
            MemoryState::Unobserved => Vec::new(),
            MemoryState::Observed { runs, .. } => runs,
            MemoryState::ResolvedUnbounded { .. } => {
                unreachable!("terminal pressure cannot expose mutable runs")
            }
        }
    }
}

fn normalize_levels(levels: &[PressureLevel]) -> BTreeMap<u16, f64> {
    let mut by_price = BTreeMap::<u16, f64>::new();
    for level in levels {
        if level.price == 0
            || level.price > PRICE_SCALE
            || !level.shares.is_finite()
            || level.shares <= 0.0
        {
            continue;
        }
        *by_price.entry(level.price).or_default() += level.shares;
    }
    by_price
}

fn changed_levels(previous: &BTreeMap<u16, f64>, next: &BTreeMap<u16, f64>) -> BTreeMap<u16, f64> {
    previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|price| {
            let previous = previous.get(&price).copied().unwrap_or(0.0);
            let next = next.get(&price).copied().unwrap_or(0.0);
            (!same_volume(previous, next)).then_some((price, next))
        })
        .collect()
}

fn apply_level_changes(
    runs: &mut Vec<PressureRun>,
    changes: &BTreeMap<u16, f64>,
    previous_valid_through_ms: Option<i64>,
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    for (&price, &shares) in changes {
        ensure!(
            price > 0 && price <= PRICE_SCALE && shares.is_finite() && shares >= 0.0,
            "invalid pressure level change"
        );
        split_at(runs, price);
    }

    let mut old_volume = 0.0;
    let mut next_volume = 0.0;

    for run in runs.iter_mut() {
        old_volume += run.shares;

        if let Some(&shares) = changes.get(&run.price) {
            run.shares = shares;
        }
        next_volume += run.shares;

        if same_volume(old_volume, next_volume) {
            continue;
        }

        transition_steps(
            &mut run.frozen_steps,
            old_volume,
            next_volume,
            previous_valid_through_ms,
        )?;
    }

    merge_adjacent_runs(runs);
    Ok(())
}

fn transition_steps(
    frozen_steps: &mut Vec<FrozenStep>,
    old_volume: f64,
    next_volume: f64,
    previous_valid_through_ms: Option<i64>,
) -> Result<()> {
    ensure!(
        old_volume.is_finite()
            && old_volume >= 0.0
            && next_volume.is_finite()
            && next_volume >= 0.0,
        "pressure volume must be finite and non-negative"
    );

    if same_volume(old_volume, next_volume) {
        return Ok(());
    }

    if next_volume < old_volume {
        let valid_through_ms = previous_valid_through_ms.ok_or_else(|| {
            anyhow::anyhow!("cannot freeze current pressure before it has a validity timestamp")
        })?;

        let extends_last = frozen_steps
            .last()
            .is_some_and(|step| step.valid_through_ms == valid_through_ms);

        if !extends_last {
            frozen_steps.push(FrozenStep {
                hi_volume: old_volume,
                valid_through_ms,
            });
        }
    } else {
        while frozen_steps.last().is_some_and(|step| {
            step.hi_volume <= next_volume || same_volume(step.hi_volume, next_volume)
        }) {
            frozen_steps.pop();
        }
    }

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

    let frozen_steps = index
        .checked_sub(1)
        .and_then(|i| runs.get(i))
        .map_or_else(Vec::new, |run| run.frozen_steps.clone());

    runs.insert(
        index,
        PressureRun {
            price,
            shares: 0.0,
            frozen_steps,
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

        let redundant = run.shares == 0.0
            && merged
                .last()
                .is_some_and(|previous| frozen_steps_equal(previous, &run));
        if redundant {
            continue;
        }

        merged.push(run);
    }
    *runs = merged;
}

fn lower_bound_run(runs: &[PressureRun], price: u16) -> usize {
    runs.partition_point(|run| run.price < price)
}

fn frozen_steps_equal(a: &PressureRun, b: &PressureRun) -> bool {
    a.frozen_steps.len() == b.frozen_steps.len()
        && a.frozen_steps.iter().zip(&b.frozen_steps).all(|(a, b)| {
            same_volume(a.hi_volume, b.hi_volume) && a.valid_through_ms == b.valid_through_ms
        })
}

fn empty_state(run: &PressureRun) -> bool {
    run.shares == 0.0 && run.frozen_steps.is_empty()
}

fn validate_runs(runs: &[PressureRun], current_valid_through_ms: i64) -> Result<()> {
    ensure!(
        current_valid_through_ms >= 0,
        "pressure current valid-through must be non-negative"
    );

    let mut previous_price = 0;
    let mut current_volume = 0.0;
    let mut previous_run: Option<&PressureRun> = None;

    for (run_index, run) in runs.iter().enumerate() {
        ensure!(
            run.price > previous_price && run.price <= PRICE_SCALE,
            "pressure run prices must be strictly increasing non-zero boundaries"
        );
        ensure!(
            run.shares.is_finite() && run.shares >= 0.0,
            "run[{run_index}].shares must be finite and non-negative"
        );

        if let Some(previous) = previous_run {
            ensure!(
                run.shares > 0.0 || !frozen_steps_equal(previous, run),
                "run[{run_index}] is a redundant zero-share price boundary"
            );
        } else {
            ensure!(
                !empty_state(run),
                "pressure field must not store a leading empty run"
            );
        }

        current_volume += run.shares;
        ensure!(
            current_volume.is_finite(),
            "run[{run_index}] cumulative current volume is not finite"
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
                step.hi_volume > current_volume && !same_volume(step.hi_volume, current_volume),
                "run[{run_index}].frozenSteps[{step_index}] must sit above current volume"
            );
            if let Some(previous_hi) = previous_hi {
                ensure!(
                    step.hi_volume < previous_hi && !same_volume(step.hi_volume, previous_hi),
                    "run[{run_index}].frozenSteps must be strictly high-to-low"
                );
            }
            if let Some(previous_time) = previous_time {
                ensure!(
                    step.valid_through_ms > previous_time,
                    "run[{run_index}].frozenSteps timestamps must increase from high to low"
                );
            }

            previous_hi = Some(step.hi_volume);
            previous_time = Some(step.valid_through_ms);
        }

        previous_price = run.price;
        previous_run = Some(run);
    }

    Ok(())
}

fn same_volume(a: f64, b: f64) -> bool {
    (a - b).abs() <= volume_tolerance(a, b)
}

fn volume_tolerance(a: f64, b: f64) -> f64 {
    32.0 * f64::EPSILON * 1.0_f64.max(a.abs()).max(b.abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falling_and_rising_frontier_is_a_step_stack() {
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
            .update_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 4.0,
                }],
                2_000,
            )
            .unwrap();

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(runs[0].shares, 4.0);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 2_000,
            }]
        );

        memory
            .update_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 8.0,
                }],
                3_000,
            )
            .unwrap();

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(runs[0].shares, 8.0);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 2_000,
            }]
        );
    }

    #[test]
    fn successive_decreases_preserve_each_observation_time() {
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
            .update_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 8.0,
                }],
                2_000,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevel {
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
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 2_000,
            }]
        );
        assert_eq!(runs[0].shares, 6.0);
    }

    #[test]
    fn discontinuous_snapshot_does_not_bridge_the_gap() {
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
            .observe_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 4.0,
                }],
                5_000,
            )
            .unwrap();

        let SnapshotState::Observed {
            valid_through_ms,
            runs,
        } = memory.snapshot().state
        else {
            panic!("expected observed pressure");
        };
        assert_eq!(valid_through_ms, 5_000);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 1_000,
            }]
        );
    }

    #[test]
    fn watermarks_cannot_move_backward() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[PressureLevel {
                    price: 5_000,
                    shares: 10.0,
                }],
                2_000,
            )
            .unwrap();

        assert!(memory.observe_through(1_999).is_err());
        assert!(
            memory
                .update_levels(
                    &[PressureLevel {
                        price: 5_000,
                        shares: 4.0,
                    }],
                    1_999,
                )
                .is_err()
        );
    }

    #[test]
    fn winner_resolution_discards_dominated_history() {
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
        memory.resolve_unbounded(2_000).unwrap();

        assert!(memory.is_resolved_unbounded());
        assert!(memory.current_levels().is_empty());
        assert_eq!(
            memory.snapshot().state,
            SnapshotState::ResolvedUnbounded {
                resolved_at_ms: 2_000,
            }
        );
        assert_eq!(
            PressureFrontierMemory::restore(memory.snapshot()).unwrap(),
            memory
        );
    }

    #[test]
    fn loser_resolution_freezes_current_liquidity_at_resolution() {
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
        memory.resolve_zero_future(2_000).unwrap();

        let SnapshotState::Observed {
            valid_through_ms,
            runs,
        } = memory.snapshot().state
        else {
            panic!("expected historical loser pressure");
        };
        assert_eq!(valid_through_ms, 2_000);
        assert_eq!(runs[0].shares, 0.0);
        assert_eq!(
            runs[0].frozen_steps,
            vec![FrozenStep {
                hi_volume: 10.0,
                valid_through_ms: 2_000,
            }]
        );
    }

    #[test]
    fn current_levels_are_stored_exactly_once_as_run_shares() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[
                    PressureLevel {
                        price: 1_000,
                        shares: 20.0,
                    },
                    PressureLevel {
                        price: 3_000,
                        shares: 35.0,
                    },
                ],
                1_000,
            )
            .unwrap();

        assert_eq!(
            memory.current_levels(),
            vec![
                PressureLevel {
                    price: 1_000,
                    shares: 20.0,
                },
                PressureLevel {
                    price: 3_000,
                    shares: 35.0,
                },
            ]
        );

        let SnapshotState::Observed { runs, .. } = memory.snapshot().state else {
            panic!("expected observed pressure");
        };
        assert_eq!(runs[0].shares, 20.0);
        assert_eq!(runs[1].shares, 35.0);
    }

    #[test]
    fn randomized_updates_preserve_exact_current_levels() {
        let mut memory = PressureFrontierMemory::default();
        let mut reference = BTreeMap::<u16, f64>::new();
        let mut seed = 0x5eed_cafe_u64;

        for step in 1..=2_000_i64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let price = (((seed >> 24) % 100) as u16 + 1) * 100;

            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let shares = if seed.is_multiple_of(7) {
                0.0
            } else {
                ((seed >> 20) % 10_000) as f64 / 10.0 + 0.1
            };

            if shares == 0.0 {
                reference.remove(&price);
            } else {
                reference.insert(price, shares);
            }

            memory
                .update_levels(&[PressureLevel { price, shares }], step * 10)
                .unwrap();

            let actual = memory
                .current_levels()
                .into_iter()
                .map(|level| (level.price, level.shares))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual, reference);

            if step % 100 == 0 {
                let restored = PressureFrontierMemory::restore(memory.snapshot()).unwrap();
                assert_eq!(restored, memory);
            }
        }
    }

    #[test]
    fn snapshot_round_trip_cannot_encode_redundant_current_or_lower_edges() {
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
            .update_levels(
                &[PressureLevel {
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
        assert_eq!(value["state"]["runs"][0]["shares"], 4.0);

        let snapshot: PressureFrontierSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(PressureFrontierMemory::restore(snapshot).unwrap(), memory);
    }
}
