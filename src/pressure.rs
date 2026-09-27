use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

pub const PRICE_SCALE: u16 = 10_000;
pub const SNAPSHOT_VERSION: u8 = 5;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PressureBand {
    pub lo_volume: f64,
    pub hi_volume: f64,
    pub valid_through_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PressureRun {
    pub price: u16,
    pub volume: f64,
    pub frozen_bands: Vec<PressureBand>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PressureFieldSnapshot {
    pub max_price: u16,
    pub current_valid_through_ms: Option<f64>,
    pub runs: Vec<PressureRun>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PressureFrontierSnapshot {
    pub version: u8,
    pub current: Vec<FrontierLevel>,
    pub field: PressureFieldSnapshot,
}

#[derive(Debug, Clone)]
struct MaterializedPressureField {
    max_price: u16,
    current_valid_through_ms: Option<f64>,
    runs: Vec<PressureRun>,
}

impl Default for MaterializedPressureField {
    fn default() -> Self {
        Self {
            max_price: PRICE_SCALE,
            current_valid_through_ms: None,
            runs: Vec::new(),
        }
    }
}

impl MaterializedPressureField {
    fn from_snapshot(snapshot: PressureFieldSnapshot) -> Result<Self> {
        validate_field_snapshot(&snapshot)?;
        Ok(Self {
            max_price: snapshot.max_price,
            current_valid_through_ms: snapshot.current_valid_through_ms,
            runs: snapshot.runs,
        })
    }

    fn snapshot(&self) -> PressureFieldSnapshot {
        PressureFieldSnapshot {
            max_price: self.max_price,
            current_valid_through_ms: self.current_valid_through_ms,
            runs: self.runs.clone(),
        }
    }

    fn clear(&mut self) {
        self.runs.clear();
        self.current_valid_through_ms = None;
    }

    fn observe_current(&mut self, valid_through_ms: f64) -> Result<()> {
        ensure!(
            valid_through_ms.is_finite(),
            "pressure observation timestamp must be finite"
        );
        self.current_valid_through_ms = Some(valid_through_ms);
        Ok(())
    }

    fn price_boundaries(&self) -> impl Iterator<Item = u16> + '_ {
        self.runs.iter().map(|run| run.price)
    }

    fn apply_deltas(
        &mut self,
        changed_prices: &[u16],
        next_frontier: &BTreeMap<u16, f64>,
    ) -> Result<()> {
        let mut actual = changed_prices
            .iter()
            .copied()
            .filter(|price| *price > 0 && *price <= self.max_price)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        actual.dedup();
        if actual.is_empty() {
            return Ok(());
        }

        for price in &actual {
            self.split_at(*price);
        }

        let first_changed = actual[0];
        let first_run = lower_bound_run(&self.runs, first_changed);

        let mut levels = next_frontier.iter().peekable();
        let mut cumulative = 0.0;
        for (index, run) in self.runs.iter_mut().enumerate() {
            while let Some((&price, &weight)) = levels.peek().copied() {
                if price > run.price {
                    break;
                }
                cumulative += weight;
                levels.next();
            }

            if index < first_run || same_volume(cumulative, run.volume) {
                continue;
            }

            transition_run(run, cumulative, self.current_valid_through_ms)?;
        }

        self.merge_adjacent_runs();
        Ok(())
    }

    fn validate_against_frontier(&self, frontier: &BTreeMap<u16, f64>) -> Result<()> {
        let mut levels = frontier.iter().peekable();
        let mut cumulative = 0.0;

        for run in &self.runs {
            while let Some((&price, &weight)) = levels.peek().copied() {
                if price > run.price {
                    break;
                }
                cumulative += weight;
                levels.next();
            }
            ensure!(
                same_volume(run.volume, cumulative),
                "materialized pressure field does not match current frontier"
            );
        }
        Ok(())
    }

    fn split_at(&mut self, price: u16) {
        if price == 0 || price > self.max_price {
            return;
        }

        let index = lower_bound_run(&self.runs, price);
        if self.runs.get(index).is_some_and(|run| run.price == price) {
            return;
        }

        let source = index.checked_sub(1).and_then(|i| self.runs.get(i)).cloned();
        self.runs.insert(
            index,
            PressureRun {
                price,
                volume: source.as_ref().map_or(0.0, |run| run.volume),
                frozen_bands: source.map_or_else(Vec::new, |run| run.frozen_bands),
            },
        );
    }

    fn merge_adjacent_runs(&mut self) {
        if self.runs.is_empty() {
            return;
        }

        let mut merged = Vec::with_capacity(self.runs.len());
        for run in self.runs.drain(..) {
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
        self.runs = merged;
    }
}

#[derive(Debug, Clone, Default)]
pub struct PressureFrontierMemory {
    current: BTreeMap<u16, f64>,
    field: MaterializedPressureField,
    last_update_ms: Option<f64>,
}

impl PressureFrontierMemory {
    pub fn restore(mut snapshot: PressureFrontierSnapshot) -> Result<Self> {
        canonicalize_snapshot(&mut snapshot);
        validate_snapshot(&snapshot)?;

        let current = snapshot
            .current
            .iter()
            .map(|level| (level.key, level.weight))
            .collect::<BTreeMap<_, _>>();
        let field = MaterializedPressureField::from_snapshot(snapshot.field)?;
        field.validate_against_frontier(&current)?;

        let boundaries = field.price_boundaries().collect::<BTreeSet<_>>();
        for price in current.keys() {
            ensure!(
                boundaries.contains(price),
                "materialized pressure field is missing a frontier boundary"
            );
        }

        let last_update_ms = newest_valid_through(&field);
        Ok(Self {
            current,
            field,
            last_update_ms,
        })
    }

    pub fn snapshot(&self) -> PressureFrontierSnapshot {
        PressureFrontierSnapshot {
            version: SNAPSHOT_VERSION,
            current: self
                .current
                .iter()
                .map(|(&key, &weight)| FrontierLevel { key, weight })
                .collect(),
            field: self.field.snapshot(),
        }
    }

    pub fn clear(&mut self) {
        self.current.clear();
        self.field.clear();
        self.last_update_ms = None;
    }

    pub fn observe_levels(
        &mut self,
        levels: &[FrontierLevel],
        valid_through_ms: f64,
    ) -> Result<bool> {
        let valid_through_ms = self.normalize_time(valid_through_ms)?;
        let previous_update_ms = self.last_update_ms;

        let normalized = normalize_levels(levels);
        let changed_prices = union_changed_prices(&self.current, &normalized);
        let geometry_changed = !changed_prices.is_empty();

        self.field.apply_deltas(&changed_prices, &normalized)?;
        if geometry_changed {
            self.current = normalized;
        }
        self.field.observe_current(valid_through_ms)?;
        self.last_update_ms = Some(valid_through_ms);

        Ok(geometry_changed || previous_update_ms != Some(valid_through_ms))
    }

    pub fn update_levels(
        &mut self,
        changes: &[PressureLevelChange],
        valid_through_ms: f64,
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

        let previous_update_ms = self.last_update_ms;
        let mut next = self.current.clone();
        let mut changed_prices = Vec::new();

        for (price, shares) in final_by_price {
            let previous = next.get(&price).copied().unwrap_or(0.0);
            if shares == previous {
                continue;
            }

            if shares > 0.0 {
                next.insert(price, shares);
            } else {
                next.remove(&price);
            }
            changed_prices.push(price);
        }

        let geometry_changed = !changed_prices.is_empty();
        self.field.apply_deltas(&changed_prices, &next)?;
        if geometry_changed {
            self.current = next;
        }
        self.field.observe_current(valid_through_ms)?;
        self.last_update_ms = Some(valid_through_ms);

        Ok(geometry_changed || previous_update_ms != Some(valid_through_ms))
    }

    pub fn observe_through(&mut self, valid_through_ms: f64) -> Result<bool> {
        let valid_through_ms = self.normalize_time(valid_through_ms)?;
        let changed = self.last_update_ms != Some(valid_through_ms);
        self.field.observe_current(valid_through_ms)?;
        self.last_update_ms = Some(valid_through_ms);
        Ok(changed)
    }

    fn normalize_time(&self, value: f64) -> Result<f64> {
        ensure!(
            value.is_finite(),
            "pressure frontier timestamp must be finite"
        );
        Ok(self.last_update_ms.map_or(value, |last| value.max(last)))
    }
}

fn canonicalize_snapshot(snapshot: &mut PressureFrontierSnapshot) {
    for run in &mut snapshot.field.runs {
        run.frozen_bands.retain(|band| {
            !(band.lo_volume.is_finite()
                && band.hi_volume.is_finite()
                && band.lo_volume >= 0.0
                && same_volume(band.lo_volume, band.hi_volume))
        });
    }
}

fn validate_snapshot(snapshot: &PressureFrontierSnapshot) -> Result<()> {
    ensure!(
        snapshot.version == SNAPSHOT_VERSION,
        "unsupported pressure frontier snapshot version: {}",
        snapshot.version
    );
    validate_field_snapshot(&snapshot.field)?;

    let mut previous = 0;
    for (index, level) in snapshot.current.iter().enumerate() {
        ensure!(
            level.key > 0 && level.key <= snapshot.field.max_price,
            "current[{index}].key must be in (0, {}]",
            snapshot.field.max_price
        );
        ensure!(
            level.key > previous,
            "current keys must be strictly increasing"
        );
        ensure!(
            level.weight.is_finite() && level.weight > 0.0,
            "current[{index}].weight must be positive and finite"
        );
        previous = level.key;
    }
    Ok(())
}

fn validate_field_snapshot(field: &PressureFieldSnapshot) -> Result<()> {
    ensure!(
        field.max_price > 0 && field.max_price <= PRICE_SCALE,
        "pressure field max price must be in (0, {PRICE_SCALE}]"
    );
    if let Some(valid_through) = field.current_valid_through_ms {
        ensure!(
            valid_through.is_finite(),
            "pressure field current valid-through must be finite"
        );
    }

    let mut previous_price = 0;
    let mut previous_volume = 0.0;

    for (index, run) in field.runs.iter().enumerate() {
        ensure!(
            run.price > previous_price && run.price <= field.max_price,
            "pressure run prices must be strictly increasing non-zero boundaries"
        );
        ensure!(
            run.volume.is_finite() && run.volume >= 0.0,
            "run[{index}].volume must be finite and non-negative"
        );
        if run.volume + volume_tolerance(run.volume, previous_volume) < previous_volume {
            bail!("edge pressure must be non-decreasing in price");
        }
        if run.volume > 0.0 {
            ensure!(
                field.current_valid_through_ms.is_some(),
                "current pressure requires a current valid-through timestamp"
            );
        }

        validate_frozen_bands(run)?;
        previous_price = run.price;
        previous_volume = run.volume;
    }

    if field.runs.first().is_some_and(empty_state) {
        bail!("pressure field must not store a leading empty run");
    }

    Ok(())
}

fn validate_frozen_bands(run: &PressureRun) -> Result<()> {
    let mut lower_edge: Option<f64> = None;

    for (index, band) in run.frozen_bands.iter().enumerate() {
        ensure!(
            band.lo_volume.is_finite()
                && band.hi_volume.is_finite()
                && band.valid_through_ms.is_finite()
                && band.lo_volume >= 0.0
                && band.hi_volume > band.lo_volume,
            "invalid frozen pressure band at price={} index={index}: lo={} hi={} validThroughMs={}",
            run.price,
            band.lo_volume,
            band.hi_volume,
            band.valid_through_ms,
        );

        if let Some(edge) = lower_edge {
            ensure!(
                same_volume(edge, band.hi_volume),
                "frozen pressure bands must be contiguous at price={} index={index}: previousLo={} nextHi={}",
                run.price,
                edge,
                band.hi_volume,
            );
        }
        lower_edge = Some(band.lo_volume);
    }

    if let Some(edge) = lower_edge {
        ensure!(
            same_volume(edge, run.volume),
            "frozen pressure bands must touch the current volume frontier at price={}: bandLo={} runVolume={}",
            run.price,
            edge,
            run.volume,
        );
    }

    Ok(())
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

fn union_changed_prices(previous: &BTreeMap<u16, f64>, next: &BTreeMap<u16, f64>) -> Vec<u16> {
    previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|price| {
            previous.get(price).copied().unwrap_or(0.0) != next.get(price).copied().unwrap_or(0.0)
        })
        .collect()
}

fn transition_run(
    run: &mut PressureRun,
    next_volume: f64,
    current_valid_through_ms: Option<f64>,
) -> Result<()> {
    ensure!(
        next_volume.is_finite() && next_volume >= 0.0,
        "pressure volume must be finite and non-negative"
    );

    let old_volume = run.volume;
    if same_volume(old_volume, next_volume) {
        run.volume = next_volume;
        return Ok(());
    }

    if next_volume < old_volume {
        let valid_through_ms = current_valid_through_ms.ok_or_else(|| {
            anyhow::anyhow!("cannot freeze current pressure before it has a validity timestamp")
        })?;

        if let Some(last) = run.frozen_bands.last() {
            ensure!(
                same_volume(last.lo_volume, old_volume),
                "frozen pressure stack is detached from the current frontier"
            );
        }

        if let Some(last) = run.frozen_bands.last_mut() {
            if last.valid_through_ms == valid_through_ms && same_volume(last.lo_volume, old_volume)
            {
                last.lo_volume = next_volume;
            } else {
                run.frozen_bands.push(PressureBand {
                    lo_volume: next_volume,
                    hi_volume: old_volume,
                    valid_through_ms,
                });
            }
        } else {
            run.frozen_bands.push(PressureBand {
                lo_volume: next_volume,
                hi_volume: old_volume,
                valid_through_ms,
            });
        }
    } else {
        while let Some(last) = run.frozen_bands.last() {
            if last.hi_volume <= next_volume {
                run.frozen_bands.pop();
                continue;
            }

            if let Some(last) = run.frozen_bands.last_mut()
                && last.lo_volume < next_volume
            {
                last.lo_volume = next_volume;
            }
            break;
        }
    }

    run.volume = next_volume;
    Ok(())
}

fn lower_bound_run(runs: &[PressureRun], price: u16) -> usize {
    runs.partition_point(|run| run.price < price)
}

fn states_equal(a: &PressureRun, b: &PressureRun) -> bool {
    same_volume(a.volume, b.volume)
        && a.frozen_bands.len() == b.frozen_bands.len()
        && a.frozen_bands.iter().zip(&b.frozen_bands).all(|(a, b)| {
            same_volume(a.lo_volume, b.lo_volume)
                && same_volume(a.hi_volume, b.hi_volume)
                && a.valid_through_ms == b.valid_through_ms
        })
}

fn empty_state(run: &PressureRun) -> bool {
    same_volume(run.volume, 0.0) && run.frozen_bands.is_empty()
}

fn newest_valid_through(field: &MaterializedPressureField) -> Option<f64> {
    let mut newest = field.current_valid_through_ms.unwrap_or(f64::NEG_INFINITY);
    for run in &field.runs {
        for band in &run.frozen_bands {
            newest = newest.max(band.valid_through_ms);
        }
    }
    newest.is_finite().then_some(newest)
}

fn same_volume(a: f64, b: f64) -> bool {
    (a - b).abs() <= volume_tolerance(a, b)
}

fn volume_tolerance(a: f64, b: f64) -> f64 {
    32.0 * f64::EPSILON * 1.0_f64.max(a).max(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_discards_numerically_empty_frozen_bands() {
        let snapshot = PressureFrontierSnapshot {
            version: SNAPSHOT_VERSION,
            current: vec![FrontierLevel {
                key: 5_000,
                weight: 4.0,
            }],
            field: PressureFieldSnapshot {
                max_price: PRICE_SCALE,
                current_valid_through_ms: Some(3_000.0),
                runs: vec![PressureRun {
                    price: 5_000,
                    volume: 4.0,
                    frozen_bands: vec![
                        PressureBand {
                            lo_volume: 10.0,
                            hi_volume: 10.0,
                            valid_through_ms: 1_000.0,
                        },
                        PressureBand {
                            lo_volume: 4.0,
                            hi_volume: 10.0,
                            valid_through_ms: 2_000.0,
                        },
                    ],
                }],
            },
        };

        let restored = PressureFrontierMemory::restore(snapshot).unwrap();
        assert_eq!(
            restored.snapshot().field.runs[0].frozen_bands,
            vec![PressureBand {
                lo_volume: 4.0,
                hi_volume: 10.0,
                valid_through_ms: 2_000.0,
            }]
        );
    }

    #[test]
    fn falling_frontier_freezes_previous_pressure() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[FrontierLevel {
                    key: 5_000,
                    weight: 10.0,
                }],
                1_000.0,
            )
            .unwrap();

        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 4.0,
                }],
                2_000.0,
            )
            .unwrap();

        let snapshot = memory.snapshot();
        assert_eq!(snapshot.field.current_valid_through_ms, Some(2_000.0));
        assert_eq!(snapshot.field.runs.len(), 1);
        assert_eq!(snapshot.field.runs[0].volume, 4.0);
        assert_eq!(
            snapshot.field.runs[0].frozen_bands,
            vec![PressureBand {
                lo_volume: 4.0,
                hi_volume: 10.0,
                valid_through_ms: 1_000.0,
            }]
        );
    }

    #[test]
    fn rising_frontier_consumes_frozen_pressure() {
        let mut memory = PressureFrontierMemory::default();
        memory
            .observe_levels(
                &[FrontierLevel {
                    key: 5_000,
                    weight: 10.0,
                }],
                1_000.0,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 4.0,
                }],
                2_000.0,
            )
            .unwrap();
        memory
            .update_levels(
                &[PressureLevelChange {
                    price: 5_000,
                    shares: 8.0,
                }],
                3_000.0,
            )
            .unwrap();

        let run = &memory.snapshot().field.runs[0];
        assert_eq!(run.volume, 8.0);
        assert_eq!(
            run.frozen_bands,
            vec![PressureBand {
                lo_volume: 8.0,
                hi_volume: 10.0,
                valid_through_ms: 1_000.0,
            }]
        );
    }
}
