use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{debug, error, info, warn};

use crate::{
    book::AskBook,
    polymarket::{
        MarketEvent, PolymarketRestClient, RawOrderBook, RawPriceChange, event_timestamp_ms,
    },
    pressure::{PressureFrontierMemory, PressureFrontierSnapshot, PressureLevelChange},
    pressure_log::RecorderPressureMutation,
    store::{
        RecorderCheckpointWrite, RecorderStore, RecorderStoreWriteRecord, RecorderTokenStatus,
    },
    subscriptions::{SubscriptionEvent, SubscriptionPool},
};

const PERSIST_DEBOUNCE: Duration = Duration::from_secs(1);
const REST_SEED_BATCH_TOKENS: usize = 20;
const REST_SEED_RETRY: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 16_384;
const COMMAND_CHANNEL_CAPACITY: usize = 128;

#[derive(Debug, Clone, Serialize)]
pub struct TransportState {
    pub pressure: PressureFrontierSnapshot,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecorderStateResponse {
    pub recording_since_ms_by_token: BTreeMap<String, i64>,
    pub states: BTreeMap<String, TransportState>,
    pub pending_token_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecorderStats {
    pub watched_tokens: usize,
    pub completed_tokens: usize,
    pub hydrated_tokens: usize,
    pub live_books: usize,
    pub subscription_connections: usize,
    pub subscription_batches: usize,
    pub dirty_tokens: usize,
    pub pending_pressure_mutations: usize,
    pub oldest_recording_since_ms: Option<i64>,
    pub newest_recording_since_ms: Option<i64>,
    pub pressure_tokens: u64,
    pub pressure_log_mutations: u64,
    pub database_path: String,
}

#[derive(Debug)]
struct BufferedPriceChangeEvent {
    timestamp_ms: i64,
    changes: Vec<RawPriceChange>,
}

#[derive(Debug)]
struct SeedResult {
    token_ids: Vec<String>,
    requested_at_ms: i64,
    result: std::result::Result<Vec<RawOrderBook>, String>,
}

enum RecorderCommand {
    State {
        token_ids: Vec<String>,
        include_states: bool,
        reply: oneshot::Sender<std::result::Result<RecorderStateResponse, String>>,
    },
    Watch {
        token_ids: Vec<String>,
        reply: oneshot::Sender<std::result::Result<(bool, RecorderStats), String>>,
    },
    Stats {
        reply: oneshot::Sender<std::result::Result<RecorderStats, String>>,
    },
    Stop {
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Clone)]
pub struct RecorderHandle {
    command_tx: mpsc::Sender<RecorderCommand>,
}

impl RecorderHandle {
    pub async fn state(
        &self,
        token_ids: Vec<String>,
        include_states: bool,
    ) -> Result<RecorderStateResponse> {
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(RecorderCommand::State {
                token_ids,
                include_states,
                reply,
            })
            .await
            .map_err(|_| anyhow!("recorder stopped"))?;
        response
            .await
            .map_err(|_| anyhow!("recorder stopped"))?
            .map_err(anyhow::Error::msg)
    }

    pub async fn watch(&self, token_ids: Vec<String>) -> Result<(bool, RecorderStats)> {
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(RecorderCommand::Watch { token_ids, reply })
            .await
            .map_err(|_| anyhow!("recorder stopped"))?;
        response
            .await
            .map_err(|_| anyhow!("recorder stopped"))?
            .map_err(anyhow::Error::msg)
    }

    pub async fn stats(&self) -> Result<RecorderStats> {
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(RecorderCommand::Stats { reply })
            .await
            .map_err(|_| anyhow!("recorder stopped"))?;
        response
            .await
            .map_err(|_| anyhow!("recorder stopped"))?
            .map_err(anyhow::Error::msg)
    }
}

pub struct RecorderRuntime {
    handle: RecorderHandle,
    task: JoinHandle<Result<()>>,
}

impl RecorderRuntime {
    pub fn handle(&self) -> RecorderHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.handle
            .command_tx
            .send(RecorderCommand::Stop { reply })
            .await
            .map_err(|_| anyhow!("recorder stopped before shutdown"))?;

        response
            .await
            .map_err(|_| anyhow!("recorder stopped before shutdown"))?
            .map_err(anyhow::Error::msg)?;

        self.task.await.context("joining recorder task")?
    }
}

pub async fn start(store: Arc<RecorderStore>) -> Result<RecorderRuntime> {
    let index = store.load_index()?;
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let (subscription_tx, subscription_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (seed_tx, seed_rx) = mpsc::channel(128);

    let mut recorder = AgeRecorder::new(
        store,
        PolymarketRestClient::default(),
        SubscriptionPool::new(subscription_tx),
        command_rx,
        subscription_rx,
        seed_tx,
        seed_rx,
    );

    for record in index {
        match record.status {
            RecorderTokenStatus::Watched => {
                recorder.watched.insert(record.token_id.clone());
            }
            RecorderTokenStatus::Completed => {
                recorder.completed.insert(record.token_id.clone());
            }
        }

        if record.has_pressure {
            recorder
                .stored_pressure_tokens
                .insert(record.token_id.clone());
            if let Some(recording_since_ms) = record.recording_since_ms {
                recorder
                    .recording_since
                    .insert(record.token_id, recording_since_ms);
            }
        }
    }

    let watched = recorder.watched.iter().cloned().collect::<Vec<_>>();
    recorder.subscriptions.add(watched).await;

    let task = tokio::spawn(recorder.run());
    Ok(RecorderRuntime {
        handle: RecorderHandle { command_tx },
        task,
    })
}

struct AgeRecorder {
    store: Arc<RecorderStore>,
    rest: PolymarketRestClient,
    subscriptions: SubscriptionPool,
    watched: HashSet<String>,
    completed: HashSet<String>,
    recording_since: HashMap<String, i64>,
    books: HashMap<String, AskBook>,
    memories: HashMap<String, PressureFrontierMemory>,
    stored_pressure_tokens: HashSet<String>,
    pending_price_changes: HashMap<String, Vec<BufferedPriceChangeEvent>>,
    pending_pressure_mutations: HashMap<String, Vec<RecorderPressureMutation>>,
    seed_in_flight: HashSet<String>,
    seed_retry_after_ms: HashMap<String, i64>,
    dirty: BTreeSet<String>,
    command_rx: mpsc::Receiver<RecorderCommand>,
    subscription_rx: mpsc::Receiver<SubscriptionEvent>,
    seed_tx: mpsc::Sender<SeedResult>,
    seed_rx: mpsc::Receiver<SeedResult>,
}

impl AgeRecorder {
    #[allow(clippy::too_many_arguments)]
    fn new(
        store: Arc<RecorderStore>,
        rest: PolymarketRestClient,
        subscriptions: SubscriptionPool,
        command_rx: mpsc::Receiver<RecorderCommand>,
        subscription_rx: mpsc::Receiver<SubscriptionEvent>,
        seed_tx: mpsc::Sender<SeedResult>,
        seed_rx: mpsc::Receiver<SeedResult>,
    ) -> Self {
        Self {
            store,
            rest,
            subscriptions,
            watched: HashSet::new(),
            completed: HashSet::new(),
            recording_since: HashMap::new(),
            books: HashMap::new(),
            memories: HashMap::new(),
            stored_pressure_tokens: HashSet::new(),
            pending_price_changes: HashMap::new(),
            pending_pressure_mutations: HashMap::new(),
            seed_in_flight: HashSet::new(),
            seed_retry_after_ms: HashMap::new(),
            dirty: BTreeSet::new(),
            command_rx,
            subscription_rx,
            seed_tx,
            seed_rx,
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut persist = tokio::time::interval(PERSIST_DEBOUNCE);
        persist.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // interval() ticks immediately; persistence is a debounce, not startup work.
        persist.tick().await;

        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        self.shutdown().await?;
                        return Ok(());
                    };
                    if self.handle_command(command).await? {
                        return Ok(());
                    }
                }
                event = self.subscription_rx.recv() => {
                    if let Some(event) = event
                        && let Err(error) = self.consume_subscription_event(event).await
                    {
                        error!(?error, "could not consume Polymarket event");
                    }
                }
                seed = self.seed_rx.recv() => {
                    if let Some(seed) = seed
                        && let Err(error) = self.consume_seed_result(seed)
                    {
                        error!(?error, "could not consume REST seed");
                    }
                }
                _ = persist.tick() => {
                    if let Err(error) = self.flush_dirty() {
                        error!(?error, "could not persist recorder state");
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: RecorderCommand) -> Result<bool> {
        match command {
            RecorderCommand::State {
                token_ids,
                include_states,
                reply,
            } => {
                let result = self
                    .state(token_ids, include_states)
                    .await
                    .map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            RecorderCommand::Watch { token_ids, reply } => {
                let result = async {
                    let changed = self.watch(token_ids).await;
                    Ok((changed, self.stats()?))
                }
                .await
                .map_err(|error: anyhow::Error| error.to_string());
                let _ = reply.send(result);
            }
            RecorderCommand::Stats { reply } => {
                let _ = reply.send(self.stats().map_err(|error| error.to_string()));
            }
            RecorderCommand::Stop { reply } => {
                let result = self.shutdown().await;
                let response = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                let _ = reply.send(response);
                result?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn watch(&mut self, token_ids: Vec<String>) -> bool {
        let mut added = Vec::new();
        for token_id in token_ids {
            if token_id.is_empty()
                || self.watched.contains(&token_id)
                || self.completed.contains(&token_id)
            {
                continue;
            }

            self.watched.insert(token_id.clone());
            self.dirty.insert(token_id.clone());
            added.push(token_id);
        }

        if added.is_empty() {
            return false;
        }

        info!(
            tokens = added.len(),
            watched = self.watched.len(),
            "watching tokens"
        );
        self.subscriptions.add(added).await;
        true
    }

    async fn state(
        &mut self,
        token_ids: Vec<String>,
        include_states: bool,
    ) -> Result<RecorderStateResponse> {
        let requested = dedupe(token_ids);
        self.watch(requested.clone()).await;
        self.seed_pending(&requested);

        let mut states = BTreeMap::new();
        if include_states {
            for token_id in &requested {
                self.ensure_memory(token_id)?;
                if let Some(memory) = self.memories.get(token_id) {
                    states.insert(
                        token_id.clone(),
                        TransportState {
                            pressure: memory.snapshot(),
                        },
                    );
                }
            }
        }

        let recording_since_ms_by_token = requested
            .iter()
            .filter_map(|token_id| {
                self.recording_since
                    .get(token_id)
                    .copied()
                    .map(|since| (token_id.clone(), since))
            })
            .collect();

        let pending_token_ids = requested
            .into_iter()
            .filter(|token_id| {
                self.watched.contains(token_id)
                    && !self.memories.contains_key(token_id)
                    && !self.stored_pressure_tokens.contains(token_id)
            })
            .collect();

        Ok(RecorderStateResponse {
            recording_since_ms_by_token,
            states,
            pending_token_ids,
        })
    }

    fn stats(&self) -> Result<RecorderStats> {
        let starts = self.recording_since.values().copied();
        let oldest_recording_since_ms = starts.clone().min();
        let newest_recording_since_ms = starts.max();
        let store = self.store.stats()?;

        Ok(RecorderStats {
            watched_tokens: self.watched.len(),
            completed_tokens: self.completed.len(),
            hydrated_tokens: self.memories.len(),
            live_books: self.books.len(),
            subscription_connections: self.subscriptions.active_connection_count(),
            subscription_batches: self.subscriptions.active_connection_count(),
            dirty_tokens: self.dirty.len(),
            pending_pressure_mutations: self
                .pending_pressure_mutations
                .values()
                .map(Vec::len)
                .sum(),
            oldest_recording_since_ms,
            newest_recording_since_ms,
            pressure_tokens: store.pressure_tokens,
            pressure_log_mutations: store.pressure_log_mutations,
            database_path: store.database_path,
        })
    }

    fn seed_pending(&mut self, token_ids: &[String]) {
        let now = now_ms();
        let candidates = token_ids
            .iter()
            .filter(|token_id| {
                self.watched.contains(*token_id)
                    && !self.completed.contains(*token_id)
                    && !self.memories.contains_key(*token_id)
                    && !self.stored_pressure_tokens.contains(*token_id)
                    && !self.seed_in_flight.contains(*token_id)
                    && self
                        .seed_retry_after_ms
                        .get(*token_id)
                        .copied()
                        .unwrap_or(0)
                        <= now
            })
            .cloned()
            .collect::<BTreeSet<_>>();

        for batch in candidates
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(REST_SEED_BATCH_TOKENS)
        {
            let token_ids = batch.to_vec();
            for token_id in &token_ids {
                self.seed_in_flight.insert(token_id.clone());
            }

            let rest = self.rest.clone();
            let seed_tx = self.seed_tx.clone();
            tokio::spawn(async move {
                let requested_at_ms = now_ms();
                let result = rest
                    .fetch_order_books(&token_ids)
                    .await
                    .map_err(|error| error.to_string());
                let _ = seed_tx
                    .send(SeedResult {
                        token_ids,
                        requested_at_ms,
                        result,
                    })
                    .await;
            });
        }
    }

    fn consume_seed_result(&mut self, seed: SeedResult) -> Result<()> {
        match seed.result {
            Ok(snapshots) => {
                for snapshot in snapshots {
                    let token_id = snapshot.asset_id.clone();
                    if !self.watched.contains(&token_id)
                        || self.completed.contains(&token_id)
                        || self.memories.contains_key(&token_id)
                    {
                        continue;
                    }

                    let snapshot_ms = seed.requested_at_ms.max(event_timestamp_ms(
                        snapshot.timestamp.as_ref(),
                        seed.requested_at_ms,
                    ));
                    let mut book = AskBook::from_snapshot(&snapshot.asks)?;
                    let levels = book.pressure_levels();
                    self.books.insert(token_id.clone(), book.clone());
                    self.update_memory_replace(&token_id, levels, snapshot_ms)?;

                    if let Some(buffered) = self.pending_price_changes.remove(&token_id) {
                        for event in buffered {
                            if event.timestamp_ms <= snapshot_ms {
                                continue;
                            }
                            let changes = apply_book_changes(&mut book, &event.changes)?;
                            self.update_memory_changes(&token_id, changes, event.timestamp_ms)?;
                        }
                        self.books.insert(token_id, book);
                    }
                }
            }
            Err(message) => {
                let retry_at = now_ms() + REST_SEED_RETRY.as_millis() as i64;
                warn!(error = %message, tokens = seed.token_ids.len(), "REST seed failed");
                for token_id in &seed.token_ids {
                    if !self.memories.contains_key(token_id) {
                        self.seed_retry_after_ms.insert(token_id.clone(), retry_at);
                    }
                }
            }
        }

        for token_id in seed.token_ids {
            self.seed_in_flight.remove(&token_id);
        }
        Ok(())
    }

    async fn consume_subscription_event(&mut self, event: SubscriptionEvent) -> Result<()> {
        match event {
            SubscriptionEvent::ContinuityLost { token_ids } => {
                for token_id in &token_ids {
                    self.books.remove(token_id);
                    self.pending_price_changes.remove(token_id);
                }
                debug!(tokens = token_ids.len(), "subscription continuity lost");
            }
            SubscriptionEvent::Market {
                event,
                snapshot_requested_at_ms,
            } => match event {
                MarketEvent::Book(event) => {
                    let token_id = event.asset_id;
                    if !self.watched.contains(&token_id) {
                        return Ok(());
                    }

                    let book = AskBook::from_snapshot(&event.asks)?;
                    let levels = book.pressure_levels();
                    self.books.insert(token_id.clone(), book);
                    self.pending_price_changes.remove(&token_id);
                    let valid_through_ms = snapshot_requested_at_ms.max(event_timestamp_ms(
                        event.timestamp.as_ref(),
                        snapshot_requested_at_ms,
                    ));
                    self.update_memory_replace(&token_id, levels, valid_through_ms)?;
                }
                MarketEvent::PriceChange(event) => {
                    let timestamp_ms = event_timestamp_ms(event.timestamp.as_ref(), now_ms());
                    let mut by_token = BTreeMap::<String, Vec<RawPriceChange>>::new();

                    for change in event.price_changes {
                        if self.watched.contains(&change.asset_id) {
                            by_token
                                .entry(change.asset_id.clone())
                                .or_default()
                                .push(change);
                        }
                    }

                    for (token_id, changes) in by_token {
                        let Some(book) = self.books.get_mut(&token_id) else {
                            self.pending_price_changes
                                .entry(token_id)
                                .or_default()
                                .push(BufferedPriceChangeEvent {
                                    timestamp_ms,
                                    changes,
                                });
                            continue;
                        };

                        let pressure_changes = apply_book_changes(book, &changes)?;
                        self.update_memory_changes(&token_id, pressure_changes, timestamp_ms)?;
                    }
                }
                MarketEvent::MarketResolved(event) => {
                    let mut removed = Vec::new();
                    for token_id in event.assets_ids.unwrap_or_default() {
                        self.ensure_memory(&token_id)?;
                        if let Some(memory) = self.memories.get_mut(&token_id) {
                            memory.clear();
                            self.pending_pressure_mutations
                                .entry(token_id.clone())
                                .or_default()
                                .push(RecorderPressureMutation::Clear);
                        }

                        if self.watched.remove(&token_id) {
                            removed.push(token_id.clone());
                        }
                        self.completed.insert(token_id.clone());
                        self.books.remove(&token_id);
                        self.pending_price_changes.remove(&token_id);
                        self.seed_retry_after_ms.remove(&token_id);
                        self.dirty.insert(token_id);
                    }

                    if !removed.is_empty() {
                        self.subscriptions.remove(removed).await;
                    }
                }
            },
        }
        Ok(())
    }

    fn update_memory_replace(
        &mut self,
        token_id: &str,
        levels: Vec<crate::pressure::FrontierLevel>,
        valid_through_ms: i64,
    ) -> Result<()> {
        self.ensure_memory(token_id)?;
        let memory = self.memories.entry(token_id.to_owned()).or_default();

        if !memory.observe_levels(&levels, valid_through_ms as f64)? {
            return Ok(());
        }

        self.pending_pressure_mutations
            .entry(token_id.to_owned())
            .or_default()
            .push(RecorderPressureMutation::Replace {
                valid_through_ms: valid_through_ms as f64,
                levels,
            });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn update_memory_changes(
        &mut self,
        token_id: &str,
        changes: Vec<PressureLevelChange>,
        valid_through_ms: i64,
    ) -> Result<()> {
        self.ensure_memory(token_id)?;
        let memory = self.memories.entry(token_id.to_owned()).or_default();

        let mut mutated = memory.update_levels(&changes, valid_through_ms as f64)?;
        if memory.observe_through(valid_through_ms as f64)? {
            mutated = true;
        }
        if !mutated {
            return Ok(());
        }

        self.pending_pressure_mutations
            .entry(token_id.to_owned())
            .or_default()
            .push(RecorderPressureMutation::Update {
                valid_through_ms: valid_through_ms as f64,
                changes,
            });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn finish_memory_update(&mut self, token_id: &str, valid_through_ms: i64) {
        if !self.recording_since.contains_key(token_id) {
            self.recording_since
                .insert(token_id.to_owned(), valid_through_ms);
            info!(token = %short_token(token_id), "recorded first snapshot");
        }
        self.dirty.insert(token_id.to_owned());
    }

    fn ensure_memory(&mut self, token_id: &str) -> Result<()> {
        if self.memories.contains_key(token_id) || !self.stored_pressure_tokens.contains(token_id) {
            return Ok(());
        }

        let record = self.store.load(token_id)?;
        match record.and_then(|record| record.pressure) {
            Some(snapshot) => {
                self.memories.insert(
                    token_id.to_owned(),
                    PressureFrontierMemory::restore(snapshot)?,
                );
            }
            None => {
                self.recording_since.remove(token_id);
                self.dirty.insert(token_id.to_owned());
            }
        }
        self.stored_pressure_tokens.remove(token_id);
        Ok(())
    }

    fn flush_dirty(&mut self) -> Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }

        let token_ids = self.dirty.iter().cloned().collect::<Vec<_>>();
        let mut writes = Vec::with_capacity(token_ids.len());

        for token_id in &token_ids {
            let mutations = self
                .pending_pressure_mutations
                .get(token_id)
                .cloned()
                .unwrap_or_default();
            let completed = self.completed.contains(token_id);
            let should_checkpoint =
                completed || self.store.should_checkpoint(token_id, mutations.len())?;

            let (mutations, checkpoint) = if should_checkpoint {
                self.ensure_memory(token_id)?;
                (
                    Vec::new(),
                    RecorderCheckpointWrite::Replace(
                        self.memories
                            .get(token_id)
                            .map(PressureFrontierMemory::snapshot),
                    ),
                )
            } else {
                (mutations, RecorderCheckpointWrite::Keep)
            };

            writes.push(RecorderStoreWriteRecord {
                token_id: token_id.clone(),
                status: if completed {
                    RecorderTokenStatus::Completed
                } else {
                    RecorderTokenStatus::Watched
                },
                recording_since_ms: self.recording_since.get(token_id).copied(),
                mutations,
                checkpoint,
            });
        }

        let stats = self.store.write(&writes)?;
        for token_id in token_ids {
            self.pending_pressure_mutations.remove(&token_id);
            self.dirty.remove(&token_id);
        }

        debug!(
            tokens = writes.len(),
            mutations = stats.mutation_count,
            mutation_kib = stats.mutation_bytes / 1024,
            checkpoints = stats.checkpoint_count,
            checkpoint_kib = stats.checkpoint_bytes / 1024,
            "sqlite flush"
        );
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.subscriptions.stop().await;
        self.flush_dirty()?;
        self.store.checkpoint()?;
        info!("recorder state flushed");
        Ok(())
    }
}

fn apply_book_changes(
    book: &mut AskBook,
    changes: &[RawPriceChange],
) -> Result<Vec<PressureLevelChange>> {
    let mut pressure_changes = Vec::new();
    for change in changes {
        if let Some(change) = book.apply_change(&change.side, &change.price, &change.size)? {
            pressure_changes.push(change);
        }
    }
    Ok(pressure_changes)
}

fn dedupe(token_ids: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    token_ids
        .into_iter()
        .filter(|token_id| !token_id.is_empty() && seen.insert(token_id.clone()))
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn short_token(token_id: &str) -> String {
    if token_id.len() <= 12 {
        token_id.to_owned()
    } else {
        format!("{}…{}", &token_id[..6], &token_id[token_id.len() - 4..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn book_changes_keep_only_ask_pressure() {
        let mut book = AskBook::default();
        let changes = vec![
            RawPriceChange {
                asset_id: "token".into(),
                price: "0.4".into(),
                size: "20".into(),
                side: "BUY".into(),
            },
            RawPriceChange {
                asset_id: "token".into(),
                price: "0.6".into(),
                size: "12".into(),
                side: "SELL".into(),
            },
        ];

        assert_eq!(
            apply_book_changes(&mut book, &changes).unwrap(),
            vec![PressureLevelChange {
                price: 6_000,
                shares: 12.0,
            }]
        );
    }
}
