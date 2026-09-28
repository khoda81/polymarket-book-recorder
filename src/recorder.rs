use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{debug, error, info};

use crate::{
    book::AskBook,
    polymarket::{MarketEvent, RawPriceChange},
    pressure::{PressureFrontierMemory, PressureFrontierSnapshot, PressureLevel},
    pressure_log::RecorderPressureMutation,
    store::{
        RecorderCheckpointWrite, RecorderStore, RecorderStoreWriteRecord, RecorderTokenStatus,
    },
    subscriptions::{SubscriptionEvent, SubscriptionPool},
};

const PERSIST_DEBOUNCE: Duration = Duration::from_secs(1);
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
    pub connected_subscription_connections: usize,
    pub assigned_subscription_tokens: usize,
    pub dirty_tokens: usize,
    pub pending_pressure_mutations: usize,
    pub oldest_recording_since_ms: Option<i64>,
    pub newest_recording_since_ms: Option<i64>,
    pub pressure_tokens: u64,
    pub pressure_log_mutations: u64,
    pub database_path: String,
}

#[derive(Debug, Default)]
struct MarketState {
    token_ids: BTreeSet<String>,
    watermark_ms: Option<i64>,
}

#[derive(Debug)]
enum PressureState {
    Missing,
    Stored,
    Loaded(PressureFrontierMemory),
}

impl PressureState {
    fn from_store(has_pressure: bool) -> Self {
        if has_pressure {
            Self::Stored
        } else {
            Self::Missing
        }
    }

    fn memory(&self) -> Option<&PressureFrontierMemory> {
        match self {
            Self::Loaded(memory) => Some(memory),
            Self::Missing | Self::Stored => None,
        }
    }

    fn memory_or_default(&mut self) -> Result<&mut PressureFrontierMemory> {
        if matches!(self, Self::Missing) {
            *self = Self::Loaded(PressureFrontierMemory::default());
        }
        match self {
            Self::Loaded(memory) => Ok(memory),
            Self::Stored => Err(anyhow!("stored pressure must be loaded before mutation")),
            Self::Missing => unreachable!("missing pressure was initialized above"),
        }
    }

    fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}

#[derive(Debug, Default)]
enum BookState {
    #[default]
    Unhydrated,
    Live(AskBook),
}

#[derive(Debug)]
struct WatchedTokenState {
    recording_since_ms: Option<i64>,
    pressure: PressureState,
    book: BookState,
    pending_pressure_mutations: Vec<RecorderPressureMutation>,
}

impl WatchedTokenState {
    fn new(recording_since_ms: Option<i64>, pressure: PressureState) -> Self {
        Self {
            recording_since_ms,
            pressure,
            book: BookState::default(),
            pending_pressure_mutations: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct CompletedTokenState {
    recording_since_ms: Option<i64>,
    pressure: PressureState,
    pending_pressure_mutations: Vec<RecorderPressureMutation>,
}

#[derive(Debug)]
enum TokenState {
    Watched(WatchedTokenState),
    Completed(CompletedTokenState),
}

impl TokenState {
    fn from_store(
        status: RecorderTokenStatus,
        recording_since_ms: Option<i64>,
        has_pressure: bool,
    ) -> Self {
        let pressure = PressureState::from_store(has_pressure);
        let recording_since_ms = has_pressure.then_some(recording_since_ms).flatten();
        match status {
            RecorderTokenStatus::Watched => {
                Self::Watched(WatchedTokenState::new(recording_since_ms, pressure))
            }
            RecorderTokenStatus::Completed => Self::Completed(CompletedTokenState {
                recording_since_ms,
                pressure,
                pending_pressure_mutations: Vec::new(),
            }),
        }
    }

    fn status(&self) -> RecorderTokenStatus {
        match self {
            Self::Watched(_) => RecorderTokenStatus::Watched,
            Self::Completed(_) => RecorderTokenStatus::Completed,
        }
    }

    fn is_watched(&self) -> bool {
        matches!(self, Self::Watched(_))
    }

    fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }

    fn recording_since_ms(&self) -> Option<i64> {
        match self {
            Self::Watched(state) => state.recording_since_ms,
            Self::Completed(state) => state.recording_since_ms,
        }
    }

    fn recording_since_ms_mut(&mut self) -> &mut Option<i64> {
        match self {
            Self::Watched(state) => &mut state.recording_since_ms,
            Self::Completed(state) => &mut state.recording_since_ms,
        }
    }

    fn pressure(&self) -> &PressureState {
        match self {
            Self::Watched(state) => &state.pressure,
            Self::Completed(state) => &state.pressure,
        }
    }

    fn pressure_mut(&mut self) -> &mut PressureState {
        match self {
            Self::Watched(state) => &mut state.pressure,
            Self::Completed(state) => &mut state.pressure,
        }
    }

    fn pending_pressure_mutations(&self) -> &[RecorderPressureMutation] {
        match self {
            Self::Watched(state) => &state.pending_pressure_mutations,
            Self::Completed(state) => &state.pending_pressure_mutations,
        }
    }

    fn pending_pressure_mutations_mut(&mut self) -> &mut Vec<RecorderPressureMutation> {
        match self {
            Self::Watched(state) => &mut state.pending_pressure_mutations,
            Self::Completed(state) => &mut state.pending_pressure_mutations,
        }
    }
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

    let mut recorder = AgeRecorder::new(
        store,
        SubscriptionPool::new(subscription_tx),
        command_rx,
        subscription_rx,
    );

    for record in index {
        recorder.tokens.insert(
            record.token_id,
            TokenState::from_store(
                record.status,
                record.recording_since_ms,
                record.has_pressure,
            ),
        );
    }

    let watched = recorder
        .tokens
        .iter()
        .filter_map(|(token_id, state)| state.is_watched().then_some(token_id.clone()))
        .collect::<Vec<_>>();
    recorder.subscriptions.add(watched).await;

    let task = tokio::spawn(recorder.run());
    Ok(RecorderRuntime {
        handle: RecorderHandle { command_tx },
        task,
    })
}

struct AgeRecorder {
    store: Arc<RecorderStore>,
    subscriptions: SubscriptionPool,
    tokens: HashMap<String, TokenState>,
    markets: HashMap<(String, u64), MarketState>,
    dirty: BTreeSet<String>,
    command_rx: mpsc::Receiver<RecorderCommand>,
    subscription_rx: mpsc::Receiver<SubscriptionEvent>,
}

impl AgeRecorder {
    fn new(
        store: Arc<RecorderStore>,
        subscriptions: SubscriptionPool,
        command_rx: mpsc::Receiver<RecorderCommand>,
        subscription_rx: mpsc::Receiver<SubscriptionEvent>,
    ) -> Self {
        Self {
            store,
            subscriptions,
            tokens: HashMap::new(),
            markets: HashMap::new(),
            dirty: BTreeSet::new(),
            command_rx,
            subscription_rx,
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
            if token_id.is_empty() || self.tokens.contains_key(&token_id) {
                continue;
            }

            self.tokens.insert(
                token_id.clone(),
                TokenState::Watched(WatchedTokenState::new(None, PressureState::Missing)),
            );
            self.dirty.insert(token_id.clone());
            added.push(token_id);
        }

        if added.is_empty() {
            return false;
        }

        let watched = self
            .tokens
            .values()
            .filter(|state| state.is_watched())
            .count();
        info!(tokens = added.len(), watched, "watching tokens");
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

        let mut states = BTreeMap::new();
        if include_states {
            for token_id in &requested {
                self.ensure_memory(token_id)?;
                if let Some(memory) = self
                    .tokens
                    .get(token_id)
                    .and_then(|state| state.pressure().memory())
                {
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
                self.tokens
                    .get(token_id)
                    .and_then(TokenState::recording_since_ms)
                    .map(|since| (token_id.clone(), since))
            })
            .collect();

        let pending_token_ids = requested
            .into_iter()
            .filter(|token_id| {
                self.tokens
                    .get(token_id)
                    .is_some_and(|state| state.is_watched() && state.pressure().is_missing())
            })
            .collect();

        Ok(RecorderStateResponse {
            recording_since_ms_by_token,
            states,
            pending_token_ids,
        })
    }

    fn stats(&self) -> Result<RecorderStats> {
        let starts = self
            .tokens
            .values()
            .filter_map(TokenState::recording_since_ms);
        let oldest_recording_since_ms = starts.clone().min();
        let newest_recording_since_ms = starts.max();
        let store = self.store.stats()?;

        Ok(RecorderStats {
            watched_tokens: self
                .tokens
                .values()
                .filter(|state| state.is_watched())
                .count(),
            completed_tokens: self
                .tokens
                .values()
                .filter(|state| state.is_completed())
                .count(),
            hydrated_tokens: self
                .tokens
                .values()
                .filter(|state| state.pressure().memory().is_some())
                .count(),
            live_books: self
                .tokens
                .values()
                .filter(|state| {
                    matches!(
                        state,
                        TokenState::Watched(WatchedTokenState {
                            book: BookState::Live(_),
                            ..
                        })
                    )
                })
                .count(),
            subscription_connections: self.subscriptions.active_connection_count(),
            connected_subscription_connections: self.subscriptions.connected_connection_count(),
            assigned_subscription_tokens: self.subscriptions.assigned_token_count(),
            dirty_tokens: self.dirty.len(),
            pending_pressure_mutations: self
                .tokens
                .values()
                .map(|state| state.pending_pressure_mutations().len())
                .sum(),
            oldest_recording_since_ms,
            newest_recording_since_ms,
            pressure_tokens: store.pressure_tokens,
            pressure_log_mutations: store.pressure_log_mutations,
            database_path: store.database_path,
        })
    }

    async fn consume_subscription_event(&mut self, event: SubscriptionEvent) -> Result<()> {
        match event {
            SubscriptionEvent::ContinuityLost {
                shard_id,
                token_ids,
            } => {
                for token_id in &token_ids {
                    if let Some(TokenState::Watched(state)) = self.tokens.get_mut(token_id) {
                        state.book = BookState::Unhydrated;
                    }
                }
                for ((_, market_shard_id), market) in &mut self.markets {
                    if *market_shard_id == shard_id {
                        market.watermark_ms = None;
                    }
                }
                debug!(
                    shard_id,
                    tokens = token_ids.len(),
                    "subscription continuity lost"
                );
            }
            SubscriptionEvent::Market {
                shard_id,
                event,
                snapshot_requested_at_ms,
            } => match event {
                MarketEvent::Book(event) => {
                    let token_id = event.asset_id;
                    if !self
                        .tokens
                        .get(&token_id)
                        .is_some_and(TokenState::is_watched)
                    {
                        return Ok(());
                    }

                    self.register_market_token(&event.market, shard_id, &token_id)?;
                    let excluded = HashSet::from([token_id.clone()]);
                    let market_watermark = self.observe_market_watermark(
                        &event.market,
                        shard_id,
                        event.timestamp_ms,
                        &excluded,
                    )?;

                    let was_live = matches!(
                        self.tokens.get(&token_id),
                        Some(TokenState::Watched(WatchedTokenState {
                            book: BookState::Live(_),
                            ..
                        }))
                    );
                    let book = AskBook::from_snapshot(&event.asks)?;
                    let levels = book.pressure_levels();

                    if snapshot_requested_at_ms.is_some() || !was_live {
                        let requested_at_ms = snapshot_requested_at_ms.ok_or_else(|| {
                            anyhow!(
                                "initial book for token {} has no subscription watermark",
                                short_token(&token_id)
                            )
                        })?;
                        let valid_through_ms = market_watermark
                            .map_or(requested_at_ms, |value| value.max(requested_at_ms));
                        self.update_memory_snapshot(&token_id, levels, valid_through_ms)?;
                    } else {
                        let valid_through_ms =
                            self.token_event_watermark(&token_id, market_watermark)?;
                        self.update_memory_continuous_replace(&token_id, levels, valid_through_ms)?;
                    }

                    if let Some(TokenState::Watched(state)) = self.tokens.get_mut(&token_id) {
                        state.book = BookState::Live(book);
                    }
                }
                MarketEvent::PriceChange(event) => {
                    let market = event.market;
                    let mut by_token = BTreeMap::<String, Vec<RawPriceChange>>::new();

                    for change in event.price_changes {
                        if self
                            .tokens
                            .get(&change.asset_id)
                            .is_some_and(TokenState::is_watched)
                        {
                            self.register_market_token(&market, shard_id, &change.asset_id)?;
                            by_token
                                .entry(change.asset_id.clone())
                                .or_default()
                                .push(change);
                        }
                    }

                    let changed_tokens = by_token.keys().cloned().collect::<HashSet<_>>();
                    let market_watermark = self.observe_market_watermark(
                        &market,
                        shard_id,
                        event.timestamp_ms,
                        &changed_tokens,
                    )?;

                    for (token_id, changes) in by_token {
                        let pressure_changes = {
                            let Some(TokenState::Watched(state)) = self.tokens.get_mut(&token_id)
                            else {
                                continue;
                            };
                            let BookState::Live(book) = &mut state.book else {
                                // The later initial book is a complete ordered-stream
                                // observation and supersedes any pre-snapshot deltas.
                                continue;
                            };
                            apply_book_changes(book, &changes)?
                        };

                        let valid_through_ms =
                            self.token_event_watermark(&token_id, market_watermark)?;
                        if pressure_changes.is_empty() {
                            self.advance_memory(&token_id, valid_through_ms)?;
                        } else {
                            self.update_memory_changes(
                                &token_id,
                                pressure_changes,
                                valid_through_ms,
                            )?;
                        }
                    }
                }
                MarketEvent::Watermark(event) => {
                    self.observe_market_watermark(
                        &event.market,
                        shard_id,
                        event.timestamp_ms,
                        &HashSet::new(),
                    )?;
                }
                MarketEvent::MarketResolved(event) => {
                    let market = event.market;
                    let mut asset_ids = self
                        .markets
                        .iter()
                        .filter(|((market_id, _), _)| market_id == &market)
                        .flat_map(|(_, state)| state.token_ids.iter().cloned())
                        .collect::<BTreeSet<_>>();
                    asset_ids.extend(event.assets_ids.unwrap_or_default());
                    if let Some(semantic_winner) = &event.winning_asset_id {
                        asset_ids.insert(semantic_winner.clone());
                    }

                    let resolving = asset_ids
                        .iter()
                        .filter(|token_id| self.tokens.contains_key(*token_id))
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let excluded = resolving.iter().cloned().collect::<HashSet<_>>();
                    let market_watermark = self.observe_market_watermark(
                        &market,
                        shard_id,
                        event.timestamp_ms,
                        &excluded,
                    )?;

                    let Some(semantic_winner) = event.winning_asset_id else {
                        return Ok(());
                    };
                    let unbounded_source = if asset_ids.len() == 2 {
                        asset_ids
                            .iter()
                            .find(|token_id| *token_id != &semantic_winner)
                            .cloned()
                    } else {
                        None
                    }
                    .ok_or_else(|| {
                        anyhow!(
                            "cannot map resolved market {market} with {} asset ids onto a binary pressure pair",
                            asset_ids.len()
                        )
                    })?;

                    let same_shard_tokens = self
                        .markets
                        .get(&(market.clone(), shard_id))
                        .map(|state| state.token_ids.clone())
                        .unwrap_or_default();

                    let mut removed = Vec::new();
                    for token_id in resolving {
                        let unbounded = token_id == unbounded_source;
                        let resolved_at_ms = if unbounded || same_shard_tokens.contains(&token_id) {
                            market_watermark
                        } else {
                            None
                        };
                        if self.complete_token(&token_id, unbounded, resolved_at_ms)? {
                            removed.push(token_id);
                        }
                    }

                    if !removed.is_empty() {
                        self.subscriptions.remove(removed).await;
                    }
                }
            },
        }
        Ok(())
    }

    fn register_market_token(
        &mut self,
        market_id: &str,
        shard_id: u64,
        token_id: &str,
    ) -> Result<()> {
        let current_key = (market_id.to_owned(), shard_id);
        let previous_keys = self
            .markets
            .iter()
            .filter(|(_, state)| state.token_ids.contains(token_id))
            .map(|(key, _)| key.clone())
            .filter(|key| key != &current_key)
            .collect::<Vec<_>>();

        for previous_key in previous_keys {
            if previous_key.0 != market_id {
                return Err(anyhow!(
                    "token {} moved from market {} to {market_id}",
                    short_token(token_id),
                    previous_key.0
                ));
            }
            if let Some(previous) = self.markets.get_mut(&previous_key) {
                previous.token_ids.remove(token_id);
            }
        }
        self.markets
            .retain(|key, state| key == &current_key || !state.token_ids.is_empty());

        self.markets
            .entry(current_key)
            .or_default()
            .token_ids
            .insert(token_id.to_owned());
        Ok(())
    }

    fn observe_market_watermark(
        &mut self,
        market_id: &str,
        shard_id: u64,
        timestamp_ms: Option<i64>,
        excluded_tokens: &HashSet<String>,
    ) -> Result<Option<i64>> {
        let key = (market_id.to_owned(), shard_id);
        let watermark_ms = {
            let market = self.markets.entry(key).or_default();
            if let Some(timestamp_ms) = timestamp_ms {
                // Exchange timestamps are evidence, not a sequence number.
                // Delivery order is the causal order; the timestamp field is
                // therefore a max-aggregated lower bound for that market on
                // this stream.
                market.watermark_ms = Some(
                    market
                        .watermark_ms
                        .map_or(timestamp_ms, |previous| previous.max(timestamp_ms)),
                );
            }
            market.watermark_ms
        };

        if let Some(watermark_ms) = watermark_ms {
            self.advance_market_through(market_id, shard_id, watermark_ms, excluded_tokens)?;
        }
        Ok(watermark_ms)
    }

    fn advance_market_through(
        &mut self,
        market_id: &str,
        shard_id: u64,
        watermark_ms: i64,
        excluded_tokens: &HashSet<String>,
    ) -> Result<()> {
        let token_ids = self
            .markets
            .get(&(market_id.to_owned(), shard_id))
            .map(|market| {
                market
                    .token_ids
                    .iter()
                    .filter(|token_id| !excluded_tokens.contains(*token_id))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for token_id in token_ids {
            let is_live = matches!(
                self.tokens.get(&token_id),
                Some(TokenState::Watched(WatchedTokenState {
                    book: BookState::Live(_),
                    ..
                }))
            );
            if is_live {
                self.advance_memory(&token_id, watermark_ms)?;
            }
        }
        Ok(())
    }

    fn token_event_watermark(
        &mut self,
        token_id: &str,
        market_watermark_ms: Option<i64>,
    ) -> Result<i64> {
        self.ensure_memory(token_id)?;
        let current = self
            .tokens
            .get(token_id)
            .and_then(|state| state.pressure().memory())
            .and_then(PressureFrontierMemory::valid_through_ms);

        match (current, market_watermark_ms) {
            (Some(current), Some(market)) => Ok(current.max(market)),
            (Some(current), None) => Ok(current),
            (None, Some(market)) => Ok(market),
            (None, None) => Err(anyhow!(
                "continuous event for token {} has no causal watermark",
                short_token(token_id)
            )),
        }
    }

    fn advance_memory(&mut self, token_id: &str, valid_through_ms: i64) -> Result<()> {
        self.ensure_memory(token_id)?;
        let state = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| anyhow!("cannot advance unknown token {token_id}"))?;
        let memory = state.pressure_mut().memory_or_default()?;
        let valid_through_ms = memory
            .valid_through_ms()
            .map_or(valid_through_ms, |current| current.max(valid_through_ms));

        if !memory.observe_through(valid_through_ms)? {
            return Ok(());
        }

        state
            .pending_pressure_mutations_mut()
            .push(RecorderPressureMutation::Advance { valid_through_ms });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn update_memory_snapshot(
        &mut self,
        token_id: &str,
        levels: Vec<PressureLevel>,
        valid_through_ms: i64,
    ) -> Result<()> {
        self.ensure_memory(token_id)?;
        let state = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| anyhow!("cannot update unknown token {token_id}"))?;
        let memory = state.pressure_mut().memory_or_default()?;
        let valid_through_ms = memory
            .valid_through_ms()
            .map_or(valid_through_ms, |current| current.max(valid_through_ms));

        if !memory.observe_levels(&levels, valid_through_ms)? {
            return Ok(());
        }

        state
            .pending_pressure_mutations_mut()
            .push(RecorderPressureMutation::Replace {
                valid_through_ms,
                levels,
            });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn update_memory_continuous_replace(
        &mut self,
        token_id: &str,
        levels: Vec<PressureLevel>,
        valid_through_ms: i64,
    ) -> Result<()> {
        self.ensure_memory(token_id)?;
        let state = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| anyhow!("cannot update unknown token {token_id}"))?;
        let memory = state.pressure_mut().memory_or_default()?;
        let valid_through_ms = memory
            .valid_through_ms()
            .map_or(valid_through_ms, |current| current.max(valid_through_ms));

        if !memory.replace_continuous(&levels, valid_through_ms)? {
            return Ok(());
        }

        state
            .pending_pressure_mutations_mut()
            .push(RecorderPressureMutation::ReplaceContinuous {
                valid_through_ms,
                levels,
            });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn update_memory_changes(
        &mut self,
        token_id: &str,
        changes: Vec<PressureLevel>,
        valid_through_ms: i64,
    ) -> Result<()> {
        self.ensure_memory(token_id)?;
        let state = self
            .tokens
            .get_mut(token_id)
            .ok_or_else(|| anyhow!("cannot update unknown token {token_id}"))?;
        let memory = state.pressure_mut().memory_or_default()?;
        let valid_through_ms = memory
            .valid_through_ms()
            .map_or(valid_through_ms, |current| current.max(valid_through_ms));

        if !memory.update_levels(&changes, valid_through_ms)? {
            return Ok(());
        }

        state
            .pending_pressure_mutations_mut()
            .push(RecorderPressureMutation::Update {
                valid_through_ms,
                changes,
            });
        self.finish_memory_update(token_id, valid_through_ms);
        Ok(())
    }

    fn complete_token(
        &mut self,
        token_id: &str,
        unbounded: bool,
        resolved_at_ms: Option<i64>,
    ) -> Result<bool> {
        if !self.tokens.contains_key(token_id) {
            return Ok(false);
        }
        self.ensure_memory(token_id)?;

        let Some(state) = self.tokens.remove(token_id) else {
            return Ok(false);
        };
        let TokenState::Watched(mut state) = state else {
            self.tokens.insert(token_id.to_owned(), state);
            return Ok(false);
        };

        let memory = state.pressure.memory_or_default()?;
        let current_watermark_ms = memory.valid_through_ms();
        let pressure_resolved_at_ms = if unbounded {
            match (current_watermark_ms, resolved_at_ms) {
                (Some(current), Some(resolved)) if resolved < current => None,
                _ => resolved_at_ms,
            }
        } else {
            resolved_at_ms
        };

        if unbounded {
            memory.resolve_unbounded(pressure_resolved_at_ms)?;
        } else {
            memory.resolve_zero_future(pressure_resolved_at_ms)?;
        }

        if state.recording_since_ms.is_none() {
            state.recording_since_ms = current_watermark_ms.or(resolved_at_ms);
        }

        self.tokens.insert(
            token_id.to_owned(),
            TokenState::Completed(CompletedTokenState {
                recording_since_ms: state.recording_since_ms,
                pressure: state.pressure,
                pending_pressure_mutations: state.pending_pressure_mutations,
            }),
        );
        self.dirty.insert(token_id.to_owned());
        Ok(true)
    }

    fn finish_memory_update(&mut self, token_id: &str, valid_through_ms: i64) {
        let Some(state) = self.tokens.get_mut(token_id) else {
            return;
        };
        if state.recording_since_ms().is_none() {
            *state.recording_since_ms_mut() = Some(valid_through_ms);
            info!(token = %short_token(token_id), "recorded first snapshot");
        }
        self.dirty.insert(token_id.to_owned());
    }

    fn ensure_memory(&mut self, token_id: &str) -> Result<()> {
        let should_load = self
            .tokens
            .get(token_id)
            .is_some_and(|state| matches!(state.pressure(), PressureState::Stored));
        if !should_load {
            return Ok(());
        }

        let pressure = self.store.load_pressure(token_id)?;
        let Some(state) = self.tokens.get_mut(token_id) else {
            return Ok(());
        };

        match pressure {
            Some(snapshot) => {
                *state.pressure_mut() =
                    PressureState::Loaded(PressureFrontierMemory::restore(snapshot)?);
            }
            None => {
                *state.pressure_mut() = PressureState::Missing;
                *state.recording_since_ms_mut() = None;
                self.dirty.insert(token_id.to_owned());
            }
        }
        Ok(())
    }

    fn flush_dirty(&mut self) -> Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }

        let token_ids = self.dirty.iter().cloned().collect::<Vec<_>>();
        let mut writes = Vec::with_capacity(token_ids.len());

        for token_id in &token_ids {
            let state = self
                .tokens
                .get(token_id)
                .ok_or_else(|| anyhow!("dirty token {token_id} has no state"))?;
            let mutations = state.pending_pressure_mutations().to_vec();
            let completed = state.is_completed();
            let status = state.status();
            let recording_since_ms = state.recording_since_ms();
            let should_checkpoint =
                completed || self.store.should_checkpoint(token_id, mutations.len())?;

            let (mutations, checkpoint) = if should_checkpoint {
                self.ensure_memory(token_id)?;
                let snapshot = self
                    .tokens
                    .get(token_id)
                    .and_then(|state| state.pressure().memory())
                    .map(PressureFrontierMemory::snapshot);
                (Vec::new(), RecorderCheckpointWrite::Replace(snapshot))
            } else {
                (mutations, RecorderCheckpointWrite::Keep)
            };

            writes.push(RecorderStoreWriteRecord {
                token_id: token_id.clone(),
                status,
                recording_since_ms,
                mutations,
                checkpoint,
            });
        }

        let stats = self.store.write(&writes)?;
        for token_id in token_ids {
            if let Some(state) = self.tokens.get_mut(&token_id) {
                state.pending_pressure_mutations_mut().clear();
            }
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
) -> Result<Vec<PressureLevel>> {
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
            vec![PressureLevel {
                price: 6_000,
                shares: 12.0,
            }]
        );
    }
}
