use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::polymarket::{DEFAULT_CLOB_MARKET_WS_URL, MarketEvent, parse_market_message};

// Empirically, Polymarket can silently stop producing initial book snapshots
// when a physical market websocket owns more than ~100 assets. Use one limit
// for both physical shards and wire subscribe batches so a shard cannot grow
// beyond the known-good request size.
const MAX_TOKENS_PER_SHARD: usize = 100;
const RETRY_DELAY: Duration = Duration::from_secs(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_STALE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    Market {
        shard_id: u64,
        event: MarketEvent,
        /// Present only for the first book snapshot caused by a subscribe.
        /// This send time is a causal lower bound on snapshot generation.
        snapshot_requested_at_ms: Option<i64>,
    },
    /// Text-frame payload bytes attributed evenly across the token ids carried
    /// by that frame. This intentionally measures upstream payload, not SQLite
    /// write size.
    Ingress {
        bytes_by_token: Vec<(String, usize)>,
    },
    ContinuityLost {
        shard_id: u64,
        token_ids: Vec<String>,
    },
}

#[derive(Debug)]
enum ShardCommand {
    Add(Vec<String>),
    Remove(Vec<String>),
    Stop,
}

struct Shard {
    command_tx: mpsc::Sender<ShardCommand>,
    token_ids: BTreeSet<String>,
    task: JoinHandle<()>,
}

pub struct SubscriptionPool {
    ws_url: Arc<str>,
    event_tx: mpsc::Sender<SubscriptionEvent>,
    shards: BTreeMap<u64, Shard>,
    owner_by_token: HashMap<String, u64>,
    connected_shards: Arc<AtomicUsize>,
    next_shard_id: u64,
}

impl SubscriptionPool {
    pub fn new(event_tx: mpsc::Sender<SubscriptionEvent>) -> Self {
        Self::with_url(DEFAULT_CLOB_MARKET_WS_URL, event_tx)
    }

    pub fn with_url(
        ws_url: impl Into<Arc<str>>,
        event_tx: mpsc::Sender<SubscriptionEvent>,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            event_tx,
            shards: BTreeMap::new(),
            owner_by_token: HashMap::new(),
            connected_shards: Arc::new(AtomicUsize::new(0)),
            next_shard_id: 1,
        }
    }

    pub fn active_connection_count(&self) -> usize {
        self.shards.len()
    }

    pub fn connected_connection_count(&self) -> usize {
        self.connected_shards.load(Ordering::Relaxed)
    }

    pub fn assigned_token_count(&self) -> usize {
        self.owner_by_token.len()
    }

    pub fn is_assigned(&self, token_id: &str) -> bool {
        self.owner_by_token.contains_key(token_id)
    }

    pub async fn add(&mut self, token_ids: impl IntoIterator<Item = String>) {
        let mut pending = token_ids
            .into_iter()
            .filter(|token_id| !token_id.is_empty() && !self.owner_by_token.contains_key(token_id))
            .collect::<BTreeSet<_>>();

        while !pending.is_empty() {
            let existing = self
                .shards
                .iter()
                .find(|(_, shard)| shard.token_ids.len() < MAX_TOKENS_PER_SHARD)
                .map(|(&id, _)| id);

            if let Some(shard_id) = existing {
                let capacity = MAX_TOKENS_PER_SHARD - self.shards[&shard_id].token_ids.len();
                let additions = take_first(&mut pending, capacity);
                self.assign_to_shard(shard_id, additions).await;
                continue;
            }

            let initial = take_first(&mut pending, MAX_TOKENS_PER_SHARD);
            self.create_shard(initial);
        }
    }

    pub async fn remove(&mut self, token_ids: impl IntoIterator<Item = String>) {
        let mut by_shard = BTreeMap::<u64, Vec<String>>::new();

        for token_id in token_ids {
            let Some(shard_id) = self.owner_by_token.remove(&token_id) else {
                continue;
            };
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                shard.token_ids.remove(&token_id);
            }
            by_shard.entry(shard_id).or_default().push(token_id);
        }

        for (shard_id, removed) in by_shard {
            let empty = self
                .shards
                .get(&shard_id)
                .is_none_or(|shard| shard.token_ids.is_empty());

            if empty {
                if let Some(shard) = self.shards.remove(&shard_id) {
                    let _ = shard.command_tx.send(ShardCommand::Stop).await;
                    shard.task.abort();
                }
                continue;
            }

            if let Some(shard) = self.shards.get(&shard_id) {
                for chunk in removed.chunks(MAX_TOKENS_PER_SHARD) {
                    let _ = shard
                        .command_tx
                        .send(ShardCommand::Remove(chunk.to_vec()))
                        .await;
                }
            }
        }
    }

    pub async fn stop(&mut self) {
        let shards = std::mem::take(&mut self.shards);
        self.owner_by_token.clear();
        for (_, shard) in shards {
            let _ = shard.command_tx.send(ShardCommand::Stop).await;
            shard.task.abort();
        }
    }

    async fn assign_to_shard(&mut self, shard_id: u64, additions: Vec<String>) {
        if additions.is_empty() {
            return;
        }

        let Some(shard) = self.shards.get_mut(&shard_id) else {
            return;
        };

        for token_id in &additions {
            shard.token_ids.insert(token_id.clone());
            self.owner_by_token.insert(token_id.clone(), shard_id);
        }

        for chunk in additions.chunks(MAX_TOKENS_PER_SHARD) {
            let _ = shard
                .command_tx
                .send(ShardCommand::Add(chunk.to_vec()))
                .await;
        }
    }

    fn create_shard(&mut self, initial: Vec<String>) {
        if initial.is_empty() {
            return;
        }

        let shard_id = self.next_shard_id;
        self.next_shard_id += 1;

        let token_ids = initial.iter().cloned().collect::<BTreeSet<_>>();
        for token_id in &token_ids {
            self.owner_by_token.insert(token_id.clone(), shard_id);
        }

        let (command_tx, command_rx) = mpsc::channel(32);
        let event_tx = self.event_tx.clone();
        let ws_url = self.ws_url.clone();
        let task_tokens = token_ids.clone();
        let connected_shards = self.connected_shards.clone();
        let task = tokio::spawn(async move {
            run_shard(
                shard_id,
                ws_url,
                task_tokens,
                command_rx,
                event_tx,
                connected_shards,
            )
            .await;
        });

        self.shards.insert(
            shard_id,
            Shard {
                command_tx,
                token_ids,
                task,
            },
        );
    }
}

fn take_first(values: &mut BTreeSet<String>, count: usize) -> Vec<String> {
    let selected = values.iter().take(count).cloned().collect::<Vec<_>>();
    for value in &selected {
        values.remove(value);
    }
    selected
}

async fn run_shard(
    shard_id: u64,
    ws_url: Arc<str>,
    mut token_ids: BTreeSet<String>,
    mut command_rx: mpsc::Receiver<ShardCommand>,
    event_tx: mpsc::Sender<SubscriptionEvent>,
    connected_shards: Arc<AtomicUsize>,
) {
    let mut snapshot_requested_at = HashMap::<String, i64>::new();

    'lifetime: loop {
        if token_ids.is_empty() {
            break;
        }

        let connection = connect_async(ws_url.as_ref()).await;
        let (socket, _) = match connection {
            Ok(connection) => connection,
            Err(error) => {
                warn!(shard_id, ?error, "Polymarket websocket connect failed");
                tokio::time::sleep(RETRY_DELAY).await;
                drain_commands_while_disconnected(
                    &mut token_ids,
                    &mut command_rx,
                    &mut snapshot_requested_at,
                );
                continue;
            }
        };

        let _connected_guard = ConnectedShardGuard::new(connected_shards.clone());
        debug!(
            shard_id,
            tokens = token_ids.len(),
            "Polymarket websocket connected"
        );
        let (mut sink, mut stream) = socket.split();
        let initial = json!({
            "assets_ids": token_ids.iter().collect::<Vec<_>>(),
            "custom_feature_enabled": true,
            "type": "market",
        });
        let requested_at_ms = now_ms();
        if sink
            .send(Message::Text(initial.to_string().into()))
            .await
            .is_err()
        {
            continue;
        }
        snapshot_requested_at.clear();
        for token_id in &token_ids {
            snapshot_requested_at.insert(token_id.clone(), requested_at_ms);
        }

        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_pong = Instant::now();

        let reconnect = loop {
            tokio::select! {
                command = command_rx.recv() => {
                    match command {
                        Some(ShardCommand::Add(added)) => {
                            let mut actual = Vec::new();
                            for token_id in added {
                                if token_ids.insert(token_id.clone()) {
                                    actual.push(token_id);
                                }
                            }
                            if actual.is_empty() {
                                continue;
                            }
                            let message = json!({
                                "assets_ids": actual,
                                "custom_feature_enabled": true,
                                "operation": "subscribe",
                            });
                            let requested_at_ms = now_ms();
                            if sink.send(Message::Text(message.to_string().into())).await.is_err() {
                                break true;
                            }
                            for token_id in &actual {
                                snapshot_requested_at
                                    .insert(token_id.clone(), requested_at_ms);
                            }
                        }
                        Some(ShardCommand::Remove(removed)) => {
                            let mut actual = Vec::new();
                            for token_id in removed {
                                if token_ids.remove(&token_id) {
                                    snapshot_requested_at.remove(&token_id);
                                    actual.push(token_id);
                                }
                            }
                            if token_ids.is_empty() {
                                let _ = sink.close().await;
                                break false;
                            }
                            if actual.is_empty() {
                                continue;
                            }
                            let message = json!({
                                "assets_ids": actual,
                                "operation": "unsubscribe",
                            });
                            if sink.send(Message::Text(message.to_string().into())).await.is_err() {
                                break true;
                            }
                        }
                        Some(ShardCommand::Stop) | None => {
                            let _ = sink.close().await;
                            break false;
                        }
                    }
                }

                _ = heartbeat.tick() => {
                    if last_pong.elapsed() > HEARTBEAT_STALE {
                        warn!(shard_id, "Polymarket websocket heartbeat stale");
                        break true;
                    }
                    if sink.send(Message::Text("PING".into())).await.is_err() {
                        break true;
                    }
                }

                message = stream.next() => {
                    let Some(message) = message else {
                        break true;
                    };
                    let message = match message {
                        Ok(message) => message,
                        Err(error) => {
                            warn!(shard_id, ?error, "Polymarket websocket read failed");
                            break true;
                        }
                    };

                    match message {
                        Message::Text(text) if text.as_str() == "PONG" => {
                            last_pong = Instant::now();
                        }
                        Message::Text(text) => {
                            let events = match parse_market_message(text.as_str()) {
                                Ok(events) => events,
                                Err(error) => {
                                    warn!(
                                        shard_id,
                                        ?error,
                                        "malformed Polymarket market event broke continuity"
                                    );
                                    break true;
                                }
                            };

                            let bytes_by_token = attribute_frame_bytes(text.len(), &events);

                            for event in events {
                                let snapshot_requested_at_ms = match &event {
                                    MarketEvent::Book(book) => {
                                        snapshot_requested_at.remove(&book.asset_id)
                                    }
                                    _ => None,
                                };
                                if event_tx
                                    .send(SubscriptionEvent::Market {
                                        shard_id,
                                        event,
                                        snapshot_requested_at_ms,
                                    })
                                    .await
                                    .is_err()
                                {
                                    break 'lifetime;
                                }
                            }

                            if !bytes_by_token.is_empty()
                                && event_tx
                                    .send(SubscriptionEvent::Ingress { bytes_by_token })
                                    .await
                                    .is_err()
                            {
                                break 'lifetime;
                            }
                        }
                        Message::Ping(payload) => {
                            if sink.send(Message::Pong(payload)).await.is_err() {
                                break true;
                            }
                        }
                        Message::Close(_) => break true,
                        _ => {}
                    }
                }
            }
        };

        if !reconnect {
            break;
        }

        if !token_ids.is_empty() {
            let _ = event_tx
                .send(SubscriptionEvent::ContinuityLost {
                    shard_id,
                    token_ids: token_ids.iter().cloned().collect(),
                })
                .await;
        }
        tokio::time::sleep(RETRY_DELAY).await;
        drain_commands_while_disconnected(
            &mut token_ids,
            &mut command_rx,
            &mut snapshot_requested_at,
        );
    }

    debug!(shard_id, "Polymarket websocket shard stopped");
}

fn attribute_frame_bytes(frame_bytes: usize, events: &[MarketEvent]) -> Vec<(String, usize)> {
    let mut token_ids = BTreeSet::new();

    for event in events {
        match event {
            MarketEvent::Book(event) => {
                token_ids.insert(event.asset_id.clone());
            }
            MarketEvent::PriceChange(event) => {
                token_ids.extend(event.price_changes.iter().map(|change| change.asset_id.clone()));
            }
            MarketEvent::Watermark(event) => {
                if let Some(token_id) = &event.asset_id {
                    token_ids.insert(token_id.clone());
                }
            }
            MarketEvent::MarketResolved(event) => {
                if let Some(asset_ids) = &event.assets_ids {
                    token_ids.extend(asset_ids.iter().cloned());
                }
                if let Some(token_id) = &event.winning_asset_id {
                    token_ids.insert(token_id.clone());
                }
            }
        }
    }

    if token_ids.is_empty() {
        return Vec::new();
    }

    let count = token_ids.len();
    let base = frame_bytes / count;
    let remainder = frame_bytes % count;
    token_ids
        .into_iter()
        .enumerate()
        .map(|(index, token_id)| (token_id, base + usize::from(index < remainder)))
        .collect()
}

fn drain_commands_while_disconnected(
    token_ids: &mut BTreeSet<String>,
    command_rx: &mut mpsc::Receiver<ShardCommand>,
    snapshot_requested_at: &mut HashMap<String, i64>,
) {
    while let Ok(command) = command_rx.try_recv() {
        match command {
            ShardCommand::Add(added) => {
                for token_id in added {
                    token_ids.insert(token_id);
                }
            }
            ShardCommand::Remove(removed) => {
                for token_id in removed {
                    token_ids.remove(&token_id);
                    snapshot_requested_at.remove(&token_id);
                }
            }
            ShardCommand::Stop => {
                token_ids.clear();
                snapshot_requested_at.clear();
            }
        }
    }
}

struct ConnectedShardGuard {
    count: Arc<AtomicUsize>,
}

impl ConnectedShardGuard {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self { count }
    }
}

impl Drop for ConnectedShardGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_bytes_are_attributed_once_across_unique_tokens() {
        let events = parse_market_message(
            r#"[{"event_type":"book","market":"m","asset_id":"a","asks":[],"timestamp":"1"},{"event_type":"price_change","market":"m","price_changes":[{"asset_id":"a","price":"0.5","size":"1","side":"SELL"},{"asset_id":"b","price":"0.6","size":"2","side":"SELL"}],"timestamp":"2"}]"#,
        )
        .unwrap();

        let attributed = attribute_frame_bytes(101, &events);
        assert_eq!(attributed.iter().map(|(_, bytes)| bytes).sum::<usize>(), 101);
        assert_eq!(
            attributed.iter().map(|(token_id, _)| token_id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn take_first_is_deterministic() {
        let mut values = ["c", "a", "b"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        assert_eq!(take_first(&mut values, 2), ["a", "b"]);
        assert_eq!(values.into_iter().collect::<Vec<_>>(), ["c"]);
    }
}
