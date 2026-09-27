use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::book::RawBookLevel;

pub const DEFAULT_CLOB_REST_URL: &str = "https://clob.polymarket.com";
pub const DEFAULT_CLOB_MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub struct PolymarketRestClient {
    client: Client,
    base_url: String,
}

impl Default for PolymarketRestClient {
    fn default() -> Self {
        Self::new(DEFAULT_CLOB_REST_URL)
    }
}

impl PolymarketRestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        }
    }

    pub async fn fetch_order_books(&self, token_ids: &[String]) -> Result<Vec<RawOrderBook>> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }

        let body = token_ids
            .iter()
            .map(|token_id| TokenRequest {
                token_id: token_id.as_str(),
            })
            .collect::<Vec<_>>();

        self.client
            .post(format!("{}/books", self.base_url))
            .json(&body)
            .send()
            .await
            .context("requesting Polymarket order books")?
            .error_for_status()
            .context("Polymarket order-books request failed")?
            .json::<Vec<RawOrderBook>>()
            .await
            .context("decoding Polymarket order books")
    }
}

#[derive(Debug, Serialize)]
struct TokenRequest<'a> {
    token_id: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawOrderBook {
    pub asset_id: String,
    #[serde(default)]
    pub bids: Vec<RawBookLevel>,
    #[serde(default)]
    pub asks: Vec<RawBookLevel>,
    pub timestamp: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Book(BookEvent),
    PriceChange(PriceChangeEvent),
    MarketResolved(MarketResolvedEvent),
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookEvent {
    pub asset_id: String,
    #[serde(default)]
    pub bids: Vec<RawBookLevel>,
    #[serde(default)]
    pub asks: Vec<RawBookLevel>,
    pub timestamp: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceChangeEvent {
    #[serde(default)]
    pub price_changes: Vec<RawPriceChange>,
    pub timestamp: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawPriceChange {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketResolvedEvent {
    #[serde(default)]
    pub assets_ids: Option<Vec<String>>,
    pub timestamp: Option<Value>,
}

pub fn parse_market_message(text: &str) -> Vec<MarketEvent> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };

    match value {
        Value::Array(values) => values.into_iter().filter_map(parse_market_event).collect(),
        value => parse_market_event(value).into_iter().collect(),
    }
}

fn parse_market_event(value: Value) -> Option<MarketEvent> {
    let event_type = value.get("event_type")?.as_str()?;
    match event_type {
        "book" => serde_json::from_value(value).ok().map(MarketEvent::Book),
        "price_change" => serde_json::from_value(value)
            .ok()
            .map(MarketEvent::PriceChange),
        "market_resolved" => serde_json::from_value(value)
            .ok()
            .map(MarketEvent::MarketResolved),
        _ => None,
    }
}

/// Match the TypeScript recorder's timestamp guard.
///
/// Exchange timestamps are epoch milliseconds encoded as strings. A missing,
/// invalid, negative, or implausibly-future timestamp falls back to the local
/// observation time.
pub fn event_timestamp_ms(value: Option<&Value>, fallback_ms: i64) -> i64 {
    let timestamp = match value {
        Some(Value::String(value)) => value.parse::<f64>().ok(),
        Some(Value::Number(value)) => value.as_f64(),
        _ => None,
    };

    let Some(timestamp) = timestamp else {
        return fallback_ms;
    };
    if !timestamp.is_finite() || timestamp < 0.0 || timestamp > fallback_ms as f64 + 60_000.0 {
        return fallback_ms;
    }

    timestamp.trunc() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_object_and_array_market_frames() {
        let book = r#"{
          "event_type":"book",
          "market":"0xabc",
          "asset_id":"123",
          "bids":[],
          "asks":[{"price":"0.55","size":"10"}],
          "timestamp":"1000"
        }"#;
        assert!(matches!(
            parse_market_message(book).as_slice(),
            [MarketEvent::Book(_)]
        ));

        let array = format!(r#"[{book},{{"event_type":"last_trade_price","asset_id":"123"}}]"#);
        assert_eq!(parse_market_message(&array).len(), 1);
    }

    #[test]
    fn timestamp_rejects_implausible_future_values() {
        assert_eq!(
            event_timestamp_ms(Some(&Value::String("1500".into())), 1000),
            1500
        );
        assert_eq!(
            event_timestamp_ms(Some(&Value::String("999999".into())), 1000),
            1000
        );
        assert_eq!(event_timestamp_ms(None, 1000), 1000);
    }
}
