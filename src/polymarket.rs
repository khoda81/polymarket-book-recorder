use anyhow::{Context, Result};
use serde::de;
use serde::Deserialize;
use serde_json::Value;

use crate::book::RawBookLevel;

pub const DEFAULT_CLOB_MARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Book(BookEvent),
    PriceChange(PriceChangeEvent),
    Watermark(MarketWatermarkEvent),
    MarketResolved(MarketResolvedEvent),
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookEvent {
    pub market: String,
    pub asset_id: String,
    #[serde(default)]
    pub asks: Vec<RawBookLevel>,
    #[serde(
        default,
        rename = "timestamp",
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceChangeEvent {
    pub market: String,
    #[serde(default)]
    pub price_changes: Vec<RawPriceChange>,
    #[serde(
        default,
        rename = "timestamp",
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketWatermarkEvent {
    pub market: String,
    #[serde(
        default,
        rename = "timestamp",
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub timestamp_ms: Option<i64>,
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
    pub market: String,
    #[serde(default)]
    pub assets_ids: Vec<String>,
    pub winning_asset_id: Option<String>,
    #[serde(
        default,
        rename = "timestamp",
        deserialize_with = "deserialize_optional_epoch_ms"
    )]
    pub timestamp_ms: Option<i64>,
}

impl MarketEvent {
    pub fn market(&self) -> &str {
        match self {
            Self::Book(event) => &event.market,
            Self::PriceChange(event) => &event.market,
            Self::Watermark(event) => &event.market,
            Self::MarketResolved(event) => &event.market,
        }
    }

    pub fn timestamp_ms(&self) -> Option<i64> {
        match self {
            Self::Book(event) => event.timestamp_ms,
            Self::PriceChange(event) => event.timestamp_ms,
            Self::Watermark(event) => event.timestamp_ms,
            Self::MarketResolved(event) => event.timestamp_ms,
        }
    }
}

pub fn parse_market_message(text: &str) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text).context("decoding market websocket JSON")?;
    let values = match value {
        Value::Array(values) => values,
        value => vec![value],
    };

    values
        .into_iter()
        .filter_map(|value| match parse_market_event(value) {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn parse_market_event(value: Value) -> Result<Option<MarketEvent>> {
    let Some(event_type) = value.get("event_type").and_then(Value::as_str) else {
        return Ok(None);
    };

    let event = match event_type {
        "book" => MarketEvent::Book(
            serde_json::from_value(value).context("decoding book market event")?,
        ),
        "price_change" => MarketEvent::PriceChange(
            serde_json::from_value(value).context("decoding price-change market event")?,
        ),
        "last_trade_price" | "tick_size_change" | "best_bid_ask" => {
            MarketEvent::Watermark(
                serde_json::from_value(value)
                    .context("decoding market watermark event")?,
            )
        }
        "market_resolved" => MarketEvent::MarketResolved(
            serde_json::from_value(value).context("decoding market-resolved event")?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(event))
}

fn deserialize_optional_epoch_ms<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    value
        .map(parse_epoch_ms)
        .transpose()
        .map_err(de::Error::custom)
}

fn deserialize_epoch_ms<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    parse_epoch_ms(value).map_err(de::Error::custom)
}

fn parse_epoch_ms(value: Value) -> std::result::Result<i64, String> {
    let timestamp = match value {
        Value::String(value) => value
            .parse::<i64>()
            .map_err(|_| format!("invalid epoch-millisecond timestamp: {value}"))?,
        Value::Number(value) => value
            .as_i64()
            .ok_or_else(|| format!("invalid epoch-millisecond timestamp: {value}"))?,
        other => return Err(format!("invalid epoch-millisecond timestamp: {other}")),
    };
    if timestamp < 0 {
        return Err(format!(
            "epoch-millisecond timestamp must be non-negative: {timestamp}"
        ));
    }
    Ok(timestamp)
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
            parse_market_message(book).unwrap().as_slice(),
            [MarketEvent::Book(_)]
        ));

        let array = format!(
            r#"[{book},{{"event_type":"last_trade_price","market":"0xabc","asset_id":"123","timestamp":"1001"}}]"#
        );
        assert_eq!(parse_market_message(&array).unwrap().len(), 2);
    }

    #[test]
    fn malformed_book_events_fail_instead_of_silently_breaking_continuity() {
        let malformed = r#"{
          "event_type":"book",
          "market":"0xabc",
          "asset_id":"123",
          "asks":[],
          "timestamp":"not-a-timestamp"
        }"#;
        assert!(parse_market_message(malformed).is_err());
    }

    #[test]
    fn resolution_requires_winner_and_timestamp() {
        let valid = r#"{
          "event_type":"market_resolved",
          "market":"0xabc",
          "assets_ids":["yes","no"],
          "winning_asset_id":"yes",
          "timestamp":"1234"
        }"#;
        let [MarketEvent::MarketResolved(event)] =
            parse_market_message(valid).unwrap().as_slice()
        else {
            panic!("expected resolution");
        };
        assert_eq!(event.winning_asset_id.as_deref(), Some("yes"));
        assert_eq!(event.timestamp_ms, Some(1_234));

        let missing_timestamp = r#"{
          "event_type":"market_resolved",
          "market":"0xabc",
          "assets_ids":["yes","no"],
          "winning_asset_id":"yes"
        }"#;
        let [MarketEvent::MarketResolved(event)] =
            parse_market_message(missing_timestamp).unwrap().as_slice()
        else {
            panic!("expected resolution");
        };
        assert_eq!(event.timestamp_ms, None);
    }
}
