use std::{collections::HashMap, str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Number;

use crate::pressure::PRICE_SCALE;

pub const DEFAULT_CLOB_REST_URL: &str = "https://clob.polymarket.com";

const REQUEST_ATTEMPTS: usize = 6;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(8);

/// Immutable taker-fee schedule attached to one CLOB market.
///
/// Polymarket's platform fee per gross matched share is:
///     rate * (p * (1 - p)) ^ exponent
///
/// Pressure uses the taker's effective BUY price, so asks are projected to:
///     p_effective = p + fee_per_share
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeSchedule {
    rate: Decimal,
    exponent: u32,
}

impl FeeSchedule {
    pub const ZERO: Self = Self {
        rate: Decimal::ZERO,
        exponent: 0,
    };

    pub fn new(rate: Decimal, exponent: u32) -> Result<Self> {
        ensure!(rate >= Decimal::ZERO, "fee rate must be non-negative");
        Ok(Self { rate, exponent })
    }

    pub fn from_persisted(rate: &str, exponent: u32) -> Result<Self> {
        Self::new(
            Decimal::from_str(rate)
                .with_context(|| format!("invalid persisted fee rate: {rate}"))?,
            exponent,
        )
    }

    pub fn rate_string(self) -> String {
        self.rate.normalize().to_string()
    }

    pub fn exponent(self) -> u32 {
        self.exponent
    }

    /// Map a raw Polymarket ask tick to the chart/storage tick whose interval
    /// upper edge contains the fee-adjusted taker price:
    ///     (0, 1e-4], (1e-4, 2e-4], ...
    pub fn effective_ask_tick(self, raw_tick: u16) -> Result<u16> {
        ensure!(
            raw_tick <= PRICE_SCALE,
            "raw price tick must be in [0, {PRICE_SCALE}]"
        );
        if raw_tick == 0 || self.rate.is_zero() {
            return Ok(raw_tick);
        }

        let scale = Decimal::from(PRICE_SCALE);
        let price = Decimal::from(raw_tick) / scale;
        let base = price * (Decimal::ONE - price);
        let mut curve = Decimal::ONE;
        for _ in 0..self.exponent {
            curve = curve.checked_mul(base).context("fee curve overflow")?;
        }

        let fee_per_share = self
            .rate
            .checked_mul(curve)
            .context("fee calculation overflow")?;
        let effective_ticks = (price + fee_per_share)
            .checked_mul(scale)
            .context("effective price overflow")?
            .ceil();

        let tick = effective_ticks
            .to_u16()
            .context("effective price tick does not fit u16")?;
        ensure!(
            tick <= PRICE_SCALE,
            "effective taker price exceeds 1.0: raw_tick={raw_tick}, rate={}, exponent={}",
            self.rate,
            self.exponent
        );
        Ok(tick)
    }

    pub fn validate_monotone(self) -> Result<()> {
        let mut previous = 0;
        for raw_tick in 0..=PRICE_SCALE {
            let effective = self.effective_ask_tick(raw_tick)?;
            ensure!(
                effective >= previous,
                "fee-adjusted ask mapping is not monotone at raw tick {raw_tick}"
            );
            previous = effective;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MarketFeeInfo {
    pub condition_id: String,
    pub schedule: FeeSchedule,
    pub token_ids: Vec<String>,
}

#[derive(Clone)]
pub struct FeeLookup {
    client: Client,
    base_url: String,
}

impl FeeLookup {
    pub async fn resolve_token(&self, token_id: &str) -> Result<MarketFeeInfo> {
        let response = self
            .get_json::<MarketByTokenWire>(
                &format!("/markets-by-token/{token_id}"),
                "resolving Polymarket market by token",
            )
            .await?;
        self.fetch_market(&response.condition_id).await
    }

    pub async fn fetch_market(&self, condition_id: &str) -> Result<MarketFeeInfo> {
        let response = self
            .get_json::<MarketInfoWire>(
                &format!("/clob-markets/{condition_id}"),
                "fetching Polymarket CLOB market info",
            )
            .await?;

        let schedule = response
            .fee
            .map(FeeSchedule::try_from)
            .transpose()?
            .unwrap_or(FeeSchedule::ZERO);
        schedule.validate_monotone()?;

        Ok(MarketFeeInfo {
            condition_id: condition_id.to_owned(),
            schedule,
            token_ids: response
                .tokens
                .into_iter()
                .map(|token| token.token_id)
                .collect(),
        })
    }

    async fn get_json<T>(&self, path: &str, context: &'static str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut delay = INITIAL_RETRY_DELAY;

        for attempt in 1..=REQUEST_ATTEMPTS {
            match self.client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return response
                            .json::<T>()
                            .await
                            .with_context(|| format!("decoding Polymarket response from {url}"));
                    }

                    if attempt < REQUEST_ATTEMPTS && is_retryable_status(status) {
                        let retry_delay = retry_after(&response).unwrap_or(delay);
                        tokio::time::sleep(retry_delay).await;
                        delay = delay.saturating_mul(2).min(MAX_RETRY_DELAY);
                        continue;
                    }

                    return Err(response
                        .error_for_status()
                        .expect_err("non-success status must fail"))
                    .with_context(|| {
                        format!("{context}: {url} failed on attempt {attempt}/{REQUEST_ATTEMPTS}")
                    });
                }
                Err(error) => {
                    if attempt == REQUEST_ATTEMPTS {
                        return Err(error).with_context(|| {
                            format!("{context}: {url} failed after {REQUEST_ATTEMPTS} attempts")
                        });
                    }

                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(MAX_RETRY_DELAY);
                }
            }
        }

        unreachable!("retry loop always returns on its final attempt")
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

pub struct FeeResolver {
    client: Client,
    base_url: String,
    by_market: HashMap<String, MarketFeeInfo>,
    market_by_token: HashMap<String, String>,
}

impl FeeResolver {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_CLOB_REST_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            by_market: HashMap::new(),
            market_by_token: HashMap::new(),
        }
    }

    pub fn seed(&mut self, market: MarketFeeInfo) {
        for token_id in &market.token_ids {
            self.market_by_token
                .insert(token_id.clone(), market.condition_id.clone());
        }
        self.by_market.insert(market.condition_id.clone(), market);
    }

    pub fn cached_for_market(&self, condition_id: &str) -> Option<&MarketFeeInfo> {
        self.by_market.get(condition_id)
    }

    pub fn cached_for_token(&self, token_id: &str) -> Option<&MarketFeeInfo> {
        let condition_id = self.market_by_token.get(token_id)?;
        self.by_market.get(condition_id)
    }

    pub fn lookup(&self) -> FeeLookup {
        FeeLookup {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
        }
    }

    pub async fn resolve_token(&mut self, token_id: &str) -> Result<MarketFeeInfo> {
        if let Some(market) = self.cached_for_token(token_id) {
            return Ok(market.clone());
        }

        let market = self.lookup().resolve_token(token_id).await?;
        self.seed(market.clone());
        Ok(market)
    }

    pub async fn fetch_market(&mut self, condition_id: &str) -> Result<MarketFeeInfo> {
        if let Some(market) = self.cached_for_market(condition_id) {
            return Ok(market.clone());
        }

        let market = self.lookup().fetch_market(condition_id).await?;
        self.seed(market.clone());
        Ok(market)
    }

    pub async fn refresh_market(&mut self, condition_id: &str) -> Result<(MarketFeeInfo, bool)> {
        let market = self.lookup().fetch_market(condition_id).await?;
        let changed = self
            .by_market
            .get(condition_id)
            .is_some_and(|old| old.schedule != market.schedule);

        self.seed(market.clone());
        Ok((market, changed))
    }
}

impl Default for FeeResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
struct MarketByTokenWire {
    condition_id: String,
}

#[derive(Debug, Deserialize)]
struct MarketInfoWire {
    #[serde(default, rename = "fd")]
    fee: Option<FeeWire>,
    #[serde(default, rename = "t")]
    tokens: Vec<MarketTokenWire>,
}

#[derive(Debug, Deserialize)]
struct MarketTokenWire {
    #[serde(rename = "t")]
    token_id: String,
}

#[derive(Debug, Deserialize)]
struct FeeWire {
    #[serde(rename = "r")]
    rate: Number,
    #[serde(rename = "e")]
    exponent: Number,
}

impl TryFrom<FeeWire> for FeeSchedule {
    type Error = anyhow::Error;

    fn try_from(value: FeeWire) -> Result<Self> {
        let rate_text = value.rate.to_string();
        let rate = Decimal::from_str(&rate_text)
            .with_context(|| format!("invalid Polymarket fee rate: {rate_text}"))?;

        let exponent_text = value.exponent.to_string();
        let exponent_decimal = Decimal::from_str(&exponent_text)
            .with_context(|| format!("invalid Polymarket fee exponent: {exponent_text}"))?;
        ensure!(
            exponent_decimal.fract().is_zero(),
            "Polymarket fee exponent must be an integer: {exponent_text}"
        );
        let exponent = exponent_decimal
            .to_u32()
            .context("Polymarket fee exponent does not fit u32")?;

        Self::new(rate, exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_fee_preserves_raw_ticks() {
        for raw in [0, 1, 1_234, 5_000, 9_999, 10_000] {
            assert_eq!(FeeSchedule::ZERO.effective_ask_tick(raw).unwrap(), raw);
        }
    }

    #[test]
    fn fee_adjusted_price_uses_upper_edge_bucket() {
        let fee = FeeSchedule::new(Decimal::from_str("0.04").unwrap(), 1).unwrap();

        // p=0.5 => fee/share=.04*.25=.01 => effective=.51 exactly.
        assert_eq!(fee.effective_ask_tick(5_000).unwrap(), 5_100);

        // p=.5001 => effective=.5100999996, so it belongs to
        // (.5100, .5101], whose upper-edge tick is 5101.
        assert_eq!(fee.effective_ask_tick(5_001).unwrap(), 5_101);
    }

    #[test]
    fn real_fee_schedules_are_monotone() {
        for (rate, exponent) in [
            ("0.03", 1),
            ("0.04", 1),
            ("0.05", 1),
            ("0.072", 1),
            ("0.25", 2),
        ] {
            FeeSchedule::new(Decimal::from_str(rate).unwrap(), exponent)
                .unwrap()
                .validate_monotone()
                .unwrap();
        }
    }

    #[test]
    fn retries_only_transient_http_statuses() {
        assert!(is_retryable_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
    }
}
