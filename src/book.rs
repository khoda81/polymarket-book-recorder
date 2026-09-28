use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

use crate::{
    fees::FeeSchedule,
    pressure::{PRICE_SCALE, PressureLevel},
};

#[derive(Debug, Clone, Deserialize)]
pub struct RawBookLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AskBook {
    levels: BTreeMap<u16, f64>,
}

impl AskBook {
    pub fn from_snapshot(levels: &[RawBookLevel]) -> Result<Self> {
        let mut book = Self::default();
        for level in levels {
            book.set_level(parse_price(&level.price)?, parse_shares(&level.size)?)?;
        }
        Ok(book)
    }

    pub fn set_level(&mut self, price: u16, shares: f64) -> Result<()> {
        ensure!(price <= PRICE_SCALE, "price ticks must be in [0, 10000]");
        ensure!(
            shares.is_finite() && shares >= 0.0,
            "book shares must be finite and non-negative"
        );

        if shares > 0.0 {
            self.levels.insert(price, shares);
        } else {
            self.levels.remove(&price);
        }
        Ok(())
    }

    /// Project the raw venue book into fee-adjusted taker-price space.
    ///
    /// Multiple raw prices may quantize into the same effective 1e-4 bucket;
    /// they are one pressure level and their shares are summed.
    pub fn pressure_levels(&self, fee: FeeSchedule) -> Result<Vec<PressureLevel>> {
        let mut projected = BTreeMap::<u16, f64>::new();
        for (&raw_price, &shares) in &self.levels {
            if raw_price == 0 {
                continue;
            }
            let price = fee.effective_ask_tick(raw_price)?;
            *projected.entry(price).or_default() += shares;
        }
        Ok(projected
            .into_iter()
            .map(|(price, shares)| PressureLevel { price, shares })
            .collect())
    }

    /// Aggregate the current shares at one effective bucket.
    pub fn pressure_level_at(
        &self,
        effective_price: u16,
        fee: FeeSchedule,
    ) -> Result<PressureLevel> {
        ensure!(
            effective_price > 0 && effective_price <= PRICE_SCALE,
            "effective pressure price must be in [1, {PRICE_SCALE}]"
        );
        let mut shares = 0.0;
        for (&raw_price, &raw_shares) in &self.levels {
            if raw_price > 0 && fee.effective_ask_tick(raw_price)? == effective_price {
                shares += raw_shares;
            }
        }
        Ok(PressureLevel {
            price: effective_price,
            shares,
        })
    }

    pub fn apply_change(
        &mut self,
        side: &str,
        price: &str,
        size: &str,
    ) -> Result<Option<u16>> {
        match side {
            // Bids belong to the reverse token edge. They still matter as an
            // observation timestamp to PressureFrontierMemory, but there is no
            // token-local pressure level to retain here.
            "BUY" => Ok(None),
            "SELL" => {
                let price = parse_price(price)?;
                let shares = parse_shares(size)?;
                self.set_level(price, shares)?;
                Ok((price > 0).then_some(price))
            }
            other => bail!("unsupported order side: {other}"),
        }
    }
}

/// Parse Polymarket's canonical [0,1] decimal without passing through a float.
pub fn parse_price(value: &str) -> Result<u16> {
    let value = value.trim();
    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) => {
            ensure!(!fraction.is_empty(), "invalid price: {value}");
            (whole, Some(fraction))
        }
        None => (value, None),
    };

    ensure!(whole == "0" || whole == "1", "invalid price: {value}");

    let fraction = fraction.unwrap_or("");
    ensure!(
        fraction.len() <= 4
            && (fraction.is_empty() || fraction.bytes().all(|byte| byte.is_ascii_digit())),
        "invalid price: {value}"
    );

    if whole == "1" {
        ensure!(
            fraction.bytes().all(|byte| byte == b'0'),
            "price must be in [0, 1]: {value}"
        );
        return Ok(PRICE_SCALE);
    }

    let mut ticks = 0_u16;
    let mut multiplier = 1_000_u16;
    for byte in fraction.bytes() {
        ticks += u16::from(byte - b'0') * multiplier;
        multiplier /= 10;
    }
    Ok(ticks)
}

pub fn parse_shares(value: &str) -> Result<f64> {
    let shares = value
        .parse::<f64>()
        .with_context(|| format!("invalid book size: {value}"))?;
    ensure!(
        shares.is_finite() && shares >= 0.0,
        "invalid book size: {value}"
    );
    Ok(shares)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_price_parser_matches_exchange_ticks() {
        assert_eq!(parse_price("0").unwrap(), 0);
        assert_eq!(parse_price("0.5").unwrap(), 5_000);
        assert_eq!(parse_price("0.0001").unwrap(), 1);
        assert_eq!(parse_price("0.1234").unwrap(), 1_234);
        assert_eq!(parse_price("1").unwrap(), 10_000);
        assert_eq!(parse_price("1.0000").unwrap(), 10_000);

        for invalid in ["", ".5", "00.5", "0.", "0.00001", "1.0001", "2", "NaN"] {
            assert!(parse_price(invalid).is_err(), "{invalid} should fail");
        }
    }

    #[test]
    fn ask_book_keeps_raw_prices_but_projects_effective_pressure() {
        let fee = FeeSchedule::new("0.04".parse().unwrap(), 1).unwrap();
        let mut book = AskBook::default();
        assert_eq!(book.apply_change("BUY", "0.5", "12").unwrap(), None);

        assert_eq!(
            book.apply_change("SELL", "0.5", "12").unwrap(),
            Some(5_000)
        );
        assert_eq!(
            book.pressure_levels(fee).unwrap(),
            vec![PressureLevel {
                price: 5_100,
                shares: 12.0,
            }]
        );

        book.apply_change("SELL", "0.5", "0").unwrap();
        assert!(book.pressure_levels(fee).unwrap().is_empty());
    }

    #[test]
    fn effective_tick_collisions_sum_raw_levels() {
        let fee = FeeSchedule::new("0.04".parse().unwrap(), 1).unwrap();
        let mut book = AskBook::default();

        // Find two adjacent raw ticks that collide after effective-price
        // quantization; the projection must expose one aggregate level.
        let pair = (1..PRICE_SCALE)
            .find(|&raw| {
                fee.effective_ask_tick(raw).unwrap()
                    == fee.effective_ask_tick(raw + 1).unwrap()
            })
            .expect("real fee curve should contain a quantization collision");

        book.set_level(pair, 3.0).unwrap();
        book.set_level(pair + 1, 5.0).unwrap();

        let effective = fee.effective_ask_tick(pair).unwrap();
        assert_eq!(
            book.pressure_levels(fee).unwrap(),
            vec![PressureLevel {
                price: effective,
                shares: 8.0,
            }]
        );
        assert_eq!(
            book.pressure_level_at(effective, fee).unwrap(),
            PressureLevel {
                price: effective,
                shares: 8.0,
            }
        );
    }
}
