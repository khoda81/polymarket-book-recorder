use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

use crate::pressure::{PressureLevel, PRICE_SCALE, PressureLevel};

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

    pub fn pressure_levels(&self) -> Vec<PressureLevel> {
        self.levels
            .iter()
            .map(|(&price, &shares)| PressureLevel { price, shares })
            .collect()
    }

    pub fn apply_change(
        &mut self,
        side: &str,
        price: &str,
        size: &str,
    ) -> Result<Option<PressureLevel>> {
        match side {
            // Bids belong to the reverse token edge. They still matter as an
            // observation timestamp to PressureFrontierMemory, but there is no
            // token-local pressure level to retain here.
            "BUY" => Ok(None),
            "SELL" => {
                let price = parse_price(price)?;
                let shares = parse_shares(size)?;
                self.set_level(price, shares)?;
                Ok(Some(PressureLevel { price, shares }))
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
    fn ask_book_ignores_bids_but_applies_sells() {
        let mut book = AskBook::default();
        assert_eq!(book.apply_change("BUY", "0.5", "12").unwrap(), None);

        assert_eq!(
            book.apply_change("SELL", "0.5", "12").unwrap(),
            Some(PressureLevel {
                price: 5_000,
                shares: 12.0,
            })
        );
        assert_eq!(
            book.pressure_levels(),
            vec![PressureLevel {
                price: 5_000,
                shares: 12.0,
            }]
        );

        book.apply_change("SELL", "0.5", "0").unwrap();
        assert!(book.pressure_levels().is_empty());
    }
}
