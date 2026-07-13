use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Polymarket,
    PredictFun,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl FromStr for Side {
    type Err = AdapterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buy" | "bid" => Ok(Self::Buy),
            "sell" | "ask" => Ok(Self::Sell),
            other => Err(AdapterError::InvalidField(format!(
                "unsupported side: {other}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Instrument {
    pub market_id: String,
    pub outcome_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderIntent {
    pub slot_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default)]
    pub reduce_only: bool,
}

impl OrderIntent {
    pub fn notional(&self) -> Decimal {
        self.price * self.quantity
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LiveOrder {
    pub order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_slot: Option<String>,
    pub account_id: String,
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub filled_quantity: Decimal,
    pub status: OrderStatus,
    #[serde(default)]
    pub post_only: bool,
}

impl LiveOrder {
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Open | OrderStatus::PartiallyFilled
        )
    }
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("invalid adapter field: {0}")]
    InvalidField(String),
    #[error("adapter venue mismatch: expected {expected:?}, got {actual:?}")]
    VenueMismatch { expected: Venue, actual: Venue },
}

pub trait VenueAdapter<RawOrder> {
    fn venue(&self) -> Venue;
    fn normalize_live_order(&self, raw: RawOrder) -> Result<LiveOrder, AdapterError>;
    fn normalize_intent(&self, intent: &OrderIntent) -> Result<OrderIntent, AdapterError>;
}

pub fn quantize_down(value: Decimal, step: Decimal) -> Result<Decimal, AdapterError> {
    if step <= Decimal::ZERO {
        return Err(AdapterError::InvalidField(
            "quantization step must be positive".to_string(),
        ));
    }
    Ok((value / step).floor() * step)
}

pub fn quantize_up(value: Decimal, step: Decimal) -> Result<Decimal, AdapterError> {
    if step <= Decimal::ZERO {
        return Err(AdapterError::InvalidField(
            "quantization step must be positive".to_string(),
        ));
    }
    Ok((value / step).ceil() * step)
}

pub fn parse_decimal(field: &str, value: &str) -> Result<Decimal, AdapterError> {
    Decimal::from_str(value).map_err(|error| {
        AdapterError::InvalidField(format!("{field}={value:?} is not decimal: {error}"))
    })
}
