use std::str::FromStr;

use maker_domain::{
    parse_decimal, quantize_down, quantize_up, AdapterError, Instrument, LiveOrder, OrderIntent,
    OrderStatus, Side, Venue, VenueAdapter,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct PredictFunAdapter {
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PredictFunOpenOrder {
    pub id: String,
    pub account_id: String,
    pub market_id: String,
    pub outcome: String,
    pub side: String,
    pub price: String,
    pub size: String,
    #[serde(default = "zero_string")]
    pub filled_size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    #[serde(default = "open_string")]
    pub status: String,
    #[serde(default)]
    pub post_only: bool,
}

fn zero_string() -> String {
    "0".to_string()
}

fn open_string() -> String {
    "open".to_string()
}

impl VenueAdapter<PredictFunOpenOrder> for PredictFunAdapter {
    fn venue(&self) -> Venue {
        Venue::PredictFun
    }

    fn normalize_live_order(&self, raw: PredictFunOpenOrder) -> Result<LiveOrder, AdapterError> {
        Ok(LiveOrder {
            order_id: raw.id,
            managed_slot: raw.intent_id,
            account_id: raw.account_id,
            venue: Venue::PredictFun,
            instrument: Instrument {
                market_id: raw.market_id,
                outcome_id: raw.outcome,
                token_id: None,
            },
            side: Side::from_str(&raw.side)?,
            price: parse_decimal("price", &raw.price)?,
            quantity: parse_decimal("size", &raw.size)?,
            filled_quantity: parse_decimal("filled_size", &raw.filled_size)?,
            status: parse_status(&raw.status)?,
            post_only: raw.post_only,
        })
    }

    fn normalize_intent(&self, intent: &OrderIntent) -> Result<OrderIntent, AdapterError> {
        if intent.venue != Venue::PredictFun {
            return Err(AdapterError::VenueMismatch {
                expected: Venue::PredictFun,
                actual: intent.venue,
            });
        }
        let mut normalized = intent.clone();
        normalized.price = match normalized.side {
            Side::Buy => quantize_down(normalized.price, self.price_tick)?,
            Side::Sell => quantize_up(normalized.price, self.price_tick)?,
        };
        normalized.quantity = quantize_down(normalized.quantity, self.quantity_step)?;
        if normalized.price <= Decimal::ZERO || normalized.quantity <= Decimal::ZERO {
            return Err(AdapterError::InvalidField(
                "normalized price and quantity must be positive".to_string(),
            ));
        }
        Ok(normalized)
    }
}

fn parse_status(value: &str) -> Result<OrderStatus, AdapterError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "open" | "active" => Ok(OrderStatus::Open),
        "partially_filled" | "partial" => Ok(OrderStatus::PartiallyFilled),
        "filled" => Ok(OrderStatus::Filled),
        "cancelled" | "canceled" => Ok(OrderStatus::Cancelled),
        "rejected" | "failed" => Ok(OrderStatus::Rejected),
        other => Err(AdapterError::InvalidField(format!(
            "unsupported Predict.fun order status: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::VenueAdapter;

    use super::*;

    fn decimal(value: i64, scale: u32) -> Decimal {
        Decimal::new(value, scale)
    }

    #[test]
    fn normalizes_predictfun_open_order() {
        let adapter = PredictFunAdapter {
            price_tick: decimal(5, 3),
            quantity_step: decimal(1, 0),
        };
        let order = adapter
            .normalize_live_order(PredictFunOpenOrder {
                id: "pf-order-1".to_string(),
                account_id: "pf-account-1".to_string(),
                market_id: "42".to_string(),
                outcome: "YES".to_string(),
                side: "ask".to_string(),
                price: "0.615".to_string(),
                size: "20".to_string(),
                filled_size: "2".to_string(),
                intent_id: Some("pf-slot-1".to_string()),
                status: "partial".to_string(),
                post_only: true,
            })
            .unwrap();

        assert_eq!(order.venue, Venue::PredictFun);
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert_eq!(order.instrument.market_id, "42");
    }
}
