use std::str::FromStr;

use maker_domain::{
    parse_decimal, quantize_down, quantize_up, AdapterError, Instrument, LiveOrder, OrderIntent,
    OrderStatus, Side, Venue, VenueAdapter,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct PolymarketAdapter {
    pub price_tick: Decimal,
    pub quantity_step: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PolymarketOpenOrder {
    pub id: String,
    pub account_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub outcome: String,
    pub side: String,
    pub price: String,
    pub original_size: String,
    #[serde(default = "zero_string")]
    pub size_matched: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
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

impl VenueAdapter<PolymarketOpenOrder> for PolymarketAdapter {
    fn venue(&self) -> Venue {
        Venue::Polymarket
    }

    fn normalize_live_order(&self, raw: PolymarketOpenOrder) -> Result<LiveOrder, AdapterError> {
        Ok(LiveOrder {
            order_id: raw.id,
            managed_slot: raw.client_order_id,
            account_id: raw.account_id,
            venue: Venue::Polymarket,
            instrument: Instrument {
                market_id: raw.condition_id,
                outcome_id: raw.outcome,
                token_id: Some(raw.token_id),
            },
            side: Side::from_str(&raw.side)?,
            price: parse_decimal("price", &raw.price)?,
            quantity: parse_decimal("original_size", &raw.original_size)?,
            filled_quantity: parse_decimal("size_matched", &raw.size_matched)?,
            status: parse_status(&raw.status)?,
            post_only: raw.post_only,
        })
    }

    fn normalize_intent(&self, intent: &OrderIntent) -> Result<OrderIntent, AdapterError> {
        if intent.venue != Venue::Polymarket {
            return Err(AdapterError::VenueMismatch {
                expected: Venue::Polymarket,
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
        "open" | "live" => Ok(OrderStatus::Open),
        "partially_filled" | "matched" => Ok(OrderStatus::PartiallyFilled),
        "filled" => Ok(OrderStatus::Filled),
        "cancelled" | "canceled" => Ok(OrderStatus::Cancelled),
        "rejected" => Ok(OrderStatus::Rejected),
        other => Err(AdapterError::InvalidField(format!(
            "unsupported Polymarket order status: {other}"
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
    fn normalizes_polymarket_open_order() {
        let adapter = PolymarketAdapter {
            price_tick: decimal(1, 2),
            quantity_step: decimal(1, 1),
        };
        let order = adapter
            .normalize_live_order(PolymarketOpenOrder {
                id: "order-1".to_string(),
                account_id: "account-1".to_string(),
                condition_id: "condition-1".to_string(),
                token_id: "token-yes".to_string(),
                outcome: "YES".to_string(),
                side: "BUY".to_string(),
                price: "0.47".to_string(),
                original_size: "25".to_string(),
                size_matched: "5".to_string(),
                client_order_id: Some("slot-1".to_string()),
                status: "open".to_string(),
                post_only: true,
            })
            .unwrap();

        assert_eq!(order.venue, Venue::Polymarket);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.managed_slot.as_deref(), Some("slot-1"));
        assert_eq!(order.filled_quantity, decimal(5, 0));
    }

    #[test]
    fn quantizes_buy_down_and_sell_up() {
        let adapter = PolymarketAdapter {
            price_tick: decimal(1, 2),
            quantity_step: decimal(1, 1),
        };
        let base = OrderIntent {
            slot_id: "slot-1".to_string(),
            account_id: "account-1".to_string(),
            strategy_id: "maker".to_string(),
            venue: Venue::Polymarket,
            instrument: Instrument {
                market_id: "condition-1".to_string(),
                outcome_id: "YES".to_string(),
                token_id: Some("token-yes".to_string()),
            },
            side: Side::Buy,
            price: decimal(475, 3),
            quantity: decimal(253, 1),
            post_only: true,
            reduce_only: false,
        };

        let buy = adapter.normalize_intent(&base).unwrap();
        assert_eq!(buy.price, decimal(47, 2));
        assert_eq!(buy.quantity, decimal(253, 1));

        let mut sell_intent = base;
        sell_intent.side = Side::Sell;
        let sell = adapter.normalize_intent(&sell_intent).unwrap();
        assert_eq!(sell.price, decimal(48, 2));
    }
}
