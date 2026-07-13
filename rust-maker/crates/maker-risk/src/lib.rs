use std::collections::BTreeMap;

use maker_domain::{Instrument, OrderIntent, Side, Venue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub min_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_account_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_account_market_notional: Decimal,
    pub max_open_orders_per_account: usize,
    pub max_book_age_ms: u64,
    #[serde(default = "default_true")]
    pub require_book_age: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BookAge {
    pub venue: Venue,
    pub instrument: Instrument,
    pub age_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RiskViolation {
    pub code: String,
    pub message: String,
    pub slot_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RiskReport {
    pub allowed: bool,
    pub violations: Vec<RiskViolation>,
}

pub fn evaluate(desired: &[OrderIntent], books: &[BookAge], limits: &RiskLimits) -> RiskReport {
    let mut violations = Vec::new();
    let mut account_notional = BTreeMap::<String, Decimal>::new();
    let mut account_market_notional = BTreeMap::<(String, Venue, Instrument), Decimal>::new();
    let mut account_order_count = BTreeMap::<String, usize>::new();
    let book_ages = books
        .iter()
        .map(|book| ((book.venue, book.instrument.clone()), book.age_ms))
        .collect::<BTreeMap<_, _>>();

    for intent in desired {
        let mut add = |code: &str, message: String| {
            violations.push(RiskViolation {
                code: code.to_string(),
                message,
                slot_ids: vec![intent.slot_id.clone()],
            });
        };

        if intent.price < limits.min_price || intent.price > limits.max_price {
            add(
                "price_out_of_range",
                format!("slot {} price is outside configured bounds", intent.slot_id),
            );
        }
        if intent.quantity <= Decimal::ZERO || intent.quantity > limits.max_quantity {
            add(
                "quantity_out_of_range",
                format!(
                    "slot {} quantity is outside configured bounds",
                    intent.slot_id
                ),
            );
        }
        if intent.notional() > limits.max_order_notional {
            add(
                "order_notional_exceeded",
                format!("slot {} exceeds max order notional", intent.slot_id),
            );
        }

        match book_ages.get(&(intent.venue, intent.instrument.clone())) {
            Some(age_ms) if *age_ms > limits.max_book_age_ms => add(
                "stale_book",
                format!("slot {} uses a stale order book", intent.slot_id),
            ),
            None if limits.require_book_age => add(
                "missing_book_age",
                format!("slot {} has no order-book freshness record", intent.slot_id),
            ),
            _ => {}
        }

        *account_notional
            .entry(intent.account_id.clone())
            .or_default() += intent.notional();
        *account_market_notional
            .entry((
                intent.account_id.clone(),
                intent.venue,
                intent.instrument.clone(),
            ))
            .or_default() += intent.notional();
        *account_order_count
            .entry(intent.account_id.clone())
            .or_default() += 1;
    }

    for (account_id, notional) in account_notional {
        if notional > limits.max_account_notional {
            violations.push(RiskViolation {
                code: "account_notional_exceeded".to_string(),
                message: format!("account {account_id} exceeds max aggregate notional"),
                slot_ids: slots_for_account(desired, &account_id),
            });
        }
    }
    for ((account_id, venue, instrument), notional) in account_market_notional {
        if notional > limits.max_account_market_notional {
            violations.push(RiskViolation {
                code: "account_market_notional_exceeded".to_string(),
                message: format!(
                    "account {account_id} exceeds market notional on {venue:?}/{}",
                    instrument.market_id
                ),
                slot_ids: desired
                    .iter()
                    .filter(|intent| {
                        intent.account_id == account_id
                            && intent.venue == venue
                            && intent.instrument == instrument
                    })
                    .map(|intent| intent.slot_id.clone())
                    .collect(),
            });
        }
    }
    for (account_id, count) in account_order_count {
        if count > limits.max_open_orders_per_account {
            violations.push(RiskViolation {
                code: "account_order_count_exceeded".to_string(),
                message: format!("account {account_id} exceeds max desired order count"),
                slot_ids: slots_for_account(desired, &account_id),
            });
        }
    }

    detect_crossing_quotes(desired, &mut violations);
    violations.sort_by(|left, right| {
        (&left.code, &left.slot_ids, &left.message).cmp(&(
            &right.code,
            &right.slot_ids,
            &right.message,
        ))
    });

    RiskReport {
        allowed: violations.is_empty(),
        violations,
    }
}

fn slots_for_account(desired: &[OrderIntent], account_id: &str) -> Vec<String> {
    desired
        .iter()
        .filter(|intent| intent.account_id == account_id)
        .map(|intent| intent.slot_id.clone())
        .collect()
}

fn detect_crossing_quotes(desired: &[OrderIntent], violations: &mut Vec<RiskViolation>) {
    let mut grouped = BTreeMap::<(Venue, Instrument), Vec<&OrderIntent>>::new();
    for intent in desired {
        grouped
            .entry((intent.venue, intent.instrument.clone()))
            .or_default()
            .push(intent);
    }

    for ((venue, instrument), intents) in grouped {
        let buys = intents
            .iter()
            .copied()
            .filter(|intent| intent.side == Side::Buy)
            .collect::<Vec<_>>();
        let sells = intents
            .iter()
            .copied()
            .filter(|intent| intent.side == Side::Sell)
            .collect::<Vec<_>>();
        for buy in &buys {
            for sell in &sells {
                if buy.price >= sell.price {
                    let scope = if buy.account_id == sell.account_id {
                        "same-account"
                    } else {
                        "cross-account"
                    };
                    violations.push(RiskViolation {
                        code: "self_trade_risk".to_string(),
                        message: format!(
                            "{scope} quotes cross on {venue:?}/{}",
                            instrument.market_id
                        ),
                        slot_ids: vec![buy.slot_id.clone(), sell.slot_id.clone()],
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maker_domain::{Instrument, Side, Venue};

    use super::*;

    fn decimal(value: i64, scale: u32) -> Decimal {
        Decimal::new(value, scale)
    }

    fn intent(account: &str, slot: &str, side: Side, price: Decimal) -> OrderIntent {
        OrderIntent {
            slot_id: slot.to_string(),
            account_id: account.to_string(),
            strategy_id: "maker".to_string(),
            venue: Venue::PredictFun,
            instrument: Instrument {
                market_id: "42".to_string(),
                outcome_id: "yes".to_string(),
                token_id: None,
            },
            side,
            price,
            quantity: decimal(10, 0),
            post_only: true,
            reduce_only: false,
        }
    }

    fn limits() -> RiskLimits {
        RiskLimits {
            min_price: decimal(1, 2),
            max_price: decimal(99, 2),
            max_quantity: decimal(100, 0),
            max_order_notional: decimal(100, 0),
            max_account_notional: decimal(200, 0),
            max_account_market_notional: decimal(200, 0),
            max_open_orders_per_account: 10,
            max_book_age_ms: 2_000,
            require_book_age: true,
        }
    }

    fn fresh_book() -> BookAge {
        BookAge {
            venue: Venue::PredictFun,
            instrument: intent("a", "slot", Side::Buy, decimal(40, 2)).instrument,
            age_ms: 250,
        }
    }

    #[test]
    fn allows_fresh_non_crossing_quotes() {
        let report = evaluate(
            &[
                intent("a", "a-buy", Side::Buy, decimal(40, 2)),
                intent("b", "b-sell", Side::Sell, decimal(60, 2)),
            ],
            &[fresh_book()],
            &limits(),
        );
        assert!(report.allowed, "{:?}", report.violations);
    }

    #[test]
    fn blocks_cross_account_self_trade_and_stale_books() {
        let mut stale = fresh_book();
        stale.age_ms = 5_000;
        let report = evaluate(
            &[
                intent("a", "a-buy", Side::Buy, decimal(61, 2)),
                intent("b", "b-sell", Side::Sell, decimal(60, 2)),
            ],
            &[stale],
            &limits(),
        );
        assert!(!report.allowed);
        assert!(report
            .violations
            .iter()
            .any(|violation| violation.code == "self_trade_risk"));
        assert!(report
            .violations
            .iter()
            .any(|violation| violation.code == "stale_book"));
    }
}
