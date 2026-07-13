use std::collections::{BTreeMap, BTreeSet};

use maker_domain::{LiveOrder, OrderIntent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReconcilePolicy {
    #[serde(with = "rust_decimal::serde::str")]
    pub price_epsilon: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_epsilon: Decimal,
}

impl Default for ReconcilePolicy {
    fn default() -> Self {
        Self {
            price_epsilon: Decimal::ZERO,
            quantity_epsilon: Decimal::ZERO,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum OrderAction {
    Create {
        intent: OrderIntent,
    },
    Keep {
        order_id: String,
        slot_id: String,
    },
    Cancel {
        order_id: String,
        reason: String,
    },
    Replace {
        order_id: String,
        intent: OrderIntent,
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReconcilePlan {
    pub actions: Vec<OrderAction>,
    pub unmanaged_order_ids: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("duplicate desired slot_id: {0}")]
    DuplicateDesiredSlot(String),
}

pub fn reconcile(
    desired: &[OrderIntent],
    actual: &[LiveOrder],
    policy: &ReconcilePolicy,
) -> Result<ReconcilePlan, ReconcileError> {
    let mut desired_by_slot = BTreeMap::<String, OrderIntent>::new();
    for intent in desired {
        if desired_by_slot
            .insert(intent.slot_id.clone(), intent.clone())
            .is_some()
        {
            return Err(ReconcileError::DuplicateDesiredSlot(intent.slot_id.clone()));
        }
    }

    let mut actual_by_slot = BTreeMap::<String, Vec<LiveOrder>>::new();
    let mut unmanaged_order_ids = Vec::new();
    for order in actual.iter().filter(|order| order.is_active()) {
        match &order.managed_slot {
            Some(slot_id) => actual_by_slot
                .entry(slot_id.clone())
                .or_default()
                .push(order.clone()),
            None => unmanaged_order_ids.push(order.order_id.clone()),
        }
    }
    unmanaged_order_ids.sort();
    for orders in actual_by_slot.values_mut() {
        orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));
    }

    let mut actions = Vec::new();
    let mut warnings = Vec::new();
    let mut consumed_slots = BTreeSet::new();

    for (slot_id, intent) in &desired_by_slot {
        consumed_slots.insert(slot_id.clone());
        let Some(orders) = actual_by_slot.get(slot_id) else {
            actions.push(OrderAction::Create {
                intent: intent.clone(),
            });
            continue;
        };

        let equivalent_index = orders
            .iter()
            .position(|order| equivalent(order, intent, policy));
        let primary_index = equivalent_index.unwrap_or(0);
        let primary = &orders[primary_index];

        if equivalent_index.is_some() {
            actions.push(OrderAction::Keep {
                order_id: primary.order_id.clone(),
                slot_id: slot_id.clone(),
            });
        } else {
            actions.push(OrderAction::Replace {
                order_id: primary.order_id.clone(),
                intent: intent.clone(),
                reason: "managed quote changed".to_string(),
            });
        }

        for (index, duplicate) in orders.iter().enumerate() {
            if index != primary_index {
                actions.push(OrderAction::Cancel {
                    order_id: duplicate.order_id.clone(),
                    reason: format!("duplicate managed order for slot {slot_id}"),
                });
            }
        }
        if orders.len() > 1 {
            warnings.push(format!(
                "slot {slot_id} has {} active managed orders",
                orders.len()
            ));
        }
    }

    for (slot_id, orders) in actual_by_slot {
        if consumed_slots.contains(&slot_id) {
            continue;
        }
        for order in orders {
            actions.push(OrderAction::Cancel {
                order_id: order.order_id,
                reason: format!("slot {slot_id} is no longer desired"),
            });
        }
    }

    Ok(ReconcilePlan {
        actions,
        unmanaged_order_ids,
        warnings,
    })
}

fn equivalent(order: &LiveOrder, intent: &OrderIntent, policy: &ReconcilePolicy) -> bool {
    order.account_id == intent.account_id
        && order.venue == intent.venue
        && order.instrument == intent.instrument
        && order.side == intent.side
        && order.post_only == intent.post_only
        && (order.price - intent.price).abs() <= policy.price_epsilon
        && (order.quantity - intent.quantity).abs() <= policy.quantity_epsilon
}

#[cfg(test)]
mod tests {
    use maker_domain::{Instrument, OrderStatus, Side, Venue};

    use super::*;

    fn decimal(value: i64, scale: u32) -> Decimal {
        Decimal::new(value, scale)
    }

    fn intent(slot: &str, price: Decimal) -> OrderIntent {
        OrderIntent {
            slot_id: slot.to_string(),
            account_id: "account-1".to_string(),
            strategy_id: "two-sided".to_string(),
            venue: Venue::Polymarket,
            instrument: Instrument {
                market_id: "market-1".to_string(),
                outcome_id: "yes".to_string(),
                token_id: Some("token-1".to_string()),
            },
            side: Side::Buy,
            price,
            quantity: decimal(10, 0),
            post_only: true,
            reduce_only: false,
        }
    }

    fn live(order_id: &str, slot: Option<&str>, price: Decimal) -> LiveOrder {
        LiveOrder {
            order_id: order_id.to_string(),
            managed_slot: slot.map(str::to_string),
            account_id: "account-1".to_string(),
            venue: Venue::Polymarket,
            instrument: intent("unused", price).instrument,
            side: Side::Buy,
            price,
            quantity: decimal(10, 0),
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::Open,
            post_only: true,
        }
    }

    #[test]
    fn creates_missing_order_and_ignores_unmanaged_order() {
        let plan = reconcile(
            &[intent("slot-1", decimal(48, 2))],
            &[live("external", None, decimal(47, 2))],
            &ReconcilePolicy::default(),
        )
        .unwrap();

        assert!(matches!(plan.actions[0], OrderAction::Create { .. }));
        assert_eq!(plan.unmanaged_order_ids, vec!["external"]);
    }

    #[test]
    fn keeps_equivalent_and_replaces_changed_order() {
        let keep = reconcile(
            &[intent("slot-1", decimal(48, 2))],
            &[live("order-1", Some("slot-1"), decimal(48, 2))],
            &ReconcilePolicy::default(),
        )
        .unwrap();
        assert!(matches!(keep.actions[0], OrderAction::Keep { .. }));

        let replace = reconcile(
            &[intent("slot-1", decimal(49, 2))],
            &[live("order-1", Some("slot-1"), decimal(48, 2))],
            &ReconcilePolicy::default(),
        )
        .unwrap();
        assert!(matches!(replace.actions[0], OrderAction::Replace { .. }));
    }

    #[test]
    fn cancels_removed_and_duplicate_orders() {
        let removed = reconcile(
            &[],
            &[live("order-1", Some("slot-1"), decimal(48, 2))],
            &ReconcilePolicy::default(),
        )
        .unwrap();
        assert!(matches!(removed.actions[0], OrderAction::Cancel { .. }));

        let duplicates = reconcile(
            &[intent("slot-1", decimal(48, 2))],
            &[
                live("order-1", Some("slot-1"), decimal(48, 2)),
                live("order-2", Some("slot-1"), decimal(48, 2)),
            ],
            &ReconcilePolicy::default(),
        )
        .unwrap();
        assert_eq!(duplicates.actions.len(), 2);
        assert_eq!(duplicates.warnings.len(), 1);
    }

    #[test]
    fn rejects_duplicate_desired_slots() {
        let error = reconcile(
            &[
                intent("slot-1", decimal(48, 2)),
                intent("slot-1", decimal(49, 2)),
            ],
            &[],
            &ReconcilePolicy::default(),
        )
        .unwrap_err();
        assert!(matches!(error, ReconcileError::DuplicateDesiredSlot(_)));
    }
}
