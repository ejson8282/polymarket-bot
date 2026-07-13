# Polymarket + Predict.fun shared Rust maker core

This workspace is a shared, deterministic core for the two prediction-market
makers. It is deliberately separate from signing, credentials, HTTP clients,
and production process control.

Current phase: **dry-run only**. Nothing in this directory can submit or cancel
a live order.

## What is shared

- Normalized `OrderIntent`, `LiveOrder`, instrument, side, and status models.
- Deterministic desired-vs-live reconciliation: create, keep, cancel, replace.
- Risk checks for price, size, notional, order count, and stale books.
- Same-account and cross-account self-trade prevention.
- Stable logical quote slots, so repricing does not lose order ownership.
- A JSON dry-run contract that can be called from the existing Python engines.

## What remains venue-specific

- Polymarket and Predict.fun authentication and signing.
- API endpoints, retries, rate limits, and error parsing.
- Market discovery and raw order-book subscriptions.
- Venue-specific tick/quantity precision.
- Live order submission and cancellation.

The shared core therefore does **not** mean the two venues share an account,
private key, order ID, or API client. They share only the decision and safety
machinery. Each adapter translates its venue data at the boundary.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `maker-domain` | Shared models, adapter trait, decimal quantization |
| `maker-core` | Deterministic reconciliation plan |
| `maker-risk` | Limits, freshness checks, cross-account self-trade guard |
| `venue-polymarket` | Pure Polymarket DTO normalization and quantization |
| `venue-predictfun` | Pure Predict.fun DTO normalization and quantization |
| `maker-dry-run` | JSON-in/JSON-out planning CLI; no live writes |

## Run

```bash
cd rust-maker
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p maker-dry-run -- fixtures/shared_plan.json
python3 ../scripts/maker_shadow_compare.py fixtures \
  --rust-bin target/debug/maker-dry-run \
  --report ../data/maker_shadow_report.json
```

The fixture should produce a risk-approved plan that:

- replaces the changed Polymarket quote;
- creates the missing Predict.fun quote;
- reports the external unmanaged order without cancelling it.

## Safety invariants

1. Only orders carrying a known `managed_slot` may be cancelled or replaced.
2. Unmanaged/manual orders are reported and left untouched.
3. A stale or missing order book blocks the plan.
4. Crossing desired quotes across any managed accounts block the plan.
5. Duplicate desired slots are rejected.
6. Decimal arithmetic is used for all price, quantity, and notional checks.
7. Risk failure returns no executable order plan.

Phase-one self-trade checks cover the same normalized venue/instrument/outcome.
Polymarket's existing sibling/cross-side sentinel remains authoritative for
complementary-outcome semantics until that rule is represented and replayed in
the shared model. The Rust core must not weaken or bypass it.

## Production migration

1. Call `maker-dry-run` from Python with recorded snapshots and compare its
   plan against the existing Python planner.
2. Run both planners in shadow mode and record differences; Rust remains
   non-authoritative.
3. Add read-only live venue adapters and operational metrics.
4. Add a separately reviewed execution interface with idempotency and a hard
   dry-run/live boundary.
5. Move one venue/account cohort at a time only after replay and shadow parity.

No production service should be switched to this workspace during phase one.

## Offline shadow parity

`scripts/maker_shadow_compare.py` does not require either production engine to
be running. It feeds each fixture to a standard-library Python reference oracle
and the Rust binary, then compares normalized risk decisions and order actions.
The command exits non-zero when any case differs.

This verifies the shared order-lifecycle contract, not strategy alpha. The next
step is to export real desired orders and read-only live-order snapshots from
the existing Python planners into this same JSON contract. Only after offline
and live shadow parity are stable should execution ownership be reconsidered.
