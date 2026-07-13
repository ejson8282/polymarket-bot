# Polymarket + Predict.fun shared Rust maker core

This workspace is a shared, deterministic core for the two prediction-market
makers. It is deliberately separate from signing, credentials, HTTP clients,
and production process control.

Current phase: **read-only shadow / dry-run only**. Nothing in this directory
or the shadow exporters can submit or cancel a live order.

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

No production service should be switched by the current Draft PR.

## Offline shadow parity

`scripts/maker_shadow_compare.py` does not require either production engine to
be running. It feeds each fixture to a standard-library Python reference oracle
and the Rust binary, then compares normalized risk decisions and order actions.
The command exits non-zero when any case differs.

This verifies the shared order-lifecycle contract, not strategy alpha. The next
step is to export real desired orders and read-only live-order snapshots from
the existing Python planners into this same JSON contract. Only after offline
and live shadow parity are stable should execution ownership be reconsidered.

## Read-only Python state export

The exporter converts state files already written by the Python engines. It
does not start an engine, call an exchange, contact a signer, or mutate the
source files.

Freshness is conservative. Predict.fun uses the older of the plan and active
order state timestamps. Polymarket adds the age already recorded for the book
to the time elapsed since the engine state was written. A stale or unknown
source can still be inspected, but the risk pass blocks an executable result.

Predict.fun dry-run state:

```bash
python3 scripts/maker_shadow_export.py \
  --output data/predictfun_rust_shadow.json \
  predictfun \
  --intents data/predictfun_mainnet_desired_orders.json \
  --actual data/predictfun_mainnet_simulation_state.json \
  --plans data/predictfun_mainnet_state.json

python3 scripts/maker_shadow_compare.py data/predictfun_rust_shadow.json \
  --rust-bin rust-maker/target/debug/maker-dry-run
```

Polymarket account states can be combined so the shared risk pass sees all
managed accounts at once:

```bash
python3 scripts/maker_shadow_export.py \
  --output data/polymarket_rust_shadow.json \
  polymarket \
  --state data/engine_state_1.json \
  --state data/engine_state_2.json

python3 scripts/maker_shadow_compare.py data/polymarket_rust_shadow.json \
  --rust-bin rust-maker/target/debug/maker-dry-run
```

Polymarket state now exposes the last successfully synchronized plan signature,
exact decimal strings, side, condition ID, and whether an order is an exit.
Exit/manual orders remain unmanaged in the Rust plan and therefore cannot be
cancelled by the shared reconciler. Older state files without the new plan
field export no desired orders; restart is not required merely to inspect them,
but true plan parity starts only after a Python engine writes the new fields.

The first production-shaped, read-only check is recorded in
`validation/2026-07-13-read-only-shadow.md`.

## Continuous difference collection

Build the read-only binary once:

```bash
cd rust-maker
cargo build --release --package maker-dry-run --bin maker-dry-run
cd ..
```

Run one Predict.fun collection pass:

```bash
python3 scripts/maker_shadow_collect.py \
  --database data/maker_shadow.sqlite3 \
  --snapshot-dir data/maker-shadow \
  --rust-bin rust-maker/target/release/maker-dry-run \
  predictfun \
  --intents data/predictfun_mainnet_desired_orders.json \
  --actual data/predictfun_mainnet_simulation_state.json \
  --plans data/predictfun_mainnet_state.json
```

Add `--loop --interval-seconds 15 --run-seconds 86400` before the venue name
for the initial 24-hour run. Use the equivalent `polymarket --state ...`
arguments from the export example above for Polymarket.

The collector fingerprints the source-file contents and records only changed
state bundles. Repeated polls update the heartbeat but do not inflate the
sample count or parity rate. Every unique normalized snapshot is retained for
replay under the configured snapshot directory. SQLite records exact parity,
safety parity, action parity, freshness, errors, and source timestamps.

```bash
python3 scripts/maker_shadow_report.py \
  --database data/maker_shadow.sqlite3 \
  --json-output data/maker_shadow_report.json
```

The collector does not refresh an engine or create market data. If an upstream
state writer is stopped, it reports `UNCHANGED` and collects no artificial new
samples. The Predict.fun dry-run systemd unit uses `maker.runner --once`, which
adds risk, reconciliation, and simulation-state updates while retaining the
existing 30-minute timer and `DryRunExecutor`. It does not submit live orders.

The supplied 24-hour collector units deny all IP networking and make the
repository read-only except for `data/`:

- `deploy/systemd/maker-shadow-predictfun.service`
- `deploy/systemd/maker-shadow-polymarket.service`

For the fast acceptance pass, review after 24 hours. Safety and action
mismatches remain zero-tolerance. Any exact-result difference is retained for
replay and never grants execution authority.
