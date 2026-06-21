# Predict.fun Testnet Maker

This is the first Predict.fun integration pass. It is intentionally dry-run only:
it reads testnet markets and order books, scores active reward markets, builds
passive quote plans, and writes a state JSON. It does not sign, place, or cancel
orders.

## Run

From the repo root:

```bash
python -m platforms.predictfun.maker.dry_run --once
```

Full JSON output:

```bash
python -m platforms.predictfun.maker.dry_run --once --json
```

Continuous dry-run loop:

```bash
python -m platforms.predictfun.maker.dry_run --interval-sec 30
```

Production-shaped dry-run runner:

```bash
python -m platforms.predictfun.maker.runner
```

One runner cycle:

```bash
python -m platforms.predictfun.maker.runner --once
```

Dry-run reconcile:

```bash
python -m platforms.predictfun.maker.reconcile
```

Ops smoke check:

```bash
python -m platforms.predictfun.maker.ops smoke
```

Local strategy self-test:

```bash
python -m platforms.predictfun.maker.selftest
```

WebSocket smoke watcher:

```bash
python -m platforms.predictfun.ws_watch --max-messages 5 --timeout-sec 8
```

Latitude dashboard:

```bash
streamlit run dashboard/app.py --server.port 8502
```

Then open `Market Making / Predict.fun` from the Latitude sidebar.

Standalone PF debug dashboard:

```bash
streamlit run dashboard/predictfun_app.py
```

Include short-window crypto markets for testnet experimentation:

```bash
python -m platforms.predictfun.maker.dry_run --once --include-crypto-updown
```

State is written to:

```text
data/predictfun_testnet_state.json
```

Desired dry-run order intents are written to:

```text
data/predictfun_desired_orders.json
```

Dry-run reconcile reports are written to:

```text
data/predictfun_execution_report.json
```

WebSocket state is written to:

```text
data/predictfun_ws_state.json
```

Simulated live state, risk gate state, and market research state are written to:

```text
data/predictfun_simulation_state.json
data/predictfun_risk_state.json
data/predictfun_market_research.json
```

The Latitude PF page can start and stop a dry-run loop. It writes:

```text
data/predictfun_dry_run.pid
data/predictfun_dry_run.log
data/predictfun_runner_state.json
```

## Current Behavior

- Uses `https://api-testnet.predict.fun`.
- Scans `hasActiveRewards=true` markets by default.
- Skips `CRYPTO_UP_DOWN` by default because short-window direction markets have
  high adverse-selection risk.
- Computes YES book top of book directly.
- Computes NO top of book by complementing YES prices.
- Builds BUY-only passive ladders inside the reward spread band.
- Never quotes at the current best bid; it backs off by at least one tick.
- Uses fresh WebSocket orderbook state when available, then falls back to REST.
- Can seed truly empty reward books with small passive YES/NO BUY quotes when
  `strategy.seed_empty_books=true`. This only applies when both REST/WS book
  levels and market top-of-book are empty; one-sided books are still skipped.
- Converts quote plans into stable desired-order intents.
- Compares desired intents against the previous cycle and reports
  `create` / `keep` / `cancel` diffs.
- Includes a dry-run executor/reconcile path so a future live executor can be
  added behind the same interface.
- Provides a PF runner that records heartbeat, cycle count, last plan summary,
  execution summary, risk summary, simulation summary, market research summary,
  and last error.
- Provides a simulated-live loop that turns dry-run order intents into simulated
  active orders, fills, positions, and marked PnL. It only fills orders when the
  observed book crosses the simulated order price.
- Supports account-aware dry-run order intents for up to 10 configured accounts.
  The default assignment is round-robin by market, so each account has its own
  intent IDs, simulated orders, fills, inventory, and risk checks.
- Uses the simulator's actual active orders as the next cycle's previous state.
  If a simulated order is filled, the runner sees that it disappeared and can
  recreate the quote on the next cycle instead of incorrectly marking it `keep`.
- Runs a fast re-quote cycle after simulated fills. The default normal interval
  is 30 seconds; after a fill it wakes after 2 seconds.
- Emits passive SELL exit intents for existing simulated inventory, so filled
  BUY orders can be worked back out instead of only accumulating long inventory.
- Provides a risk gate with a dashboard kill switch, stale-state checks,
  desired-notional limits, account-level limits, per-market notional limits,
  simulated-position limits, and runner error checks.
- Provides a market research view that scores PF markets by reward efficiency,
  spread room, book quality, quote availability, and risk penalties.
- Provides a local self-test with a synthetic book so quote planning, intent
  diffing, and dry-run reconciliation can be validated even when testnet books
  are empty.
- Shows Predict.fun under the shared Latitude dashboard as `Market Making /
  Predict.fun`.
- Keeps Predict.fun code, config, state, dry-run loop, and controls separate
  from the Polymarket maker engine.

## Mainnet Readiness Checklist

- Get Predict.fun mainnet API key and set `PREDICTFUN_API_KEY`.
- Decide whether to trade from an EOA or Predict Account smart wallet.
- Keep any private key or Privy export on the Mac signer, not on VPS.
- Add JWT/auth flow and signed order creation through `predict-sdk`.
- Treat `/v1/orders/remove` as a fast off-book removal only; pair it with SDK
  on-chain cancellation for safe live operation.
- Map the 10 dry-run account IDs to real signer-controlled accounts before
  enabling live quotes.
- Add live order reconciliation and fill handling before enabling live quotes.
