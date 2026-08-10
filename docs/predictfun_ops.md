# Predict.fun Ops Notes

Predict.fun has a separately confirmed one-shot live order path through the
Mac mini signer. The continuous runner defaults to dry-run. Its only supported
live deployment is the explicitly confirmed VPS1 limited-live profile described
below. API keys and wallet keys stay on the Mac mini; the VPS must not receive
either secret.

## Poly-parity architecture

The Predict runner now follows the reusable parts of the Polymarket design:

- a persistent public-market WebSocket with heartbeat handling, reconnect
  backoff, periodic resubscription, and exact Decimal orderbook parsing;
- per-market trading/market status checks before planning any quote;
- a liquidity sentinel that detects abrupt depth removal and cools the market;
- stable market admission, account-aware intents, managed-order ownership,
  inventory exits, and post-only preflight;
- a versioned status snapshot for the unified Dashboard;
- exact-SHA immutable releases with rollback and independent VPS1/VPS2 account
  profiles.

Predict-specific signing stays on the Mac mini. Its restricted WebSocket relay
uses the existing API-key secret only for public market topics and sends no
secret to either VPS. This does not modify or share a Polymarket process.

## Smoke Check

Run from repo root:

```bash
python -m platforms.predictfun.maker.ops smoke
```

Include a WebSocket subscription smoke:

```bash
python -m platforms.predictfun.maker.ops smoke --include-ws
```

The smoke check compiles the PF modules, runs the synthetic strategy self-test,
and runs one runner cycle. The runner cycle writes:

```text
data/predictfun_testnet_state.json
data/predictfun_desired_orders.json
data/predictfun_execution_report.json
data/predictfun_runner_state.json
data/predictfun_simulation_state.json
data/predictfun_risk_state.json
data/predictfun_market_research.json
data/predictfun_status.json
```

## Risk Gate

The PF runner evaluates the risk gate before reconcile/simulation. It has three
execution modes:

- `normal` reconciles all desired maker and inventory-exit intents;
- `reduce_only` cancels engine-managed maker quotes and permits only
  `inventory_exit` creates when BUY exposure or a position cap is exceeded;
- `blocked` cancels engine-managed orders and creates nothing when data is
  stale, the kill switch is active, or the runner error guard fires.

Position-reducing SELL notional does not consume BUY risk budgets. This avoids
the dangerous case where a large position breaches a cap and the cap then
prevents its own exit. Website/manual orders are outside the managed ownership
set in every mode.

WebSocket orderbook cache is optional in testnet. Mainnet requires it and does
not fall back to REST for quoting. A disconnected relay, stale heartbeat,
missing book, invalid sequence, crossed book, non-open trading status, or
market-level parse error blocks that market's new quotes. An isolated market
parse error is surfaced as attention without stopping healthy markets. The
connection freshness threshold is longer than Predict.fun's heartbeat
interval. A cached market book may live
until the five-minute forced resubscription because an unchanged book does not
produce updates; the live connection and status topics still have to remain
healthy. A REST error is never treated as a genuinely empty book, so it cannot
activate empty-book seeding.

The dashboard kill switch writes:

```text
data/predictfun_kill_switch.json
```

Keep this switch off for normal dry-run/simulated-live testing. Turn it on when
testing the blocked path or before any risky maintenance.

## Account-Aware Simulated Market Making

The dry-run maker is now account-aware. `config.testnet.json` defines up to 10
logical account IDs and assigns markets round-robin across them by default.
Intent IDs include the account ID, so two accounts can quote the same market
without colliding. Risk checks also include total account count, per-account
desired notional, per-account/market desired notional, and per-account market
position size.

The runner uses simulated active orders, not the previous desired-order file, as
the next cycle's previous state. Mainnet sets
`inventory.halt_market_buys_while_position=true`, so a filled market does not
replenish BUY liquidity while inventory remains. It produces only a protected
`inventory_exit` intent for that market. If a cycle observes new fills, the
next wake-up uses `runner.fast_requote_after_fill_sec` rather than the normal
interval.

Dry-run inventory handling is simulated, but no longer BUY-only: existing
simulated long inventory can generate passive SELL exit intents, bounded by the
inventory config. Live mode reads official orders, balances, and positions from
the account-scoped Mac mini proxy before every planning cycle.

Desired BUY orders reserve inventory while intents are built. This prevents two
passive levels on the same account/market/outcome from each passing the cap
independently and exceeding `inventory.max_long_size_per_outcome` if both fill.

## Empty-Book Seeding

For testnet or explicitly configured mainnet coverage,
`strategy.seed_empty_books=true` can quote
small symmetric YES/NO BUY orders on reward markets whose REST/WS orderbook and
market top-of-book are both empty. This is deliberately not used for one-sided
books, because a visible YES ask or NO bid already carries price information.
The seed price defaults to `seed_mid_price - seed_distance_ticks * tick`, then
backs off by `backoff_ticks` for additional levels.

Empty-book seeding is evaluated before normal two-sided depth requirements.
Otherwise enabling both seeding and a minimum depth would make seeding
unreachable. All expiry, reward, status, notional, and account limits still
apply.

## Stable Market Admission

The scanner fetches a candidate pool larger than the active market limit. The
admission book applies a minimum dwell time and score-improvement margin before
replacing an active market. Markets with a non-zero position are pinned so they
remain available to the inventory planner after their reward ranking falls.

The continuous maker supports exactly two canonical `YES`/`NO` outcomes. It
maps tokens by outcome name rather than array order. Multi-outcome or
non-canonical markets are rejected explicitly instead of silently treating
their first two outcomes as YES/NO.

`strategy.profile` separates reward appetite from safety plumbing:

- `conservative` keeps the existing midpoint exclusion and full risk penalty;
- `balanced` permits midpoint quotes but retains a smaller ranking penalty;
- `points` permits midpoint quotes and treats the midpoint as a points zone.

Mainnet stays on `conservative` until a separately reviewed live rollout
chooses otherwise.

## Managed Orders

Reconcile reports include a managed-order registry keyed by the official order
ID returned by an executor. Only orders created by this engine enter the
registry. Inventory exits are labelled separately from normal maker quotes so
a future live executor can protect them during ordinary quote refreshes.
Strategy cancel intents are resolved through this registry to official order
IDs. An unknown intent produces no cancel request, which keeps website/manual
orders out of ordinary and risk-driven cancellation.

## Continuous Live Executor Boundary

Continuous live Predict.fun order code should be added only behind
`PredictFunLiveExecutor` in:

```text
platforms/predictfun/maker/executor.py
```

The strategy, intents, reconcile, risk gate, and dashboard should continue to
depend on the executor protocol rather than Predict.fun SDK details. The live
executor accepts only a signer URL and account alias; it must never accept or
store an API key or private key on the VPS.

`live_order_once.py` is a canary tool, not a persistent order launcher. Live
mode requires `--cancel-after` (the old `--remove-after` spelling is only a
compatibility alias). Before submission it verifies that the signer has the
configured minimum BNB balance for cancellation, then performs the fresh
market, allowance, post-only and notional checks. After submission it calls
the full `/cancel-orders` route and succeeds only when the BSC transaction
receipt is mined successfully and the official account API no longer reports
the order as open. Any failed preflight, submission or verified cancellation
returns a non-zero process status.

The separate `remove-order-by-hash` route remains `off_book_only` and must not
be used as proof of cancellation. A real canary still requires a distinct user
authorization after the reviewed release has been deployed.

### VPS1 limited-live profile

`deploy_release.py --mode live` is deliberately narrower than the general
runner capability. Activation fails closed unless all of these conditions are
true:

- deployment profile is `vps1`;
- the only account alias is `account_01`;
- the exact immutable release SHA is supplied to the execution gate;
- the dedicated confirmation is
  `DEPLOY_PREDICTFUN_VPS1_LIMITED_LIVE`;
- simulation is disabled and live submit/cancel/read capabilities pass the
  post-start acceptance checks.

The generated runtime config admits one market and one quote level. Per-order,
per-account, per-account/market, per-market, and total desired BUY notional are
all capped at `1.60` USDT. The observed capital tier is clamped to those static
limits before intents are built, so an oversized plan is not generated and then
merely rejected later by the risk gate. Reward-threshold quote sizes above the
cap are reduced to `1.60` rather than silently skipped. At Predict.fun's `0.90`
USDT minimum, this first profile can maintain only one passive side at a time.
After any account position appears, all new BUY intents are suppressed across
markets; the runner may only manage passive exits until the account is flat.
The risk gate independently caps live position value plus desired BUY notional
at `1.60`, and an open manual BUY also suppresses new managed BUY intents.

VPS2 and multi-account deployments reject `--mode live`. Their existing
dry-run path and confirmation remain unchanged. A reviewed merge and exact-SHA
deployment still require a separate user authorization; creating this profile
does not activate it.

Unified Dashboard code and service operations belong to `latitude-alpha`, not
this repository.

## Dashboard contract

Each profile writes `predictfun_mainnet_status.json`. It contains deployment
identity, runner/WS/signer/risk health, market plans, desired and simulated
orders, simulated positions, recent actions, and explicit capability flags.
The Dashboard may mirror Polymarket's layout using this file, but it must label
all current Predict order/position rows as simulated and keep live controls
disabled while `live_order_submit` or `live_order_cancel` is false.
