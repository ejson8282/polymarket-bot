# Predict.fun Ops Notes

Predict.fun is still blocked from live trading until the mainnet API key,
wallet route, JWT/auth flow, and signer location are confirmed. The current
operational target is a production-shaped dry-run and simulated-live loop.

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
```

## Risk Gate

The PF runner evaluates the risk gate before reconcile/simulation. If blocked,
it writes a `risk_blocked` execution report and skips order actions.

WebSocket orderbook cache is optional. The runner falls back to REST orderbooks
when WS state is stale or unavailable. Set `risk.warn_on_stale_ws_state=true`
only after the VPS has the `websockets` Python package installed and the watcher
is expected to run.

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
the next cycle's previous state. That matters after fills: when a quote is eaten
and removed from active orders, the next cycle creates a replacement quote
instead of incorrectly keeping the filled order. If a cycle observes new fills,
the next wake-up uses `runner.fast_requote_after_fill_sec` rather than the normal
interval.

Inventory handling is still simulated, but no longer BUY-only: existing
simulated long inventory can generate passive SELL exit intents, bounded by the
inventory config. Live account mapping and signer routing must be reviewed
before enabling real orders.

Desired BUY orders reserve inventory while intents are built. This prevents two
passive levels on the same account/market/outcome from each passing the cap
independently and exceeding `inventory.max_long_size_per_outcome` if both fill.

## Empty-Book Seeding

For testnet market-making coverage, `strategy.seed_empty_books=true` can quote
small symmetric YES/NO BUY orders on reward markets whose REST/WS orderbook and
market top-of-book are both empty. This is deliberately not used for one-sided
books, because a visible YES ask or NO bid already carries price information.
The seed price defaults to `seed_mid_price - seed_distance_ticks * tick`, then
backs off by `backoff_ticks` for additional levels.

## Future Live Executor Boundary

Live Predict.fun order code should be added only behind
`PredictFunLiveExecutor` in:

```text
platforms/predictfun/maker/executor.py
```

The strategy, intents, reconcile, risk gate, and dashboard should continue to
depend on the executor protocol rather than Predict.fun SDK details.

## VPS1 Dashboard

Latitude dashboard service:

```bash
sudo systemctl restart polymarket-dashboard.service
sudo systemctl status polymarket-dashboard.service --no-pager
curl -fsS http://127.0.0.1:8501 >/dev/null
```

External Tailscale/nginx entry remains:

```text
http://100.122.255.98:8502
```
