# Polymarket multi-account runtime

This runtime allows one global account roster to be split across VPS1 and
VPS2. The roster is non-secret. Private keys remain on the Mac mini signer and
the signer token is supplied through the host environment.

This document describes the normal multi-host runtime. The aggressive LP
domain uses a separate roster, signer, service, data root, and Redis namespace;
see `README_AGGRESSIVE_ISOLATED_RUNTIME.md`.

## Routing model

- Every account has one immutable global `account_index` and one `host_id`.
- Each VPS starts only enabled accounts assigned to its own `host_id`.
- Every engine receives the same global LP profile set. Exclusive allocation
  therefore chooses the same event owner on both hosts.
- Order books are shared only inside one VPS process. Orders, balances,
  positions, exits, pause flags, and engine state remain account-local.
- Splitting one event across accounts is not enabled by this runtime. Quote
  sizing and reward-efficiency research belongs to the aggressive LP policy,
  not the host-routing layer.
- Automatic lifecycle changes are disabled by default. A reviewed
  `set_stable_market_lifecycle` runtime command may enable them independently
  for each host-local account only when its fresh schema-3 proposal matches
  the engine account UID and host identity. The same command is supported by
  the direct single-account runtime only when exactly one stable account is
  present, runtime market updates are enabled, and its account UID and host
  identity are complete. Ambiguous multi-account legacy runtimes remain
  rejected. The command changes only the lifecycle gate; it cannot relax the
  canary, scoring, depth, weather, expiry, or capital limits.
- A first enable after a process started with the gate off persists the config
  but reports `restart_required`, because the read-only scoring observer was
  not started. Once an engine has started with the gate on, later off/on
  commands apply without a restart. Static day/night switching and dedicated
  safety removals remain independent.
- The direct single-account stable entrypoint may start with zero enabled
  markets only when the account pause flag already exists, the account/host
  identity is complete, and runtime market updates are allowed. Before any
  worker starts it verifies that maker BUYs are absent while preserving every
  live SELL exit. Its internal hold remains active if the pause flag is removed
  early and is released only after a market is provisioned while the account
  is still pause-flagged. `multi_runner`, aggressive profiles, ambiguous
  runtimes, and unpaused empty starts continue to fail closed. This narrow path
  supports lifecycle enable -> restart -> canary provisioning; it never quotes
  an empty market set. Public-book and authenticated fill streams both wait for
  that first market and resubscribe whenever the runtime market set changes.

## Generate host-local configs

Start from `scripts/accounts.example.json`, replace the public funder addresses,
and store the real roster outside Git. It must not contain any private key,
signer token, API credential, password, cookie, mnemonic, or webhook.

First build one non-secret market universe. Exact duplicates are removed only
when `--dedupe-exact` is explicit. Different rows for the same event stop the
build unless a reviewed source is selected; identity and day/night conflicts
always stop it.

```bash
python scripts/build_market_universe.py \
  --source vps1=/tmp/config_1.json \
  --source vps2=/tmp/config_2.json \
  --prefer-source vps2 \
  --dedupe-exact \
  --disable-conflicts \
  --output /home/ubuntu/polymarket-runtime/markets.runtime.json
```

The output contains market fields only. It does not copy account, proxy,
signer, API, or webhook configuration from either source.
`--prefer-source` is an explicit review decision for strategy-field conflicts;
it never overrides token, pair, side, condition, or day/night identity conflicts.
`--disable-conflicts` keeps conflicting events visible but prevents them from
quoting until a later eligibility review explicitly enables them.

```bash
python scripts/generate_configs.py \
  --roster /home/ubuntu/polymarket-runtime/accounts.runtime.json \
  --base platforms/polymarket/maker/config.json \
  --market-universe /home/ubuntu/polymarket-runtime/markets.runtime.json \
  --out-dir platforms/polymarket/maker \
  --host-id vps1
```

Run the same command with `--host-id vps2` on VPS2. The generated
`runtime_account.routing_roster_sha256` and
`runtime_account.market_universe_sha256` must be identical on both hosts.
Multi-host config generation fails when `--market-universe` is omitted.

Before changing the service, run the signer-aware validation path while every
local account is paused:

```bash
python platforms/polymarket/maker/multi_runner.py \
  --config-dir /home/ubuntu/polymarket-bot/platforms/polymarket/maker \
  --roster /home/ubuntu/polymarket-runtime/accounts.runtime.json \
  --host-id vps1 \
  --data-dir /home/ubuntu/polymarket-bot/data \
  --require-paused \
  --validate-only \
  --expected-roster-sha256 "$POLYMARKET_EXPECTED_ROSTER_SHA256" \
  --expected-market-sha256 "$POLYMARKET_EXPECTED_MARKET_SHA256"
```

This initializes the signer and validates every local config, funder, LP
profile, proxy port, data directory, roster digest, and market digest, then
exits before any worker or quote loop starts.
Both expected SHA values are mandatory when the roster spans multiple hosts,
so each VPS fails closed if it was generated from a different roster or market
file.

## First cutover

1. Merge and publish one reviewed immutable release to both VPSs.
2. Write `POLYMARKET_HOST_ID=vps1` or `vps2` in the host-local runtime env.
3. Create every local `.account_N.paused` flag before starting the service.
4. Start with `--require-paused`. Missing configs, stale roster metadata,
   signer initialization failure, or any account worker failure stops the
   entire local runner.
5. Verify signer address, funder, account index, roster SHA, positions, and
   open orders for every account while all accounts remain paused. Compare the
   market-universe SHA across VPS1 and VPS2 before removing any pause flag.
6. Remove pause flags one account at a time only after explicit live approval.

On SIGTERM/SIGINT the runner stops all workers, cancels maker BUY orders, and
preserves every live SELL order. A failed cancellation verification exits with
an error instead of claiming a clean stop.

## State

Each account continues to write `data/engine_state_N.json`. The state contains
`runtime.mode`, `host_id`, global/local account counts, and the roster SHA.
Redis state is namespaced as `polymarket:state:account:N`; the normal runtime
event stream remains unchanged. Isolated runtimes namespace events, history,
and state under `polymarket:<runtime_scope>:*`.

## Current boundary

- Top-up and withdrawals remain manual.
- Principal sizing and reward optimization are a separate aggressive LP
  research track.
- Cross-host order-book caching and a cross-host sibling order registry are not
  implemented. Deterministic exclusive event ownership is the cross-host
  duplicate-quote control.
