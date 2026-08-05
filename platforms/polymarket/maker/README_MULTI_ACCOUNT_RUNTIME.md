# Polymarket multi-account runtime

This runtime allows one global account roster to be split across VPS1 and
VPS2. The roster is non-secret. Private keys remain on the Mac mini signer and
the signer token is supplied through the host environment.

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
- Automatic and dashboard-triggered market additions stay disabled in roster
  mode until one reviewed coordinator can apply the same market universe to
  both hosts. Static day/night session switching and safety removals remain.

## Generate host-local configs

Start from `scripts/accounts.example.json`, replace the public funder addresses,
and store the real roster outside Git. It must not contain any private key,
signer token, API credential, password, cookie, mnemonic, or webhook.

```bash
python scripts/generate_configs.py \
  --roster /home/ubuntu/polymarket-runtime/accounts.runtime.json \
  --base platforms/polymarket/maker/config.json \
  --out-dir platforms/polymarket/maker \
  --host-id vps1
```

Run the same command with `--host-id vps2` on VPS2. The generated
`runtime_account.routing_roster_sha256` and
`runtime_account.market_universe_sha256` must be identical on both hosts.

Before changing the service, run the signer-aware validation path while every
local account is paused:

```bash
python platforms/polymarket/maker/multi_runner.py \
  --config-dir /home/ubuntu/polymarket-bot/platforms/polymarket/maker \
  --roster /home/ubuntu/polymarket-runtime/accounts.runtime.json \
  --host-id vps1 \
  --data-dir /home/ubuntu/polymarket-bot/data \
  --require-paused \
  --validate-only
```

This initializes the signer and validates every local config, funder, LP
profile, proxy port, data directory, roster digest, and market digest, then
exits before any worker or quote loop starts.

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
Redis state is namespaced as `polymarket:state:account:N`; the shared event
stream remains unchanged.

## Current boundary

- Top-up and withdrawals remain manual.
- Principal sizing and reward optimization are a separate aggressive LP
  research track.
- Cross-host order-book caching and a cross-host sibling order registry are not
  implemented. Deterministic exclusive event ownership is the cross-host
  duplicate-quote control.
