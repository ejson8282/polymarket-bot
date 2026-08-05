# Polymarket aggressive LP isolated runtime

Aggressive LP is a separate operating domain. It does not extend, replace, or
share runtime state with the existing VPS1/VPS2 normal LP services.

## Hard boundary

The aggressive runtime has its own:

- host identifiers: `aggressive-a`, `aggressive-b`, and future
  `aggressive-*` hosts;
- service: `polymarket-aggressive-engine.service`;
- immutable release root: `/home/ubuntu/polymarket-aggressive-releases`;
- Python environment: `/home/ubuntu/polymarket-aggressive-venv`;
- config, roster, market, pause, state, and log root:
  `/home/ubuntu/polymarket-aggressive-runtime`;
- Mac mini signer process, account aliases, bearer token, and URL;
- dedicated Redis process plus the `polymarket:aggressive:*` key/channel
  namespace;
- account roster, market universe, sibling-order registry, and allocation
  decisions.

The normal `polymarket-engine.service`, `/home/ubuntu/polymarket-bot`,
`/home/ubuntu/polymarket-runtime`, normal signer URL, normal account aliases,
and normal Redis keys are forbidden inputs to this runtime.

Private keys remain only on the Mac mini. The repository, runtime hosts, unit,
roster, generated configs, and logs must not contain private keys.

## Non-secret roster

Copy `scripts/accounts.aggressive.example.json` to the isolated runtime root,
replace only public funder addresses and reviewed profile values, then generate
host-local configs. The roster declares `runtime_scope: aggressive`; startup
rejects a missing or different scope.

```bash
python scripts/generate_configs.py \
  --roster /home/ubuntu/polymarket-aggressive-runtime/accounts.runtime.json \
  --base platforms/polymarket/maker/config.json \
  --market-universe /home/ubuntu/polymarket-aggressive-runtime/markets.runtime.json \
  --out-dir /home/ubuntu/polymarket-aggressive-runtime/platforms/polymarket/maker \
  --host-id aggressive-a
```

Use `aggressive-b` on the other isolated host. Generated configs embed both
the aggressive scope and reviewed roster/market digests.

## Runtime environment

`/home/ubuntu/polymarket-aggressive-runtime/env/runtime.env` is host-local and
not committed. It must provide:

```text
POLYMARKET_HOST_ID=aggressive-a
POLYMARKET_EXPECTED_SIGNER_URL=http://100.91.159.54:8421
POLY_SIGNER_SERVER_URL=http://100.91.159.54:8421
SIGNER_TOKEN=<aggressive-signer-token>
POLY_REDIS_URL=redis://127.0.0.1:6380/0
POLYMARKET_EXPECTED_ROSTER_SHA256=<reviewed-roster-sha>
POLYMARKET_EXPECTED_MARKET_SHA256=<reviewed-market-sha>
```

The two signer URL variables must be identical. Port `8421` is an example
dedicated aggressive signer endpoint; it must not be the normal signer
process. Port `6380` is an example dedicated aggressive Redis process; do not
reuse the normal Redis process. Runtime keys remain namespaced as a second
line of separation.

## Fail-closed startup

The runner refuses aggressive mode unless all of these are true:

1. the roster and requested runtime scope are both `aggressive`;
2. every enabled host id starts with `aggressive-`;
3. the config directory, roster, and data directory are below the isolated
   runtime root;
4. every enabled account is a managed aggressive profile;
5. generated metadata and both reviewed digests match;
6. config and environment signer URLs match the dedicated expected URL;
7. every local `.account_N.paused` flag already exists.

Validation starts signers and clients but never starts quote workers:

```bash
python /home/ubuntu/polymarket-aggressive-releases/current/platforms/polymarket/maker/multi_runner.py \
  --config-dir /home/ubuntu/polymarket-aggressive-runtime/platforms/polymarket/maker \
  --roster /home/ubuntu/polymarket-aggressive-runtime/accounts.runtime.json \
  --host-id aggressive-a \
  --data-dir /home/ubuntu/polymarket-aggressive-runtime/data \
  --runtime-scope aggressive \
  --runtime-root /home/ubuntu/polymarket-aggressive-runtime \
  --expected-signer-url "$POLYMARKET_EXPECTED_SIGNER_URL" \
  --require-paused --validate-only \
  --expected-roster-sha256 "$POLYMARKET_EXPECTED_ROSTER_SHA256" \
  --expected-market-sha256 "$POLYMARKET_EXPECTED_MARKET_SHA256"
```

## Activation boundary

This isolation layer does not authorize deployment, service restart, signer
changes, funding, withdrawal, order placement, or removal of pause flags.
Daily-loss and pause-equity values are still recorded rather than enforced, so
the aggressive runtime must remain paused until an enforcement change is
separately reviewed, merged, deployed, and verified.

The existing VPS1/VPS2 normal LP services continue independently throughout
that work.
