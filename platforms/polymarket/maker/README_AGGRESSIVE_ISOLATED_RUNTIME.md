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
- loopback-only CONNECT proxy ports generated from the aggressive roster;
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

## Isolated reward observation

The aggressive runner refreshes public Gamma/CLOB reward observations into
its own `data/reward_observer_state.json` and history file every five minutes.
It does not read the normal LP runtime state and the observer never signs,
posts, or cancels orders. Refresh failures are logged and retried without
stopping the engine; stale observer data remains ineligible.

Generate a review-only market-universe candidate with:

```bash
python platforms/polymarket/maker/aggressive_market_selector.py \
  --observer /home/ubuntu/polymarket-aggressive-runtime/data/reward_observer_state.json \
  --principal-usdc 200 \
  --min-front-bid-notional-usdc 5000 \
  --sponsored-risk-config /home/ubuntu/polymarket-aggressive-runtime/base.config.json \
  --limit 1
```

Without `--output`, the command only prints JSON. Even with `--output`, it only
writes a candidate document; it never changes the running config, restarts a
service, removes a pause flag, or places an order. Selection fails closed for
stale snapshots, live markets, unverified candidates, excessive fill risk,
insufficient stability, probe capital above the account tier, missing/stale
depth, either YES/NO leg below the supplied runtime front-depth threshold, or
a sponsored-reward size cap that leaves fewer shares than the reward minimum.
The selector refreshes the official sponsored-reward source, records depth and
sponsor feasibility rejections, and falls through to the next ranked candidate
instead of lowering the engine's production gates.

## Activation boundary

This isolation layer does not authorize deployment, service restart, signer
changes, funding, withdrawal, order placement, or removal of pause flags.
Daily-loss and pause-equity values are enforced by an account-local guard loop:

- total equity = CLOB collateral balance + official Data API position value;
- the daily baseline rolls at Beijing 08:00 and persists across restarts;
- stale equity data fails closed after 90 seconds by default;
- a breach creates `.account_N.aggressive_guardrail`, creates the normal pause
  flag, cancels maker orders, and preserves active exit SELL orders;
- the latch survives restarts and day changes.

The operator must review the account before resetting it. To request a reset,
create `.account_N.aggressive_guardrail_reset` in the isolated aggressive data
directory. The engine only clears the latch and pause flag when fresh total
equity is above `pause_equity_usdc`; the new daily-loss baseline becomes that
fresh equity. A rejected reset request is removed and must be requested again.

For paused maintenance such as staging a reviewed market, create
`.account_N.aggressive_guardrail_reset_paused` instead. It applies the same
fresh-equity checks and clears the guardrail latch, but keeps the account pause
flag in place. If both reset requests exist, the paused reset takes precedence.

These guardrails do not authorize activation. The aggressive runtime remains
paused until its isolation and guardrail PRs are reviewed, merged, deployed,
and verified through a separate approved cutover.

The existing VPS1/VPS2 normal LP services continue independently throughout
that work.

## Exact-SHA release workflow

`deploy_aggressive_runtime.py` is the only supported release path for this
runtime. It uses the same VPS-wide deployment lock as the normal LP deployment,
but writes only the aggressive release, runtime, unit, Redis, and audit paths.

The first account should be one independently funded 200 USDC account assigned
to `aggressive-a`. Before activation, the operator stages these host-local
inputs outside Git:

- `accounts.runtime.json`: one public funder address and a managed aggressive
  profile with `target_principal_usdc: 200`;
- `base.config.json`: the reviewed strategy/risk template with an empty
  `account` object. It must not contain a funder, private key, signer URL/token,
  or API credentials;
- `markets.runtime.json`: a reviewed, non-secret market universe;
- `env/runtime.env`: dedicated signer URL/token and the reviewed roster/market
  digests;
- `/home/ubuntu/polymarket-aggressive-venv`: a dedicated Python environment.

The host must also provide executable `/usr/bin/redis-server` for the dedicated
localhost `6380` service. Activation checks both the standalone Python
environment and Redis binary before installing or starting the aggressive
units. The release-managed proxy listens only on roster ports at `127.0.0.1`,
accepts HTTPS CONNECT traffic only, and rejects non-public destinations.

The normal signer endpoint on port 8420 and normal Redis on port 6379 are
rejected. The initial service activation is always paused and therefore cannot
quote until the account mapping and public funder have been verified.

### Pause-only market staging

Use `stage_aggressive_market.py` when the selected observer market changes. It
refuses to stage anything unless every local aggressive account has a fresh
`paused=true` state, a pause flag, active guardrails, zero live orders, zero
pending unwinds, and zero position value. The candidate must be a fresh,
review-only output from `aggressive_market_selector.py`, use at least the
engine's front-depth threshold, and contain exactly one unexpired day market.
For a shared multi-account roster, feasibility is checked against the account
that deterministic event allocation will actually use. A 200 USDC candidate
may therefore be assigned to a 200 USDC profile even when another account on
the roster has a smaller principal; it is rejected if the selected owner is too
small.

First obtain the candidate digest without changing the host:

```bash
python platforms/polymarket/maker/stage_aggressive_market.py plan \
  /tmp/aggressive-candidate.json --profile aggressive-a
```

Apply requires the exact confirmation printed by `plan` and an audit ID:

```bash
python platforms/polymarket/maker/stage_aggressive_market.py apply \
  /tmp/aggressive-candidate.json --profile aggressive-a \
  --confirm STAGE-AGGRESSIVE-MARKET:<candidate-market-sha256> \
  --authorization-id <authorization-id>
```

The command only atomically replaces `markets.runtime.json` and the matching
reviewed digest in `env/runtime.env`. It does not restart or resume the engine,
does not contact the signer, and does not touch orders. Afterwards run the
normal exact-SHA `plan` / `prepare` / `activate` workflow below; activation
regenerates account configs and remains paused.

Read-only plan:

```bash
python platforms/polymarket/maker/deploy_aggressive_runtime.py plan \
  <full-sha> --profile aggressive-a --expected-current none
```

Prepare an immutable release without installing or starting a service:

```bash
python platforms/polymarket/maker/deploy_aggressive_runtime.py prepare \
  <full-sha> --profile aggressive-a --expected-current none \
  --confirm PREPARE-AGGRESSIVE:aggressive-a:<full-sha>
```

The separately authorized first activation uses
`ACTIVATE-AGGRESSIVE:aggressive-a:<full-sha>` plus an audit authorization ID.
It validates the dedicated Python environment, local proxy, signer mapping,
roster and market digests, installs isolated systemd units, starts isolated
Redis, starts the engine with every account pause flag present, and waits for a
fresh paused state. If verification fails, the aggressive engine is stopped and
the previous aggressive release/config is restored. The normal LP service is
snapshotted before and after; any change fails the activation.
# Paused Real-Account Acceptance

The existing aggressive runtime remains the only aggressive execution system.
The `small_cap_observation` adapter is reused as a read-only input, not deployed
as a second account allocator or a third engine. Existing roster/profile limits
remain authoritative, including 50/100/150/200 or other reviewed principal
amounts; this command does not change them.

After an explicitly authorized, reviewed signer rollout, the dedicated signer
supports `POST /derive-existing-creds` with a mandatory exact registered funder.
Its upstream operation is standard SDK `derive_api_key` (`GET /auth/derive-api-key`),
never `create_api_key` or `create_or_derive_api_creds`. Authentication, IP allowlist,
manual lock and rate limits remain in force. A missing existing key fails closed;
there is no fallback to the legacy unspecified-funder account. Legacy routes are
unchanged. Returned L2 credentials are NOT exchange-enforced read-only tokens;
only the acceptance client's allowlisted GET operations make this workflow
read-only. Credentials stay in memory and must not be printed or archived.

Run from the reviewed immutable release, using its compatible isolated Python:

```bash
PYTHONDONTWRITEBYTECODE=1 /home/ubuntu/polymarket-aggressive-venv/bin/python \
  -m platforms.polymarket.maker.aggressive_acceptance --profile aggressive-a
```

Use `aggressive-b` on VPS2. The command reads the existing fixed runtime root,
roster, generated config and managed `env/runtime.env`; do not copy tokens into
arguments or shell history. This first deployment integration pins the existing
dedicated Mac signer `http://100.91.159.54:8421` and the account's local HTTP proxy.
It does not use `/ready`, legacy `/derive-creds`, engine initialization, service
control, runtime commands, journals, config writes or order/cancel operations.

Checks include:

- Current release manifest, roster/config identity and digests, enabled managed
  aggressive profile, service active, fresh paused state and pause marker both
  before and after collection. A stale state never proves a running service.
- Authenticated collateral, complete bounded open-order/24-hour trade pages,
  live BUY scoring and re-read orders via the merged observation adapter.
- Official public positions and six-decimal chain collateral/CTF balances at a
  recent, hash-rechecked Polygon block; discovered position and cash mismatches
  are blocked, not counted as profit or silently corrected.
- No unresolved BUY liquidity during paused acceptance; SELL exits are read but
  never cancelled. Cash must cover the existing configured principal, and the
  selected standard V2 exchange allowance must cover it. Negative-risk market
  allowance and executable market admission still require their own preflight.
- Per-request connect/read limits, no redirects, bounded pages/response sizes,
  per-sample 60-second freshness and a 240-second CLI deadline. Unknown/stale
  results remain unknown and fail the audit; no data is silently changed to zero.

Exit 0 means only `aggressive_paused_account_acceptance.status=pass`.
Exit 2 is blocked and prints a sanitized failing phase/check. Neither result
enables trading. Empty orders mean scoring is untested, not passed. Public
position discovery is not exhaustive on-chain inventory; 24-hour trade pages
are not complete fill coverage or a capital-journal watermark. Fees, realized
PnL and journal ingress remain outside this check. Fresh executable market
planning, guardrail verification, explicit live authorization and actual scoring
observation are still required to finish aggressive LP delivery.

## Rollout Order and Rollback

1. Review and merge the fixed source head under the existing authorization gate.
2. Separately authorize updating ONLY `ai.codex.polymarket-aggressive-signer`
   (8421) to the merged SHA. The two current signers share a source working
   directory even though their processes and account sets are separate. Do NOT
   overwrite that shared directory. Stage an immutable aggressive-only source
   and dependency bundle, then repoint only the aggressive launcher to it,
   preserving its environment, credential storage and other launchd settings.
   Keep the original aggressive launcher target and bundle as rollback. Verify `/health`, authenticated
   existing-only derivation for both registered accounts (output success/failure
   only), auth denial and unavailable-key behavior. Failure restores the old
   launcher target and restarts only that aggressive signer. Verify the stable
   signer's source hashes, launcher and PID remain unchanged; do not restart8420.
3. Run this acceptance command from a reviewed immutable tooling release against
   each existing paused aggressive runtime first. No engine restart is needed
   merely to collect the report. Keep reports in protected runtime audit storage,
   not GitHub; they include account economic data, never credentials.
4. The aggressive engine itself is still on its prior release. Review the full
   cumulative engine/runtime diff before separately authorizing deployment through
   `deploy_aggressive_runtime.py`, with exact target/current rollback SHAs. Preserve
   existing roster/principal/market limits and paused state. A new acceptance
   command passing does not waive the cumulative-diff review or permit activation.
5. After approved paused deployment and acceptance, obtain explicit authority for
   the one-market small-principal live trial; verify actual orders, scoring,
   cancellation/exit protection and account reconciliation. Do not auto-start VPS2.

No new production service, schema migration or budget-ledger import is introduced
by the acceptance integration. Removing the command and restoring the previous
dedicated signer bundle rolls back this capability without changing accounts,
orders, profiles or balances.
