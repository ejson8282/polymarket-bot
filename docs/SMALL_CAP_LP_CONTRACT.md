# small_cap_lp Contract v1 (Non-Live Foundation)

Base: `064706e6938240d2b321c9103e91709b00151a71`.
Scope: `platforms/polymarket/maker/small_cap_contract.py`, its focused test,
and `tests/fixtures/polymarket_small_cap_contract/`.

This is a proposed business wire contract, not a deployed feature. There is no
runner, allocator, account integration, HTTP handler, signer access, command
delivery, durable idempotency store, or trading capability in this change.
It does not add `small_cap_lp` to the existing profile parser or change standard
or aggressive LP. No live endpoint, service, port, auth token, production account,
successful execution receipt, or live scoring result is provided.

The independent-review corrections add required account
`assignment_effective_at` and receipt `generated_at` fields to this still
unpublished v1 draft. Consumers must use the updated fixtures together with the
reviewed module, not mix earlier draft fixture shapes with this validator.

## Consumer Handoff

| Item | v1 value / meaning |
| --- | --- |
| `schema_version` | Integer `1`; exact matching, not coerced from bool/string |
| `kind` | `small_cap_lp_state` |
| `strategy_type`, `runtime_scope` | `small_cap_lp`; a separate proposed policy and namespace |
| `source` | Always `synthetic`; never interpreted as production data |
| `mode` | `unavailable`, `proposal_only`, or `shadow`; no `live` value |
| `capabilities.stage` | `contract_only` |
| `transport.status` | `unavailable` |
| `transport.service_url` | JSON `null` |
| `transport.authentication` | JSON `null`; authentication reuse is not implemented or selected |
| `transport.dashboard_route_family` | `/api/pm/small-cap`, reserved for a future Dashboard adapter only |
| `capabilities.mutation_commands` | Every named command is `false` |
| `receipts` in a state | Empty; no runtime receipt feed exists |

The Dashboard must not call a guessed localhost/VPS URL or reuse the stable LP
command directory/endpoint. Production should show unavailable/disabled while
this integration is absent; synthetic fixtures belong only in development or
explicitly labeled previews. A synthetic fresh sample is not a healthy runner.
Do not render `synthetic_validated` as live scoring success or show an enabled
button based on it. Wire version changes need coordinated producer/consumer
review. Unknown versions or fields fail closed; they are not silently ignored.

Concrete disabled reasons are `runtime_not_deployed`,
`transport_not_implemented`, `atomic_budget_not_integrated`, and
`cross_host_conflict_guard_not_integrated`. All remain present even when a
synthetic proposal has positive Q or a locally valid command envelope.

## Pure Python Interface

All input/output values are decoded JSON objects. No function reads runtime
files, network, environment, wall clock, credentials, or account configuration.
`now` is supplied explicitly; validation returns a detached copy and does not
refresh source timestamps or replace missing numbers with defaults.
Generation-time consistency and consumption-time expiration are separate checks.
An unchanged snapshot remains usable after delivery delay while every claim that
was fresh at generation remains within its own TTL. Newly expired current claims
raise `evidence_expired`; they are not silently returned as current values.

```python
from platforms.polymarket.maker.small_cap_contract import (
    FIXTURE_TIME, synthetic_state, synthetic_command,
    validate_state, validate_transition, validate_command,
    reject_command, validate_receipt, freshness_status_at,
)

state = synthetic_state("shadow", now=FIXTURE_TIME)
validate_state(state, now=FIXTURE_TIME)
command = synthetic_command(state, action="resume_account", now=FIXTURE_TIME)
validate_command(command, state, now=FIXTURE_TIME)  # References only, no permission.
receipt = reject_command(command, state, now=FIXTURE_TIME)
assert receipt["status"] == "rejected"
assert receipt["dispatched"] is False
validate_receipt(receipt, command, now=FIXTURE_TIME)
```

`SmallCapContractError` exposes a stable `code` and schema `path`; messages do
not echo raw untrusted values. Examples: `invalid_fields`, `invalid_money`,
`unknown_budget`, `identity_mismatch`, `universe_hash_mismatch`,
`revision_conflict`, `duplicate_evidence`, `state_not_fresh`, `command_expired`.
Economic horizons are exclusive end times: consuming a report at or after any
declared `economics.horizon_end` raises `horizon_expired`, even when individual
metric TTLs remain fresh. Command validation inherits this report guard. A
still-cached estimate must not become a current recommendation after its window.
Review guards additionally report `evidence_expired`, `evidence_before_revision`,
`revision_boundary_not_advanced`, `revision_boundary_mismatch`,
`condition_token_remap`, and `duplicate_maker`.
Malformed commands raise an error, rather than manufacturing a runtime receipt.

## Identity and Assignment

- `group.strategy_group` is the group ID. `group.revision` covers roster and
  shared-universe changes. `routing_roster_sha256` identifies a future trusted
  roster; the fixture hash is explicitly a synthetic placeholder, not attestation
  of any actual roster. This PR checks its format/reference, not roster contents.
- `universe` has `markets` and `night_markets`. Each row has `condition_id`,
  YES `token_id`, and NO `paired_token_id`. These are the complete canonical
  candidate identity inputs for this contract. It deliberately excludes
  account-specific choices and is not a ready-to-run config file.
- `group.market_universe_sha256` uses existing
  `account_roster.market_universe_sha256` on that exact universe object. Every
  account references the same digest. Per-account subsets must never replace
  the common universe or become different host-local `markets` hashes. Future
  roster/config integration must explicitly map its richer input schema and
  publish the correct digest, not compare hashes of different representations.
- `accounts[].identity` contains global `account_index` (existing 1-30 range),
  `account_id`, canonical lowercase `host_id`, `chain_id`, `signature_type`,
  lowercase public `maker_address`, `account_uid`, and `account_uid_key`.
  UID reuses `reward_ledger.canonical_account_uid`:
  `chain_id:signature_type:maker_address`. The key is the first 16 lowercase
  hex characters of SHA-256(UID), matching the engine/runtime convention. The
  full UID remains accounting authority; the short key is not authentication.
  v1 accepts signature types 0, 1, and 2; expanding that domain requires review.
- Duplicated indexes, IDs, UIDs, or short keys are rejected in an aggregate
  report. A separate `(chain_id, normalized maker_address)` guard rejects the
  same economic maker represented by different signature types, even if their
  canonical UIDs and short keys differ. This preserves existing UID semantics
  without treating a metadata difference as a second maker. Copying the maker
  to another host cannot create a second income account. Distinct makers on the
  same market, and distinct chain identities, remain allowed.
- `accounts[].assignment_revision` versions that account's entire assignment
  set. Each assignment repeats revision and full maker UID and references one
  condition in the shared universe. One account can have several markets; one
  market can have several accounts. No exclusive ownership policy is inferred.
- `accounts[].assignment_effective_at` is the explicit UTC time when that
  account revision took effect, not a report packaging or first-scoring time.
  It must not be later than report `generated_at`. Every scoring, percentage and
  Q observation for the revision must be strictly AFTER this boundary. Equal
  timestamps, older observations and even stale evidence predating it are
  rejected; new local IDs do not make such data eligible. Unverified revisions
  may have no samples while revalidation is pending.
- Assignments include `assignment_id`, `status` (`proposed`, `shadow`, or
  `retire_pending`), `quote`, `scoring`, two per-token `books`, `cancellation`,
  and `economics`. None is an actual managed order or a proof of admission.

`validate_transition(previous, current, previous_now=..., now=...)` checks two
reports at their respective observation times: no revision regression, no host
or maker/index reassignment, group revision advancement for roster/universe
changes, and assignment revision advancement for allocation input changes.
Prior identities are matched by account index, logical `account_id`, full UID,
and chain/maker; all matching keys must refer to the same prior identity.
Keeping a logical ID while replacing its index, host and maker is not a new
account, even if the group revision and roster hash advance.
For every condition appearing in both supplied universes, the ordered YES/NO
token pair is immutable: token replacement or swapping outcomes is rejected even
if group and assignment revisions advance. Condition remaps require a separate
identity-repair/migration design, not automatic reuse of scoring from another pair.
Ordering of assignments is immaterial; observation updates do not force a new
allocation revision, while a budget-tier change does. Unavailable reports cannot
participate in transition validation: absence of observations is not removal of
the roster. The future adapter must retain the last authoritative identity record.
An unchanged revision must retain the same effective timestamp. When a revision
advances, its effective time must be strictly later than the previous report's
`generated_at` as well as the prior effective time. Thus a new revision cannot
backdate its boundary to validate older observations with new IDs. After an
allocation revision changes, old sample IDs, source IDs, and source timestamps
also cannot be relabeled as fresh revalidation. Matching previous identity by
chain/maker additionally prevents hiding a signature-type change by renumbering
the account. Reports cannot regress their generation timestamps.
There is no persistent ownership store, atomic compare-and-swap, or replay
protection across process restarts yet. A future durable registry must retain
condition-to-token and maker ownership history across missing/removal snapshots;
two supplied reports alone cannot reconstruct a condition that vanished earlier.

## Money, Budget, and Unknowns

All money, prices, shares, Q and ratio values are decimal strings (no float,
scientific notation, NaN, infinity, commas, bool, or implicit zero). Monetary
values accept at most 24 integral and 18 fractional digits. Proposed shares are
positive integer strings, at most 18 digits. A quote's exact notional must equal
`yes_price * yes_shares + no_price * no_shares`, with both prices in `(0, 1)`.
This verifies serialization/arithmetic only, not tick, minimum size, or execution.

A metric always has `current`, `last_known`, and `freshness`. These serialized
slots describe the producer's view at `generated_at`, not an assertion that
values stay fresh forever after receipt:

| Freshness | `current` | `last_known` |
| --- | --- | --- |
| `fresh` | Required decimal string | `null` |
| `stale` | `null` | Required last-known decimal string |
| `missing` or `unavailable` | `null` | `null` |

`budget.tier_usdc` is exactly `"100"`, `"150"`, or `"200"`. Other budget fields
are metric envelopes:

| Field | Meaning required of a future producer |
| --- | --- |
| `asset_equity_usdc` | Nonnegative conservative asset value, from reconciled cash/inventory |
| `cash_usdc` | Settlement cash including cash encumbered by listed BUY reservations, not exchange free cash |
| `inventory_cost_usdc` | Conservatively carried inventory occupation |
| `remaining_buy_usdc` | Unfilled, exchange-confirmed BUY notional |
| `pending_buy_reserved_usdc` | Locally reserved, not yet acknowledged BUY exposure |
| `unknown_buy_reserved_usdc` | Still-reserved submission/acknowledgment-unknown BUY exposure, not zero |
| `fee_reserve_usdc` | Required fees, kept separate from notional |
| `effective_limit_usdc` | `min(tier, asset_equity)` |
| `occupied_usdc` | Inventory + remaining BUY + pending + unknown + fees |
| `available_usdc` | `max(0, min(limit - occupied, cash - BUY - pending - unknown - fees))` |

Fields are disjoint: an unknown submission is not also counted as pending or
remaining BUY. SELL reserves shares, not a second cash amount. Inventory remains
occupied until reconciliation. Insufficient cash or asset value reduces capacity;
profit does not raise the tier, and recovery of assets can restore capacity up
to the original tier. No automatic sweep, top-up, fixed cash fraction, or
"profits cannot repair losses" rule is introduced.

The validator checks arithmetic when all inputs are current. Any missing/stale
input requires all derived current budget values to be unknown. Over-limit
existing occupation is representable with zero available capacity; reporting it
does not authorize more BUY. Proposed allocations are not reservations and are
not added to actual occupation by this contract. There is no atomic reservation
implementation or proof that multiple concurrent markets cannot overspend.

## Time, Books, Scoring, and Cancellation

Timestamps use explicit UTC RFC3339 `Z`, optionally with up to six fractional
digits. Naive/local timestamps and future timestamps fail. Freshness has
`status`, `observed_at`, `max_age_sec` (1-86400), `age_sec`, and `reason`.
The state and each standalone receipt have a required `generated_at`: the time
that serialized envelope was assembled. Source observation time remains in each
`observed_at`. In particular, a newly packaged stale report can have a recent
generation time while its observations are ten minutes old.

At generation, `age_sec` must equal `ceil(generated_at - observed_at)` in seconds;
serialized status/value slots must agree with that age and the declared TTL.
At consumption, actual elapsed time is checked separately against the TTL using
microsecond precision. Equality at TTL is fresh; one microsecond beyond it is
expired. Consumers must NOT rewrite nested ages, timestamps or value slots to
make a cached report validate. Generation in the future, tampered generation-time
ages, and future observations still fail closed.

`validate_state` and `validate_receipt` return the unchanged wire content only
while none of its declared-fresh claims has expired. Otherwise they reject with
`evidence_expired`; the consumer must treat that envelope as unavailable/unknown
for current decisions, retaining only explicitly labeled historical display.
An already published stale/missing/unavailable field remains readable without
continual serialization updates. The pure `freshness_status_at(field,
generated_at=..., now=...)` helper validates the original generation-time claim
and returns effective freshness at read time without mutation. It can be used
to explain an expired report/receipt; it does not validate the rest of the report
or authorize commands. The producer, not the consumer, may publish a NEW envelope
with recalculated ages and stale/last-known slots while preserving original
observation timestamps.

Missing/unavailable evidence has null time and age plus a concrete reason.
Every non-fresh field needs a reason; a fresh field has no reason. Values expire
independently of report age, including books, scoring and percentage evidence.

Each token's book separately reports:

- `ws_received`: last validated complete WS book reception, not socket connection,
  ping/pong, any other market's update, or arbitrary traffic.
- `book_event_at`: source exchange book-event time; a quiet book can have an old
  event timestamp while a complete REST fetch is current.
- `rest_fetched`, `rest_complete`: actual completed fresh REST acquisition for
  this token. Reading a cache must not change this time. Partial/failed fetches
  cannot be usable. Fresh complete REST can cover WS silence.
- `usable_source`: `ws`, `rest`, or `none`; the named source must be fresh.
- `external_front_depth_usdc`, `own_orders_scope_complete`: current external
  depth is unavailable unless all relevant self orders have been excluded and
  a usable book exists. This flag is a proposed assertion, not a cross-host
  order registry implemented by this PR.

Scoring is assignment/maker scoped, not an aggregate across different makers.
Each sample contains a unique `sample_id` and `source_snapshot_id`, current
`assignment_revision`, distinct YES/NO order references, independent
`scoring_freshness`, boolean `scoring`, and separate percentage and Q metrics.
Order references use their own canonical CLOB shape: lowercase `0x` followed by
exactly 64 hexadecimal digits (66 characters). They do NOT use the shorter
generic sample-ID validator. Fixtures use fabricated references of that shape;
validating a reference does not prove the order exists or belongs to the maker.
`percentage` is a fraction in `[0,1]`, not a percent in `[0,100]`. Combined
`scoring=true` is intended to mean both referenced maker orders passed the
official check; collecting that evidence is not implemented here.

Source observations must be strictly post-`assignment_effective_at` and advance
independently for scoring, percentage and Q;
new local IDs on repeated cache timestamps do not count. Independent count is
the trailing consecutive run where scoring is true, percentage is positive,
and `q_min>0`, all fresh. Failed/missing/stale samples reset the run. Three such
synthetic observations yield only `synthetic_validated`, never live promotion.
The contract can check supplied evidence identity/time, not prove an external
API really produced it. Durable deduplication and real official collection are
future work. Freshness TTL values in fixtures are examples, not approved runtime
policy. A producer must enforce the independently reviewed TTL bounds.

Cancellation reports protection trigger, request, and exchange confirmation
timestamps separately. `confirmation_latency_ms` ends at confirmation and is
rounded up to milliseconds. It is null for pending, unknown, or unavailable
cancellation, not zero. Only `synthetic_confirmed` can carry measured sample
latency in v1. This does not confirm cancellation of any actual order.
When a status observation is present, it must be at or after every included
trigger/request/confirmation. Equality is allowed; a later report generation
time cannot make an earlier state observation prove a later cancellation.

## Economics and Accounting

Per-assignment `economics` reports `status`, `horizon_end`, lower reward increment,
scoring uptime, YES/NO exit stress, verified-history stress, fees, switching cost,
net increment, history calibration, missing reasons, and `expansion_allowed`.
All numeric inputs have their own metric envelopes. Horizon cannot extend more
than 24 hours ahead; applying the earlier reward/exit deadline is not implemented.
Missing inputs cannot produce a current zero/positive net figure. Missing
history is labeled `missing`; v1 keeps net estimates unknown until the later
economic model defines the uncalibrated case. It never allows expansion.
This is not an optimizer, a calibrated profitability claim, or a tail-risk model.
For a fully supplied `shadow_only` synthetic assessment, validation checks
`lower_reward_increment * uptime - max(YES stress, NO stress, history stress)
- fees - switching_cost == net_increment`. This is arithmetic on supplied
numbers, not derivation or verification of those numbers. `shadow_only` requires
all inputs, synthetic history calibration and no missing reasons. Incomplete
assessments remain `uncalibrated` or `unavailable`, with no current net figure.

Accounting records are separate by `business_day`, full `account_uid`,
`condition_id`, `accounting_type`, and lowercase `asset_address`; duplicate keys
are rejected. Types: `native_lp`, `sponsored_lp`, `maker_rebate`, `trading_pnl`,
`inventory_unrealized_pnl`, `estimated_lp`, `fees`, `net_cash_flow`, and
`cash_flow_adjusted_nav_change`. PnL, cash flow, and NAV change may be signed;
income and fees are nonnegative. `business_day(at)` uses BJT 08:00-start days,
equivalent to UTC midnight boundaries. Historical conditions need not still be
in the current universe. Current means source freshness, NOT today's income;
daily views must filter the explicit business day.

`accounting_complete` is always false; no complete/net total is fabricated.
In particular, NAV change must not have already-reflected rewards added again.
Persistent ledger writes, official earnings/fees reconciliation, cash-flow
adjustment, cross-host cache consolidation, and historical retention after
market/account removal remain unimplemented here.

## Proposed Commands and Receipts

These are the only proposed commands. ALL are unavailable in v1, including
pause and reassessment; this is not an operational emergency-control interface.

| Action | Exact payload | Future intended scope |
| --- | --- | --- |
| `pause_account` | `{}` | Stop new group BUY, verify cancellation, preserve exits |
| `resume_account` | `{}` | Request independently gated account admission |
| `set_budget_tier` | `{"tier_usdc":"100"}` (also 150/200) | Change a ceiling, never move funds |
| `request_reassessment` | `{}` | Ask the allocator to recompute proposals |
| `apply_assignment` | `{"proposal_id":"<64 lowercase hex>"}` | Reference a validated proposal, never arbitrary token/price/order payload |

Envelope fields: `schema_version`, `strategy_type`, canonical UUIDv4
`command_id`, independent UUIDv4 `idempotency_key`, `action`, `created_at`,
`expires_at`, `strategy_group`, `group_revision`, `account_index`, full UID,
short UID key, fixed `host_id`, `routing_roster_sha256`,
`market_universe_sha256`, `expected_assignment_revision`, and exact `payload`.
The expiry interval is positive and at most five minutes. Current time must be
inside it; command expiry uses consumption time, NOT report generation time.
The referenced state and its current evidence must still be fresh at consumption.
Identities, hashes and revisions
must exactly match the account/group report. No host failover, wildcard/group
fanout, raw order, cancel-all, funding, credential, account-creation, or arbitrary
configuration command is offered.

`validate_command` checks shape/references only. It does not dispatch, validate
the economic proposal, or grant permission. `reject_command` always returns a
LOCAL `contract_validator` rejection with `runtime_not_deployed`,
`dispatched=false`, `result=null`, `applied_assignment_revision=null`, and
unavailable runtime-receipt freshness. The command and SHA-256 of its canonical
sorted compact JSON remain attached so the exact identity and payload can be
checked. Repeating it is deterministic, but there is no durable deduplication
or automatic retry mechanism.

Every receipt must satisfy `command.created_at <= receipt.generated_at <= now`,
including missing/unknown receipts without an observation timestamp. Available
receipt observations must also be at or after command creation. Reading a
historical receipt does not require the command to remain unexpired.

The receipt validator also accepts explicitly `synthetic_fixture` pending or
unknown examples for consumer tests. A newly published stale local receipt must
show `unknown` with optional `last_known_status`. If an unchanged pending receipt
expires in transit/cache, validation raises `evidence_expired`; the consumer can
use `freshness_status_at` to render unknown without rewriting the receipt. A
one-millisecond delivery delay within TTL is valid. Receipt ages are anchored to
the receipt's own `generated_at`, not the enclosed command's creation time and
not the consumer clock. Missing receipts cannot be called success.
No `success`, `applied`, live dispatch, result body, or applied revision is
accepted in this version. Status freshness and runtime-receipt freshness are
separate; a current locally generated rejection is not a fresh runner response.
Future idempotency must bind the key to account/group/host and full payload hash:
same key/same command returns the existing receipt; same key/different payload
is rejected; timeouts remain unknown until authoritative reconciliation. This
PR documents that requirement but does NOT implement the persistent mechanism.

## Fixtures and Verification

All fixture identities, hashes, prices, timestamps, orders and monetary values
are fabricated. Accounts 7 and 12 illustrate nonconsecutive existing-range
indexes, not a first-live-account choice. One has two markets, the other shares
one market. The synthetic universe/roster must never be installed in production.

- `unavailable.json`: disabled controls, null transport/auth, no runtime accounts.
- `proposed.json`: proposed multi-account/multi-market allocations, no scoring.
- `shadow.json`: the same layout with three synthetic independent observations.
- `stale.json`: last-known values only, zero valid-current scoring observations.
- `command_rejected.json`: exact local rejection; no delivery or success claim.

Tests compare fixtures to the deterministic producer at `FIXTURE_TIME` and test
identity, money, revision, freshness, repeated evidence, receipt and non-live
boundary rejections. They do not import the live engine or access production.
The five independent-review reproductions (`test_review_*`) were run against
the reviewed module before changes and all exposed their respective blockers.
Regressions now include unchanged state/command/receipt consumption after 1ms,
generation-age integrity versus exact TTL expiry, explicit post-change evidence,
condition token remapping, 66-character CLOB references and chain/maker conflicts
under different signature types. These remain wire-contract tests, not live
acceptance or a claim of implemented runtime protections.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s tests -p test_polymarket_small_cap_contract.py -v
```

## Remaining Implementation and Required Acceptance

Every row below is NOT implemented/proven end-to-end by this contract PR.
Wire-format/arithmetic examples are not allocator, protection, or live acceptance.

| Required later acceptance | Work still needed |
| --- | --- |
| 100U, concurrent two-market 70U requests cannot both reserve | Account-atomic reserve/submit transaction, shared across markets, durable restart reconciliation |
| 200U may hold two 98U pairs only when fees permit | Real cash/assets/fees ceilings; no arbitrary fixed cash buffer |
| Minimum 100 shares at YES .94 / NO .04 costs 98U, not a 100-share dollar guess | Planner/engine tick, size and actual-price consistency; 100/150/200 tiers allow 1/1/2 pairs with 2/52/4U left |
| YES fills 94U, NO remains 4U: headroom 2/52/102U | Inventory/cash transfers and combined exposure; only 200U can additionally reserve 94U, occupation 192U |
| Fixed 100U tier, assets 100 -> 120 -> 95 -> 105 gives limit 100 -> 100 -> 95 -> 100 | Real asset measurement, losses and recovery; no tier increase or extra loss-repair prohibition |
| Partial fill .70 x100: 70 -> inventory14+BUY56; cancel race adds10 shares -> inventory21+BUY49 | Fill/order reconciliation; release49 ONLY after cancellation confirmation; SELL shares do not double-charge cash |
| Partial fill makes remaining order smaller than reward minimum | Actual quote/score requalification and cancellation/resize behavior |
| 20U pool externalQ300, makerQ100 earns5; adding100Q to original/new maker gives combined8, increment3 | Official percentages and per-maker Q calibration; never combine different makers' opposing legs |
| Alternative pool externalQ400 earns4, wins over increment3; externalQ900 earns2, loses | Incremental integer-share search, recomputation each step, one-for-one/two-for-one replacement, stable ties, keep-cash alternative |
| Self competition includes stable/aggressive and all managed hosts | Real own-order/intent inventory, individual maker Q, no anonymous total-depth-as-exact-Q denominator, no first-account reward multiplication |
| New pool: one maker, one minimum eligible pair, three independent official scoring/share samples; repeat after EVERY expansion | Official authenticated source acquisition, durable cache dedup, ordered evidence, reset on failed/future/stale/partial observations |
| Stress net increment >0 and better than feasible alternatives | Shorter of24h/reward/exit horizon, conservative observed reward increment and uptime, external YES/NO exit depth, worst loss/history/fees/switch cost; unknowns block, no universal ROI floor |
| Quiet book old source event + fresh complete REST is healthy | Existing engine WS, batched REST, independent defense reused directly; cache rereads do not refresh time, token-local age, partial REST invalid |
| Protection trigger to confirmed cancellation is measurable | Real receipt wiring, monotonic latency measurement; not a claim of millisecond execution |
| 60s allocation loop with event-driven reassessment, 10min economic switching cooldown | Independent from immediate book defense, inventory exit and risk cancellation; group token REST dedup, per-engine WS, per-runner cache |
| Submission unknown, cancel race, duplicate command, restart, outage | Persistent holds, CAS, idempotency, authoritative order queries and fail-closed recovery |
| Own/complementary cross-host conflicts are blocked, exits have priority | Real cross-host intent/order contract plus cancellation confirmation; incomplete guard keeps related live pools disabled |
| No accidental old-strategy exit changes or cross-host takeover | Isolated runtime/service/data/pause ownership; fixed host; no claimed process-level isolation of accounts sharing a runner |
| Multi-market one-account with common universe | Roster/config integration, separate account assignments/revisions, no diverging group universe arrays; retain 30-number system |
| Native/sponsored/rebate/PnL/fees/unrealized/estimates stay distinct; BJT08 and maker/day/type/asset idempotency | Authenticated ledger ingestion, no copied-host double count, no stale-current merge, no reward double-count in cash-flow-adjusted NAV, retained history after retirement/disable |
| Separate small-cap policy, conservative execution core, no hard stable50U pool floor | Admission by executable net increment; preserve market type/expiry/book/inventory/exit guards, not aggressive policy or Canary10% sizing; no real policy hook yet |
| Existing funded/connected account pool only; profit stays capped; insufficient assets shrink capacity | Explicitly authorized runtime account integration; never account creation, key export, transfers, auto-sweep or top-up |
| Dashboard status and commands work against a real trusted service | Owner-reviewed adapter, URL/auth choice, read/status freshness, asynchronous delivery/results and receipts; this PR provides none |

Suggested follow-up sequence: review this foundation; implement account budget
reservations and versioned assignments; implement calibrated allocation and
cross-host protective blocking; integrate isolated shadow runner and real read-only
sources; then seek separate merge/deployment/canary authorization with exact SHA,
migration, rollback, and live acceptance evidence. Do not turn this module's
synthetic-only constraints off as a shortcut to any of those stages.
