# Small-Cap Budget Model v2: Event-Local Capital Reuse

Status: implemented synthetic/offline transaction model, pending independent
review. Review base `e8f37233c24e033920dd06a00b03f81a1b3aaff3`; lease #143.
The original implementation base was `4bbc3e84afe263bf71f2643b02943f06346a8e22`.
This is not an executor, allocator, exchange adapter, or live clearance.
The existing stable/aggressive engines are neither imported nor modified.

## Corrections to the Draft

- Economic identity is `(chain_id, lowercase maker_address)`, independent of
  aliases/signature types. An event is one **condition_id** and its fixed YES/NO
  pair under that maker, not a Gamma parent event.
- The 100/150/200 USDC tier caps actual capital, not aggregate unfilled quotes.
  Independent conditions may each quote 70 USDC on a 100 USDC account.
- "Two requests of 70 in the same event must fail" was wrong without side/unit:
  paired capacity sums remaining shares per side, then takes their maximum;
  single-side mode uses price times shares.
- Cash and allowance need separate coverage/watermarks. Never take their minimum
  and subtract the same fill again. New observation time alone proves no coverage.
- Unknown BUY blocks that maker's new BUY, not other makers or exits from proven
  stock. Unknown SELL still reserves its potentially sold shares.

## Versions and Disabled Capabilities

Module: `platforms/polymarket/maker/small_cap_budget.py`.
Input events/output views use `schema_version=2`,
`budget_model=event_local_reuse_v1`, `source=synthetic`.
Outputs keep `live_enabled=false`, `mutation_enabled=false`,
`proposal_only=true`; views have `mode=offline`.
Mutation means exchange/runtime mutation, not writes to this temporary journal.

SQLite `PRAGMA user_version=1` is the **storage** version, not wire schema 1.
The database also stores a model marker. Unknown storage versions, wrong markers
or existing unversioned tables are rejected. No migration of old v1
`occupied_usdc`/`available_usdc` is performed. V1 validators and fixtures remain
unchanged and are not imported. No v1 downgrade, production DB migration,
Dashboard adapter or HTTP route is provided.

## Public API

```python
ledger = BudgetLedger(temporary_synthetic_path, timeout=0.2)
result = ledger.apply(idempotency_key, event, now="2026-09-09T02:00:00Z")
view = ledger.snapshot(maker, now="2026-09-09T02:00:01Z")
ledger.assert_current(maker, view["conditions"][condition_id]["binding"], now=now)
ledger.close()
```

`apply` returns `{replayed, receipt, current}`. Receipt includes key, input hash,
account version, time, accepted/rejected reason and disabled capabilities.
An accepted submit means **synthetic intents recorded**, not orders submitted,
cash locked at CLOB, a fresh executable command, or earned rewards.
Malformed inputs raise `BudgetError` and roll back; lock timeout raises
`BudgetBusy("writer_timeout")`. Policy rejection is journaled as accepted=false
without inserting intents. Some malformed shapes can raise standard
KeyError/TypeError; these also roll back and never produce an accepted receipt.

Replay returns the historical receipt plus a newly evaluated current view.
Old successful receipts are not fresh approvals. There is no proposal setter.
`assert_current` checks identity/version/evidence binding and current reasons;
its success is synthetic validation only.

`event_capacity(orders, yes_token, no_token, mode)` is a pure arithmetic helper:
it accepts remaining quantity and states but does **not** check freshness,
minimum size, pairing admission, scoring or available cash. Single-leg arithmetic
tests must not be presented as paired planner acceptance.

### Envelope

```json
{
  "schema_version": 2,
  "budget_model": "event_local_reuse_v1",
  "source": "synthetic",
  "maker": {"chain_id": 137, "maker_address": "0x1111111111111111111111111111111111111111"},
  "route": {
    "account_id": "synthetic-1", "account_index": 1,
    "account_uid": "137:0x1111111111111111111111111111111111111111:2",
    "signature_type": 2, "host_id": "synthetic-host-1"
  },
  "type": "register",
  "data": {"tier_usdc": "100", "margin_usdc": "0"}
}
```

This offline UID format does not replace the production roster UID. Events must
match the registered route exactly. Maker, account ID, index and UID have unique
constraints: another alias/signature cannot duplicate cash or rebind a host.
Indices remain 1..30. Register only creates a synthetic test identity in SQLite,
not an actual account. Condition tokens cannot be rebound or assigned to a
different condition.

### Event Data

| Type | Input and effect |
| --- | --- |
| register | Fixed tier_usdc (100/150/200), absolute margin_usdc; immutable route. |
| sources | Nonempty subset of cash/allowance/assets/inventory; independent mock evidence. |
| configure | condition_id, yes_token, no_token, mode paired/single, increasing assignment_revision, effective_at, tick, minimum_shares, min_front_depth_usdc, end_at, min_seconds_to_end, category, mock_eligible. Clears active book/scoring evidence. Mode changes only before any intent exists on the condition. |
| books | condition_id, assignment_revision, per-token samples; one token update does not refresh another. |
| submit | condition_id, assignment_revision, exact current account_version, nonempty orders batch. Each order: unique intent_id, token, BUY/SELL side, quantity, price. One batch has one side; paired BUY evaluates final aggregate including existing intents. |
| ack | intent_id, unique exchange_order_id; pending becomes live without double reserve. Unknown needs reconciliation. |
| unknown | intent_id; persist uncertain submission risk, with no TTL release. |
| cancel_requested | intent_id; retains remaining quantity/risk. |
| cancel_confirmed | intent_id, exchange_order_id, fresh exhaustive proof with exact known includes_fill_ids and remaining. Releases only unfilled quantity. |
| reconcile_order | Same proof plus resolved_state live/cancelled; resolves unknown and cannot resurrect cancellation. |
| fill | trade_id, component_id, intent_id, exchange_order_id, immutable quantity/price/fee_usdc (nullable), inventory_cost_usdc (SELL only), occurred_at, status. |
| reconcile_account | Exact resolved_issues; only late-report flags can clear after all four fresh sources explicitly include all fills. Failed/contradictory corrections remain unimplemented. |

Amounts are nonnegative decimal **strings**, up to 18 integer and 12 fractional
digits. Floats, NaN, infinity and exponential inputs are rejected. Computed
deficits may be negative and block new BUY. Private Decimal precision is 80;
SQLite stores amounts in JSON TEXT, not REAL. Bool is rejected for integer
identity/revision/watermark fields.
Computed products and sums retain full Decimal precision; the wire input scale
limit is not reapplied to internally calculated notional or aggregate shares.

### Mock Evidence

Source records contain source=synthetic, source_id, sample_id, positive watermark,
UTC observed_at, max_age_sec (1..300), boolean trusted, includes_fill_ids and value.
Scalars are decimal text/null. Inventory is
`{token: {shares, cost_usdc}}` or null. Coverage uses fill IDs from receipts, not
parent trade IDs. Covered fills must be known and occur no later than the sample.
Coverage cannot regress. Changed evidence needs a greater watermark, new sample
ID, non-regressing time and unchanged source identity. Rereading an identical
sample does not refresh its age. Previously used sample IDs are retained and
cannot be relabeled after intermediate samples. Assignment changes take effect
at their transaction time and require newer per-token book samples, not a
previous revision's scoring/evidence relabeled with a new revision.

Book samples add complete, best_ask, reward_low, reward_high, front_depth_usdc,
fee_rate and scoring true/false/null. A fresh complete REST-like sample may have
an old informational event_at. Actual sample time controls age; cached reads and
another token's success cannot refresh it. All are caller-supplied **mock
assertions routed in the maker envelope**, not authentication or official data.
A future live adapter must supply its own reviewed identity/freshness guarantees.
Probability inputs require 0 < best_ask <= 1 and 0 <= reward_low <= reward_high
<= 1. Null remains unknown rather than zero. Invalid samples roll back without
changing the previous evidence. Cancel confirmation observations cannot predate
the cancel request or the fills that they cover.
Each new cancel attempt and transition to unknown records a reconciliation time
floor and the last known proof watermark. Both cancellation confirmation and
order reconciliation require evidence at/after that transition and a greater
watermark than its pre-transition proof, including when timestamps are equal.
Retrying the same pending cancellation does not move its time floor. A proof
that still reports live can resolve an attempt, but cannot cancel a later one.
Every consumed proof is also durably bound to its resolved outcome. An identical
proof can repeat only that outcome; changing live to cancelled requires a new
sample with a greater watermark and valid observation time. This applies even
without another cancel/unknown transition and survives reopen/replay. Existing
proofs lacking an outcome marker cannot be reused as cancellation evidence;
new evidence is required. Historical event-key replay returns its old receipt
without changing the current order state.

## Capacity, Capital and Examples

Three quantities are reported separately: account real capital, condition-local
capacity and cross-condition displayed notional. No new reuse multiplier,
market-count cap, fixed $50 reward floor or automatic tier upgrade is introduced.
Displayed notional is contingent exposure, not a cash reservation or proof that
all simultaneous fills can be funded.

| Scenario | Result |
| --- | --- |
| YES70 shares + NO70 shares, same condition | Paired capacity70; cash depends on prices. |
| Two YES70-share orders, same condition | Helper capacity140; paired planner additionally needs qualified NO. |
| Two complete pairs of70 shares | Same-condition capacity140. |
| Two independent conditions each with pair70 | Each capacity70, not account aggregate140. |
| Single side .70 x100 shares | Capacity70 USDC; two conditions may each do this on100. |
| YES.94/NO.04 x100 shares | Actual cost98 USDC, legacy capacity100 shares, margin separate. |
| That pair, tiers100/150/200, zero fee/margin | One/one/two complete pairs per condition; notional headroom2/52/4. |
| Tier100, assets100->120->95->105 | Effective capital100->100->95->100. |

Legacy paired capacity and actual cash are separately gated. With available100
and absolute margin1, cost98 passes but capacity100 exceeds99 and fails.
This does not rename shares as cash or relax the existing engine.

Paired mode requires two positive, minimum-qualified legs whenever BUY remains.
Pending/live legs qualify the pair; cancelling/unknown legs only reserve risk
and cannot qualify its missing side. A replacement must still fit alongside
every cancelling leg until exhaustive cancellation confirmation releases it.
Each order also meets minimum, tick, mock reward zone, no-cross, trusted front
depth and expiry/category gates. Partial/full fills recheck both sides and may
produce cancel_buy_proposal for the remaining side. Weather/up_down and near-end
mock conditions stay blocked. Scoring false/null blocks BUY and assert_current.
True means only mock_pass; reward_eligible is **always null**, not official Q or
scoring. Bootstrap probes and three-sample scoring promotion are not implemented.

## Source-Specific Arithmetic

For a BUY fill debit price*quantity+fee only from sources not explicitly covering
it. Adjust cash and allowance separately before taking min:

```text
old cash100, not including fill70                 -> cash30
new cash30, explicitly including the same fill70 -> cash30
old cash100 + new allowance30 covering fill70     -> min(30,30), not -40
```

Uncovered BUY fills add projected inventory shares/cost; covered inventory is
not added twice. Uncovered fees reduce conservative assets. Uncovered SELL
inventory removes shares and explicit cost basis. SELL proceeds are not credited
until the cash source includes them. Uncovered SELL losses reduce conservative
assets; unobserved positive proceeds do not raise capital.

```text
effective_capital = min(fixed_tier, conservative_assets_after_unreflected_costs)
capital_headroom = max(0, effective_capital - inventory_carried_cost)
event_quote_ceiling = max(0, min(adjusted_cash, adjusted_allowance,
                               capital_headroom) - absolute_margin)
```

Inventory headroom combines by min; inventory is not subtracted from already
debited cash again. Allowance is not assets. Other conditions' resting BUY is
absent from all source equations. Unknown/stale data cannot produce a passing
ceiling. Unknown fees retain the fill and block new BUY; affected unreflected
scalar outputs are null, not fictitious zero-fee totals.

SELL uses independently observed stock, subtracting known unreflected SELL fills
and active SELL reservations. Unreflected BUY fills are not offered as settled
stock. Unknown BUY/cash does not block a proven-stock exit; unknown SELL keeps
share reservation. SELL reserves no cash and takes priority over new BUY on its
condition while remaining shares are positive. A fully filled historical SELL
does not permanently block new BUY; all finance, inventory, uncertainty and
market-evidence gates still apply. Unknown/untrusted inventory or insufficient
shares still block SELL.

## Atomicity and Replay

Each apply uses BEGIN IMMEDIATE and commits input, idempotency result,
maker/order/fill/source state, account version and every condition proposal in
one SQLite transaction. Maker state is one JSON aggregate; exchange IDs also have
a durable uniqueness table. Snapshot loads one aggregate and recalculates expiry.

Same key/different payload conflicts. Pending->live remains one intent.
Exchange-order IDs cannot bind a second intent/maker. Fill keys hash economic
maker + trade_id + exchange_order_id + component_id. Same payload is idempotent;
different amounts/identity conflict, never INSERT OR IGNORE. Status advancement
MATCHED/MINED/RETRYING/CONFIRMED/FAILED does not add quantity twice. FAILED retains
conservative debit and blocks BUY. Overfills/limit violations are retained as
blocking issues, not discarded evidence.

Cancel request releases nothing. Confirmation needs exhaustive fresh mock proof,
all known fills and exact remaining quantity. A late fill after cancellation
still debits and flags reconciliation. Replacements must fit alongside the old
order until confirmed cancellation.

Every accepted source/fill input synchronously recomputes all tracked conditions
for that maker in the same transaction. Binding includes account version, every
source identity/sample/watermark, assignment revision and book hash. A submit
requires exact current account_version; concurrent stale callers reload and
construct a new request. Two independent conditions can pass after retry; the
same condition's capacity is recomputed under lock and cannot be bypassed.
No external old proposal can overwrite the current stored proposals.

Failure yields cancel_buy_proposal for existing BUY, blocked if none, or
revalidate_proposal when synthetic checks pass. This batch intentionally requests
cancel/replanning rather than inventing a price/quantity resize allocator.
It never executes cancellation or claims that a smaller invalid order still
earns rewards. Pending risk remains until explicit confirmation.

Unknown persists over timeout/reopen. Recovery needs fresh exhaustive order
reconciliation. Only that economic maker's new BUY is blocked, not other makers.
No 60-second allocator/10-minute cooldown delays this event path. Observation and
cancel latency are not wished away: adverse simultaneous fills can produce a
persisted deficit; the model does not promise prevention.

Atomicity is same-host/same-DB, **not distributed across VPSs**. Tests use
os._exit before/after COMMIT, reopen and replay the exact event to prove no
half-write/duplicate. synchronous=FULL is used, but no claim is made about
power-loss, disk-corruption or distributed durability.

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_polymarket_small_cap_budget.py' -v
python3 -m unittest discover -s tests -p 'test_polymarket_small_cap*.py'
```

Only TemporaryDirectory SQLite files and local child processes are used.
Coverage includes units, reuse and same-condition contention, stale-version
retry, independent source coverage, fixed tiers, partial/late/failed fills,
unknown persistence, identity collisions, paired/scoring revalidation, SELL
reservation, idempotency/conflict, writer timeout, commit-before/after process
death, restart replay and per-token/source freshness. Exact counts are reported
with the parent task's fixed head, not claimed as live acceptance.

## Official Evidence and Remaining Gaps

The official [order lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
describes balance less unfilled orders, settlement states, and cancellation of
only unfilled portions. It does not establish cross-condition scope of that
formula. Existing behavior is compatibility evidence, **not a CLOB guarantee**.

The official [scoring endpoint](https://docs.polymarket.com/api-reference/trade/get-order-scoring-status)
also requires live status, minimum size, spread and required duration. Budget,
mock scoring and a local timer are not official scoring. Pages checked
2026-09-09; live behavior still needs separate authorized verification.

Explicitly unimplemented:
- Allocator, marginal economics, risk-adjusted resize, new-pool bootstrap,
  three-observation scoring expansion or live market selection.
- Real snapshots/order books, CLOB/WS/HTTP, auth, signer, account configuration,
  account creation, transfers, runner or trading.
- Actual cancel/SELL receipts, latency guarantees, simultaneous-fill prevention,
  distributed ownership or production migration.
- Upstream reordering buffer: coverage can name only fills already in this DB.
- Explicit FAILED/contradictory-fill correction/reversal; these remain blocked.
- FIFO/realized PnL, real earnings ledger, Dashboard v2 adapter or LIVE controls.

All trust/exhaustiveness/scoring inputs are synthetic assertions for offline
state-transition tests. These tests do not prove live ingestion or trading safety.
