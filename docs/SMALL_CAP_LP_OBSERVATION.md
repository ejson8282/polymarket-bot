# Small-Cap Read-Only Observation

Stage: first source adapter, pending review and real-account acceptance.
Base: c7a9630a859f9ebcc87d879718296dc0e79ad0f0 (merged #145); lease #148.
This change does not alter any existing runtime, strategy, account or order.

## Scope

`small_cap_observation.py` consumes official-shaped CLOB responses and emits a
separate `small_cap_lp_observation` schema1. This is neither the historical
small-cap contract schema1 nor the synthetic budget journal's schema2.

An existing, already authenticated V2 SDK client may be injected through
`ClobReadTransport`. It exposes only GET access to four fixed paths:

- `/balance-allowance`: collateral balance and one explicitly selected spender.
- `/data/orders`: complete pagination within explicit request/row limits.
- `/data/trades`: paginated history within a caller-specified time interval.
- `/order-scoring`: existing live BUY order IDs, followed by another open-order
  read to detect disappearance, size/price/status changes during sampling.

No client initialization, key-file loading, credential derivation, order signing,
POST/DELETE, allowance update, account creation, service command or journal write
is implemented. GET authentication uses the existing SDK's HMAC helper; it is
not permission to sign or place an order. The caller owns client lifecycle and
must configure a finite HTTP timeout. Limits bound request counts, not a hard
wall-clock deadline for an arbitrary injected transport.

The transport checks the fixed CLOB host and public chain/signature/funder
configuration before each request. Order rows and own trade components are
checked again against that maker. This is configured-client binding, not an
independent proof of which account owns the supplied credentials. Authenticated
identity bootstrap and real-source acceptance remain separate work.

## Interface

```python
transport = ClobReadTransport(existing_client, expected_public_identity)
report = collect_account_observation(
    transport,
    expected_public_identity,
    collateral_spender=verified_spender_address,
    trade_after=window_start_epoch_seconds,
    trade_before=window_end_epoch_seconds,
)
assert report["budget_admission_enabled"] is False
```

Identity reuses production `canonical_account_uid(chain_id, signature_type,
maker_address)`. It does not reinterpret the offline budget journal's distinct
route-UID format. This batch supports chain137 and existing signature types0/1/2.
The caller must explicitly choose an appropriate collateral spender; there is
no max-allowance, first-map-entry, other-account or other-host fallback.

## Meaning of the Data

Collateral follows the existing engine codec: integer raw six-decimal units.
Order/trade size uses CLOB display-share decimal strings. There is no magnitude
guessing or float coercion. The codec must be re-reviewed if upstream units
change. An absent spender stays unknown even when another spender has approval.
Allowance is not cash or assets; `effective_capital_usdc`, inventory and fill
coverage remain null. Only the balance and selected allowance are observations.

Maker fills use the account's matching `maker_orders` components, including that
component's own token, side, matched quantity and price, not the top-level trade.
For example, a top-level complementary trade can have a different side, size and
price. Within the verified maker scope, stable component identity is trade/order;
role remains evidence, so reporting the same order as both MAKER and TAKER
invalidates the section instead of counting twice. Different order IDs within
the same trade remain distinct. Exact duplicates coalesce and conflicting
duplicates invalidate the section, including across pages. Taker rows require matching
public maker identity. MATCHED/MINED/RETRYING/CONFIRMED/FAILED stay distinct and
are all visible. Fees are unknown, not zero; there is no PnL or refund inference.

`pagination_complete` means only that the endpoint returned its terminal cursor
within this interval and bound. Repeated cursors, missing cursors, changed
duplicates, partial-page errors or exceeded bounds produce unknown, never an
empty-success fallback. Partial rows are discarded. Even a completed traversal
is not an atomic account snapshot and cannot prove cancellation or fill coverage.

Scoring is per sampled existing LIVE BUY with positive remaining size. False
means observed false; errors/malformed responses mean unknown. Sampling limits
and newly appeared orders are listed in `scoring_unchecked_order_ids`. The
second orders pass invalidates scoring if the corresponding row changed or
cannot be checked. It does not make subsequent changes impossible or imply a
matched pair, minimum duration, Q, profitability or promotion readiness.
Known `ORDER_STATUS_` values normalize to their bare status before deduplication
and scoring selection. Unknown statuses remain unknown, never implicitly LIVE.

## Freshness and Output Safety

Each section and scoring request preserves local request start/end timestamps.
`generated_at` is packaging time, not a source refresh or server watermark.
Age is measured conservatively from request start, including multi-page work.
Slow early sections may already be stale when the report is packaged.
`observation_at(report, now=...)` preserves original timestamps, moves expired
current values to last-known and never revives stale data by rereading a report.
Local query time is not exchange event time or balance/fill synchronization.

Only normalized whitelisted economic fields are returned. API owner IDs,
headers, raw payloads, credential fields and exception messages are not emitted.
Errors are fixed codes. Missing data is unknown rather than numeric zero.
The output always keeps live/mutation/budget-admission disabled and explicitly
blocks on absent inventory/assets, unproved fill coverage and absent runtime.

No automatic conversion to `BudgetLedger.apply` exists. In particular, do not
relabel a real observation as `source=synthetic`, invent includes_fill_ids or
clear an unknown/cancel hold merely because an open-order row disappeared.

## Verification and Remaining Work

Tests use fabricated response shapes and mock transports only, not private
production snapshots. They exercise pagination, exact Decimal units, own maker
components, duplicates/conflicts, unknowns, read-only routing, identity/host
drift, scoring races, redaction and independent sample expiration.

Still unverified or absent: real-account read acceptance, authenticated identity
bootstrap, inventory/assets adapters, source-specific fill coverage/watermarks,
cross-source reconciliation, fee/PnL evidence, distributed ownership, economic
allocator, budget integration, runner, Dashboard and live acceptance. No schema
or production DB migration is performed. Removing this unused adapter needs no
service rollback; it cannot undo an exchange order because it issues none.

## References

Official documentation checked 2026-09-10:

- [User orders](https://docs.polymarket.com/api-reference/trade/get-user-orders)
  defines the authenticated paginated orders response.
- [Trades](https://docs.polymarket.com/api-reference/trade/get-trades)
  defines trade history, roles and maker components.
- [Order scoring](https://docs.polymarket.com/api-reference/trade/get-order-scoring-status)
  returns a boolean for a particular order. Local observation is not a substitute
  for that endpoint's eligibility result.

Local SDK source was checked for get_open_orders/get_trades pagination and
balance/scoring GET paths. The facade preserves the cursor metadata that the
SDK's high-level list-returning methods discard. Engine and PnL implementations
were consulted for existing unit and own-maker component conventions; neither
file is changed or imported by this adapter.
