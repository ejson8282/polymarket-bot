# Predict exit recovery: review and acceptance

Owner: Predict business task. Domain: Predict only.
Base: `4bbc3e84afe263bf71f2643b02943f06346a8e22`.
Branch: `agent/predict-exit-recovery-diagnostics`.
Related owner lease: polymarket-bot issue #40.

## Scope

This change fixes submission bookkeeping and preserves safe proxy error codes.
It does not replenish cancellation gas, repair network connectivity, or remove
historical pending records by itself.
No configuration, signing, service, Dashboard or Polymarket changes are included.
No deployment, restart, live submission or cancellation is authorized by this
document. Deployment must remain a separate exact-SHA action.

## Reproduced defects

1. `recover_uncertain_submissions` registered every planned replacement before
   checking whether a submission existed. If cancellation then failed, the new
   order was never submitted but remained pending. Changing prices accumulated
   these records across cycles. Now a plan becomes pending only after an actual
   create attempt or a signer-ledger response confirming an unknown submission.
2. An unresolved attempt did not prevent a different intent/idempotency key from
   being submitted on the same account. New keys are now blocked until pending
   attempts resolve. Other accounts and engine-owned cancellation remain usable.
   A ledger-confirmed unknown attempt is not immediately submitted again. The
   existing same-key retry path remains for transport failures with no ledger
   result; this patch does not manufacture a new key for those attempts.
3. Cancel failures collapsed all proxy failures into `RuntimeError`. Reports now
   retain a fixed allowlist of error codes and HTTP status. Unknown error text,
   response bodies, exception details, URLs and credentials are never copied into
   this diagnostic. Cancellation still requires verified success or an official
   terminal order status; missing order data is not success.

## Historical pending records

Old pending records are not deleted merely because a lookup returns no result,
the price changed, or the records are old. A lost response can also represent a
real order. Resolve them with exact account/idempotency-ledger and official
order evidence before activating this version. Legacy phantom records can
therefore keep new keys blocked; this is an explicit deployment prerequisite,
not evidence that reconciliation has already succeeded in production.

An absent signer-ledger lookup alone is insufficient: the current proxy loader
also returns an empty ledger for a missing, unreadable or malformed ledger file.
Do not add an automatic age-based or `found=false` purge. Even an empty official
open-order list does not rule out an already filled order.

Any historical cleanup needs a separately authorized, account-scoped maintenance
window, not a side effect of deploying this patch:

1. Quiesce the affected account's runner and other submission writers. Do not
   stop or alter another account or Polymarket. Confirm the runner cannot write
   its in-memory registry back while maintenance is in progress.
2. Archive the original registry outside the source repository, with its hash
   and an exact account/idempotency-key allowlist. Never clear the whole registry.
3. Read and validate the actual ledger file as a dictionary with the expected
   account entries; reject missing, corrupt, unreadable or incomplete evidence.
   Match each selected key against the ledger, official orders/activity and
   current positions. Classify only proven never-submitted records as phantoms;
   preserve ambiguous records, real orders, generations and manual orders.
4. Recheck the registry and ledger hashes before any authorized atomic change.
   Abort if either changed. Preserve all other accounts and keys, and retain an
   external audit of exactly which pending rows were removed and why.
5. Validate the resulting registry and official position/order agreement while
   still paused. Resuming submissions requires its own explicit authorization.

## Exit sizing boundary

The existing per-order cap is applied in three places: the account intent
planner, the VPS executor, and the Mac mini proxy's server-side cap. Inventory
exit SELLs currently share this cap with new maker orders. Increasing only the
planner cap can still fail at either subsequent boundary.

A later separately reviewed exit-limit change should use an explicit finite
exit allowance, default to the current cap when absent, leave new BUY limits
unchanged, require fresh available inventory and account/token matching, and
subtract manual SELL reservations. It must also preserve post-only handling,
unknown-submission protection, and cancel-before-replace verification. A new
exit cap must not allow unbacked SELLs or silently increase signer authority.

## Points and capital qualification

Official rules require minimum size and spread eligibility. Market PP/hour is
a pool allocation, not a promised personal reward or a dollar amount:
https://docs.predict.fun/the-basics/how-to-earn-points

An LP qualification check must run after all quote resizing, using the final
order size, price, current reward window and fresh book. A valid small exit order
can be non-qualifying for points and should remain labelled as an exit, not LP.

For a binary market with minimum size S, a two-outcome BUY quote costs
`S * (YES_bid + NO_bid)` in total notional. For example, 100 shares per outcome
at 0.49 and 0.49 require 98 USDT, before reserve and execution costs. This is an
illustration, not a selected market, capital authorization or return forecast.
Do not resize a quote below S and still call it points-eligible. If the budget
cannot support the final bilateral quote, report insufficient qualifying
capital instead of presenting a canary order as a working LP strategy.

## Acceptance gates

1. Run Predict regression tests and compile/diff checks. The GitHub Predict
   safety job must explicitly include `tests/test_predictfun_exit_recovery.py`;
   a green job that omits this file is not evidence for these regressions.
2. With read-only runtime access explicitly permitted, identify the actual cancel
   error and reconcile all historical unknown submissions. Preserve manual orders.
3. Review the exact patch and obtain the required merge/deployment authorization.
4. After an authorized release, verify the error classification, no new phantom
   pending records, no duplicate new keys, and official order/position agreement.
5. Only then choose fresh eligible markets and separately authorize any new-order
   or exit cap. A service being active does not satisfy these acceptance gates.
