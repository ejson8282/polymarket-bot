# Predict legacy recovery: nonce barrier and quarantine foundation

## Scope and status

- Owner: codex:predict / domain:polymarket (Predict files only).
- Repository: ejson8282/polymarket-bot.
- Base: main at `4b36da6adcc21ca1a1502895ebb59959c214e6e1`.
- Branch: `agent/predict-recovery-quarantine-20260910`, independent worktree.
- Files: Predict API proxy, offline recovery planner, two focused test files,
  and this document. No Polymarket engine or Dashboard changes.
- Runtime mode of this work: RESEARCH. No services touched; no merge, deployment,
  restart, nonce transaction, runtime migration or trading activation authorized
  by this document. Keep this PR Draft pending recovery design review.
- Dependency: the previous Predict release is deployed; no intervening main
  commits change Predict runtime files. Existing historical pending remains.

This is a recovery foundation, not an automatic unblocker. Existing pending
records remain blocking. There is deliberately no apply, discard, cancel,
incrementNonce, service-control or activation command in this change.

## Verified mechanism and limits

The official Python SDK ABI at
`PredictDotFun/sdk-python@6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1`
contains `nonces(address)`, `isValidNonce(address,uint256)`, `incrementNonce()`,
`NonceIncremented`, `OrderExpired`, and tuple-based `cancelOrders`.

Sources:
- https://github.com/PredictDotFun/sdk-python/blob/6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1/src/predict_sdk/abis/CTFExchange.json
- https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0
- https://dev.predict.fun/-deployed-contracts-1860295m0

An ABI is evidence of an interface, not proof that a particular account recovery
transaction has succeeded. Before a transaction, verify the deployed exchange,
its market mode, the Predict smart-account caller, and the old nonce bound. After
it, require a successful confirmed receipt and `isValidNonce` reads showing the
old nonce invalid and the new nonce valid in the same maker/exchange context.
Read each affected exchange separately. The EOA owner is not interchangeable
with the Predict smart-account maker.

The local proxy defaults expiration to signing time + 24 hours, but also accepts
explicit expirations; repeated signing could refresh it. The pending record age
therefore does NOT prove expiry. The planner does not use this inference.

Incrementing a maker's exchange nonce is broader than one idempotency key: it
may invalidate that same maker's manual orders too. It must have its own explicit
account/exchange-scoped approval after current orders/positions are reconciled.
Never send it from the EOA when the required maker is the smart account, and do
not touch a different account or a Polymarket exchange.

## Proxy protections, initially opt-in

Two per-account settings are introduced, neither enabled by this PR:

- `require_order_ledger: true`: missing, malformed or unreadable ledger blocks
  submission and status recovery instead of treating it as an empty account.
- `use_exchange_nonce: true`: requires strict ledger mode; signs at the fresh
  nonce read for that order's maker and exchange on chain 56. Provider failures,
  wrong chain, invalid current nonce or a still-valid immediately preceding nonce
  abort signing. An explicit old nonce is rejected, never silently upgraded.

Other accounts retain their existing mode. Enabling these settings is a separate
configuration operation on the existing Mac mini service; no new private-key
file or local signer is required.

Submissions persist `order_nonce` alongside expiration in the existing nonsecret
ledger before sending to the venue. An uncertain key always retains that nonce
on retry. In nonce-aware mode, legacy uncertain ledger entries without this
field fail closed with `legacy_submission_nonce_unknown`; a default zero is not
proof of the nonce originally signed and is never used to justify recovery.

An existing ledger row marked `quarantined: true` is a permanent do-not-resubmit
marker for that exact `alias:idempotency_key`. Submission is refused before
signing, even if its old cached status was successful. Status remains
`quarantined/unknown`, NOT rejected, cancelled, filled, or proven-never-submitted.
The current engine consequently continues to preserve pending until a separately
reviewed migration; this PR does not teach it to discard quarantine automatically.

Markers must be installed while all affected writers are quiesced. Preserve the
entire original row and external hash-checked backup. Strict mode cannot detect
a syntactically valid but manually rolled-back ledger: release rollback and
ledger restore after a barrier must therefore be prohibited until separately
reviewed. Do not deploy an older proxy that ignores markers after using them.

## Offline review planner

`platforms.predictfun.maker.recovery_plan.assess_recovery` takes explicit evidence
and returns a review plan. It has no filesystem, network or transaction adapter.
It does not remove pending, rotate generations, generate a replacement registry,
or claim the historical submission was rejected.

Required checks include exact account/key generation, enforced signer fence,
quiesced writers, a fresh complete empty order/position baseline, and per-key
old-nonce upper-bound evidence. The barrier must match the known BSC exchange for
the pending market flags and the baseline maker, have a successful receipt with
at least 12 confirmations (a conservative review policy, not a guarantee of
irreversibility), and invalidate the old nonce. Baseline collection must follow
the barrier. Missing, duplicate, stale, future or mismatched evidence blocks.

Evidence provenance still requires independent review: JSON booleans and hashes
are not cryptographic proof of RPC results or operator authorization. Even a
consistent plan always returns `activation_allowed=false` and
`runtime_write_allowed=false`; its best status is `ready_for_independent_review`.

## Remaining implementation and activation gates

1. Review this foundation, including actual smart-account nonce semantics and
   the upper bound for every old signing path. Do not assume all unknown orders
   used nonce zero merely because it was the default.
2. Implement/review the evidence collector and an account-exclusive, backed-up,
   compare-and-swap maintenance adapter. No production editing shortcut.
3. Deploy the approved proxy version and enable protections for the selected
   account before any invalidation. Verify denied exact-key replay and fresh
   nonce reads without releasing a new order.
4. Obtain explicit scope for any chain invalidation. Verify its receipt and the
   resulting nonce barrier on every affected Predict exchange.
5. Under the same account maintenance window, re-read complete official state;
   archive selected pending with permanent key fences, preserving history,
   unselected accounts and generation bookkeeping. Require independent review.
6. Only after that, separately authorize limited-live recovery at the unchanged
   existing cap. Verify official placement/cancellation and inventory handling;
   no automatic capital expansion.

No claim is made that the account is ready today or that these steps have already
been performed. Official historical support is useful but no longer the only
possible recovery path.
