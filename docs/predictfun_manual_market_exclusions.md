# Predict manual-market automation exclusions

Status: code and synthetic tests only. No production policy has been installed.
This is part of the unfinished recovery work in PR #150, not a deployment or
permission to resume an account.

## Scope

A runtime config may explicitly reserve whole markets for manual management:

```json
{
  "manual_market_exclusions": {
    "test_account": [42]
  }
}
```

The names above are synthetic. Actual account/market selections belong only in
the separately authorized private runtime config, never in Git. Match exact
configured account IDs and positive integer market IDs, not titles, slugs or
token IDs. All outcomes, BUY and SELL, and all quote levels in that account's
market are excluded. Another account in the same market is unaffected.

This is explicit market segregation, **not** a claim to reconstruct ownership
of fungible shares. If robot and manual inventory coexist in an excluded
market, both are left for manual management. Other markets keep their existing
exit logic; this does not add universal automatic manual-position recognition.

## Enforced runner behavior

- No maker quote or inventory-exit intent is generated for an excluded scope.
- Previous excluded intents do not become automatic cancellations when the
  next plan omits them.
- Reconciliation independently checks the current policy, so a stale create
  or exit intent cannot bypass the planner. Cancel decisions use the managed
  order's actual account/market, not a market ID claimed by a stale diff.
- Normal, reduce-only, risk-blocked, exception cleanup and shutdown paths
  preserve excluded active managed orders instead of cancelling them.
- Pending submissions in the excluded scope remain unresolved records. No
  signer submission lookup/adoption is performed for them by the runner.
  Their existing account-wide new-key block remains; exclusion does not erase
  history, rotate keys, prove expiration or clear a recovery block.
- Full balances, live positions and manual-order reservations remain available
  to capital and risk calculations. Exclusion does not manufacture free funds
  or pretend an account has no position.

The parsed policy is immutable for a runner session and is reloaded from its
runtime config on restart. It is not inferred from a currently resting manual
order: a partial fill, full fill, order disappearance, or market-status change
does not remove the configured exclusion. Policy changes require a separately
authorized deployment/configuration window and restart; this implementation
does not hot-reload policy or write a production config.

Missing configuration preserves legacy behavior. A present invalid policy
(including null, wrong types, blank account, boolean/nonpositive/string market
ID) fails before runner client/executor creation. Removing a valid config entry
explicitly removes the protection at the next start; no tamper-resistant marker
or protection against an old release is claimed here.

## Existing orders and reporting

An exclusion **does not cancel an order already at the exchange**. An existing
resting order can still fill. Execution reports and the runner account summary
include `manual_market_exclusions` with:

- `markets_by_account`;
- `excluded_managed_active_orders`;
- `requires_manual_order_review`.

The active count covers engine-managed orders, not all manual website orders.
Their normal account readout remains unchanged. Zero automatic actions is not
proof of zero exposure. Excluded records stay in the managed registry; they are
not relabelled cancelled. Existing release shutdown-cleanup checks still reject
nonzero managed active orders, including excluded ones. This change does not
bypass that deployment gate.

Scope is the configured maker runner and standalone dry-run planner/reconciler.
It does not constrain separately authorized one-shot trading tools, the generic
Mac signer/API, a website session or an old binary. Account-wide nonce
invalidation may still invalidate manual orders and requires its own analysis
and explicit authorization. The recovery planner's empty-account/evidence gates
remain unchanged.

## Acceptance before production use

1. Independently review the fixed commit and synthetic regression results.
2. Separately approve the exact release and private account/market policy.
3. Read official orders and positions before activation. Any existing excluded
   engine order requires a user decision; do not cancel it automatically.
4. Verify the effective policy in fresh runner and execution output, including
   after restart. Confirm no excluded creates, exits or automatic cancellations.
5. Confirm all raw holdings still appear in account/risk data and another
   permitted market retains its normal controls. Keep unresolved-submission,
   live-mode and recovery gates intact.

No tests or policy in this document establish historical signed-order nonce
bounds, complete PR #150 recovery, or authorize a transaction.
