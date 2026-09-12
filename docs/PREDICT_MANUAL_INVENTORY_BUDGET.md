# Explicit manual inventory budget separation

Predict can separate an explicitly designated manual market from bot inventory
limits without selling that position or treating its value as deployable cash.
This is opt-in and does not infer ownership from missing managed orders.

The runtime config can declare both protections (synthetic example only):

```json
{
  "manual_market_exclusions": {"example_account": [42]},
  "manual_inventory_budget_exclusions": {"example_account": [42]}
}
```

Every budget-excluded account/market must also be operation-excluded. Invalid
configuration fails before executor construction. Default behavior is unchanged.
The entire selected market, both outcomes, is reserved for manual management;
do not use this for mixed manual/bot inventory requiring lot-level attribution.

For live and authenticated read-only inventory, selected positions no longer
consume bot share, inventory-value, unrealized-loss or total-exposure limits.
They also do not increase the bot capital tier. Full positions remain available
in status and risk counts. Risk summary separately reports manual excluded
position value and remaining bot position value. Unselected accounts/markets
retain existing limits. Unknown or malformed identity is never exempted.

Cash is still read from account balances, reduced by the existing manual BUY
reservation logic, and capped by the existing capital and deployment limits.
Manual cash is not added back. Manual-order entry pauses, quote freshness,
kill switches, managed-order ownership and uncertain-submission guards remain
unchanged. Market exclusions continue to prevent automated entry, inventory
exit and cancellation in the selected markets, including shutdown paths.

This change does not clear pending submissions, invalidate signatures, relax
recovery evidence requirements, or authorize resuming live BUYs. It does not
add a production account/market policy to repository templates. Activation of
an exact runtime policy and release requires a separate scoped authorization.

The account-wide position BUY pause also excludes only explicitly designated
manual inventory; other bot positions still pause BUYs. The release wrapper
accepts an optional `manual_market_policy` in its Python `execute` activation
API, containing exactly the two exclusion fields above. It validates the policy
and target accounts before stopping services, includes it in the same atomic
configuration/rollback transaction, and preserves these fields on subsequent
deployments when no replacement policy is supplied.
