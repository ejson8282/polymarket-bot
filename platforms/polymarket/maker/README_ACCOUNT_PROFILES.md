# Polymarket LP account profiles

`lp_account` adds non-sensitive operating metadata to each generated account
config. It does not move funds and never contains a private key.

```json
{
  "lp_account": {
    "account_id": "aggressive_50",
    "enabled": true,
    "profile_type": "aggressive",
    "strategy_group": "aggressive",
    "target_principal_usdc": 50,
    "allocation_mode": "exclusive"
  }
}
```

## Principal

`target_principal_usdc` accepts any positive amount. The example roster uses
50, 100, 150, and 200 USDC only as convenient presets.

For a managed account, quote sizing uses:

```text
effective available = min(live available collateral, target principal)
```

The existing strategy budget percentage, feasibility cap, and per-market caps
are applied after this account-level cap. An account with 1,000 USDC available
and a 50 USDC target therefore cannot size quotes from the full 1,000 USDC.

When `lp_account` is absent, the engine keeps its legacy sizing behavior.

## Shared market allocation

`allocation_mode: "exclusive"` assigns each event to one account in the same
`strategy_group`. The assignment is deterministic and weighted by target
principal, so larger accounts receive proportionally more events. Both YES and
NO tokens of one event always stay on the same account. Orders, fills,
positions, and exits remain account-local.

All exclusive accounts in one group must use the same market universe. Do not
enable exclusive allocation for a group whose generated configs diverge.

## Multi-host runtime

The reviewed host-routing design, roster format, first-cutover procedure, and
state contract are documented in `README_MULTI_ACCOUNT_RUNTIME.md`.
The completely separate aggressive operating domain is documented in
`README_AGGRESSIVE_ISOLATED_RUNTIME.md`.

## Current safety boundary

- Automatic top-up and automatic withdrawal are rejected by config validation.
- Managed aggressive profiles enforce `pause_equity_usdc` and
  `daily_loss_limit_usdc` only inside the isolated aggressive runtime. Total
  equity is collateral plus the official Data API position value. A breach
  creates an account-local persistent latch, pauses quoting, and cancels maker
  orders while preserving exit SELLs.
- The risk day resets at Beijing 08:00. A latched account never resumes merely
  because the day changed or the process restarted; an operator must request a
  healthy manual reset. Automatic top-up and automatic withdrawal remain
  forbidden.
- Production currently launches one `engine.py` process per VPS. Switching to
  `multi_runner.py` remains a separate reviewed and explicitly approved cutover.
- The first multi-account cutover must cancel existing maker quotes, start
  paused, verify every signer/account mapping, and only then resume quoting.
