# Discord notification channels

Latitude uses exactly two Discord channels so routine activity does not bury
incidents. The only place allowed to configure them is Dashboard > 通知.

## Normal operations

Examples:

- automation started or stopped by the user
- orders opened or closed successfully
- partial fills that finished safely hedged
- daily summaries and console configuration changes
- confirmed rewards or refunds

All VPS1 projects read:

```text
data/discord_normal_webhook.txt
```

Saving in Dashboard atomically copies the same file to VPS2. Polymarket,
Var/Decibel, Predict.fun, Grid, Single Account, IPO and infrastructure alerts
must use this shared route rather than keeping project-specific webhooks.

## Important incidents

Examples:

- order failures and non-routine preflight blocks
- single-leg exposure or rescued-flat recovery
- liquidation protection
- persistent service or data-source failures
- exceptions and bugs that require attention

All VPS1 projects read:

```text
data/discord_important_webhook.txt
```

Saving in Dashboard atomically copies the same file to VPS2.

Critical alerts also continue to reach Feishu. Routine spread or cost guard
rejections stay suppressed because no order was sent and no position changed.

## Secrets and retired settings

Legacy `data/discord_webhook.txt`, Polymarket config webhook fields, environment
webhook variables and Var/Decibel local notification settings are not valid
routing sources. Webhook URLs are secrets: keep the two shared files at mode
`0600`, never commit them, and never print their values in logs.
