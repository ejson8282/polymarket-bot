# Discord notification channels

Latitude uses two Discord channels so routine activity does not bury incidents.

## Normal operations

Examples:

- automation started or stopped by the user
- orders opened or closed successfully
- partial fills that finished safely hedged
- daily summaries and console configuration changes
- confirmed rewards or refunds

The unified console reads:

```text
data/discord_normal_webhook.txt
```

Var/Decibel reads `discord_normal_webhook_url` from its local notification
settings. Polymarket uses its normal reporting webhook for fills, exits, and
other routine events; the older dedicated fill webhook is no longer used for
routing.

## Important incidents

Examples:

- order failures and non-routine preflight blocks
- single-leg exposure or rescued-flat recovery
- liquidation protection
- persistent service or data-source failures
- exceptions and bugs that require attention

The unified console and Polymarket read:

```text
data/discord_important_webhook.txt
```

Var/Decibel reads `discord_important_webhook_url` from its local notification
settings.

Critical alerts also continue to reach Feishu. Routine spread or cost guard
rejections stay suppressed because no order was sent and no position changed.

## Compatibility and secrets

`data/discord_webhook.txt` remains a fallback for an older deployment. Webhook
URLs are secrets: keep these files and local settings at mode `0600`, never
commit them, and never print their values in logs.
