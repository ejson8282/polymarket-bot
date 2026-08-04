# Predict.fun Dry-Run Deployment

This deployment is intentionally separate from every Polymarket service and
release path. The VPS wrapper controls only `predictfun-ws.service`,
`predictfun-dryrun.service`, and `predictfun-dryrun.timer`. The Mac mini
wrapper atomically controls the Predict.fun account API proxy and public-data
WebSocket relay LaunchAgents.

## Market-data path

Predict.fun requires an API key for its public market WebSocket. The key stays
in the existing Mac mini secret file. A restricted relay on the Mac mini adds
that key to the upstream connection and permits only public market topics from
the two known VPS Tailscale addresses:

```text
Predict.fun WS -> Mac mini :8792 relay -> VPS watcher -> runtime JSON
```

The relay rejects wallet topics, arbitrary methods, duplicate clients, and
unknown client addresses. It never accepts an API key from a VPS and never
returns one. The VPS watcher validates market IDs, timestamps, level ranges,
book ordering, trading status, and market status before a book can be quoted.

## Host profiles

The same reviewed commit is deployed to both hosts. Each host has a conservative
single-account default, while the release wrapper can pin up to ten explicit
account aliases when more accounts have been enrolled on the Mac mini:

| Profile | Host | Account | Deployment lock |
| --- | --- | --- | --- |
| `vps1` | VPS1 | `account_01` | `vps1-production-deploy.lock` |
| `vps2` | VPS2 | `account_02` | `vps2-production-deploy.lock` |

An unknown or omitted profile fails closed. Runtime config generation pins
`accounts.ids` to the exact ordered account set and activation requires every
account's Mac mini auth check to pass. Public market data and the WS relay are
shared; balances, positions, managed orders, signing and submission remain
account-scoped.

To bind more than the default account, pass the same explicit list to both
`prepare` and `activate`, for example:

```bash
--account-ids account_01,account_03,account_04
```

The wrapper rejects duplicate/invalid aliases and more than ten accounts. It
does not create accounts or copy credentials.

## Production paths

- Bare source: `/home/ubuntu/repos/predictfun.git`
- Immutable releases: `/home/ubuntu/predictfun-releases/<full-sha>`
- Current link: `/home/ubuntu/predictfun-releases/current`
- Mutable runtime: `/home/ubuntu/predictfun-runtime`
- Host-global deployment lock under `/home/ubuntu/latitude-runtime/locks/`

Mac mini service paths:

- Bare source: `~/repos/predictfun.git`
- Immutable releases: `~/predictfun-ws-releases/<full-sha>`
- Current link: `~/predictfun-ws-releases/current`
- Mutable runtime: `~/predictfun-ws-runtime`
- API LaunchAgent: `~/Library/LaunchAgents/ai.codex.predictfun-api-proxy.plist`
- WS LaunchAgent: `~/Library/LaunchAgents/ai.codex.predictfun-ws-relay.plist`
- Existing secret: `~/.macmini-secrets/predictfun.env`

No private key or API key is copied into a release, plist, VPS, or Git. Both
Mac services read the existing mode-0600 secret file at runtime. The mainnet
config talks to the Mac mini Predict API proxy over Tailscale.

## Fixed release sequence

After the reviewed PR is merged, mirror the exact GitHub `main` commit into the
dedicated Predict bare repositories. Deploy and accept both Mac mini services
first:

```bash
TARGET=<40-character-merged-sha>

/path/to/reviewed/python \
  /path/to/reviewed/platforms/predictfun/deploy_ws_relay.py prepare \
  --target-sha "$TARGET"

/path/to/reviewed/python \
  /path/to/reviewed/platforms/predictfun/deploy_ws_relay.py activate \
  --target-sha "$TARGET" \
  --expected-current none \
  --confirm DEPLOY_PREDICTFUN_WS_RELAY \
  --authorization-id <recorded-user-authorization>
```

For later Mac-service releases, replace `none` with their exact current SHA.
Activation starts and probes the account API proxy first, requiring at least
one ready account and an exact release-SHA match. It then requires the WS relay
to complete a real public-market subscription. Any failure restores the
previous link and both LaunchAgents as one unit.

Then prepare and activate VPS1:

```bash
TARGET=<40-character-merged-sha>

sudo /home/ubuntu/.venv2/bin/python \
  /path/to/reviewed/deploy_release.py prepare \
  --profile vps1 \
  --account-ids account_01 \
  --target-sha "$TARGET"

sudo /home/ubuntu/.venv2/bin/python \
  /path/to/reviewed/deploy_release.py activate \
  --profile vps1 \
  --account-ids account_01 \
  --target-sha "$TARGET" \
  --expected-current none \
  --confirm DEPLOY_PREDICTFUN_DRYRUN \
  --authorization-id <recorded-user-authorization>
```

Repeat the commands on VPS2 with `--profile vps2`. For later releases, replace
`none` with the exact currently deployed Predict SHA. Activation runs one real
read-only/dry-run cycle. It first requires an active WebSocket watcher with a
fresh schema-v2 snapshot and at least one valid market book, then accepts the
runner only when state reports the exact SHA, profile, account, successful
account auth, `mode=dry_run`, one completed cycle, zero errors, and a status
snapshot that explicitly reports live submit/cancel disabled. A failure
restores all previous Predict units, config, environment, and release link.

The wrapper never starts, stops, reloads, or restarts
`polymarket-engine.service`.
