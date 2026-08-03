# Predict.fun Dry-Run Deployment

This deployment is intentionally separate from every Polymarket service and
release path. It controls only `predictfun-dryrun.service` and
`predictfun-dryrun.timer`.

## Host profiles

The same reviewed commit is deployed to both hosts, but each host gets one
explicit account identity:

| Profile | Host | Account | Deployment lock |
| --- | --- | --- | --- |
| `vps1` | VPS1 | `account_01` | `vps1-production-deploy.lock` |
| `vps2` | VPS2 | `account_02` | `vps2-production-deploy.lock` |

An unknown or omitted profile fails closed. Runtime config generation pins
`accounts.ids` to the profile account and activation requires that exact
account's Mac mini auth check to pass.

## Production paths

- Bare source: `/home/ubuntu/repos/predictfun.git`
- Immutable releases: `/home/ubuntu/predictfun-releases/<full-sha>`
- Current link: `/home/ubuntu/predictfun-releases/current`
- Mutable runtime: `/home/ubuntu/predictfun-runtime`
- Host-global deployment lock under `/home/ubuntu/latitude-runtime/locks/`

No private key or API key is stored in these paths. The mainnet config talks to
the Mac mini Predict API proxy over Tailscale.

## Fixed release sequence

After the reviewed PR is merged, mirror the exact GitHub `main` commit into the
dedicated Predict bare repository. Then run:

```bash
TARGET=<40-character-merged-sha>

sudo /home/ubuntu/.venv2/bin/python \
  /path/to/reviewed/deploy_release.py prepare \
  --profile vps1 \
  --target-sha "$TARGET"

sudo /home/ubuntu/.venv2/bin/python \
  /path/to/reviewed/deploy_release.py activate \
  --profile vps1 \
  --target-sha "$TARGET" \
  --expected-current none \
  --confirm DEPLOY_PREDICTFUN_DRYRUN \
  --authorization-id <recorded-user-authorization>
```

Repeat the commands on VPS2 with `--profile vps2`. For later releases, replace
`none` with the exact currently deployed Predict SHA. Activation runs one real
read-only/dry-run cycle and accepts the release only when the state reports the
exact SHA, profile, account, successful account auth, `mode=dry_run`, one
completed cycle, and zero errors. A failure restores the previous Predict unit
and release link.

The wrapper never starts, stops, reloads, or restarts
`polymarket-engine.service`.
