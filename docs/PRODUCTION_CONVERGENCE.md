# Polymarket 生产源码收口状态

审计日期：2026-07-30（Asia/Shanghai）

## 已确认

- GitHub `main` 与 VPS 内部 bare `main` 已对齐。
- 旧生产 worktree 的关键 maker、curator、reward 文件内容与当前 `main` 一致；其 Git HEAD
  仍明显落后，所以这些文件显示为修改或未跟踪。
- 两个本地独有提交的有效内容已被后续 `main` 覆盖或以等价 patch 合入，不应再次 cherry-pick。
- 统一 Dashboard 已收回 `latitude-alpha` 并从独立 immutable release 运行；本仓库中的 UI
  副本不再是运行来源。
- 仓库内的 `.bak*`、candidate、macOS sidecar 和过期文档/测试已移到仓库外归档，并保存
  SHA-256 清单；没有直接删除。

## 当前限制

VPS1 真钱引擎已经切换到精确 SHA 的 immutable release。VPS2 的独立账号仍保留在旧
worktree 配置中，必须在单独授权的维护窗口中使用 `vps2` profile 切换；在此之前不得直接启动
旧 worktree 服务。旧目录仍可能显示 dirty；这不代表还存在未救回的有效业务代码。

在独立 maker release 和服务回滚流程准备好之前：

- 不得在旧 worktree 继续开发；
- 不得用 `git pull/reset/checkout/clean` 追求表面干净；
- 不得删除剩余与 `main` 一致、但因旧 HEAD 而显示未跟踪的运行文件；
- 下一次真钱引擎维护必须先重新核对文件哈希、活动订单、仓位、服务 unit 和回滚点。

## 下一次维护窗口

1. 从 GitHub 已审核的完整 SHA 建只读 release；
2. 将账户配置、数据、日志、PID、虚拟环境和备份放在 release 外；
3. 在不触发下单的模式下运行自检和核心风控测试；
4. 记录当前活动订单、仓位和旧进程回滚点；
5. 逐账户切换 service，验证状态、订单对账和安全暂停；
6. 只有所有账户稳定后，才归档旧 worktree；仍不得直接删除。

该步骤会触及真钱交易服务，必须取得针对该维护窗口的明确授权。

## Maker release startup invariant

Production maker services must point at
`/home/ubuntu/polymarket-releases/current/platforms/polymarket/maker/engine.py`
and set both `POLYMARKET_REQUIRE_RELEASE=1` and the full
`POLYMARKET_RELEASE_SHA`. Startup fails closed unless:

- `current` resolves to a directory named with that exact commit;
- `.release-manifest.json` names `ejson8282/polymarket-bot` and the same commit;
- the running `engine.py` SHA-256 matches the manifest.

Install the drop-in as `zz-immutable-release.conf` so it takes precedence over
older runtime-specific overrides. Its independent `ExecStartPre` runs the same
verification before Python can enter the trading engine, including when
`current` accidentally points to an older release without the guard.

The runtime config, data, logs and virtual environment remain outside the
immutable release. The reviewed systemd drop-in example is
`deploy/systemd/polymarket-engine-immutable.conf.example`.

## Serialized release wrapper

All later maker deployments and manual rollbacks must run through
`platforms/polymarket/maker/deploy_release.py`. The wrapper:

- takes the selected node-local lock（`vps1-production-deploy.lock` 或
  `vps2-production-deploy.lock`）；
- requires the reviewed full SHA at an immutable internal
  `refs/deploy-candidates/<sha>` ref, then promotes internal `main` only as a
  fast-forward while holding the lock;
- requires the exact currently deployed rollback SHA;
- builds and tests a read-only immutable release before any service change;
- uses an explicit `vps1` or `vps2` profile so the lock, pause flag, state file
  and runtime config all target the same account;
- refuses activation unless the selected account pause flag and a fresh engine state
  both confirm the current release is paused;
- fixes the service name to `polymarket-engine.service`;
- requires an exact profile-scoped confirmation plus a user authorization ID;
  VPS1 keeps `ACTIVATE:<full-sha>` compatibility, while VPS2 requires
  `ACTIVATE:vps2:<full-sha>` (and the corresponding rollback form);
- restores and restarts the previous release automatically if post-restart
  verification fails;
- writes a non-secret audit record below
  `/home/ubuntu/polymarket-runtime/deployments/`.

`plan` is read-only. `prepare` may build a release but never changes the
`current` symlink or service. `activate` and `rollback` always restart while
holding the global lock.

The control machine first confirms the target is merged GitHub `main`, then
stages that exact object in the internal bare repository. This unique candidate
ref does not change internal `main`, the running release, or any service:

```bash
git push \
  ssh://ubuntu@100.122.255.98/home/ubuntu/repos/polymarket-bot.git \
  <target-full-sha>:refs/deploy-candidates/<target-full-sha>
```

VPS1 can therefore deploy a private repository without storing GitHub
credentials. Example future flow after staging:

```bash
python3 platforms/polymarket/maker/deploy_release.py plan \
  <target-full-sha> \
  --profile vps1 \
  --expected-current <current-full-sha>

python3 platforms/polymarket/maker/deploy_release.py prepare \
  <target-full-sha> \
  --profile vps1 \
  --expected-current <current-full-sha> \
  --confirm PREPARE:<target-full-sha>

python3 platforms/polymarket/maker/deploy_release.py activate \
  <target-full-sha> \
  --profile vps1 \
  --expected-current <current-full-sha> \
  --confirm ACTIVATE:<target-full-sha> \
  --authorization-id <approved-maintenance-id>
```

VPS2 uses the same flow with `--profile vps2`, the reviewed
`polymarket-engine-immutable-vps2.conf.example` drop-in, and an account-scoped
confirmation such as `ACTIVATE:vps2:<target-full-sha>`. A VPS1 confirmation is
therefore not reusable for VPS2. VPS2 currently has no immutable `current`
release, so its first migration still requires a separately authorized
bootstrap window to create the internal bare repository, release link, pause
marker and reviewed drop-in before this normal flow can be used.

The release that first introduces this wrapper still needs one explicitly
authorized bootstrap preparation under the same global lock. After that release
is active, subsequent releases use the wrapper above. Neither this tool nor the
bootstrap procedure reads or copies secrets into a release.
