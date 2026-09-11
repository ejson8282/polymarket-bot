# Predict external recovery startup gate

Status: implementation in Draft PR #150, not installed. The overall recovery
goal includes real installation, historical-order invalidation, report migration
and separately authorized limited-live acceptance. Passing this module's tests
does not complete that goal or permit a nonce transaction.

## Implemented standalone guard

`platforms/predictfun/recovery_startup_guard.py` uses only the standard library,
with no imports from a replaceable release or runtime configuration. It refuses
to run from a checkout. Its future fixed installations are:

- VPS1/VPS2: `/etc/predictfun-recovery/guard.py` and `anchor.json`.
- Mac mini: `/Library/Application Support/PredictFunRecovery/guard.py` and
  `anchor.json`.

The guard and anchor must be root-owned, read-only regular files. Their parents
must be canonical root-owned directories with no group/other write permissions.
There is no absent-anchor legacy mode. The anchor pins the profile, the installed
guard's own hash, the exact release-floor policy hash, and approved full release
SHAs mapped to exact manifest hashes.

Every startup checks the required policy, current symlink, approved manifest and
all immutable release files, not just the entrypoint filename. Policy deletion,
changed source with a forged manifest, unapproved current SHA, missing/extra files,
writable release paths and symlinks fail closed. Anchor/policy/current are checked
again after hashing. The launch command uses the verified absolute release, not
the mutable `current` symlink.

Default mode prints a small read-only verification result. `--exec` accepts only
the profile's fixed Predict component: runner/WS on a VPS, API/relay on Mac mini.
There is no arbitrary executable, runtime root, policy path, skip flag or trailing
command argument. The child Python uses `-I -B`; Python/loader injection environment
variables are removed. The verified release SHA is set, but existing live flags
are never granted or repaired: stale live-release authorization remains stale.

Check mode does not import the Predict SDK, read credentials, contact accounts,
modify files, reload a service, start a process or send a transaction. Exec mode
is a future service startup operation and must not be run without explicit
service activation authorization.

## One-time installation integration

`recovery_bootstrap.py` now provides read-only `plan` and explicit root-only
`apply`. It has not been run in production. The separate root authorization is
not supplied by the program's confirmation string. The operator must first get
approval for the exact node, reviewed merged release, plan digest and service
maintenance window.

The initial merged recovery release must already be the exact `current` release.
This installer does not prepare/deploy a release or change the current symlink.
It must be invoked from that reviewed immutable release, using the existing
trusted Python interpreter. Its implementation can also be called via tests with
synthetic paths; those tests do not prove production ownership or service state.

The procedure is deliberately initial-install-only:

1. `plan --profile <node> --current-sha <full-sha> --recovery-id <64-hex>` hashes
   the entire immutable artifact and captures exact startup-file preimages. It
   emits a plan envelope and its digest, never changes a service or file. The
   envelope is to be retained outside the source/release tree for review.
2. `apply --plan <reviewed-envelope.json> --plan-sha256 <reviewed-digest>
   --authorization-id <approved-id>
   --confirm INSTALL_PREDICT_RECOVERY:<node>:<full-sha>` requires root and takes
   the same node deployment lock used by that node's deployment wrapper.
3. Independent command probes verify the node IP and known Predict processes.
   VPS services/timer must be stopped and disabled, with zero service MainPID;
   Mac agents must be disabled and fully unloaded and API/WS listeners drained.
   Command errors are not interpreted as stopped. The installer never issues a
   stop, start, enable, bootstrap, kickstart or trading command.
4. A private root-owned external backup contains the plan/authorization record
   and original startup files before writes. Bindings are compared again after
   the backup. The policy initially approves only this exact current artifact;
   neither ancestor commits nor arbitrary future releases are approved.
5. The root-owned external anchor pins the installed guard, policy, exact
   artifact manifest and exact service binding hashes. A VPS persistent drop-in
   clears old executable hooks and replaces ExecStart. On Mac only the two
   Predict plists are replaced with the fixed guard commands, made root-owned
   read-only and marked `UF_IMMUTABLE` to reject the legacy wrapper's atomic
   replacement. The enclosing LaunchAgents directory is not locked or modified.
6. The installer validates installed bytes/permissions, systemd's effective
   commands after daemon-reload (including later overrides), guard check-mode
   results and final stopped/disabled state. It then writes an installation
   receipt. Failure does not remove protection or restart/restore an old service.

Backups are `/var/lib/predictfun-recovery-backups/<plan-digest>` on each VPS and
`/Library/Application Support/PredictFunRecoveryBackups/<plan-digest>` on Mac.
The root binding checker and both new deployment wrappers preserve these
bindings, pin target AND rollback artifact hashes, and reject missing/changed
anchors/policies/bindings. Mac deployment does not rewrite or restore protected
plists; explicit future activation can enable/bootstrap the validated bindings.
VPS deployment verifies effective commands before starting either service and
before restarting an approved rollback. A damaged binding prevents rollback
restart. Legacy installations with no external root directory remain compatible.

An existing or partially installed protection directory makes another initial
`apply` fail. A failed install, masked main unit, changed service hook or a future
new-release allowlist requires a separately reviewed root maintenance/repair;
this tool provides no overwrite/unlock/skip-protection option. In particular,
masked units must not be interpreted as having verifiable effective ExecStart.

These probes establish the selected node's known process/service state, not
global signer quiescence or absence of every possible external client. Cross-node
writer fencing, the signer-ledger CAS and nonce evidence remain separate gates.
No secret file, ledger, account configuration or execution report is read or
written by this installation command.

This targets accidental rollback through the supported deployment paths. A
privileged operator intentionally replacing the guard, anchor, interpreter or
service-manager bindings is outside a Python guard's security boundary. Local
interpreter/dependency integrity and exclusive maintenance remain trusted inputs.

## Remaining end-to-end work

- Install and verify the signer ledger fence through a reviewed stopped-writer,
  backed-up CAS procedure; do not hot-edit a running signer.
- Establish historical signed nonce bounds, or present an explicitly reviewed
  alternative if missing historical evidence makes that impossible.
- Implement/verify the exact Predict smart-account nonce transaction path and
  independently collect confirmed invalidation evidence on affected exchanges.
- Bind fresh fence/chain/account observations to the report replacement entrypoint;
  migrate only the reviewed unknown records without fabricating terminal results.
- Merge/deploy only after the required reviews and explicit exact-SHA approvals.
- Re-test network freshness and separately authorize limited-live placement,
  cancellation and inventory-exit acceptance. Preserve VPS2 dry-run and do not
  modify Polymarket.

The September 11 goal remains active until these production outcomes are proven.
The pending request for read-only historical Mac mini ledger/request metadata
does not authorize reading private keys, `.env`, API keys, or transaction execution.

Validation before this follow-up: **536 Predict tests passed**. The new focused
suite exercises synthetic root installation, initial-install refusal, independent
stopped probes, durable backups/CAS, effective command mismatch and preservation
of protected bindings in both real wrapper control flows with fake service
managers. On macOS an additional temporary-file test runs the actual legacy
atomic-write helper and confirms `UF_IMMUTABLE` rejects replacement; it restores
the flag on only its own file afterwards. This test does not prove the ownership
or flags of production plists. No test starts or modifies production services.
The Mac immutable archive includes the bootstrap and binding-check modules.
Final follow-up validation: **579 Predict tests passed in 12.00 seconds**,
nine changed Python files compiled, and diff whitespace checks passed.
