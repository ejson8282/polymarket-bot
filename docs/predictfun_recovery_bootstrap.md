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

## Required installation integration, still open

The standalone guard only protects launches that actually invoke it. It is not
yet connected to any production service. The maintenance installer must:

1. Verify the exact host, current immutable release and reviewed installation-plan
   digest, plus explicit profile-scoped authorization. Hold the node deployment
   lock throughout. Verify all affected Predict writers are stopped/drained.
2. Derive the guard/policy/manifest digests from the reviewed artifacts and create
   external private backups and a compare-and-swap manifest before any writes.
   Do not derive approval merely from self-declared JSON booleans.
3. Install the standalone file and required root anchor. On a VPS, add a persistent
   root-owned systemd drop-in that replaces ExecStart with the external guard and
   cannot disappear when an old wrapper overwrites the main unit. The drop-in
   must also prevent legacy ExecStartPre commands running unverified release code.
4. On Mac mini, bind both launch agents to the external guard, with verified
   protection against the known legacy wrapper replacing those plists. A
   root-owned file alone is insufficient if its parent is user-writable: atomic
   replacement must also be prevented. Do not lock the whole LaunchAgents
   directory or change unrelated projects to achieve this.
5. Update the current Mac deployment wrapper to preserve and verify installed
   guarded launch bindings. It must not overwrite them with legacy templates.
6. Verify effective service-manager commands and guard check-mode results on every
   affected host. Keep services disabled/paused; installation must not trade or
   silently restart. Installation failure leaves a stopped recovery state, never
   an automatic restore to an unguarded pre-barrier setup.

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

Validation so far: **536 Predict tests passed**, including 22 external-guard
tests, plus changed-file compilation and diff check. Tests use a synthetic trust
root and fake exec; they do not install root files or start services. The Mac
immutable archive carries the guard source for a future reviewed installer.
The recovery floor tests now restore only their own temporary directory
permissions in teardown; the isolated full-suite run had no cleanup warnings.
