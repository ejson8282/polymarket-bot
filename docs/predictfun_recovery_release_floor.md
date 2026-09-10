# Predict recovery release allowlist

Status: Draft PR #150, code and isolated tests only. No policy is installed by
this change. No deployment, service restart, nonce transaction or runtime data
migration is performed. The signing-fence follow-up at `0758f4b8` remains unchanged.

## Implemented deployment checks

`platforms/predictfun/recovery_release_floor.py` reads a nonsecret, node-specific
policy at `<Predict runtime root>/recovery-release-floor/policy.json`:

- VPS1/VPS2: `/home/ubuntu/predictfun-runtime/recovery-release-floor/policy.json`.
- Mac mini: `~/predictfun-ws-runtime/recovery-release-floor/policy.json`.

The policy requires integer version 1, repository `ejson8282/polymarket-bot`,
the exact profile (`vps1`, `vps2` or `macmini`), a reviewed 64-character recovery
identifier and a sorted unique list of approved full release SHAs. It is an
explicit reviewed allowlist, not a chronological comparison of SHA strings or
an assumption that every descendant commit retains recovery protections.

The policy directory and file must have no write permission bits. Symlinks,
special files, oversized files, malformed/duplicate JSON fields, invalid hashes,
profile mismatch and a missing policy inside an existing directory fail closed.
The parser opens the directory/file without following symlinks and bounds reads
to 64 KiB. It never reads signer credentials, a ledger, order state or config.

Both Predict deployment wrappers check **target and rollback SHA** before any
release verification or service mutation. An allowed new target is not sufficient
if its automatic rollback point would remove recovery protection. With a policy
active, a `none` rollback point is also refused.

The policy is re-read before service changes, before changing the `current`
symlink, after acceptance, and before restoring a failed deployment. Its exact
content digest must remain unchanged. If it disappears, becomes corrupt or is
replaced during acceptance, the error path attempts to stop only the selected
Predict services and refuses rollback: no old symlink, unit/plist restoration,
bootstrap or restart is attempted afterward. Stop commands are not independent
proof of quiescence; a failed stop still requires actual process verification.
Successful deployment output records the policy SHA-256 (null for legacy mode).

The Mac mini immutable archive includes the shared checker. Existing deployments
with no policy directory retain their prior behavior, so this code can be reviewed
and deployed before a recovery is authorized. There is no switch to skip an
existing policy. Prepare only builds an artifact; activation is the policy gate.

## Remaining machine-level gate

These checks are enforced by the **new reviewed deployment wrappers**, not by
arbitrary old scripts, manual symlink edits, service-manager commands or root.
Deleting the entire directory *before* an invocation is indistinguishable from
an installation which has never enabled recovery. This is not represented as a
tamper-proof or already-active machine-wide rollback floor.

Before a nonce barrier or report migration, the separately authorized maintenance
adapter must install and independently verify all of the following:

1. The reviewed wrapper on every affected node and a reviewed recovery-safe
   current/rollback pair. No pre-recovery version may remain an automatic fallback.
2. External policy ownership, backups, content hashes and file protections,
   installed under the same node-local deployment lock. Allowlist updates are
   separately reviewed writes, not automatic additions made by a deployment.
3. A persistent startup guard outside the replaceable `current` tree, or an
   equivalent independently verified service-manager restriction, preventing an
   old wrapper/unit/plist from bypassing the policy. This change does **not**
   install that guard or change any unit/plist template.
4. A release-independent policy-presence anchor and the maintenance entrypoint's
   cross-node verification, so complete policy deletion cannot silently select
   legacy mode after recovery has been activated.

Until these steps exist and pass real acceptance, the overall hard rollback-floor
gate remains open and the recovery must not be activated. The policy cannot prove
historical order nonce bounds, invalidate signatures or authorize trading.

## Tests

Isolated tests cover all three profiles, forbidden targets and rollback SHAs,
absent versus damaged policies, duplicate JSON keys, malformed identities,
permissions, symlinks, FIFO/oversize rejection and mid-operation policy changes.
Both real wrapper control flows are exercised with fake service managers: a
simulated acceptance failure plus changed/missing/corrupt policy must not restore
the old release or issue rollback starts. No actual services or accounts are used.

Validation: all `tests/test_predictfun*.py` **514 passed**; six changed Python
modules/tests compile; `git diff --check` passes. Fixed-head independent review
is still required. The earlier signing-fence scope at `0758f4b8` independently
passed 471 tests and its separate review; that is not approval of this follow-up.
