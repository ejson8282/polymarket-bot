"""One-time, separately authorized installation of Predict startup bindings.

Plan is read-only. Apply never stops, starts or resumes a writer: it requires
disabled/stopped services before and after the operation. It does not touch the
signer ledger, secrets, order reports, current symlink or account configuration.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from platforms.predictfun import recovery_startup_guard as guard
from platforms.predictfun import recovery_service_binding as binding


BOOTSTRAP_SOURCE = Path(__file__).resolve()
NODE_IPS = {"vps1": "100.122.255.98", "vps2": "100.101.50.40", "macmini": "100.91.159.54"}
LOCKS = {name: Path(f"/home/ubuntu/latitude-runtime/locks/{name}-production-deploy.lock")
         for name in ("vps1", "vps2")}
LOCKS["macmini"] = Path("/Users/kevinsmacmini/predictfun-ws-runtime/deploy.lock")
BACKUPS = {name: Path("/var/lib/predictfun-recovery-backups") for name in ("vps1", "vps2")}
BACKUPS["macmini"] = Path("/Library/Application Support/PredictFunRecoveryBackups")
GUARD_SOURCE = "platforms/predictfun/recovery_startup_guard.py"
MAC_DOMAIN = "gui/501"


class BootstrapError(RuntimeError):
    pass


class Runner:
    def result(self, args):
        return subprocess.run(list(args), capture_output=True, text=True, timeout=30, check=False)

    def run(self, args):
        result = self.result(args)
        if result.returncode:
            # Never include process listings, service environment or stderr.
            raise BootstrapError("bootstrap_probe_failed")
        return result.stdout.strip()


def _encode(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _snapshot(path: Path) -> tuple[dict, bytes | None]:
    if path.parent.resolve(strict=True) != path.parent:
        raise BootstrapError("bootstrap_parent_not_canonical")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {"present": False}, None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 65536 or before.st_nlink != 1:
            raise BootstrapError("bootstrap_preimage_invalid")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read(65537)
        after = os.fstat(fd)
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or len(raw) != before.st_size):
            raise BootstrapError("bootstrap_preimage_changed")
        return {"present": True, "sha256": guard._sha(raw), "uid": before.st_uid,
                "gid": before.st_gid, "mode": stat.S_IMODE(before.st_mode),
                "flags": getattr(before, "st_flags", 0), "inode": before.st_ino}, raw
    finally:
        os.close(fd)


def _binding_snapshot(path: Path) -> tuple[dict, bytes | None]:
    # A new systemd drop-in directory may not exist yet. No reads through links.
    if not path.parent.exists():
        if path.parent.parent.resolve(strict=True) != path.parent.parent:
            raise BootstrapError("bootstrap_parent_not_canonical")
        return {"present": False}, None
    return _snapshot(path)


def build_plan(profile: str, current_sha: str, recovery_id: str) -> dict:
    if profile not in guard.PROFILES or not guard._hex(current_sha, 40) or not guard._hex(recovery_id, 64):
        raise BootstrapError("bootstrap_identity_invalid")
    spec = guard.PROFILES[profile]
    link = spec.release_root / "current"
    release = spec.release_root / current_sha
    if not link.is_symlink() or link.resolve(strict=True) != release:
        raise BootstrapError("bootstrap_current_mismatch")
    for path in (spec.security_root, spec.runtime_root / "recovery-release-floor"):
        # Initial installation only. Partial/repeated installation needs a new
        # explicit repair review, never an overwrite or a legacy fallback.
        if path.exists() or path.is_symlink():
            raise BootstrapError("bootstrap_already_or_partially_installed")
    raw_manifest = guard._read(release / ".release-manifest.json")
    manifest_hash = guard._sha(raw_manifest)
    manifest = guard.verify_artifact(profile, release, manifest_hash)
    if GUARD_SOURCE not in manifest["files"]:
        raise BootstrapError("bootstrap_guard_source_missing")
    source = guard._read(release / GUARD_SOURCE)
    for component in spec.components.values():
        if component[0] not in manifest["files"]:
            raise BootstrapError("bootstrap_entrypoint_missing")
    snapshots = {str(path): _binding_snapshot(path)[0] for path in binding.binding_files(profile)}
    if any(item.get("flags", 0) for item in snapshots.values()):
        raise BootstrapError("bootstrap_existing_file_flags_require_review")
    policy = {"version": 1, "repository": guard.REPOSITORY, "profile": profile,
              "recovery_id": recovery_id, "allowed_releases": [current_sha]}
    return {"version": 1, "profile": profile, "current_sha": current_sha,
            "recovery_id": recovery_id, "manifest_sha256": manifest_hash,
            "guard_sha256": guard._sha(source), "policy": policy,
            "before_bindings": snapshots,
            "after_bindings": {str(p): guard._sha(v) for p, v in binding.binding_files(profile).items()},
            "activation_allowed": False}


def require_stopped(profile: str, runner: Runner) -> None:
    """Check this node and known Predict service/manual writers; no state changes."""
    if profile == "macmini":
        ip = runner.run(("/Applications/Tailscale.app/Contents/MacOS/Tailscale", "ip", "-4"))
        if ip != NODE_IPS[profile]:
            raise BootstrapError("bootstrap_wrong_host")
        disabled = runner.run(("launchctl", "print-disabled", MAC_DOMAIN))
        for label in binding.LABELS.values():
            if not re.search(r'"' + re.escape(label) + r'"\s*=>\s*true', disabled):
                raise BootstrapError("bootstrap_agent_not_disabled")
            result = runner.result(("launchctl", "print", MAC_DOMAIN + "/" + label))
            if result.returncode != 113 or "Could not find service" not in result.stderr:
                raise BootstrapError("bootstrap_agent_not_unloaded")
        for port in (8791, 8792):
            result = runner.result(("/usr/sbin/lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"))
            if result.returncode != 1 or result.stdout.strip() or result.stderr.strip():
                raise BootstrapError("bootstrap_listener_not_drained")
    else:
        interfaces = json.loads(runner.run(("ip", "-j", "address", "show")))
        addresses = {a.get("local") for interface in interfaces for a in interface.get("addr_info", [])}
        if NODE_IPS[profile] not in addresses:
            raise BootstrapError("bootstrap_wrong_host")
        for unit in (*binding.UNITS.values(), "predictfun-dryrun.timer"):
            state = runner.run(("systemctl", "show", unit, "--property=LoadState,ActiveState,MainPID,UnitFileState"))
            fields = dict(line.split("=", 1) for line in state.splitlines() if "=" in line)
            if (fields.get("LoadState") not in {"loaded", "masked"}
                    or fields.get("ActiveState") != "inactive"
                    or (unit.endswith(".service") and fields.get("MainPID") != "0")
                    or fields.get("UnitFileState") not in {"disabled", "masked", "masked-runtime"}):
                raise BootstrapError("bootstrap_service_not_disabled_and_stopped")
    # pgrep prints PIDs only; do not return potentially sensitive command lines.
    pattern = (r"(platforms/predictfun/(maker/runner|ws_watch|ws_relay)\.py|"
               r"platforms\.predictfun\.(maker\.runner|ws_watch|ws_relay)|"
               r"predictfun_api_proxy\.py|predictfun-recovery/guard\.py.*--exec|"
               r"PredictFunRecovery/guard\.py.*--exec)")
    result = runner.result(("pgrep", "-f", pattern))
    if result.returncode != 1 or result.stdout.strip() or result.stderr.strip():
        raise BootstrapError("bootstrap_manual_writer_or_probe_error")


@contextmanager
def _lock(path: Path):
    if path.parent.resolve(strict=True) != path.parent:
        raise BootstrapError("bootstrap_lock_parent_invalid")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise BootstrapError("bootstrap_lock_invalid")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _write(path: Path, raw: bytes, mode: int) -> None:
    if path.parent.resolve(strict=True) != path.parent:
        raise BootstrapError("bootstrap_write_parent_invalid")
    fd, temporary = tempfile.mkstemp(prefix=".predict-recovery-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fchmod(handle.fileno(), mode)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def apply_plan(plan: dict, *, plan_sha256: str, authorization_id: str, confirmation: str,
               runner: Runner | None = None) -> dict:
    profile = plan.get("profile")
    sha = plan.get("current_sha")
    if (os.geteuid() != 0 or profile not in guard.PROFILES
            or confirmation != f"INSTALL_PREDICT_RECOVERY:{profile}:{sha}"
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", authorization_id)
            or not guard._hex(plan_sha256, 64) or guard._sha(_encode(plan)) != plan_sha256):
        raise BootstrapError("bootstrap_authorization_or_plan_invalid")
    spec = guard.PROFILES[profile]
    release = spec.release_root / sha
    if BOOTSTRAP_SOURCE != release / "platforms/predictfun/recovery_bootstrap.py":
        raise BootstrapError("bootstrap_must_run_from_reviewed_release")
    runner = runner or Runner()
    with _lock(LOCKS[profile]):
        if build_plan(profile, sha, plan["recovery_id"]) != plan:
            raise BootstrapError("bootstrap_plan_changed")
        require_stopped(profile, runner)
        backup_root = BACKUPS[profile]
        guard._protected_parents(backup_root)
        backup_root.mkdir(mode=0o700, exist_ok=True)
        info = backup_root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != guard.TRUSTED_UID or info.st_mode & 0o077:
            raise BootstrapError("bootstrap_backup_root_unprotected")
        backup = backup_root / plan_sha256
        backup.mkdir(mode=0o700)
        _write(backup / "plan.json", _encode({"plan": plan, "authorization_id": authorization_id}), 0o600)
        for index, path in enumerate(binding.binding_files(profile)):
            snapshot, raw = _binding_snapshot(path)
            if snapshot != plan["before_bindings"][str(path)]:
                raise BootstrapError("bootstrap_binding_changed")
            if raw is not None:
                _write(backup / f"binding-{index}.before", raw, 0o600)
        # Recheck after durable backup, before any protection/binding write.
        require_stopped(profile, runner)
        if build_plan(profile, sha, plan["recovery_id"]) != plan:
            raise BootstrapError("bootstrap_plan_changed_after_backup")
        guard._protected_parents(spec.security_root)
        spec.security_root.mkdir(mode=0o755)
        spec.security_root.chmod(0o755)
        source = guard._read(release / GUARD_SOURCE)
        if guard._sha(source) != plan["guard_sha256"]:
            raise BootstrapError("bootstrap_guard_source_changed")
        _write(spec.security_root / "guard.py", source, 0o444)
        policy_root = spec.runtime_root / "recovery-release-floor"
        if policy_root.parent.resolve(strict=True) != policy_root.parent:
            raise BootstrapError("bootstrap_policy_parent_invalid")
        policy_root.mkdir(mode=0o755)
        policy_raw = _encode(plan["policy"])
        _write(policy_root / "policy.json", policy_raw, 0o444)
        policy_root.chmod(0o555)
        anchor = {"version": 1, "repository": guard.REPOSITORY, "profile": profile,
                  "guard_sha256": plan["guard_sha256"], "policy_sha256": guard._sha(policy_raw),
                  "approved_manifests": {sha: plan["manifest_sha256"]},
                  "service_bindings": plan["after_bindings"]}
        _write(spec.security_root / "anchor.json", _encode(anchor), 0o444)
        for path, raw in binding.binding_files(profile).items():
            path.parent.mkdir(mode=0o755, exist_ok=True)
            if _snapshot(path)[0] != plan["before_bindings"][str(path)]:
                raise BootstrapError("bootstrap_binding_cas_failed")
            if profile != "macmini":
                guard._protected_parents(path)
            _write(path, raw, 0o444)
            if profile == "macmini":
                os.chflags(path, stat.UF_IMMUTABLE, follow_symlinks=False)
        installed = binding.inspect_installed(profile)
        if installed is None:
            raise BootstrapError("bootstrap_binding_install_missing")
        if profile != "macmini":
            runner.run(("systemctl", "daemon-reload"))
            binding.verify_effective_vps(profile, runner)
        for component in spec.components:
            # Root being able to read the installation is not evidence that
            # the unprivileged service account can traverse/read it.
            as_user = (("/usr/bin/sudo", "-n", "-u", "kevinsmacmini", "--") if profile == "macmini"
                       else ("/usr/sbin/runuser", "-u", "ubuntu", "--"))
            result = json.loads(runner.run((*as_user, str(spec.python), "-I", "-B",
                                            str(spec.security_root / "guard.py"),
                                            "--profile", profile, "--component", component)))
            if result.get("ok") is not True or result.get("release_sha") != sha:
                raise BootstrapError("bootstrap_startup_check_failed")
        require_stopped(profile, runner)
        installed.require_unchanged()
        receipt = {"status": "installed_services_stopped", "profile": profile, "release_sha": sha,
                   "plan_sha256": plan_sha256, "anchor_sha256": installed.anchor_digest,
                   "authorization_id": authorization_id, "activation_allowed": False}
        _write(backup / "installed.json", _encode(receipt), 0o600)
        return receipt


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Predict startup binding maintenance; never activates services")
    sub = parser.add_subparsers(dest="action", required=True)
    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--profile", choices=tuple(guard.PROFILES), required=True)
    plan_parser.add_argument("--current-sha", required=True)
    plan_parser.add_argument("--recovery-id", required=True)
    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--plan", type=Path, required=True)
    apply_parser.add_argument("--plan-sha256", required=True)
    apply_parser.add_argument("--authorization-id", required=True)
    apply_parser.add_argument("--confirm", required=True)
    args = parser.parse_args(argv)
    if args.action == "plan":
        plan = build_plan(args.profile, args.current_sha, args.recovery_id)
        print(json.dumps({"plan": plan, "plan_sha256": guard._sha(_encode(plan))}))
    else:
        _, raw = _snapshot(args.plan.absolute())
        if raw is None:
            raise BootstrapError("bootstrap_plan_missing")
        envelope = guard._json(raw)
        print(json.dumps(apply_plan(envelope["plan"], plan_sha256=args.plan_sha256,
                                   authorization_id=args.authorization_id, confirmation=args.confirm)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
