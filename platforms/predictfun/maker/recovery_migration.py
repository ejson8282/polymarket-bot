"""Reviewed recovery proposals and a locked, backed-up report replacement.

No CLI, signer mutation, transaction, service control or resume operation.
The maintenance caller must supply fresh independently verified evidence twice.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
from typing import Callable, Iterator, Any

from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker.recovery_plan import assess_recovery, _digest

VPS1_DEPLOY_LOCK = Path("/home/ubuntu/latitude-runtime/locks/vps1-production-deploy.lock")
VPS1_REPORT = Path("/home/ubuntu/predictfun-runtime/data/predictfun_mainnet_execution_report.json")


def _bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def verify_vps1_writers_stopped(release_sha: str) -> None:
    """Read-only local verification, not a stop/mask/restart command.

    The replacement adapter holds the global VPS1 deployment lock during migration.
    Runtime masking is deliberate: inactive alone does not exclude timer restarts.
    Only approved runner versions participate in report_writer_lease; reject any
    visible manual/legacy runner process as an additional check, not an exhaustive
    proof against other hosts or arbitrary root-level writers.
    """
    if re.fullmatch(r"[0-9a-f]{40}", release_sha) is None:
        raise ValueError("exact_release_required")
    addresses = subprocess.run(["ip", "-j", "address", "show", "dev", "tailscale0"],
                               capture_output=True, text=True, timeout=10, check=True)
    interfaces = json.loads(addresses.stdout)
    if not isinstance(interfaces, list) or not any(
        isinstance(interface, dict) and interface.get("ifname") == "tailscale0"
        and any(isinstance(info, dict) and info.get("local") == "100.122.255.98"
                for info in interface.get("addr_info", []))
        for interface in interfaces
    ):
        raise ValueError("not_vps1")
    current = Path("/home/ubuntu/predictfun-releases/current").resolve(strict=True)
    if current.name != release_sha:
        raise ValueError("unexpected_predict_release")
    for unit in ("predictfun-dryrun.service", "predictfun-dryrun.timer"):
        result = subprocess.run([
            "systemctl", "show", unit, "--no-pager",
            "--property=Id,LoadState,ActiveState,MainPID,ControlPID,UnitFileState",
        ], capture_output=True, text=True, timeout=10, check=True)
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if (values.get("Id") != unit or values.get("LoadState") != "masked"
                or values.get("UnitFileState") not in {"masked", "masked-runtime"}
                or values.get("ActiveState") != "inactive"
                or values.get("MainPID", "0") != "0" or values.get("ControlPID", "0") != "0"):
            raise ValueError("predict_writer_not_stopped_and_masked")
    processes = subprocess.run(["ps", "-eo", "args="], capture_output=True,
                               text=True, timeout=10, check=True).stdout
    if any(marker in processes for marker in
           ("platforms/predictfun/maker/runner.py", "platforms.predictfun.maker.runner")):
        raise ValueError("manual_or_legacy_predict_runner_present")


@contextmanager
def report_writer_lease(path: Path) -> Iterator[None]:
    """Held for the entire runner lifetime, including final shutdown writes."""
    with _exclusive_lease(Path(str(path) + ".recovery.lock")):
        yield


@contextmanager
def _exclusive_lease(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
            raise ValueError("invalid_recovery_lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("predict_report_writer_active") from None
        yield
    finally:
        os.close(fd)


def prepare_report_recovery(report: dict[str, Any], *, account_id: str, keys: list[str],
                            fence: dict, baseline: dict, nonce_evidence: list,
                            now: datetime) -> dict[str, Any]:
    """Construct a proposal only. A ready review plan is NOT write permission."""
    state = report.get("managed_orders") if isinstance(report, dict) else None
    plan = assess_recovery(state, account_id=account_id, keys=keys, fence=fence,
                           baseline=baseline, nonce_evidence=nonce_evidence, now=now)
    if plan["blocks"]:
        raise ValueError("recovery_evidence_blocked")
    prepared_at = now.isoformat()
    replacement = _replacement(report, plan, account_id, keys, prepared_at)
    return {"schema_version": 1, "account_id": account_id, "keys": sorted(keys),
            "source_report_sha256": _digest(report), "plan": plan, "prepared_at": prepared_at,
            "replacement": replacement, "activation_allowed": False}


def _replacement(report: dict, plan: dict, account_id: str, keys: list[str],
                 prepared_at: str) -> dict:
    state = report["managed_orders"]
    candidates = [row for row in state["pending_submissions"]
                  if isinstance(row, dict) and row.get("account_id") == account_id
                  and row.get("idempotency_key") in keys]
    if (not keys or len(set(keys)) != len(keys) or len(candidates) != len(keys)
            or plan.get("account_id") != account_id or plan.get("blocks") != []
            or plan.get("registry_sha256") != _digest(state)
            or candidates != plan.get("archive_candidates")):
        raise ValueError("proposal_scope_mismatch")
    replacement = deepcopy(report)
    managed = replacement["managed_orders"]
    # Validate the existing archive without normalizing or dropping source rows.
    archive = ManagedOrderRegistry._validate_recovery_archive(managed.get("recovery_archive"))
    for pending in plan["archive_candidates"]:
        archive.append({
            "pending": pending, "resolution": "nonce_invalidated_unknown",
            "historical_submission_outcome": "unknown", "archived_at": prepared_at,
            "evidence_sha256": plan["evidence_sha256"],
            "source_registry_sha256": plan["registry_sha256"],
        })
    archive = ManagedOrderRegistry._validate_recovery_archive(archive)
    generations = managed.get("submission_generations", {})
    if not isinstance(generations, dict) or any(
        not isinstance(rows, dict) or any(not isinstance(k, str) or not k or
                                         type(v) is not int or v < 0
                                         for k, v in rows.items())
        for rows in generations.values()
    ):
        raise ValueError("invalid_submission_generations")
    selected = set(keys)
    # Preserve every unselected row and all known/manual order records verbatim.
    managed["pending_submissions"] = [row for row in managed["pending_submissions"]
        if not (isinstance(row, dict) and row.get("account_id") == account_id
                and row.get("idempotency_key") in selected)]
    managed["recovery_archive"] = archive
    managed["submission_generations"] = deepcopy(generations)
    account_generations = managed["submission_generations"].setdefault(account_id, {})
    for row in plan["archive_candidates"]:
        intent = row["intent_id"]
        account_generations[intent] = max(account_generations.get(intent, 0),
            ManagedOrderRegistry._generation_for_key(intent, row["idempotency_key"]))
    if not isinstance(managed.get("summary"), dict):
        raise ValueError("invalid_registry_summary")
    managed["summary"]["pending_submissions"] = len(managed["pending_submissions"])
    return replacement


def _read_regular(path: Path) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 32 * 1024 * 1024:
            raise ValueError("invalid_report_file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise ValueError("invalid_report_file")
            return raw
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def replace_reviewed_report(
    target: Path, backup_root: Path, *, proposal: dict, expected_file_sha256: str,
    reviewed_proposal_sha256: str, release_sha: str, authorization_id: str,
    verify_maintenance: Callable[[], None],
) -> dict:
    """Only call inside an explicitly authorized, independently verified window.

    verify_maintenance must check the signer fence and fresh barrier/baseline,
    raising on any failure. It has no default and must not be replaced by a
    JSON boolean or a cached planner result.
    The adapter checks VPS1 identity/release/units twice under the global lock.
    The file lease excludes this version's runner, not an old binary or other host.
    """
    proposal = deepcopy(proposal)
    if (not callable(verify_maintenance) or not authorization_id.strip()
            or proposal.get("account_id") != "account_01"
            or re.fullmatch(r"[0-9a-f]{40}", release_sha) is None
            or re.fullmatch(r"[0-9a-f]{64}", expected_file_sha256) is None
            or _digest(proposal) != reviewed_proposal_sha256
            or proposal.get("activation_allowed") is not False
            or proposal.get("plan", {}).get("status") != "ready_for_independent_review"):
        raise ValueError("reviewed_maintenance_authorization_required")
    target, backup_root = Path(target).absolute(), Path(backup_root).absolute()
    if target != VPS1_REPORT:
        raise ValueError("unexpected_report_target")
    if target.is_symlink() or target.parent.resolve() != target.parent:
        raise ValueError("report_path_must_be_canonical")
    if not backup_root.is_dir() or backup_root.resolve() != backup_root:
        raise ValueError("external_backup_directory_required")
    # Backups must not live inside a checkout/release or in the runtime data dir.
    if backup_root == target.parent or target.parent in backup_root.parents or any(
        (p / ".git").exists() or (p / ".release-manifest.json").exists()
        for p in (backup_root, *backup_root.parents)
    ):
        raise ValueError("external_backup_directory_required")
    with _exclusive_lease(VPS1_DEPLOY_LOCK), report_writer_lease(target):
        verify_vps1_writers_stopped(release_sha)
        verify_maintenance()
        original = _read_regular(target)
        if _sha(original) != expected_file_sha256:
            raise ValueError("report_changed_since_review")
        source = json.loads(original)
        if _digest(source) != proposal["source_report_sha256"]:
            raise ValueError("proposal_source_mismatch")
        rebuilt = _replacement(source, proposal["plan"], proposal["account_id"],
                               proposal["keys"], proposal["prepared_at"])
        if rebuilt != proposal["replacement"]:
            raise ValueError("proposal_changes_outside_recovery_scope")
        replacement = _bytes(proposal["replacement"])
        folder = Path(tempfile.mkdtemp(prefix="predict-recovery-", dir=backup_root))
        manifest = {"account_id": proposal["account_id"], "keys": proposal["keys"],
                    "authorization_id": authorization_id, "release_sha": release_sha,
                    "original_sha256": _sha(original), "replacement_sha256": _sha(replacement),
                    "reviewed_proposal_sha256": reviewed_proposal_sha256,
                    "automatic_rollback_allowed": False, "activation_allowed": False}
        _write_new(folder / "original.json", original)
        _write_new(folder / "replacement.json", replacement)
        _write_new(folder / "manifest.json", _bytes(manifest))
        _fsync_directory(folder)
        _fsync_directory(backup_root)
        if _read_regular(folder / "original.json") != original:
            raise ValueError("backup_verification_failed")
        temporary = None
        try:
            fd, name = tempfile.mkstemp(prefix=".predict-recovery-", dir=target.parent)
            temporary = Path(name)
            with os.fdopen(fd, "wb") as stream:
                stream.write(replacement)
                stream.flush()
                os.fsync(stream.fileno())
            verify_vps1_writers_stopped(release_sha)
            verify_maintenance()
            if _read_regular(target) != original:
                raise ValueError("report_changed_during_maintenance")
            os.replace(temporary, target)
            _fsync_directory(target.parent)
            if _read_regular(target) != replacement:
                raise ValueError("replacement_verification_failed_keep_paused")
            _write_new(folder / "applied.json", _bytes(manifest))
            _fsync_directory(folder)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return {**manifest, "status": "applied_keep_paused", "backup_directory": str(folder)}
