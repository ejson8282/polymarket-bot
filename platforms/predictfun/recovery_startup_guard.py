"""Standalone, root-installed Predict startup gate. No imports from current.

Installation is a separate authorized maintenance operation. Running this file
from a checkout is intentionally refused. Check mode cannot start a process;
exec mode accepts only the fixed Predict component commands below.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat


TRUSTED_UID = 0
TRUST_BOUNDARY = Path("/")
RUNNING_GUARD = Path(__file__).absolute()
REPOSITORY = "ejson8282/polymarket-bot"


@dataclass(frozen=True)
class Profile:
    security_root: Path
    runtime_root: Path
    release_root: Path
    python: Path
    components: dict[str, tuple[str, ...]]
    artifact: str


VPS_COMPONENTS = {
    "runner": ("platforms/predictfun/maker/runner.py", "--config",
               "/home/ubuntu/predictfun-runtime/config.mainnet.json"),
    "ws": ("platforms/predictfun/ws_watch.py", "--config",
           "/home/ubuntu/predictfun-runtime/config.mainnet.json", "--discover", "20",
           "--forever", "--refresh-sec", "300", "--idle-timeout-sec", "900"),
}
PROFILES = {name: Profile(Path("/etc/predictfun-recovery"),
    Path("/home/ubuntu/predictfun-runtime"), Path("/home/ubuntu/predictfun-releases"),
    Path("/home/ubuntu/.venv2/bin/python"), VPS_COMPONENTS, "predictfun-dryrun")
    for name in ("vps1", "vps2")}
PROFILES["macmini"] = Profile(Path("/Library/Application Support/PredictFunRecovery"),
    Path("/Users/kevinsmacmini/predictfun-ws-runtime"), Path("/Users/kevinsmacmini/predictfun-ws-releases"),
    Path("/Users/kevinsmacmini/dev/varia-decibel-farming/.venv/bin/python"),
    {"api": ("deploy/mac-mini/predictfun_api_proxy.py", "--host", "100.91.159.54", "--port", "8791"),
     "relay": ("platforms/predictfun/ws_relay.py", "--host", "100.91.159.54", "--port", "8792")},
    "predictfun-mac-services")


class StartupGuardError(RuntimeError):
    pass


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError("nonfinite value")


def _json(raw: bytes) -> dict:
    value = json.loads(raw, object_pairs_hook=_object, parse_constant=_invalid_constant)
    if not isinstance(value, dict):
        raise StartupGuardError("guard_object_required")
    return value


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value) is not None


def _protected_parents(path: Path) -> None:
    for parent in path.parents:
        details = parent.lstat()
        if (not stat.S_ISDIR(details.st_mode) or details.st_uid != TRUSTED_UID
                or details.st_mode & 0o022):
            raise StartupGuardError("guard_parent_unprotected")
        if parent == TRUST_BOUNDARY:
            return
    raise StartupGuardError("guard_trust_boundary_missing")


def _read(path: Path, *, protected: bool = False, limit: int = 2_000_000) -> bytes:
    if path.resolve(strict=True) != path:
        raise StartupGuardError("guard_path_not_canonical")
    if protected:
        _protected_parents(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_mode & 0o222
                or before.st_size > limit or (protected and before.st_uid != TRUSTED_UID)):
            raise StartupGuardError("guard_file_unprotected")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read(limit + 1)
        after = os.fstat(fd)
        if (len(raw) != before.st_size or len(raw) > limit
                or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise StartupGuardError("guard_file_changed")
        return raw
    finally:
        os.close(fd)


def verify_artifact(profile: str, release: Path, expected_manifest_hash: str) -> dict:
    """Hash the complete immutable artifact against an independently pinned manifest."""
    spec = PROFILES[profile]
    if (release.parent != spec.release_root or release.resolve(strict=True) != release
            or not _hex(release.name, 40)):
        raise StartupGuardError("guard_release_path_invalid")
    manifest_raw = _read(release / ".release-manifest.json")
    if _sha(manifest_raw) != expected_manifest_hash:
        raise StartupGuardError("guard_manifest_digest_mismatch")
    manifest = _json(manifest_raw)
    files = manifest.get("files")
    if (manifest.get("source_repository") != REPOSITORY or manifest.get("artifact") != spec.artifact
            or manifest.get("commit") != release.name or not isinstance(files, dict) or not files):
        raise StartupGuardError("guard_manifest_invalid")
    actual = set()
    for path in [release, *release.rglob("*")]:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_mode & 0o222:
            raise StartupGuardError("guard_release_not_immutable")
        if stat.S_ISREG(info.st_mode) and path != release / ".release-manifest.json":
            actual.add(path.relative_to(release).as_posix())
        elif not stat.S_ISDIR(info.st_mode) and path != release / ".release-manifest.json":
            raise StartupGuardError("guard_release_special_file")
    if set(files) != actual:
        raise StartupGuardError("guard_release_file_set_mismatch")
    for name, digest in files.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or not _hex(digest, 64):
            raise StartupGuardError("guard_manifest_path_invalid")
        if _sha(_read(release / relative, limit=64_000_000)) != digest:
            raise StartupGuardError("guard_release_file_hash_mismatch")
    return manifest


def verify_startup(profile: str, component: str) -> dict:
    """Verify installed guard, required root anchor, policy and entire release."""
    if profile not in PROFILES or component not in PROFILES[profile].components:
        raise StartupGuardError("guard_component_not_allowed")
    spec = PROFILES[profile]
    guard = spec.security_root / "guard.py"
    if RUNNING_GUARD != guard:
        raise StartupGuardError("guard_must_run_from_external_installation")
    try:
        guard_raw = _read(guard, protected=True)
        anchor_raw = _read(spec.security_root / "anchor.json", protected=True, limit=65536)
        anchor = _json(anchor_raw)
        if (type(anchor.get("version")) is not int or anchor["version"] != 1
                or anchor.get("repository") != REPOSITORY or anchor.get("profile") != profile
                or anchor.get("guard_sha256") != _sha(guard_raw)
                or not _hex(anchor.get("policy_sha256"), 64)):
            raise StartupGuardError("guard_anchor_invalid")
        manifests = anchor.get("approved_manifests")
        if (not isinstance(manifests, dict) or not 1 <= len(manifests) <= 100
                or any(not _hex(k, 40) or not _hex(v, 64) for k, v in manifests.items())):
            raise StartupGuardError("guard_manifests_invalid")
        policy_path = spec.runtime_root / "recovery-release-floor/policy.json"
        policy_raw = _read(policy_path, limit=65536)
        if _sha(policy_raw) != anchor["policy_sha256"]:
            raise StartupGuardError("guard_policy_digest_mismatch")
        policy = _json(policy_raw)
        if (type(policy.get("version")) is not int or policy["version"] != 1
                or policy.get("repository") != REPOSITORY or policy.get("profile") != profile
                or not _hex(policy.get("recovery_id"), 64)
                or policy.get("allowed_releases") != sorted(manifests)):
            raise StartupGuardError("guard_policy_invalid")
        link = spec.release_root / "current"
        if not link.is_symlink() or spec.release_root.resolve(strict=True) != spec.release_root:
            raise StartupGuardError("guard_current_invalid")
        release = link.resolve(strict=True)
        if release.parent != spec.release_root or release.name not in manifests:
            raise StartupGuardError("guard_release_not_approved")
        manifest = verify_artifact(profile, release, manifests[release.name])
        files = manifest["files"]
        entry, *arguments = spec.components[component]
        if entry not in files:
            raise StartupGuardError("guard_entrypoint_missing")
        # Bind the result again after hashing the release; no refresh from an
        # untrusted new policy is accepted inside a single startup check.
        if (_read(spec.security_root / "anchor.json", protected=True, limit=65536) != anchor_raw
                or _read(policy_path, limit=65536) != policy_raw
                or link.resolve(strict=True) != release):
            raise StartupGuardError("guard_inputs_changed")
        return {"ok": True, "profile": profile, "component": component,
                "release_sha": release.name, "release": str(release),
                "anchor_sha256": _sha(anchor_raw), "policy_sha256": _sha(policy_raw),
                "argv": [str(spec.python), "-I", "-B", str(release / entry), *arguments]}
    except StartupGuardError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise StartupGuardError("guard_required_evidence_unavailable") from exc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Installed Predict startup verification")
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument("--component", choices=("runner", "ws", "api", "relay"), required=True)
    parser.add_argument("--exec", action="store_true", dest="execute")
    args = parser.parse_args(argv)
    result = verify_startup(args.profile, args.component)
    if not args.execute:
        print(json.dumps({k: v for k, v in result.items() if k not in {"argv", "release"}}))
        return 0
    # Retain existing live authorizations but never grant/repair them. A stale
    # LIVE_RELEASE_SHA remains stale and is rejected by the existing live gate.
    environment = {k: v for k, v in os.environ.items()
                   if not k.startswith(("PYTHON", "LD_", "DYLD_"))}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1",
                       PREDICTFUN_REQUIRE_RELEASE="1", PREDICTFUN_RELEASE_SHA=result["release_sha"])
    os.chdir(result["release"])
    os.execve(result["argv"][0], result["argv"], environment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
