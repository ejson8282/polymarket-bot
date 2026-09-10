"""Read-only release allowlist for a separately authorized Predict recovery.

Never creates or changes policy. Reviewed deployment wrappers call this before
activation and before automatic rollback. It is not protection against running
an old wrapper or a privileged operator deleting the entire policy directory.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat


class RecoveryReleaseFloorError(RuntimeError):
    pass


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate policy key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("nonfinite policy value")


@dataclass(frozen=True)
class ReleaseFloor:
    runtime_root: Path
    profile: str
    digest: str | None
    allowed: tuple[str, ...] = ()

    def require_release(self, sha: str | None) -> None:
        if self.digest is not None and sha not in self.allowed:
            raise RecoveryReleaseFloorError("recovery_release_not_approved")

    def require_unchanged(self) -> None:
        current = read_release_floor(self.runtime_root, self.profile)
        if current != self:
            raise RecoveryReleaseFloorError("recovery_release_floor_changed")


def read_release_floor(runtime_root: Path, profile: str) -> ReleaseFloor:
    if profile not in {"vps1", "vps2", "macmini"}:
        raise RecoveryReleaseFloorError("recovery_release_profile_invalid")
    directory = Path(runtime_root) / "recovery-release-floor"
    try:
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        # Legacy installations have no recovery policy. An existing but empty
        # policy directory, unlike an absent one, must fail closed below.
        return ReleaseFloor(Path(runtime_root), profile, None)
    except OSError as exc:
        raise RecoveryReleaseFloorError("recovery_release_floor_unavailable") from exc
    try:
        if os.fstat(dir_fd).st_mode & 0o222:
            raise ValueError("writable floor directory")
        fd = os.open("policy.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o222 or before.st_size > 65536:
                raise ValueError("invalid floor file")
            with os.fdopen(fd, "rb", closefd=False) as handle:
                raw = handle.read(65537)
            after = os.fstat(fd)
            if (len(raw) > 65536 or before.st_size != len(raw)
                    or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise ValueError("floor changed while reading")
        finally:
            os.close(fd)
        policy = json.loads(raw, object_pairs_hook=_object, parse_constant=_invalid_constant)
        if (not isinstance(policy, dict) or type(policy.get("version")) is not int
                or policy["version"] != 1
                or policy.get("repository") != "ejson8282/polymarket-bot"
                or policy.get("profile") != profile
                or not isinstance(policy.get("recovery_id"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", policy["recovery_id"])):
            raise ValueError("invalid floor identity")
        allowed = policy.get("allowed_releases")
        if (not isinstance(allowed, list) or not 1 <= len(allowed) <= 100
                or any(not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha)
                       for sha in allowed)
                or allowed != sorted(set(allowed))):
            raise ValueError("invalid release allowlist")
        return ReleaseFloor(Path(runtime_root), profile, hashlib.sha256(raw).hexdigest(), tuple(allowed))
    except (OSError, ValueError, TypeError) as exc:
        raise RecoveryReleaseFloorError("recovery_release_floor_invalid") from exc
    finally:
        os.close(dir_fd)


def check_release_transition(runtime_root: Path, profile: str,
                             target: str, previous: str | None) -> ReleaseFloor:
    floor = read_release_floor(runtime_root, profile)
    floor.require_release(target)
    # A permitted target with a forbidden automatic rollback point is unsafe.
    floor.require_release(previous)
    return floor
