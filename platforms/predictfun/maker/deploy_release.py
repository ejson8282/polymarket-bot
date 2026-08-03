"""Exact-SHA deployment wrapper for the Predict.fun dry-run service only."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from typing import Any, Iterator, Mapping, Optional, Sequence

from platforms.predictfun.maker.release_guard import (
    ARTIFACT,
    SOURCE_REPOSITORY,
    verify_release,
)


FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
CONFIRMATION = "DEPLOY_PREDICTFUN_DRYRUN"
ARCHIVE_PATHS = (
    "platforms/__init__.py",
    "platforms/predictfun",
    "deploy/systemd/predictfun-dryrun.service",
    "deploy/systemd/predictfun-dryrun.timer",
    "deploy/systemd/predictfun-ws.service",
)


@dataclass(frozen=True)
class DeploymentProfile:
    name: str
    account_id: str
    lock_file: Path


DEPLOYMENT_PROFILES = {
    "vps1": DeploymentProfile(
        name="vps1",
        account_id="account_01",
        lock_file=Path(
            "/home/ubuntu/latitude-runtime/locks/vps1-production-deploy.lock"
        ),
    ),
    "vps2": DeploymentProfile(
        name="vps2",
        account_id="account_02",
        lock_file=Path(
            "/home/ubuntu/latitude-runtime/locks/vps2-production-deploy.lock"
        ),
    ),
}


class DeploymentError(RuntimeError):
    """Raised when a Predict.fun deployment invariant is not satisfied."""


@dataclass(frozen=True)
class DeploymentPaths:
    profile: str = "vps1"
    account_id: str = "account_01"
    bare_repo: Path = Path("/home/ubuntu/repos/predictfun.git")
    release_root: Path = Path("/home/ubuntu/predictfun-releases")
    current_link: Path = Path("/home/ubuntu/predictfun-releases/current")
    runtime_root: Path = Path("/home/ubuntu/predictfun-runtime")
    data_root: Path = Path("/home/ubuntu/predictfun-runtime/data")
    runtime_config: Path = Path(
        "/home/ubuntu/predictfun-runtime/config.mainnet.json"
    )
    release_env: Path = Path("/home/ubuntu/predictfun-runtime/release.env")
    runner_state: Path = Path(
        "/home/ubuntu/predictfun-runtime/data/"
        "predictfun_mainnet_runner_state.json"
    )
    ws_state: Path = Path(
        "/home/ubuntu/predictfun-runtime/data/"
        "predictfun_mainnet_ws_state.json"
    )
    status_state: Path = Path(
        "/home/ubuntu/predictfun-runtime/data/"
        "predictfun_mainnet_status.json"
    )
    lock_file: Path = Path(
        "/home/ubuntu/latitude-runtime/locks/vps1-production-deploy.lock"
    )
    service_unit: Path = Path(
        "/etc/systemd/system/predictfun-dryrun.service"
    )
    timer_unit: Path = Path(
        "/etc/systemd/system/predictfun-dryrun.timer"
    )
    ws_service_unit: Path = Path(
        "/etc/systemd/system/predictfun-ws.service"
    )
    python: Path = Path("/home/ubuntu/.venv2/bin/python")
    service_name: str = "predictfun-dryrun.service"
    timer_name: str = "predictfun-dryrun.timer"
    ws_service_name: str = "predictfun-ws.service"
    service_user: str = "ubuntu"

    @classmethod
    def for_profile(cls, name: str) -> "DeploymentPaths":
        normalized = str(name or "").strip().lower()
        profile = DEPLOYMENT_PROFILES.get(normalized)
        if profile is None:
            raise DeploymentError(
                f"unsupported Predict.fun deployment profile: {name}"
            )
        return cls(
            profile=profile.name,
            account_id=profile.account_id,
            lock_file=profile.lock_file,
        )


@dataclass(frozen=True)
class FileSnapshot:
    content: Optional[bytes]
    mode: int = 0o644
    uid: int = -1
    gid: int = -1


class CommandRunner:
    """Subprocess seam used by deployment tests and production."""

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = True,
    ) -> str:
        result = subprocess.run(
            [str(arg) for arg in args],
            cwd=str(cwd) if cwd is not None else None,
            env=dict(env) if env is not None else None,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return result.stdout.strip()


def _require_full_sha(value: str, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not FULL_SHA_RE.fullmatch(normalized):
        raise DeploymentError(f"{label} must be a full 40-character commit SHA")
    return normalized


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, content: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        descriptor = os.open(
            str(temporary),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            mode,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary), str(path))
        path.chmod(mode)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    temporary = link.with_name(f".{link.name}.{os.getpid()}.tmp")
    try:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        os.symlink(str(target), str(temporary))
        os.replace(str(temporary), str(link))
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@contextmanager
def deployment_lock(lock_file: Path) -> Iterator[None]:
    if not lock_file.parent.is_dir():
        raise DeploymentError(
            f"global deployment lock directory missing: {lock_file.parent}"
        )
    with lock_file.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise DeploymentError(
                f"another deployment owns the host-global lock: {lock_file}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _git(
    paths: DeploymentPaths,
    runner: CommandRunner,
    *args: str,
) -> str:
    return runner.run(("git", f"--git-dir={paths.bare_repo}", *args))


def _require_exact_internal_main(
    paths: DeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
) -> None:
    if not paths.bare_repo.is_dir():
        raise DeploymentError(
            f"Predict.fun bare repository missing: {paths.bare_repo}"
        )
    if _git(paths, runner, "rev-parse", "--is-bare-repository") != "true":
        raise DeploymentError("Predict.fun release source must be a bare repository")
    target = _git(
        paths,
        runner,
        "rev-parse",
        "--verify",
        f"{target_sha}^{{commit}}",
    )
    main = _git(
        paths,
        runner,
        "rev-parse",
        "--verify",
        "refs/heads/main^{commit}",
    )
    if target != target_sha or main != target_sha:
        raise DeploymentError(
            "Predict.fun internal main must equal the exact requested commit"
        )


def _safe_extract(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r") as archive:
        for member in archive.getmembers():
            relative = Path(member.name)
            if relative.is_absolute() or ".." in relative.parts:
                raise DeploymentError(f"unsafe archive path: {member.name}")
            if member.issym() or member.islnk():
                raise DeploymentError(f"release archive contains a link: {member.name}")
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                target.chmod(member.mode & 0o777)
                continue
            if not member.isfile():
                raise DeploymentError(
                    f"unsupported release archive member: {member.name}"
                )
            source = archive.extractfile(member)
            if source is None:
                raise DeploymentError(f"release archive file unreadable: {member.name}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            target.chmod(member.mode & 0o777)


def _release_files(release_root: Path) -> list[Path]:
    return sorted(
        path
        for path in release_root.rglob("*")
        if path.is_file() and path.name != ".release-manifest.json"
    )


def _make_immutable(release_root: Path) -> None:
    files = sorted(
        (path for path in release_root.rglob("*") if path.is_file()),
        reverse=True,
    )
    for path in files:
        executable = bool(path.stat().st_mode & 0o111)
        path.chmod(0o555 if executable else 0o444)
    directories = sorted(
        (path for path in release_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in directories:
        path.chmod(0o555)
    release_root.chmod(0o555)


def _remove_release_tree(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except OSError:
            pass
    path.chmod(0o700)
    shutil.rmtree(path)


def _release_environment(target_sha: str) -> dict[str, str]:
    return {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": "",
        "PREDICTFUN_REQUIRE_RELEASE": "1",
        "PREDICTFUN_RELEASE_SHA": target_sha,
    }


def prepare_release(
    paths: DeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
) -> dict[str, Any]:
    target_sha = _require_full_sha(target_sha, "target SHA")
    _require_exact_internal_main(paths, runner, target_sha)
    if not paths.python.is_file():
        raise DeploymentError(f"deployment Python is missing: {paths.python}")

    paths.release_root.mkdir(parents=True, exist_ok=True)
    release = paths.release_root / target_sha
    if release.exists():
        manifest = verify_release(
            release,
            {
                "PREDICTFUN_REQUIRE_RELEASE": "1",
                "PREDICTFUN_RELEASE_SHA": target_sha,
            },
        )
        return {"status": "already_prepared", "manifest": manifest}

    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{target_sha}.prepare-",
            dir=str(paths.release_root),
        )
    )
    archive_path = temporary / ".source.tar"
    promoted = False
    try:
        _git(
            paths,
            runner,
            "archive",
            "--format=tar",
            f"--output={archive_path}",
            target_sha,
            "--",
            *ARCHIVE_PATHS,
        )
        _safe_extract(archive_path, temporary)
        archive_path.unlink()
        runner.run(
            (
                str(paths.python),
                "-m",
                "platforms.predictfun.maker.selftest",
            ),
            cwd=temporary,
            env=_release_environment(target_sha),
        )
        files = {
            path.relative_to(temporary).as_posix(): _sha256(path)
            for path in _release_files(temporary)
        }
        manifest = {
            "source_repository": SOURCE_REPOSITORY,
            "artifact": ARTIFACT,
            "commit": target_sha,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "files": files,
        }
        manifest_path = temporary / ".release-manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _make_immutable(temporary)
        os.replace(str(temporary), str(release))
        promoted = True
        verified = verify_release(
            release,
            {
                "PREDICTFUN_REQUIRE_RELEASE": "1",
                "PREDICTFUN_RELEASE_SHA": target_sha,
            },
        )
        return {"status": "prepared", "manifest": verified}
    except Exception:
        _remove_release_tree(release if promoted else temporary)
        raise


def _current_release_sha(paths: DeploymentPaths) -> Optional[str]:
    if not paths.current_link.exists() and not paths.current_link.is_symlink():
        return None
    if not paths.current_link.is_symlink():
        raise DeploymentError(
            f"Predict.fun current release is not a symlink: {paths.current_link}"
        )
    try:
        resolved = paths.current_link.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DeploymentError("Predict.fun current release link is broken") from exc
    if resolved.parent != paths.release_root.resolve():
        raise DeploymentError(f"Predict.fun current release escapes root: {resolved}")
    return _require_full_sha(resolved.name, "current release")


def _require_expected_current(
    paths: DeploymentPaths,
    expected_current: str,
) -> Optional[str]:
    actual = _current_release_sha(paths)
    normalized = str(expected_current or "").strip().lower()
    expected = None if normalized == "none" else _require_full_sha(
        normalized,
        "expected current SHA",
    )
    if actual != expected:
        raise DeploymentError(
            f"Predict.fun current release changed: expected {expected}, found {actual}"
        )
    return actual


def _runtime_config_payload(release: Path, paths: DeploymentPaths) -> bytes:
    source = release / "platforms/predictfun/maker/config.mainnet.json"
    try:
        config = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentError(f"Predict.fun mainnet config is invalid: {source}") from exc
    if not isinstance(config, dict) or config.get("environment") != "mainnet":
        raise DeploymentError("Predict.fun deployment requires a mainnet config object")
    output = config.get("output")
    if not isinstance(output, dict) or not output:
        raise DeploymentError("Predict.fun mainnet config is missing output paths")
    rewritten: dict[str, str] = {}
    for key, value in output.items():
        filename = Path(str(value)).name
        if not filename or filename in {".", ".."}:
            raise DeploymentError(f"unsafe Predict.fun output path: {key}")
        rewritten[str(key)] = str(paths.data_root / filename)
    config["output"] = rewritten
    accounts = config.get("accounts")
    if not isinstance(accounts, dict):
        raise DeploymentError("Predict.fun mainnet config is missing accounts")
    accounts["enabled"] = True
    accounts["max_active_accounts"] = 1
    accounts["ids"] = [paths.account_id]
    config["accounts"] = accounts
    risk = config.get("risk")
    if not isinstance(risk, dict):
        raise DeploymentError("Predict.fun mainnet config is missing risk")
    risk["max_active_accounts"] = 1
    config["risk"] = risk
    config["deployment"] = {
        "profile": paths.profile,
        "account_id": paths.account_id,
    }
    return (json.dumps(config, indent=2) + "\n").encode("utf-8")


def _validate_predict_only_unit(content: bytes, unit_kind: str) -> None:
    text = content.decode("utf-8")
    forbidden = (
        "polymarket-engine",
        "/home/ubuntu/polymarket-bot",
        "/home/ubuntu/polymarket-releases",
        "/home/ubuntu/polymarket-runtime",
        "platforms.polymarket",
        "live_order_once",
    )
    found = [value for value in forbidden if value in text]
    if found:
        raise DeploymentError(
            f"{unit_kind} crosses the Predict.fun boundary: {found}"
        )
    if unit_kind == "dry-run service unit":
        required = ("platforms.predictfun.maker.runner", "--once")
    elif unit_kind == "dry-run timer unit":
        required = ("[Timer]", "OnUnitActiveSec=")
    elif unit_kind == "WebSocket service unit":
        required = (
            "platforms.predictfun.ws_watch",
            "--forever",
            "predictfun-runtime/config.mainnet.json",
        )
    else:
        raise DeploymentError(f"unknown Predict.fun unit kind: {unit_kind}")
    missing = [value for value in required if value not in text]
    if missing:
        raise DeploymentError(
            f"{unit_kind} is invalid: missing {missing}"
        )
    if "PREDICTFUN_API_KEY" in text:
        raise DeploymentError(
            f"{unit_kind} must not load the Predict.fun API key on a VPS"
        )


def _snapshot(path: Path) -> FileSnapshot:
    if not path.exists():
        return FileSnapshot(None)
    stat = path.stat()
    return FileSnapshot(
        path.read_bytes(),
        stat.st_mode & 0o777,
        stat.st_uid,
        stat.st_gid,
    )


def _restore(path: Path, snapshot: FileSnapshot) -> None:
    if snapshot.content is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return
    _atomic_write(path, snapshot.content, snapshot.mode)
    if os.geteuid() == 0 and snapshot.uid >= 0 and snapshot.gid >= 0:
        os.chown(path, snapshot.uid, snapshot.gid)


def _prepare_runtime_permissions(paths: DeploymentPaths) -> tuple[int, int]:
    paths.runtime_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_root.chmod(0o755)
    paths.data_root.mkdir(parents=True, exist_ok=True)
    if os.geteuid() == 0:
        try:
            account = pwd.getpwnam(paths.service_user)
        except KeyError as exc:
            raise DeploymentError(
                f"Predict.fun service user does not exist: {paths.service_user}"
            ) from exc
        os.chown(paths.data_root, account.pw_uid, account.pw_gid)
        paths.data_root.chmod(0o700)
        return account.pw_uid, account.pw_gid
    paths.data_root.chmod(0o700)
    return os.geteuid(), os.getegid()


def _parse_timestamp(value: Any) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise DeploymentError(f"invalid runner timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_fresh_after(value: Any, observed_after: datetime) -> bool:
    return _parse_timestamp(value) >= observed_after.replace(microsecond=0)


def _verify_runner_state(
    paths: DeploymentPaths,
    target_sha: str,
    observed_after: datetime,
) -> dict[str, Any]:
    try:
        state = json.loads(paths.runner_state.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentError(
            f"Predict.fun runner state unavailable: {paths.runner_state}"
        ) from exc
    if not isinstance(state, dict):
        raise DeploymentError("Predict.fun runner state must be a JSON object")
    auth = state.get("last_auth_summary")
    auth_rows = auth.get("accounts") if isinstance(auth, dict) else None
    auth_account_ids = [
        str(row.get("account_id") or "")
        for row in auth_rows or []
        if isinstance(row, dict)
    ]
    auth_accounts_ok = bool(auth_rows) and all(
        isinstance(row, dict) and row.get("ok") is True
        for row in auth_rows
    )
    checks = {
        "mode": state.get("mode") == "dry_run",
        "release_sha": state.get("release_sha") == target_sha,
        "release_required": state.get("release_required") is True,
        "profile": state.get("deployment_profile") == paths.profile,
        "account": state.get("account_ids") == [paths.account_id],
        "auth": (
            isinstance(auth, dict)
            and auth.get("enabled") is True
            and auth.get("ok") is True
            and auth_account_ids == [paths.account_id]
            and auth_accounts_ok
        ),
        "cycle_count": int(state.get("cycle_count") or 0) >= 1,
        "error_count": int(state.get("error_count") or 0) == 0,
        "last_error": not str(state.get("last_error") or ""),
        "fresh": _is_fresh_after(
            state.get("last_cycle_finished_at"),
            observed_after,
        ),
        "stopped": state.get("running") is False,
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise DeploymentError(
            f"Predict.fun dry-run acceptance failed: {', '.join(failed)}"
        )
    return state


def _verify_ws_state(
    paths: DeploymentPaths,
    observed_after: datetime,
    *,
    attempts: int = 30,
    interval_sec: float = 1.0,
) -> dict[str, Any]:
    last_failure = "state unavailable"
    for attempt in range(max(1, attempts)):
        try:
            state = json.loads(paths.ws_state.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                raise DeploymentError(
                    "Predict.fun WebSocket state must be a JSON object"
                )
            books = state.get("orderbooks")
            market_ids = state.get("market_ids")
            trading_statuses = state.get("trading_statuses")
            market_statuses = state.get("market_statuses")
            upstream_timestamps = state.get(
                "orderbook_upstream_updated_at_ms"
            )
            book_ids = set(books) if isinstance(books, dict) else set()
            statuses_cover_books = bool(book_ids) and all(
                isinstance(trading_statuses, dict)
                and isinstance(trading_statuses.get(market_id), dict)
                and trading_statuses[market_id].get("status") == "OPEN"
                and isinstance(market_statuses, dict)
                and isinstance(market_statuses.get(market_id), dict)
                and market_statuses[market_id].get("status")
                in {"OPEN", "REGISTERED"}
                for market_id in book_ids
            )
            timestamps_cover_books = bool(book_ids) and all(
                isinstance(upstream_timestamps, dict)
                and int(upstream_timestamps.get(market_id) or 0) > 0
                for market_id in book_ids
            )
            books_are_valid = bool(book_ids) and all(
                isinstance(books[market_id], dict)
                and isinstance(books[market_id].get("bids"), list)
                and isinstance(books[market_id].get("asks"), list)
                for market_id in book_ids
            )
            checks = {
                "schema": int(state.get("schema_version") or 0) >= 2,
                "connected": state.get("connected") is True,
                "error": not str(state.get("error") or ""),
                "markets": isinstance(market_ids, list) and bool(market_ids),
                "orderbooks": books_are_valid,
                "book_timestamps": timestamps_cover_books,
                "market_statuses": statuses_cover_books,
                "file_fresh": paths.ws_state.stat().st_mtime
                >= observed_after.timestamp(),
                "fresh": _is_fresh_after(
                    state.get("last_message_at"),
                    observed_after,
                ),
            }
            failed = sorted(name for name, passed in checks.items() if not passed)
            if not failed:
                return state
            last_failure = ", ".join(failed)
        except (DeploymentError, OSError, ValueError, json.JSONDecodeError) as exc:
            last_failure = str(exc)
        if attempt + 1 < max(1, attempts):
            time.sleep(max(0.0, interval_sec))
    raise DeploymentError(
        f"Predict.fun WebSocket acceptance failed: {last_failure}"
    )


def _verify_status_state(
    paths: DeploymentPaths,
    target_sha: str,
    observed_after: datetime,
) -> dict[str, Any]:
    try:
        state = json.loads(paths.status_state.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentError(
            f"Predict.fun status snapshot unavailable: {paths.status_state}"
        ) from exc
    if not isinstance(state, dict):
        raise DeploymentError("Predict.fun status snapshot must be a JSON object")
    deployment = (
        state.get("deployment")
        if isinstance(state.get("deployment"), dict)
        else {}
    )
    capabilities = (
        state.get("capabilities")
        if isinstance(state.get("capabilities"), dict)
        else {}
    )
    health = (
        state.get("health")
        if isinstance(state.get("health"), dict)
        else {}
    )
    checks = {
        "schema": int(state.get("schema_version") or 0) >= 1,
        "project": state.get("project") == "predictfun",
        "fresh": _is_fresh_after(state.get("ts"), observed_after),
        "profile": deployment.get("profile") == paths.profile,
        "account": deployment.get("account_id") == paths.account_id,
        "release": deployment.get("release_sha") == target_sha,
        "mode": deployment.get("mode") == "dry_run",
        "not_blocked": health.get("status") in {"healthy", "attention"},
        "no_live_submit": capabilities.get("live_order_submit") is False,
        "no_live_cancel": capabilities.get("live_order_cancel") is False,
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise DeploymentError(
            f"Predict.fun status acceptance failed: {', '.join(failed)}"
        )
    return state


def activate_release(
    paths: DeploymentPaths,
    runner: CommandRunner,
    *,
    target_sha: str,
    expected_current: str,
    confirm: str,
    authorization_id: str,
) -> dict[str, Any]:
    target_sha = _require_full_sha(target_sha, "target SHA")
    if confirm != CONFIRMATION:
        raise DeploymentError(f"confirmation must equal {CONFIRMATION}")
    if not str(authorization_id or "").strip():
        raise DeploymentError("authorization ID is required")
    previous_sha = _require_expected_current(paths, expected_current)
    release = paths.release_root / target_sha
    verify_release(
        release,
        {
            "PREDICTFUN_REQUIRE_RELEASE": "1",
            "PREDICTFUN_RELEASE_SHA": target_sha,
        },
    )

    service_source = release / "deploy/systemd/predictfun-dryrun.service"
    timer_source = release / "deploy/systemd/predictfun-dryrun.timer"
    ws_service_source = release / "deploy/systemd/predictfun-ws.service"
    service_content = service_source.read_bytes()
    timer_content = timer_source.read_bytes()
    ws_service_content = ws_service_source.read_bytes()
    _validate_predict_only_unit(service_content, "dry-run service unit")
    _validate_predict_only_unit(timer_content, "dry-run timer unit")
    _validate_predict_only_unit(ws_service_content, "WebSocket service unit")

    service_uid, service_gid = _prepare_runtime_permissions(paths)
    snapshots = {
        "service": _snapshot(paths.service_unit),
        "timer": _snapshot(paths.timer_unit),
        "ws_service": _snapshot(paths.ws_service_unit),
        "config": _snapshot(paths.runtime_config),
        "env": _snapshot(paths.release_env),
    }
    timer_enabled = runner.run(
        ("systemctl", "is-enabled", paths.timer_name),
        check=False,
    ) == "enabled"
    timer_active = runner.run(
        ("systemctl", "is-active", paths.timer_name),
        check=False,
    ) == "active"
    ws_service_enabled = runner.run(
        ("systemctl", "is-enabled", paths.ws_service_name),
        check=False,
    ) == "enabled"
    ws_service_active = runner.run(
        ("systemctl", "is-active", paths.ws_service_name),
        check=False,
    ) == "active"
    previous_target = (
        paths.current_link.resolve(strict=True) if previous_sha is not None else None
    )

    try:
        runner.run(("systemctl", "stop", paths.timer_name), check=False)
        runner.run(("systemctl", "stop", paths.service_name), check=False)
        runner.run(("systemctl", "stop", paths.ws_service_name), check=False)
        _atomic_symlink(paths.current_link, release)
        _atomic_write(
            paths.release_env,
            f"PREDICTFUN_RELEASE_SHA={target_sha}\n".encode("utf-8"),
            0o444,
        )
        _atomic_write(
            paths.runtime_config,
            _runtime_config_payload(release, paths),
            0o400,
        )
        if os.geteuid() == 0:
            os.chown(paths.runtime_config, service_uid, service_gid)
        _atomic_write(paths.service_unit, service_content, 0o644)
        _atomic_write(paths.timer_unit, timer_content, 0o644)
        _atomic_write(paths.ws_service_unit, ws_service_content, 0o644)
        runner.run(("systemctl", "daemon-reload"))
        observed_after = datetime.now(timezone.utc)
        runner.run(("systemctl", "enable", paths.ws_service_name))
        runner.run(("systemctl", "start", paths.ws_service_name))
        if runner.run(
            ("systemctl", "is-active", paths.ws_service_name)
        ) != "active":
            raise DeploymentError(
                "Predict.fun WebSocket service is not active after deployment"
            )
        ws_state = _verify_ws_state(paths, observed_after)
        runner.run(("systemctl", "start", paths.service_name))
        state = _verify_runner_state(paths, target_sha, observed_after)
        status_state = _verify_status_state(paths, target_sha, observed_after)
        status_health = (
            status_state.get("health")
            if isinstance(status_state.get("health"), dict)
            else {}
        )
        status_deployment = (
            status_state.get("deployment")
            if isinstance(status_state.get("deployment"), dict)
            else {}
        )
        runner.run(("systemctl", "enable", "--now", paths.timer_name))
        if runner.run(("systemctl", "is-active", paths.timer_name)) != "active":
            raise DeploymentError("Predict.fun timer is not active after deployment")
        return {
            "status": "activated",
            "target_sha": target_sha,
            "previous_sha": previous_sha,
            "profile": paths.profile,
            "account_id": paths.account_id,
            "authorization_id": authorization_id,
            "timer": "active",
            "websocket": {
                "active": True,
                "connected": ws_state.get("connected"),
                "market_count": len(ws_state.get("market_ids") or []),
                "book_count": len(ws_state.get("orderbooks") or {}),
                "last_message_at": ws_state.get("last_message_at"),
            },
            "status_snapshot": {
                "schema_version": status_state.get("schema_version"),
                "health": status_health.get("status"),
                "mode": status_deployment.get("mode"),
            },
            "runner": {
                "mode": state.get("mode"),
                "cycle_count": state.get("cycle_count"),
                "error_count": state.get("error_count"),
                "release_sha": state.get("release_sha"),
                "deployment_profile": state.get("deployment_profile"),
                "account_ids": state.get("account_ids"),
                "auth_ok": bool(
                    isinstance(state.get("last_auth_summary"), dict)
                    and state["last_auth_summary"].get("ok") is True
                ),
                "last_cycle_finished_at": state.get("last_cycle_finished_at"),
            },
        }
    except Exception as exc:
        runner.run(("systemctl", "stop", paths.timer_name), check=False)
        runner.run(("systemctl", "stop", paths.service_name), check=False)
        runner.run(("systemctl", "stop", paths.ws_service_name), check=False)
        if previous_target is None:
            try:
                paths.current_link.unlink()
            except FileNotFoundError:
                pass
        else:
            _atomic_symlink(paths.current_link, previous_target)
        _restore(paths.service_unit, snapshots["service"])
        _restore(paths.timer_unit, snapshots["timer"])
        _restore(paths.ws_service_unit, snapshots["ws_service"])
        _restore(paths.runtime_config, snapshots["config"])
        _restore(paths.release_env, snapshots["env"])
        runner.run(("systemctl", "daemon-reload"), check=False)
        if timer_enabled:
            runner.run(
                ("systemctl", "enable", "--now", paths.timer_name),
                check=False,
            )
        elif timer_active:
            runner.run(("systemctl", "start", paths.timer_name), check=False)
        else:
            runner.run(
                ("systemctl", "disable", "--now", paths.timer_name),
                check=False,
            )
        if ws_service_enabled:
            runner.run(
                ("systemctl", "enable", "--now", paths.ws_service_name),
                check=False,
            )
        elif ws_service_active:
            runner.run(
                ("systemctl", "start", paths.ws_service_name),
                check=False,
            )
        else:
            runner.run(
                ("systemctl", "disable", "--now", paths.ws_service_name),
                check=False,
            )
        raise DeploymentError(
            "Predict.fun activation failed; previous Predict-only service state "
            f"was restored: {exc}"
        ) from exc


def status(paths: DeploymentPaths, runner: CommandRunner) -> dict[str, Any]:
    return {
        "profile": paths.profile,
        "account_id": paths.account_id,
        "current_sha": _current_release_sha(paths),
        "timer_active": runner.run(
            ("systemctl", "is-active", paths.timer_name),
            check=False,
        ),
        "timer_enabled": runner.run(
            ("systemctl", "is-enabled", paths.timer_name),
            check=False,
        ),
        "ws_service_active": runner.run(
            ("systemctl", "is-active", paths.ws_service_name),
            check=False,
        ),
        "ws_service_enabled": runner.run(
            ("systemctl", "is-enabled", paths.ws_service_name),
            check=False,
        ),
        "ws_state_present": paths.ws_state.is_file(),
        "status_state_present": paths.status_state.is_file(),
        "runner_state_present": paths.runner_state.is_file(),
    }


def execute(
    action: str,
    *,
    paths: DeploymentPaths,
    runner: CommandRunner,
    target_sha: str = "",
    expected_current: str = "none",
    confirm: str = "",
    authorization_id: str = "",
) -> dict[str, Any]:
    if action == "status":
        return status(paths, runner)
    with deployment_lock(paths.lock_file):
        if action == "prepare":
            return prepare_release(paths, runner, target_sha)
        if action == "activate":
            return activate_release(
                paths,
                runner,
                target_sha=target_sha,
                expected_current=expected_current,
                confirm=confirm,
                authorization_id=authorization_id,
            )
    raise DeploymentError(f"unsupported action: {action}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare or activate an exact Predict.fun dry-run release."
    )
    parser.add_argument("action", choices=("status", "prepare", "activate"))
    parser.add_argument(
        "--profile",
        required=True,
        choices=tuple(sorted(DEPLOYMENT_PROFILES)),
    )
    parser.add_argument("--target-sha", default="")
    parser.add_argument("--expected-current", default="none")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--authorization-id", default="")
    args = parser.parse_args(argv)
    result = execute(
        args.action,
        paths=DeploymentPaths.for_profile(args.profile),
        runner=CommandRunner(),
        target_sha=args.target_sha,
        expected_current=args.expected_current,
        confirm=args.confirm,
        authorization_id=args.authorization_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
