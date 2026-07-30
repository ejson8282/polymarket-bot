"""Serialized, exact-SHA deployment for the Polymarket maker service.

The tool intentionally owns only the immutable maker release and its service
switch. Runtime configuration, secrets, account data, and the dashboard stay
outside the release tree.
"""

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
import re
import shutil
import subprocess
import sys
import tarfile
import time
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence


SOURCE_REPOSITORY = "ejson8282/polymarket-bot"
SERVICE_NAME = "polymarket-engine.service"
FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

RELEASE_TESTS = (
    "tests/test_polymarket_release_guard.py",
    "tests/test_polymarket_maker_engine.py",
)


class DeploymentError(RuntimeError):
    """A fail-closed deployment invariant was not satisfied."""


@dataclass(frozen=True)
class DeploymentPaths:
    bare_repo: Path = Path("/home/ubuntu/repos/polymarket-bot.git")
    release_root: Path = Path("/home/ubuntu/polymarket-releases")
    current_link: Path = Path("/home/ubuntu/polymarket-releases/current")
    runtime_root: Path = Path("/home/ubuntu/polymarket-runtime")
    release_env: Path = Path("/home/ubuntu/polymarket-runtime/engine-release.env")
    lock_file: Path = Path(
        "/home/ubuntu/latitude-runtime/locks/vps1-production-deploy.lock"
    )
    drop_in: Path = Path(
        "/etc/systemd/system/"
        "polymarket-engine.service.d/zz-immutable-release.conf"
    )
    pause_flag: Path = Path("/home/ubuntu/polymarket-bot/data/.account_1.paused")
    state_file: Path = Path("/home/ubuntu/polymarket-bot/data/engine_state_1.json")
    python: Path = Path("/home/ubuntu/.venv2/bin/python")


@dataclass(frozen=True)
class DeploymentRequest:
    action: str
    target_sha: str
    expected_current_sha: str
    confirm: str = ""
    authorization_id: str = ""


class CommandRunner:
    """Small subprocess seam used by the deployment code and tests."""

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> str:
        result = subprocess.run(
            [str(arg) for arg in args],
            cwd=str(cwd) if cwd is not None else None,
            env=dict(env) if env is not None else None,
            check=True,
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


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
        raise DeploymentError(f"global deployment lock directory missing: {lock_file.parent}")
    with lock_file.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise DeploymentError(
                f"another VPS1 deployment owns the global lock: {lock_file}"
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


def _current_release_sha(paths: DeploymentPaths) -> str:
    if not paths.current_link.is_symlink():
        raise DeploymentError(f"current release link is missing: {paths.current_link}")
    try:
        resolved = paths.current_link.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DeploymentError("current release link is broken") from exc
    if resolved.parent != paths.release_root.resolve():
        raise DeploymentError(f"current release escapes release root: {resolved}")
    return _require_full_sha(resolved.name, "current release")


def _verify_or_promote_candidate(
    paths: DeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
    *,
    promote: bool,
) -> None:
    if not paths.bare_repo.is_dir():
        raise DeploymentError(f"internal bare repository missing: {paths.bare_repo}")
    is_bare = _git(paths, runner, "rev-parse", "--is-bare-repository")
    if is_bare != "true":
        raise DeploymentError("release source must be a bare repository")

    target_commit = _git(
        paths,
        runner,
        "rev-parse",
        "--verify",
        f"{target_sha}^{{commit}}",
    )
    main_commit = _git(
        paths,
        runner,
        "rev-parse",
        "--verify",
        "refs/heads/main^{commit}",
    )
    if target_commit != target_sha:
        raise DeploymentError("target SHA does not resolve to the exact requested commit")
    if main_commit == target_sha:
        return

    candidate_ref = f"refs/deploy-candidates/{target_sha}"
    try:
        candidate_commit = _git(
            paths,
            runner,
            "rev-parse",
            "--verify",
            f"{candidate_ref}^{{commit}}",
        )
    except subprocess.CalledProcessError as exc:
        raise DeploymentError(
            f"candidate ref is missing: {candidate_ref}; push the reviewed "
            "commit to the internal bare repository first"
        ) from exc
    if candidate_commit != target_sha:
        raise DeploymentError("candidate ref does not resolve to the target SHA")
    if not promote:
        return
    try:
        _git(
            paths,
            runner,
            "merge-base",
            "--is-ancestor",
            main_commit,
            target_sha,
        )
    except subprocess.CalledProcessError as exc:
        raise DeploymentError(
            "candidate is not a fast-forward descendant of internal main"
        ) from exc
    _git(
        paths,
        runner,
        "update-ref",
        "refs/heads/main",
        target_sha,
        main_commit,
    )
    promoted_main = _git(
        paths,
        runner,
        "rev-parse",
        "--verify",
        "refs/heads/main^{commit}",
    )
    if promoted_main != target_sha:
        raise DeploymentError(
            f"internal main promotion failed: expected {target_sha}, "
            f"found {promoted_main}"
        )


def _load_json(path: Path, label: str) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DeploymentError(f"{label} unavailable: {path}") from exc
    if not isinstance(payload, dict):
        raise DeploymentError(f"{label} must be a JSON object")
    return payload


def _parse_timestamp(value: Any) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise DeploymentError("engine state timestamp is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _require_paused(
    paths: DeploymentPaths,
    expected_release_sha: str,
    *,
    now: Optional[datetime] = None,
    max_age_seconds: float = 120.0,
) -> Dict[str, Any]:
    if not paths.pause_flag.is_file():
        raise DeploymentError(f"account pause flag is missing: {paths.pause_flag}")
    state = _load_json(paths.state_file, "engine state")
    if state.get("paused") is not True:
        raise DeploymentError("engine state does not confirm paused=true")
    if state.get("release_sha") != expected_release_sha:
        raise DeploymentError("engine state release does not match current release")
    observed_at = _parse_timestamp(state.get("ts"))
    reference = (now or _utc_now()).astimezone(timezone.utc)
    age = (reference - observed_at).total_seconds()
    if age < -5 or age > max_age_seconds:
        raise DeploymentError(f"engine state is stale ({age:.1f}s old)")
    return state


def _require_service_active(runner: CommandRunner) -> None:
    status = runner.run(("systemctl", "is-active", SERVICE_NAME))
    if status != "active":
        raise DeploymentError(f"{SERVICE_NAME} is not active: {status}")


def _require_drop_in(paths: DeploymentPaths) -> None:
    try:
        content = paths.drop_in.read_text(encoding="utf-8")
    except OSError as exc:
        raise DeploymentError(f"immutable release drop-in unavailable: {paths.drop_in}") from exc

    required_fragments = (
        "Environment=POLYMARKET_REQUIRE_RELEASE=1",
        f"EnvironmentFile={paths.release_env}",
        f"{paths.current_link}/platforms/polymarket/maker/release_guard.py",
        f"{paths.current_link}/platforms/polymarket/maker/engine.py",
    )
    missing = [fragment for fragment in required_fragments if fragment not in content]
    if missing:
        raise DeploymentError(
            "immutable release drop-in is incomplete: " + ", ".join(missing)
        )


def _safe_extract(archive_path: Path, destination: Path) -> None:
    destination_resolved = destination.resolve()
    with tarfile.open(str(archive_path), mode="r:") as archive:
        for member in archive.getmembers():
            member_path = (destination / member.name).resolve()
            try:
                member_path.relative_to(destination_resolved)
            except ValueError as exc:
                raise DeploymentError(f"unsafe archive member: {member.name}") from exc
            if member.issym() or member.islnk() or member.isdev():
                raise DeploymentError(f"unsupported archive member: {member.name}")
        if sys.version_info >= (3, 12):
            archive.extractall(str(destination), filter="fully_trusted")
        else:
            archive.extractall(str(destination))


def _manifest_for(release_dir: Path, target_sha: str) -> Dict[str, Any]:
    engine = release_dir / "platforms/polymarket/maker/engine.py"
    guard = release_dir / "platforms/polymarket/maker/release_guard.py"
    if not engine.is_file() or not guard.is_file():
        raise DeploymentError("release is missing maker engine or release guard")
    return {
        "source_repository": SOURCE_REPOSITORY,
        "commit": target_sha,
        "prepared_at": _iso_now(),
        "engine_sha256": _sha256(engine),
    }


def _verify_release_manifest(release_dir: Path, target_sha: str) -> Dict[str, Any]:
    if release_dir.name != target_sha:
        raise DeploymentError("release directory name does not match target SHA")
    manifest = _load_json(release_dir / ".release-manifest.json", "release manifest")
    if manifest.get("source_repository") != SOURCE_REPOSITORY:
        raise DeploymentError("release manifest repository mismatch")
    if manifest.get("commit") != target_sha:
        raise DeploymentError("release manifest commit mismatch")
    engine = release_dir / "platforms/polymarket/maker/engine.py"
    if manifest.get("engine_sha256") != _sha256(engine):
        raise DeploymentError("release manifest engine hash mismatch")
    return manifest


def _verification_env(target_sha: str) -> Dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "POLYMARKET_REQUIRE_RELEASE": "1",
            "POLYMARKET_RELEASE_SHA": target_sha,
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def _run_release_checks(
    paths: DeploymentPaths,
    runner: CommandRunner,
    release_dir: Path,
    target_sha: str,
    tests: Sequence[str] = RELEASE_TESTS,
) -> None:
    env = _verification_env(target_sha)
    engine = release_dir / "platforms/polymarket/maker/engine.py"
    guard = release_dir / "platforms/polymarket/maker/release_guard.py"
    runner.run((str(paths.python), str(guard), str(engine)), env=env)
    if tests:
        runner.run(
            (
                str(paths.python),
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                *tests,
            ),
            cwd=release_dir,
            env=env,
        )


def _make_release_read_only(release_dir: Path) -> None:
    for path in sorted(release_dir.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o555)
        elif path.is_file():
            executable = bool(path.stat().st_mode & 0o111)
            path.chmod(0o555 if executable else 0o444)
    release_dir.chmod(0o555)


def prepare_release(
    paths: DeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
    *,
    tests: Sequence[str] = RELEASE_TESTS,
) -> Path:
    release_dir = paths.release_root / target_sha
    if release_dir.exists():
        _verify_release_manifest(release_dir, target_sha)
        _run_release_checks(paths, runner, release_dir, target_sha, tests)
        return release_dir

    paths.release_root.mkdir(parents=True, exist_ok=True)
    paths.runtime_root.mkdir(parents=True, exist_ok=True)
    staging_parent = paths.release_root / f".staging-{target_sha}-{os.getpid()}"
    staging = staging_parent / target_sha
    archive_path = paths.runtime_root / f".{target_sha}.archive-{os.getpid()}.tar"
    if staging_parent.exists() or archive_path.exists():
        raise DeploymentError("stale release staging path already exists")

    try:
        staging.mkdir(mode=0o700, parents=True)
        _git(
            paths,
            runner,
            "archive",
            "--format=tar",
            f"--output={archive_path}",
            target_sha,
        )
        _safe_extract(archive_path, staging)
        manifest = _manifest_for(staging, target_sha)
        (staging / ".release-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _verify_release_manifest(staging, target_sha)
        _run_release_checks(paths, runner, staging, target_sha, tests)
        os.replace(str(staging), str(release_dir))
        _make_release_read_only(release_dir)
    finally:
        try:
            archive_path.unlink()
        except FileNotFoundError:
            pass
        if staging_parent.exists():
            shutil.rmtree(staging_parent)
    return release_dir


def _expected_confirmation(action: str, target_sha: str) -> str:
    return f"{action.upper()}:{target_sha}"


def _validate_request(request: DeploymentRequest) -> DeploymentRequest:
    target = _require_full_sha(request.target_sha, "target SHA")
    expected = _require_full_sha(request.expected_current_sha, "expected current SHA")
    action = str(request.action or "").strip().lower()
    if action not in {"plan", "prepare", "activate", "rollback"}:
        raise DeploymentError(f"unsupported deployment action: {action}")
    if target == expected and action in {"activate", "rollback"}:
        raise DeploymentError("target release is already current")
    if action != "plan":
        expected_confirmation = _expected_confirmation(action, target)
        if request.confirm != expected_confirmation:
            raise DeploymentError(
                f"confirmation must exactly equal {expected_confirmation}"
            )
    if action in {"activate", "rollback"} and not request.authorization_id.strip():
        raise DeploymentError("an explicit authorization ID is required")
    authorization_id = request.authorization_id.strip()
    if authorization_id and not re.fullmatch(r"[A-Za-z0-9._:-]{3,120}", authorization_id):
        raise DeploymentError(
            "authorization ID must use only letters, numbers, dot, dash, "
            "underscore, or colon"
        )
    return DeploymentRequest(
        action=action,
        target_sha=target,
        expected_current_sha=expected,
        confirm=request.confirm,
        authorization_id=authorization_id,
    )


def _audit_dir(paths: DeploymentPaths, target_sha: str) -> Path:
    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    path = paths.runtime_root / "deployments" / f"{timestamp}-{target_sha[:8]}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _write_audit(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write(
        path / "result.json",
        (json.dumps(dict(payload), indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )


def _wait_for_engine_state(
    paths: DeploymentPaths,
    target_sha: str,
    *,
    timeout_seconds: float = 30.0,
    sleep=time.sleep,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error = "state unavailable"
    while time.monotonic() < deadline:
        try:
            state = _load_json(paths.state_file, "engine state")
            if (
                state.get("release_sha") == target_sha
                and state.get("release_required") is True
                and state.get("paused") is True
                and state.get("quotes_sent") == 0
                and state.get("fills_seen") == 0
            ):
                return state
            last_error = "state has not confirmed exact release + paused zero-activity"
        except DeploymentError as exc:
            last_error = str(exc)
        sleep(0.5)
    raise DeploymentError(f"post-restart engine verification timed out: {last_error}")


def _activate_release(
    paths: DeploymentPaths,
    runner: CommandRunner,
    request: DeploymentRequest,
    release_dir: Path,
) -> Dict[str, Any]:
    previous_sha = _current_release_sha(paths)
    if previous_sha != request.expected_current_sha:
        raise DeploymentError(
            f"current release changed: expected {request.expected_current_sha}, "
            f"found {previous_sha}"
        )
    _require_paused(paths, previous_sha)
    _require_service_active(runner)
    _require_drop_in(paths)

    audit_path = _audit_dir(paths, request.target_sha)
    old_env = paths.release_env.read_bytes() if paths.release_env.exists() else None
    base_audit: Dict[str, Any] = {
        "source_repository": SOURCE_REPOSITORY,
        "target_sha": request.target_sha,
        "rollback_sha": previous_sha,
        "service": SERVICE_NAME,
        "authorization_id": request.authorization_id,
        "action": request.action,
        "started_at": _iso_now(),
        "lock_file": str(paths.lock_file),
    }
    _write_audit(audit_path, dict(base_audit, status="activating"))

    try:
        _atomic_write(
            paths.release_env,
            f"POLYMARKET_RELEASE_SHA={request.target_sha}\n".encode("utf-8"),
            mode=0o600,
        )
        _atomic_symlink(paths.current_link, release_dir)
        _require_paused(paths, previous_sha)
        runner.run(("sudo", "-n", "systemctl", "restart", SERVICE_NAME))
        _require_service_active(runner)
        state = _wait_for_engine_state(paths, request.target_sha)
        result = dict(
            base_audit,
            status="succeeded",
            completed_at=_iso_now(),
            engine_state={
                "release_sha": state.get("release_sha"),
                "release_required": state.get("release_required"),
                "paused": state.get("paused"),
                "quotes_sent": state.get("quotes_sent"),
                "fills_seen": state.get("fills_seen"),
                "ts": state.get("ts"),
            },
        )
        _write_audit(audit_path, result)
        return result
    except Exception as deployment_error:
        rollback_error: Optional[str] = None
        try:
            if old_env is None:
                _atomic_write(
                    paths.release_env,
                    f"POLYMARKET_RELEASE_SHA={previous_sha}\n".encode("utf-8"),
                    mode=0o600,
                )
            else:
                _atomic_write(paths.release_env, old_env, mode=0o600)
            _atomic_symlink(paths.current_link, paths.release_root / previous_sha)
            runner.run(("sudo", "-n", "systemctl", "restart", SERVICE_NAME))
            _require_service_active(runner)
            _wait_for_engine_state(paths, previous_sha)
        except Exception as exc:
            rollback_error = str(exc)
        result = dict(
            base_audit,
            status="failed",
            completed_at=_iso_now(),
            error=str(deployment_error),
            rollback_error=rollback_error,
        )
        _write_audit(audit_path, result)
        if rollback_error:
            raise DeploymentError(
                f"deployment failed ({deployment_error}); rollback also failed "
                f"({rollback_error})"
            ) from deployment_error
        raise DeploymentError(
            f"deployment failed and previous release was restored: {deployment_error}"
        ) from deployment_error


def execute(
    request: DeploymentRequest,
    *,
    paths: DeploymentPaths = DeploymentPaths(),
    runner: Optional[CommandRunner] = None,
    tests: Sequence[str] = RELEASE_TESTS,
) -> Dict[str, Any]:
    normalized = _validate_request(request)
    commands = runner or CommandRunner()

    with deployment_lock(paths.lock_file):
        current_sha = _current_release_sha(paths)
        if current_sha != normalized.expected_current_sha:
            raise DeploymentError(
                f"current release mismatch: expected "
                f"{normalized.expected_current_sha}, found {current_sha}"
            )

        if normalized.action == "rollback":
            release_dir = paths.release_root / normalized.target_sha
            _verify_release_manifest(release_dir, normalized.target_sha)
            _run_release_checks(
                paths,
                commands,
                release_dir,
                normalized.target_sha,
                tests,
            )
        else:
            _verify_or_promote_candidate(
                paths,
                commands,
                normalized.target_sha,
                promote=normalized.action in {"prepare", "activate"},
            )
            release_dir = paths.release_root / normalized.target_sha

        plan = {
            "ok": True,
            "action": normalized.action,
            "source_repository": SOURCE_REPOSITORY,
            "target_sha": normalized.target_sha,
            "current_sha": current_sha,
            "service": SERVICE_NAME,
            "lock_file": str(paths.lock_file),
            "will_restart": normalized.action in {"activate", "rollback"},
        }
        if normalized.action == "plan":
            return plan

        if normalized.action != "rollback":
            release_dir = prepare_release(
                paths,
                commands,
                normalized.target_sha,
                tests=tests,
            )
        plan["release_dir"] = str(release_dir)
        if normalized.action == "prepare":
            plan["prepared"] = True
            return plan
        return _activate_release(paths, commands, normalized, release_dir)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build and optionally activate one exact Polymarket maker release "
            "under the VPS1 global deployment lock."
        )
    )
    parser.add_argument("action", choices=("plan", "prepare", "activate", "rollback"))
    parser.add_argument("target_sha")
    parser.add_argument(
        "--expected-current",
        required=True,
        dest="expected_current_sha",
        help="Full SHA currently expected behind /home/ubuntu/polymarket-releases/current.",
    )
    parser.add_argument(
        "--confirm",
        default="",
        help="Exact ACTION:TARGET_SHA confirmation; required except for plan.",
    )
    parser.add_argument(
        "--authorization-id",
        default="",
        help="User authorization/audit identifier; required for activate or rollback.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    request = DeploymentRequest(
        action=args.action,
        target_sha=args.target_sha,
        expected_current_sha=args.expected_current_sha,
        confirm=args.confirm,
        authorization_id=args.authorization_id,
    )
    try:
        result = execute(request)
    except (DeploymentError, subprocess.CalledProcessError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
