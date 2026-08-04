"""Exact-SHA Mac mini deployment for Predict.fun API and WS services."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence
import urllib.parse
import urllib.request

from platforms.predictfun.ws_relay import probe_relay


SOURCE_REPOSITORY = "ejson8282/polymarket-bot"
ARTIFACT = "predictfun-mac-services"
LABEL = "ai.codex.predictfun-ws-relay"
API_LABEL = "ai.codex.predictfun-api-proxy"
CONFIRMATION = "DEPLOY_PREDICTFUN_WS_RELAY"
FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
ARCHIVE_PATHS = (
    "platforms/__init__.py",
    "platforms/predictfun/__init__.py",
    "platforms/predictfun/deploy_ws_relay.py",
    "platforms/predictfun/ws_relay.py",
    "deploy/mac-mini/predictfun_api_proxy.py",
    "deploy/mac-mini/ai.codex.predictfun-api-proxy.plist",
    "deploy/mac-mini/ai.codex.predictfun-ws-relay.plist",
)


class RelayDeploymentError(RuntimeError):
    pass


@dataclass(frozen=True)
class RelayDeploymentPaths:
    bare_repo: Path = Path.home() / "repos/predictfun.git"
    release_root: Path = Path.home() / "predictfun-ws-releases"
    current_link: Path = Path.home() / "predictfun-ws-releases/current"
    runtime_root: Path = Path.home() / "predictfun-ws-runtime"
    lock_file: Path = Path.home() / "predictfun-ws-runtime/deploy.lock"
    launch_agent: Path = (
        Path.home()
        / "Library/LaunchAgents/ai.codex.predictfun-ws-relay.plist"
    )
    api_launch_agent: Path = (
        Path.home()
        / "Library/LaunchAgents/ai.codex.predictfun-api-proxy.plist"
    )
    secret_file: Path = Path.home() / ".macmini-secrets/predictfun.env"
    python: Path = (
        Path.home() / "dev/varia-decibel-farming/.venv/bin/python"
    )
    rest_proxy_url: str = "http://100.91.159.54:8791"
    relay_url: str = "ws://100.91.159.54:8792/ws"
    uid: int = 501


@dataclass(frozen=True)
class FileSnapshot:
    content: bytes | None
    mode: int = 0o644


class CommandRunner:
    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
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


def _full_sha(value: str, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not FULL_SHA_RE.fullmatch(normalized):
        raise RelayDeploymentError(
            f"{label} must be a full 40-character commit SHA"
        )
    return normalized


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, content: bytes, mode: int) -> None:
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
        os.replace(temporary, path)
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
        os.symlink(target, temporary)
        os.replace(temporary, link)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@contextmanager
def deployment_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RelayDeploymentError(
                "another Predict.fun WS relay deployment owns the lock"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _git(
    paths: RelayDeploymentPaths,
    runner: CommandRunner,
    *args: str,
) -> str:
    return runner.run(("git", f"--git-dir={paths.bare_repo}", *args))


def _require_exact_internal_main(
    paths: RelayDeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
) -> None:
    if not paths.bare_repo.is_dir():
        raise RelayDeploymentError(
            f"Predict.fun Mac mini bare repository missing: {paths.bare_repo}"
        )
    if _git(paths, runner, "rev-parse", "--is-bare-repository") != "true":
        raise RelayDeploymentError("Mac mini release source must be a bare repo")
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
        raise RelayDeploymentError(
            "Mac mini Predict.fun internal main must equal the requested SHA"
        )


def _safe_extract(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r") as archive:
        for member in archive.getmembers():
            relative = Path(member.name)
            if relative.is_absolute() or ".." in relative.parts:
                raise RelayDeploymentError(f"unsafe archive path: {member.name}")
            if member.issym() or member.islnk():
                raise RelayDeploymentError(
                    f"release archive contains a link: {member.name}"
                )
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise RelayDeploymentError(
                    f"unsupported archive member: {member.name}"
                )
            source = archive.extractfile(member)
            if source is None:
                raise RelayDeploymentError(
                    f"archive member unreadable: {member.name}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            with source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)


def _manifest_files(release: Path) -> list[Path]:
    return sorted(
        path
        for path in release.rglob("*")
        if path.is_file() and path.name != ".release-manifest.json"
    )


def _make_immutable(release: Path) -> None:
    for path in _manifest_files(release):
        path.chmod(0o444)
    for directory in sorted(
        (path for path in release.rglob("*") if path.is_dir()),
        key=lambda value: len(value.parts),
        reverse=True,
    ):
        directory.chmod(0o555)
    release.chmod(0o555)


def _remove_tree(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except OSError:
            pass
    path.chmod(0o700)
    shutil.rmtree(path)


def verify_release(release: Path, target_sha: str) -> dict[str, Any]:
    target_sha = _full_sha(target_sha, "target SHA")
    resolved = release.resolve(strict=True)
    if resolved.name != target_sha or resolved.is_symlink():
        raise RelayDeploymentError("relay release path does not match target SHA")
    try:
        manifest = json.loads(
            (resolved / ".release-manifest.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        raise RelayDeploymentError("relay release manifest unavailable") from exc
    if not isinstance(manifest, dict):
        raise RelayDeploymentError("relay release manifest must be an object")
    if manifest.get("source_repository") != SOURCE_REPOSITORY:
        raise RelayDeploymentError("relay release repository mismatch")
    if manifest.get("artifact") != ARTIFACT:
        raise RelayDeploymentError("relay release artifact mismatch")
    if manifest.get("commit") != target_sha:
        raise RelayDeploymentError("relay release commit mismatch")
    manifest_path = resolved / ".release-manifest.json"
    files = manifest.get("files")
    actual_files = {
        path.relative_to(resolved).as_posix()
        for path in _manifest_files(resolved)
    }
    if (
        not isinstance(files, dict)
        or set(files) != actual_files
        or actual_files != set(ARCHIVE_PATHS)
    ):
        raise RelayDeploymentError("relay release file set mismatch")
    for relative, expected_hash in files.items():
        path = resolved / str(relative)
        if path.stat().st_mode & 0o222:
            raise RelayDeploymentError(f"relay release file writable: {relative}")
        if _sha256(path) != str(expected_hash):
            raise RelayDeploymentError(f"relay release hash mismatch: {relative}")
    if manifest_path.stat().st_mode & 0o222:
        raise RelayDeploymentError("relay release manifest is writable")
    return manifest


def prepare_release(
    paths: RelayDeploymentPaths,
    runner: CommandRunner,
    target_sha: str,
) -> dict[str, Any]:
    target_sha = _full_sha(target_sha, "target SHA")
    _require_exact_internal_main(paths, runner, target_sha)
    if not paths.python.is_file():
        raise RelayDeploymentError(f"relay Python missing: {paths.python}")
    paths.release_root.mkdir(parents=True, exist_ok=True)
    release = paths.release_root / target_sha
    if release.exists():
        return {
            "status": "already_prepared",
            "manifest": verify_release(release, target_sha),
        }
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
                "-c",
                "import ast,pathlib; "
                "ast.parse(pathlib.Path('platforms/predictfun/ws_relay.py').read_text()); "
                "ast.parse(pathlib.Path('deploy/mac-mini/predictfun_api_proxy.py').read_text())",
            ),
            cwd=temporary,
        )
        files = {
            path.relative_to(temporary).as_posix(): _sha256(path)
            for path in _manifest_files(temporary)
        }
        (temporary / ".release-manifest.json").write_text(
            json.dumps(
                {
                    "source_repository": SOURCE_REPOSITORY,
                    "artifact": ARTIFACT,
                    "commit": target_sha,
                    "files": files,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (temporary / ".release-manifest.json").chmod(0o444)
        os.replace(temporary, release)
        promoted = True
        _make_immutable(release)
        return {
            "status": "prepared",
            "manifest": verify_release(release, target_sha),
        }
    except Exception:
        _remove_tree(release if promoted else temporary)
        raise


def _current_sha(paths: RelayDeploymentPaths) -> str | None:
    if not paths.current_link.exists() and not paths.current_link.is_symlink():
        return None
    if not paths.current_link.is_symlink():
        raise RelayDeploymentError("relay current path must be a symlink")
    resolved = paths.current_link.resolve(strict=True)
    if resolved.parent != paths.release_root.resolve():
        raise RelayDeploymentError("relay current link escapes release root")
    return _full_sha(resolved.name, "current SHA")


def _snapshot(path: Path) -> FileSnapshot:
    if not path.exists():
        return FileSnapshot(None)
    return FileSnapshot(path.read_bytes(), path.stat().st_mode & 0o777)


def _restore(path: Path, snapshot: FileSnapshot) -> None:
    if snapshot.content is None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return
    _atomic_write(path, snapshot.content, snapshot.mode)


def _validate_secret_file(path: Path) -> None:
    if not path.is_file():
        raise RelayDeploymentError(f"Predict.fun secret file missing: {path}")
    if path.stat().st_mode & 0o077:
        raise RelayDeploymentError(
            "Predict.fun secret file must not be group/world accessible"
        )


def discover_probe_market(rest_proxy_url: str, timeout_sec: float = 10.0) -> int:
    query = urllib.parse.urlencode(
        {"first": 1, "status": "OPEN", "hasActiveRewards": "true"}
    )
    request = urllib.request.Request(
        f"{rest_proxy_url.rstrip('/')}/v1/markets?{query}",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise RelayDeploymentError("no open reward market available for WS probe")
    try:
        market_id = int(rows[0].get("id") or 0)
    except (TypeError, ValueError) as exc:
        raise RelayDeploymentError("probe market ID invalid") from exc
    if market_id <= 0:
        raise RelayDeploymentError("probe market ID invalid")
    return market_id


def probe_api_proxy(rest_proxy_url: str, timeout_sec: float = 10.0) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{rest_proxy_url.rstrip('/')}/health",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RelayDeploymentError("Predict.fun API proxy health check failed")
    accounts = payload.get("accounts")
    accounts = accounts if isinstance(accounts, dict) else {}
    ready = int(accounts.get("ready") or 0)
    if ready <= 0:
        raise RelayDeploymentError("Predict.fun API proxy has no ready account")
    return {
        "ok": True,
        "project": payload.get("project"),
        "mode": payload.get("mode"),
        "release_sha": payload.get("release_sha"),
        "accounts_configured": int(accounts.get("configured") or 0),
        "accounts_ready": ready,
    }


def run_relay_probe(ws_url: str, market_id: int) -> dict[str, Any]:
    return asyncio.run(probe_relay(ws_url, market_id, timeout_sec=10.0))


def activate_release(
    paths: RelayDeploymentPaths,
    runner: CommandRunner,
    *,
    target_sha: str,
    expected_current: str,
    confirm: str,
    authorization_id: str,
    api_probe: Callable[[str], dict[str, Any]] = probe_api_proxy,
    discover_market: Callable[[str], int] = discover_probe_market,
    relay_probe: Callable[[str, int], dict[str, Any]] = run_relay_probe,
) -> dict[str, Any]:
    target_sha = _full_sha(target_sha, "target SHA")
    if confirm != CONFIRMATION:
        raise RelayDeploymentError(f"confirmation must equal {CONFIRMATION}")
    if not str(authorization_id or "").strip():
        raise RelayDeploymentError("authorization ID is required")
    expected = None if expected_current == "none" else _full_sha(
        expected_current,
        "expected current SHA",
    )
    previous_sha = _current_sha(paths)
    if previous_sha != expected:
        raise RelayDeploymentError(
            f"relay current changed: expected {expected}, found {previous_sha}"
        )
    release = paths.release_root / target_sha
    verify_release(release, target_sha)
    _validate_secret_file(paths.secret_file)
    runner.run(
        (
            str(paths.python),
            "-c",
            "import eth_account,predict_sdk,web3,websockets",
        )
    )

    plist_source = (
        release / "deploy/mac-mini/ai.codex.predictfun-ws-relay.plist"
    )
    api_plist_source = (
        release / "deploy/mac-mini/ai.codex.predictfun-api-proxy.plist"
    )
    plist_template = plist_source.read_text(encoding="utf-8")
    api_plist_template = api_plist_source.read_text(encoding="utf-8")
    template_values = {
        "__PREDICTFUN_PYTHON__": str(paths.python),
        "__PREDICTFUN_CURRENT__": str(paths.current_link),
        "__PREDICTFUN_HOME__": str(paths.launch_agent.parents[2]),
    }
    missing_placeholders = [
        key
        for key in template_values
        if key not in plist_template or key not in api_plist_template
    ]
    if missing_placeholders:
        raise RelayDeploymentError(
            f"relay launch agent template missing: {missing_placeholders}"
        )
    plist_text = plist_template
    api_plist_text = api_plist_template
    for placeholder, value in template_values.items():
        plist_text = plist_text.replace(placeholder, value)
        api_plist_text = api_plist_text.replace(placeholder, value)
    if "__PREDICTFUN_RELEASE_SHA__" not in api_plist_text:
        raise RelayDeploymentError(
            "API proxy launch agent is missing release SHA placeholder"
        )
    api_plist_text = api_plist_text.replace(
        "__PREDICTFUN_RELEASE_SHA__", target_sha
    )
    plist_content = plist_text.encode("utf-8")
    api_plist_content = api_plist_text.encode("utf-8")
    required = (
        LABEL,
        str(paths.python),
        str(paths.current_link / "platforms/predictfun/ws_relay.py"),
        "100.91.159.54",
        "8792",
    )
    missing = [value for value in required if value not in plist_text]
    if missing or "PREDICTFUN_API_KEY" in plist_text:
        raise RelayDeploymentError(
            f"relay launch agent is invalid: missing={missing}"
        )
    api_required = (
        API_LABEL,
        str(paths.python),
        str(
            paths.current_link / "deploy/mac-mini/predictfun_api_proxy.py"
        ),
        "100.91.159.54",
        "8791",
        target_sha,
    )
    api_missing = [value for value in api_required if value not in api_plist_text]
    if api_missing or "PREDICTFUN_API_KEY" in api_plist_text:
        raise RelayDeploymentError(
            f"API proxy launch agent is invalid: missing={api_missing}"
        )

    snapshot = _snapshot(paths.launch_agent)
    api_snapshot = _snapshot(paths.api_launch_agent)
    previous_target = (
        paths.current_link.resolve(strict=True)
        if previous_sha is not None
        else None
    )
    domain = f"gui/{paths.uid}"
    service = f"{domain}/{LABEL}"
    api_service = f"{domain}/{API_LABEL}"
    try:
        runner.run(("launchctl", "bootout", service), check=False)
        runner.run(("launchctl", "bootout", api_service), check=False)
        _atomic_symlink(paths.current_link, release)
        _atomic_write(paths.launch_agent, plist_content, 0o644)
        _atomic_write(paths.api_launch_agent, api_plist_content, 0o644)
        runner.run(("plutil", "-lint", str(paths.launch_agent)))
        runner.run(("plutil", "-lint", str(paths.api_launch_agent)))
        runner.run(
            ("launchctl", "bootstrap", domain, str(paths.api_launch_agent))
        )
        runner.run(("launchctl", "kickstart", "-k", api_service))
        api_service_state = ""
        for attempt in range(10):
            api_service_state = runner.run(("launchctl", "print", api_service))
            if "state = running" in api_service_state:
                break
            if attempt < 9:
                time.sleep(0.25)
        if "state = running" not in api_service_state:
            raise RelayDeploymentError("API proxy launch agent is not running")
        api_probe_result: dict[str, Any] | None = None
        api_probe_error = ""
        for attempt in range(10):
            try:
                api_probe_result = api_probe(paths.rest_proxy_url)
                break
            except Exception as exc:
                api_probe_error = f"{type(exc).__name__}: {exc}"
                if attempt < 9:
                    time.sleep(1)
        if (
            not isinstance(api_probe_result, dict)
            or api_probe_result.get("ok") is not True
            or api_probe_result.get("release_sha") != target_sha
        ):
            raise RelayDeploymentError(
                f"API proxy health probe failed: {api_probe_error}"
            )
        runner.run(("launchctl", "bootstrap", domain, str(paths.launch_agent)))
        runner.run(("launchctl", "kickstart", "-k", service))
        service_state = ""
        for attempt in range(10):
            service_state = runner.run(("launchctl", "print", service))
            if "state = running" in service_state:
                break
            if attempt < 9:
                time.sleep(0.25)
        if "state = running" not in service_state:
            raise RelayDeploymentError("relay launch agent is not running")

        market_id = discover_market(paths.rest_proxy_url)
        probe_result: dict[str, Any] | None = None
        last_error = ""
        for attempt in range(10):
            try:
                probe_result = relay_probe(paths.relay_url, market_id)
                break
            except Exception as exc:
                last_error = f"{exc.__class__.__name__}: {exc}"
                if attempt < 9:
                    time.sleep(1)
        if not isinstance(probe_result, dict) or probe_result.get("ok") is not True:
            raise RelayDeploymentError(
                f"relay public market subscription failed: {last_error}"
            )
        return {
            "status": "activated",
            "target_sha": target_sha,
            "previous_sha": previous_sha,
            "authorization_id": authorization_id,
            "service": LABEL,
            "api_service": API_LABEL,
            "api_probe": api_probe_result,
            "market_id": market_id,
            "probe": probe_result,
        }
    except Exception as exc:
        runner.run(("launchctl", "bootout", service), check=False)
        runner.run(("launchctl", "bootout", api_service), check=False)
        if previous_target is None:
            try:
                paths.current_link.unlink()
            except FileNotFoundError:
                pass
        else:
            _atomic_symlink(paths.current_link, previous_target)
        _restore(paths.launch_agent, snapshot)
        _restore(paths.api_launch_agent, api_snapshot)
        if api_snapshot.content is not None:
            runner.run(
                (
                    "launchctl",
                    "bootstrap",
                    domain,
                    str(paths.api_launch_agent),
                ),
                check=False,
            )
            runner.run(
                ("launchctl", "kickstart", "-k", api_service),
                check=False,
            )
        if snapshot.content is not None:
            runner.run(
                ("launchctl", "bootstrap", domain, str(paths.launch_agent)),
                check=False,
            )
            runner.run(
                ("launchctl", "kickstart", "-k", service),
                check=False,
            )
        raise RelayDeploymentError(
            f"relay activation failed; previous state restored: {exc}"
        ) from exc


def status(
    paths: RelayDeploymentPaths,
    runner: CommandRunner,
) -> dict[str, Any]:
    service = f"gui/{paths.uid}/{LABEL}"
    api_service = f"gui/{paths.uid}/{API_LABEL}"
    output = runner.run(("launchctl", "print", service), check=False)
    api_output = runner.run(
        ("launchctl", "print", api_service), check=False
    )
    return {
        "current_sha": _current_sha(paths),
        "service": LABEL,
        "running": "state = running" in output,
        "api_service": API_LABEL,
        "api_running": "state = running" in api_output,
        "secret_present": paths.secret_file.is_file(),
    }


def execute(
    action: str,
    *,
    paths: RelayDeploymentPaths,
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
    raise RelayDeploymentError(f"unsupported action: {action}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deploy exact Predict.fun API and WS services on Mac mini."
    )
    parser.add_argument("action", choices=("status", "prepare", "activate"))
    parser.add_argument("--target-sha", default="")
    parser.add_argument("--expected-current", default="none")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--authorization-id", default="")
    args = parser.parse_args(argv)
    result = execute(
        args.action,
        paths=RelayDeploymentPaths(),
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
