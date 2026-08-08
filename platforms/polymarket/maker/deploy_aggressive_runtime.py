"""Exact-SHA bootstrap and paused activation for the isolated aggressive LP.

This tool never reads a private key. Public account routing and market inputs
live below the aggressive runtime root; signer authentication stays in the
host-local runtime environment and on the dedicated Mac mini signer.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence

try:
    from .account_roster import (
        local_runtime_accounts,
        market_universe_sha256,
        parse_runtime_roster,
        roster_hosts,
        routing_roster_sha256,
        runtime_roster_scope,
    )
    from .deploy_release import (
        CommandRunner,
        DeploymentError,
        DeploymentPaths,
        _atomic_symlink,
        _atomic_write,
        _load_json,
        _parse_timestamp,
        _require_full_sha,
        _run_release_checks,
        _verify_or_promote_candidate,
        _verify_release_manifest,
        deployment_lock,
        prepare_release,
    )
    from .market_universe import apply_market_universe, load_json_object
except ImportError:  # pragma: no cover - direct script execution
    from account_roster import (
        local_runtime_accounts,
        market_universe_sha256,
        parse_runtime_roster,
        roster_hosts,
        routing_roster_sha256,
        runtime_roster_scope,
    )
    from deploy_release import (
        CommandRunner,
        DeploymentError,
        DeploymentPaths,
        _atomic_symlink,
        _atomic_write,
        _load_json,
        _parse_timestamp,
        _require_full_sha,
        _run_release_checks,
        _verify_or_promote_candidate,
        _verify_release_manifest,
        deployment_lock,
        prepare_release,
    )
    from market_universe import apply_market_universe, load_json_object


SOURCE_REPOSITORY = "ejson8282/polymarket-bot"
SERVICE_NAME = "polymarket-aggressive-engine.service"
REDIS_SERVICE_NAME = "polymarket-aggressive-redis.service"
PROFILES = {
    "aggressive-a": "vps1-production-deploy.lock",
    "aggressive-b": "vps2-production-deploy.lock",
}
RELEASE_TESTS = (
    "tests/test_polymarket_release_guard.py",
    "tests/test_polymarket_account_profiles.py",
    "tests/test_polymarket_account_roster.py",
    "tests/test_polymarket_multi_runner.py",
    "tests/test_polymarket_aggressive_guardrails.py",
    "tests/test_polymarket_aggressive_deploy.py",
)
REQUIRED_ENV_KEYS = (
    "POLYMARKET_HOST_ID",
    "POLYMARKET_EXPECTED_SIGNER_URL",
    "POLY_SIGNER_SERVER_URL",
    "SIGNER_TOKEN",
    "POLY_REDIS_URL",
    "POLYMARKET_EXPECTED_ROSTER_SHA256",
    "POLYMARKET_EXPECTED_MARKET_SHA256",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_AUTHORIZATION_RE = re.compile(r"^[A-Za-z0-9._:-]{3,120}$")


@dataclass(frozen=True)
class AggressivePaths:
    profile_name: str
    bare_repo: Path = Path("/home/ubuntu/repos/polymarket-bot.git")
    release_root: Path = Path("/home/ubuntu/polymarket-aggressive-releases")
    current_link: Path = Path("/home/ubuntu/polymarket-aggressive-releases/current")
    runtime_root: Path = Path("/home/ubuntu/polymarket-aggressive-runtime")
    python: Path = Path("/home/ubuntu/polymarket-aggressive-venv/bin/python")
    unit_file: Path = Path(
        "/etc/systemd/system/polymarket-aggressive-engine.service"
    )
    redis_unit_file: Path = Path(
        "/etc/systemd/system/polymarket-aggressive-redis.service"
    )
    lock_root: Path = Path("/home/ubuntu/latitude-runtime/locks")

    @property
    def lock_file(self) -> Path:
        return self.lock_root / PROFILES[self.profile_name]

    @property
    def release_env(self) -> Path:
        return self.runtime_root / "env" / "release.env"

    @property
    def runtime_env(self) -> Path:
        return self.runtime_root / "env" / "runtime.env"

    @property
    def roster(self) -> Path:
        return self.runtime_root / "accounts.runtime.json"

    @property
    def market_universe(self) -> Path:
        return self.runtime_root / "markets.runtime.json"

    @property
    def config_dir(self) -> Path:
        return self.runtime_root / "platforms" / "polymarket" / "maker"

    @property
    def data_dir(self) -> Path:
        return self.runtime_root / "data"

    @property
    def redis_dir(self) -> Path:
        return self.runtime_root / "redis"

    @property
    def audit_root(self) -> Path:
        return self.runtime_root / "deployments"

    def release_paths(self, *, build_python: Optional[Path] = None) -> DeploymentPaths:
        return DeploymentPaths(
            bare_repo=self.bare_repo,
            release_root=self.release_root,
            current_link=self.current_link,
            runtime_root=self.runtime_root,
            release_env=self.release_env,
            lock_file=self.lock_file,
            drop_in=self.unit_file,
            pause_flag=self.data_dir / ".account_1.paused",
            state_file=self.data_dir / "engine_state_1.json",
            runtime_config=self.config_dir / "config_1.json",
            python=build_python or self.python,
            profile_name="vps1",
            account_index=1,
        )


@dataclass(frozen=True)
class AggressiveRequest:
    action: str
    target_sha: str
    expected_current: str
    profile_name: str
    confirm: str = ""
    authorization_id: str = ""


@dataclass(frozen=True)
class ServiceState:
    active: bool
    enabled: bool


def aggressive_paths_for_profile(profile_name: str) -> AggressivePaths:
    profile = str(profile_name or "").strip().lower()
    if profile not in PROFILES:
        raise DeploymentError(f"unsupported aggressive profile: {profile}")
    return AggressivePaths(profile_name=profile)


def _validate_paths(paths: AggressivePaths) -> None:
    if paths.profile_name not in PROFILES:
        raise DeploymentError("aggressive deployment profile is invalid")
    required_aggressive = (
        paths.release_root,
        paths.current_link,
        paths.runtime_root,
        paths.python,
        paths.unit_file,
        paths.redis_unit_file,
    )
    if any("polymarket-aggressive" not in str(path) for path in required_aggressive):
        raise DeploymentError("aggressive deployment path escaped its isolated domain")
    forbidden = {
        Path("/home/ubuntu/polymarket-runtime"),
        Path("/home/ubuntu/polymarket-releases"),
        Path("/home/ubuntu/polymarket-bot"),
        Path("/home/ubuntu/.venv2"),
    }
    if any(path in forbidden for path in required_aggressive):
        raise DeploymentError("aggressive deployment reuses a normal LP path")
    if paths.lock_file.name != PROFILES[paths.profile_name]:
        raise DeploymentError("aggressive deployment lock does not match its VPS")


def _normalize_request(request: AggressiveRequest) -> AggressiveRequest:
    action = str(request.action or "").strip().lower()
    if action not in {"plan", "prepare", "activate"}:
        raise DeploymentError(f"unsupported aggressive deployment action: {action}")
    target = _require_full_sha(request.target_sha, "target SHA")
    expected_raw = str(request.expected_current or "").strip().lower()
    expected = (
        "none"
        if expected_raw == "none"
        else _require_full_sha(expected_raw, "expected current SHA")
    )
    profile = str(request.profile_name or "").strip().lower()
    if profile not in PROFILES:
        raise DeploymentError(f"unsupported aggressive profile: {profile}")
    if action != "plan":
        required = f"{action.upper()}-AGGRESSIVE:{profile}:{target}"
        if request.confirm != required:
            raise DeploymentError(f"confirmation must exactly equal {required}")
    authorization = str(request.authorization_id or "").strip()
    if action == "activate" and not authorization:
        raise DeploymentError("activate requires an explicit authorization ID")
    if authorization and not _AUTHORIZATION_RE.fullmatch(authorization):
        raise DeploymentError("authorization ID contains unsupported characters")
    return AggressiveRequest(
        action=action,
        target_sha=target,
        expected_current=expected,
        profile_name=profile,
        confirm=request.confirm,
        authorization_id=authorization,
    )


def _current_release(paths: AggressivePaths) -> str:
    if not paths.current_link.exists() and not paths.current_link.is_symlink():
        return "none"
    if not paths.current_link.is_symlink():
        raise DeploymentError("aggressive current path exists but is not a symlink")
    try:
        resolved = paths.current_link.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DeploymentError("aggressive current release link is broken") from exc
    if resolved.parent != paths.release_root.resolve():
        raise DeploymentError("aggressive current release escapes release root")
    return _require_full_sha(resolved.name, "aggressive current release")


def _parse_env_file(path: Path) -> Dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise DeploymentError(f"aggressive runtime env unavailable: {path}") from exc
    values: Dict[str, str] = {}
    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise DeploymentError(f"runtime env line {line_number} is malformed")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise DeploymentError(f"runtime env line {line_number} has invalid key")
        if key in values:
            raise DeploymentError(f"runtime env repeats {key}")
        values[key] = value
    missing = [key for key in REQUIRED_ENV_KEYS if not values.get(key)]
    if missing:
        raise DeploymentError("aggressive runtime env is missing: " + ", ".join(missing))
    return values


def _runtime_contract(paths: AggressivePaths, release_dir: Path) -> Dict[str, Any]:
    env = _parse_env_file(paths.runtime_env)
    if env["POLYMARKET_HOST_ID"].strip().lower() != paths.profile_name:
        raise DeploymentError("runtime env host does not match aggressive profile")
    expected_signer = env["POLYMARKET_EXPECTED_SIGNER_URL"].rstrip("/")
    actual_signer = env["POLY_SIGNER_SERVER_URL"].rstrip("/")
    if expected_signer != actual_signer:
        raise DeploymentError("aggressive signer URL variables do not match")
    if re.search(r":8420(?:/|$)", expected_signer):
        raise DeploymentError("aggressive runtime must not reuse normal signer port 8420")
    if env["POLY_REDIS_URL"].rstrip("/") != "redis://127.0.0.1:6380/0":
        raise DeploymentError("aggressive runtime must use isolated Redis on 127.0.0.1:6380")

    roster_payload = _load_json(paths.roster, "aggressive roster")
    if runtime_roster_scope(roster_payload) != "aggressive":
        raise DeploymentError("aggressive roster must declare runtime_scope=aggressive")
    accounts = parse_runtime_roster(roster_payload)
    if any(not host.startswith("aggressive-") for host in roster_hosts(accounts)):
        raise DeploymentError("aggressive roster contains a normal LP host")
    local_accounts = local_runtime_accounts(accounts, paths.profile_name)
    if not local_accounts:
        raise DeploymentError(f"roster has no account for {paths.profile_name}")
    if any(
        not account.profile.managed or account.profile.profile_type != "aggressive"
        for account in accounts
        if account.enabled
    ):
        raise DeploymentError("aggressive roster contains a non-aggressive profile")

    roster_sha = routing_roster_sha256(accounts, "aggressive")
    expected_roster = env["POLYMARKET_EXPECTED_ROSTER_SHA256"].lower()
    if not _SHA256_RE.fullmatch(expected_roster) or expected_roster != roster_sha:
        raise DeploymentError("reviewed aggressive roster digest does not match")

    base = _load_json(
        release_dir / "platforms" / "polymarket" / "maker" / "config.json",
        "aggressive base config",
    )
    market_payload = load_json_object(paths.market_universe)
    rendered_base = apply_market_universe(base, market_payload)
    market_sha = market_universe_sha256(rendered_base)
    expected_market = env["POLYMARKET_EXPECTED_MARKET_SHA256"].lower()
    if not _SHA256_RE.fullmatch(expected_market) or expected_market != market_sha:
        raise DeploymentError("reviewed aggressive market digest does not match")

    return {
        "env": env,
        "accounts": accounts,
        "local_accounts": local_accounts,
        "signer_url": expected_signer,
        "roster_sha256": roster_sha,
        "market_sha256": market_sha,
    }


def _unit_contract(paths: AggressivePaths, release_dir: Path) -> None:
    engine_unit = (
        release_dir / "deploy" / "systemd" / f"{SERVICE_NAME}.example"
    ).read_text(encoding="utf-8")
    redis_unit = (
        release_dir / "deploy" / "systemd" / f"{REDIS_SERVICE_NAME}.example"
    ).read_text(encoding="utf-8")
    required = (
        "Environment=POLYMARKET_REQUIRE_RELEASE=1",
        "Environment=POLYMARKET_RUNTIME_SCOPE=aggressive",
        str(paths.current_link / "platforms/polymarket/maker/release_guard.py"),
        str(paths.current_link / "platforms/polymarket/maker/engine.py"),
        str(paths.current_link / "platforms/polymarket/maker/multi_runner.py"),
        str(paths.runtime_root),
        "--runtime-scope aggressive",
        "--require-paused",
        f"Requires={REDIS_SERVICE_NAME}",
    )
    missing = [item for item in required if item not in engine_unit]
    if missing:
        raise DeploymentError("aggressive engine unit is incomplete: " + ", ".join(missing))
    if "--port 6380" not in redis_unit or str(paths.redis_dir) not in redis_unit:
        raise DeploymentError("aggressive Redis unit is incomplete")
    forbidden = (
        "/home/ubuntu/polymarket-runtime",
        "/home/ubuntu/polymarket-releases",
        "/home/ubuntu/polymarket-bot",
        "/home/ubuntu/.venv2",
        ":8420",
        "--port 6379",
    )
    combined = engine_unit + "\n" + redis_unit
    present = [item for item in forbidden if item in combined]
    if present:
        raise DeploymentError("aggressive units reuse normal LP inputs: " + ", ".join(present))


def _tcp_ready(host: str, port: int, timeout_seconds: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _generate_configs(
    paths: AggressivePaths,
    runner: CommandRunner,
    release_dir: Path,
    contract: Mapping[str, Any],
) -> Path:
    staging = paths.runtime_root / f".configs-{os.getpid()}"
    if staging.exists():
        raise DeploymentError(f"stale aggressive config staging exists: {staging}")
    staging.mkdir(mode=0o700, parents=True)
    try:
        runner.run(
            (
                str(paths.python),
                str(release_dir / "scripts" / "generate_configs.py"),
                "--roster",
                str(paths.roster),
                "--base",
                str(release_dir / "platforms/polymarket/maker/config.json"),
                "--market-universe",
                str(paths.market_universe),
                "--out-dir",
                str(staging),
                "--host-id",
                paths.profile_name,
                "--signer-url",
                str(contract["signer_url"]),
            )
        )
        expected = {
            f"config_{account.account_index}.json"
            for account in contract["local_accounts"]
        }
        actual = {path.name for path in staging.glob("config_*.json")}
        if actual != expected:
            raise DeploymentError(
                f"generated config set mismatch: expected {sorted(expected)}, found {sorted(actual)}"
            )
        return staging
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _validation_env(contract: Mapping[str, Any], target_sha: str) -> Dict[str, str]:
    env = dict(os.environ)
    env.update(contract["env"])
    env.update(
        {
            "POLYMARKET_REQUIRE_RELEASE": "1",
            "POLYMARKET_RELEASE_SHA": target_sha,
            "POLYMARKET_RUNTIME_SCOPE": "aggressive",
            "POLYMARKET_RUNTIME_ROOT": "/home/ubuntu/polymarket-aggressive-runtime",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def _validate_runtime(
    paths: AggressivePaths,
    runner: CommandRunner,
    release_dir: Path,
    config_dir: Path,
    contract: Mapping[str, Any],
    target_sha: str,
) -> None:
    runner.run(
        (
            str(paths.python),
            "-c",
            "import httpx, redis, requests, py_clob_client_v2",
        )
    )
    for account in contract["local_accounts"]:
        if not _tcp_ready("127.0.0.1", account.clash_port):
            raise DeploymentError(
                f"aggressive proxy port is unavailable: 127.0.0.1:{account.clash_port}"
            )
        (paths.data_dir / f".account_{account.account_index}.paused").touch()
    signer_host = str(contract["signer_url"]).split("//", 1)[-1].split("/", 1)[0]
    if signer_host.endswith(":8420"):
        raise DeploymentError("aggressive signer unexpectedly points to normal port")
    runner.run(
        (
            str(paths.python),
            str(release_dir / "platforms/polymarket/maker/multi_runner.py"),
            "--config-dir",
            str(config_dir),
            "--roster",
            str(paths.roster),
            "--host-id",
            paths.profile_name,
            "--data-dir",
            str(paths.data_dir),
            "--runtime-scope",
            "aggressive",
            "--runtime-root",
            str(paths.runtime_root),
            "--expected-signer-url",
            str(contract["signer_url"]),
            "--require-paused",
            "--validate-only",
            "--expected-roster-sha256",
            str(contract["roster_sha256"]),
            "--expected-market-sha256",
            str(contract["market_sha256"]),
        ),
        env=_validation_env(contract, target_sha),
    )


def _service_snapshot(runner: CommandRunner, service: str) -> str:
    try:
        return runner.run(
            (
                "systemctl",
                "show",
                service,
                "--property=LoadState,ActiveState,SubState,MainPID,ActiveEnterTimestampMonotonic",
                "--value",
            )
        )
    except subprocess.CalledProcessError:
        return "unavailable"


def _service_is_active(runner: CommandRunner, service: str) -> bool:
    try:
        return runner.run(("systemctl", "is-active", service)) == "active"
    except subprocess.CalledProcessError:
        return False


def _service_is_enabled(runner: CommandRunner, service: str) -> bool:
    try:
        return runner.run(("systemctl", "is-enabled", service)) == "enabled"
    except subprocess.CalledProcessError:
        return False


def _service_state(runner: CommandRunner, service: str) -> ServiceState:
    return ServiceState(
        active=_service_is_active(runner, service),
        enabled=_service_is_enabled(runner, service),
    )


def _restore_optional_unit(
    runner: CommandRunner,
    *,
    destination: Path,
    previous: Optional[bytes],
    staging_root: Path,
) -> None:
    if previous is None:
        runner.run(("sudo", "-n", "rm", "-f", str(destination)))
        return
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    staged = staging_root / f".{destination.name}.rollback-{os.getpid()}"
    _atomic_write(staged, previous, mode=0o600)
    try:
        runner.run(
            (
                "sudo",
                "-n",
                "install",
                "-m",
                "0644",
                str(staged),
                str(destination),
            )
        )
    finally:
        staged.unlink(missing_ok=True)


def _restore_service_state(
    runner: CommandRunner,
    service: str,
    state: ServiceState,
) -> None:
    if state.enabled:
        runner.run(("sudo", "-n", "systemctl", "enable", service))
    else:
        try:
            runner.run(("sudo", "-n", "systemctl", "disable", service))
        except subprocess.CalledProcessError:
            # A first bootstrap has no old unit left to disable.
            pass
    if state.active:
        runner.run(("sudo", "-n", "systemctl", "start", service))


def _validate_host_dependencies(
    paths: AggressivePaths,
    *,
    redis_server: Path = Path("/usr/bin/redis-server"),
) -> None:
    if not paths.python.is_file() or not os.access(paths.python, os.X_OK):
        raise DeploymentError(f"dedicated aggressive Python is missing: {paths.python}")
    if not redis_server.is_file() or not os.access(redis_server, os.X_OK):
        raise DeploymentError(
            f"dedicated aggressive Redis dependency is missing: {redis_server}"
        )


def _wait_for_paused_states(
    paths: AggressivePaths,
    runner: CommandRunner,
    contract: Mapping[str, Any],
    target_sha: str,
    *,
    observed_after: datetime,
    timeout_seconds: float = 45.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    minimum = observed_after.astimezone(timezone.utc) - timedelta(seconds=1)
    last_error = "state unavailable"
    while time.monotonic() < deadline:
        if not _service_is_active(runner, SERVICE_NAME):
            last_error = "aggressive engine became inactive"
            time.sleep(0.5)
            continue
        states: Dict[str, Any] = {}
        try:
            for account in contract["local_accounts"]:
                state = _load_json(
                    paths.data_dir / f"engine_state_{account.account_index}.json",
                    f"aggressive engine state {account.account_index}",
                )
                runtime = state.get("runtime")
                if not isinstance(runtime, dict):
                    raise DeploymentError("aggressive engine state runtime is missing")
                if (
                    state.get("release_sha") != target_sha
                    or state.get("release_required") is not True
                    or state.get("paused") is not True
                    or runtime.get("scope") != "aggressive"
                    or runtime.get("host_id") != paths.profile_name
                    or runtime.get("routing_roster_sha256") != contract["roster_sha256"]
                    or runtime.get("market_universe_sha256") != contract["market_sha256"]
                    or _parse_timestamp(state.get("ts")) < minimum
                ):
                    raise DeploymentError("aggressive engine state has not confirmed paused runtime")
                states[str(account.account_index)] = {
                    "paused": True,
                    "release_sha": target_sha,
                    "quotes_sent": state.get("quotes_sent"),
                    "fills_seen": state.get("fills_seen"),
                    "ts": state.get("ts"),
                }
            return states
        except DeploymentError as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise DeploymentError(f"paused aggressive runtime verification timed out: {last_error}")


def _write_audit(paths: AggressivePaths, target_sha: str, payload: Mapping[str, Any]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    audit = paths.audit_root / f"{stamp}-{target_sha[:8]}"
    audit.mkdir(parents=True, exist_ok=False)
    _atomic_write(
        audit / "result.json",
        (json.dumps(dict(payload), indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )
    return audit


def _activate(
    paths: AggressivePaths,
    runner: CommandRunner,
    request: AggressiveRequest,
    release_dir: Path,
) -> Dict[str, Any]:
    _validate_host_dependencies(paths)
    _unit_contract(paths, release_dir)
    contract = _runtime_contract(paths, release_dir)
    paths.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    paths.redis_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging = _generate_configs(paths, runner, release_dir, contract)
    try:
        _validate_runtime(paths, runner, release_dir, staging, contract, request.target_sha)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    service_before = _service_snapshot(runner, "polymarket-engine.service")
    aggressive_service_before = _service_state(runner, SERVICE_NAME)
    redis_service_before = _service_state(runner, REDIS_SERVICE_NAME)
    old_engine_unit = paths.unit_file.read_bytes() if paths.unit_file.exists() else None
    old_redis_unit = (
        paths.redis_unit_file.read_bytes() if paths.redis_unit_file.exists() else None
    )
    prior_config = paths.config_dir.with_name(f"maker.previous-{os.getpid()}")
    config_existed = paths.config_dir.exists()
    prior_current = _current_release(paths)
    old_release_env = paths.release_env.read_bytes() if paths.release_env.exists() else None
    activated = False
    try:
        paths.config_dir.parent.mkdir(parents=True, exist_ok=True)
        if paths.config_dir.exists():
            os.replace(str(paths.config_dir), str(prior_config))
        os.replace(str(staging), str(paths.config_dir))
        _atomic_write(
            paths.release_env,
            f"POLYMARKET_RELEASE_SHA={request.target_sha}\n".encode("utf-8"),
            mode=0o600,
        )
        _atomic_symlink(paths.current_link, release_dir)
        runner.run(
            (
                "sudo",
                "-n",
                "install",
                "-m",
                "0644",
                str(release_dir / "deploy/systemd" / f"{REDIS_SERVICE_NAME}.example"),
                str(paths.redis_unit_file),
            )
        )
        runner.run(
            (
                "sudo",
                "-n",
                "install",
                "-m",
                "0644",
                str(release_dir / "deploy/systemd" / f"{SERVICE_NAME}.example"),
                str(paths.unit_file),
            )
        )
        runner.run(("sudo", "-n", "systemctl", "daemon-reload"))
        runner.run(("sudo", "-n", "systemctl", "enable", "--now", REDIS_SERVICE_NAME))
        if not _tcp_ready("127.0.0.1", 6380, timeout_seconds=5.0):
            raise DeploymentError("isolated aggressive Redis did not open port 6380")
        started_at = datetime.now(timezone.utc)
        action = "restart" if _service_is_active(runner, SERVICE_NAME) else "start"
        runner.run(("sudo", "-n", "systemctl", action, SERVICE_NAME))
        states = _wait_for_paused_states(
            paths,
            runner,
            contract,
            request.target_sha,
            observed_after=started_at,
        )
        service_after = _service_snapshot(runner, "polymarket-engine.service")
        if service_before != service_after:
            raise DeploymentError("normal Polymarket service changed during aggressive activation")
        activated = True
        result = {
            "ok": True,
            "action": "activate",
            "source_repository": SOURCE_REPOSITORY,
            "profile_name": paths.profile_name,
            "target_sha": request.target_sha,
            "rollback_sha": prior_current,
            "service": SERVICE_NAME,
            "redis_service": REDIS_SERVICE_NAME,
            "authorization_id": request.authorization_id,
            "paused": True,
            "local_accounts": [
                account.account_index for account in contract["local_accounts"]
            ],
            "roster_sha256": contract["roster_sha256"],
            "market_sha256": contract["market_sha256"],
            "states": states,
        }
        audit = _write_audit(paths, request.target_sha, result)
        result["audit"] = str(audit)
        return result
    except Exception as exc:
        rollback_errors: List[str] = []
        try:
            runner.run(("sudo", "-n", "systemctl", "stop", SERVICE_NAME))
        except Exception as rollback_exc:
            rollback_errors.append(f"stop engine: {rollback_exc}")
        try:
            runner.run(("sudo", "-n", "systemctl", "stop", REDIS_SERVICE_NAME))
        except Exception as rollback_exc:
            rollback_errors.append(f"stop redis: {rollback_exc}")
        try:
            if prior_current != "none":
                _atomic_symlink(paths.current_link, paths.release_root / prior_current)
            else:
                paths.current_link.unlink(missing_ok=True)
            if old_release_env is not None:
                _atomic_write(paths.release_env, old_release_env, mode=0o600)
            else:
                paths.release_env.unlink(missing_ok=True)
            if paths.config_dir.exists():
                shutil.rmtree(paths.config_dir)
            if prior_config.exists():
                os.replace(str(prior_config), str(paths.config_dir))
            elif config_existed:
                raise DeploymentError("previous aggressive config backup is missing")
        except Exception as rollback_exc:
            rollback_errors.append(f"restore runtime inputs: {rollback_exc}")
        try:
            _restore_optional_unit(
                runner,
                destination=paths.redis_unit_file,
                previous=old_redis_unit,
                staging_root=paths.runtime_root,
            )
            _restore_optional_unit(
                runner,
                destination=paths.unit_file,
                previous=old_engine_unit,
                staging_root=paths.runtime_root,
            )
            runner.run(("sudo", "-n", "systemctl", "daemon-reload"))
            _restore_service_state(
                runner,
                REDIS_SERVICE_NAME,
                redis_service_before,
            )
            _restore_service_state(
                runner,
                SERVICE_NAME,
                aggressive_service_before,
            )
        except Exception as rollback_exc:
            rollback_errors.append(f"restore units/services: {rollback_exc}")
        details = (
            "; rollback incomplete: " + " | ".join(rollback_errors)
            if rollback_errors
            else ""
        )
        raise DeploymentError(
            f"aggressive activation failed and rollback was attempted: {exc}{details}"
        ) from exc
    finally:
        if activated and prior_config.exists():
            shutil.rmtree(prior_config)
        if staging.exists():
            shutil.rmtree(staging)


def execute(
    request: AggressiveRequest,
    *,
    paths: Optional[AggressivePaths] = None,
    runner: Optional[CommandRunner] = None,
    tests: Sequence[str] = RELEASE_TESTS,
) -> Dict[str, Any]:
    normalized = _normalize_request(request)
    selected = paths or aggressive_paths_for_profile(normalized.profile_name)
    _validate_paths(selected)
    if selected.profile_name != normalized.profile_name:
        raise DeploymentError("aggressive request profile does not match paths")
    commands = runner or CommandRunner()

    with deployment_lock(selected.lock_file):
        current = _current_release(selected)
        if current != normalized.expected_current:
            raise DeploymentError(
                f"aggressive current release mismatch: expected {normalized.expected_current}, found {current}"
            )
        release_paths = selected.release_paths(build_python=Path(sys.executable))
        _verify_or_promote_candidate(
            release_paths,
            commands,
            normalized.target_sha,
            promote=normalized.action in {"prepare", "activate"},
        )
        plan: Dict[str, Any] = {
            "ok": True,
            "action": normalized.action,
            "source_repository": SOURCE_REPOSITORY,
            "profile_name": selected.profile_name,
            "target_sha": normalized.target_sha,
            "current_sha": current,
            "service": SERVICE_NAME,
            "redis_service": REDIS_SERVICE_NAME,
            "lock_file": str(selected.lock_file),
            "will_touch_normal_service": False,
            "will_start_orders": False,
        }
        if normalized.action == "plan":
            return plan
        release_dir = prepare_release(
            release_paths,
            commands,
            normalized.target_sha,
            tests=tests,
        )
        _verify_release_manifest(release_dir, normalized.target_sha)
        _unit_contract(selected, release_dir)
        plan["release_dir"] = str(release_dir)
        if normalized.action == "prepare":
            plan["prepared"] = True
            return plan
        return _activate(selected, commands, normalized, release_dir)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build and activate the isolated aggressive LP in paused mode."
    )
    parser.add_argument("action", choices=("plan", "prepare", "activate"))
    parser.add_argument("target_sha")
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    parser.add_argument(
        "--expected-current",
        required=True,
        help="Full current aggressive SHA, or 'none' for the first bootstrap.",
    )
    parser.add_argument("--confirm", default="")
    parser.add_argument("--authorization-id", default="")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    request = AggressiveRequest(
        action=args.action,
        target_sha=args.target_sha,
        expected_current=args.expected_current,
        profile_name=args.profile,
        confirm=args.confirm,
        authorization_id=args.authorization_id,
    )
    try:
        result = execute(request)
    except (DeploymentError, subprocess.CalledProcessError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
