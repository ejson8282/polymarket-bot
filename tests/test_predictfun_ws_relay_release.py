from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Mapping, Optional, Sequence

import pytest

import platforms.predictfun.deploy_ws_relay as relay_deploy
from platforms.predictfun.deploy_ws_relay import (
    ARCHIVE_PATHS,
    CONFIRMATION,
    CommandRunner,
    RelayDeploymentError,
    RelayDeploymentPaths,
    activate_release,
    prepare_release,
    verify_release,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("fail_probe", [False, True])
def test_guarded_mac_activation_never_rewrites_protected_plists(tmp_path, monkeypatch, fail_probe):
    from types import SimpleNamespace
    paths, sha = _prepare(tmp_path)
    paths.current_link.symlink_to(paths.release_root / sha)
    for path in (paths.launch_agent, paths.api_launch_agent):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic protected binding")
    checks = []
    installed = SimpleNamespace(require_unchanged=lambda: checks.append(True))
    monkeypatch.setattr(relay_deploy.recovery_service_binding, "check_transition", lambda *a: installed)
    monkeypatch.setattr(relay_deploy.recovery_service_binding, "binding_files",
                        lambda p: {paths.launch_agent: b"", paths.api_launch_agent: b""})
    monkeypatch.setattr(relay_deploy, "_atomic_write", lambda *a: pytest.fail("protected plist overwrite"))
    monkeypatch.setattr(relay_deploy, "_restore", lambda *a: pytest.fail("protected plist restore"))
    runner = LaunchctlRunner()
    args = dict(target_sha=sha, expected_current=sha, confirm=CONFIRMATION,
                authorization_id="synthetic-guarded-test",
                api_probe=lambda url: {"ok": not fail_probe, "release_sha": sha},
                discover_market=lambda url: 10835, relay_probe=lambda *a: {"ok": True})
    if fail_probe:
        with pytest.raises(RelayDeploymentError):
            activate_release(paths, runner, **args)
    else:
        assert activate_release(paths, runner, **args)["status"] == "activated"
    assert checks
    assert ("launchctl", "enable", "gui/501/ai.codex.predictfun-api-proxy") in runner.calls
    assert all(p.read_bytes() == b"synthetic protected binding" for p in (paths.launch_agent, paths.api_launch_agent))


def test_guarded_mac_damage_refuses_rollback_restarts(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from platforms.predictfun.recovery_service_binding import BindingError
    paths, sha = _prepare(tmp_path)
    paths.current_link.symlink_to(paths.release_root / sha)
    damaged = False
    def recheck():
        if damaged:
            raise BindingError("synthetic removed anchor")
    def probe(url):
        nonlocal damaged
        damaged = True
        return {"ok": False}
    installed = SimpleNamespace(require_unchanged=recheck)
    monkeypatch.setattr(relay_deploy.recovery_service_binding, "check_transition", lambda *a: installed)
    monkeypatch.setattr(relay_deploy.recovery_service_binding, "binding_files",
                        lambda p: {paths.launch_agent: b"", paths.api_launch_agent: b""})
    monkeypatch.setattr(relay_deploy, "_restore", lambda *a: pytest.fail("must not restore after damage"))
    runner = LaunchctlRunner()
    with pytest.raises(BindingError):
        activate_release(paths, runner, target_sha=sha, expected_current=sha, confirm=CONFIRMATION,
                         authorization_id="synthetic-damage-test", api_probe=probe)
    assert runner.calls[-2:] == [("launchctl", "bootout", "gui/501/ai.codex.predictfun-ws-relay"),
                                ("launchctl", "bootout", "gui/501/ai.codex.predictfun-api-proxy")]


@pytest.fixture(autouse=True)
def restore_floor_permissions(tmp_path):
    yield
    directory = tmp_path / "home/predictfun-ws-runtime/recovery-release-floor"
    if directory.is_dir() and not directory.is_symlink():
        directory.chmod(0o700)


@pytest.mark.parametrize("damage", ["changed", "missing", "corrupt"])
def test_failed_mac_activation_never_restarts_old_release_after_floor_damage(tmp_path, damage):
    from platforms.predictfun.recovery_release_floor import RecoveryReleaseFloorError
    paths, sha = _prepare(tmp_path)
    old = "a" * 40
    previous = paths.release_root / old
    previous.mkdir()
    paths.current_link.symlink_to(previous)
    directory = paths.runtime_root / "recovery-release-floor"
    directory.mkdir(parents=True)
    policy_file = directory / "policy.json"
    policy = {"version": 1, "repository": "ejson8282/polymarket-bot", "profile": "macmini",
              "recovery_id": "c" * 64, "allowed_releases": sorted([old, sha])}
    policy_file.write_text(json.dumps(policy))
    policy_file.chmod(0o400)
    directory.chmod(0o500)
    def fail_probe(*args):
        directory.chmod(0o700)
        if damage == "missing":
            policy_file.unlink()
            directory.rmdir()
        else:
            policy_file.chmod(0o600)
            policy_file.write_text(json.dumps({**policy, "allowed_releases": [sha]}) if damage == "changed" else "{")
            policy_file.chmod(0o400)
            directory.chmod(0o500)
        return {"ok": False}
    runner = LaunchctlRunner()
    with pytest.raises(RecoveryReleaseFloorError):
        activate_release(paths, runner, target_sha=sha, expected_current=old,
                         confirm=CONFIRMATION, authorization_id="synthetic-floor-test",
                         api_probe=fail_probe)
    assert paths.current_link.resolve() == paths.release_root / sha
    assert runner.calls[-2:] == [
        ("launchctl", "bootout", "gui/501/ai.codex.predictfun-ws-relay"),
        ("launchctl", "bootout", "gui/501/ai.codex.predictfun-api-proxy")]


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=str(cwd),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def _paths(tmp_path: Path) -> tuple[RelayDeploymentPaths, str]:
    source = tmp_path / "source"
    for relative in ARCHIVE_PATHS:
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    _git(source, "init")
    _git(source, "config", "user.email", "tests@example.com")
    _git(source, "config", "user.name", "Predict Relay Tests")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "relay release fixture")
    _git(source, "branch", "-M", "main")
    sha = _git(source, "rev-parse", "HEAD")

    bare = tmp_path / "predictfun.git"
    _git(tmp_path, "clone", "--bare", str(source), str(bare))
    home = tmp_path / "home"
    paths = RelayDeploymentPaths(
        bare_repo=bare,
        release_root=home / "predictfun-ws-releases",
        current_link=home / "predictfun-ws-releases/current",
        runtime_root=home / "predictfun-ws-runtime",
        lock_file=home / "predictfun-ws-runtime/deploy.lock",
        launch_agent=(
            home / "Library/LaunchAgents/ai.codex.predictfun-ws-relay.plist"
        ),
        api_launch_agent=(
            home / "Library/LaunchAgents/ai.codex.predictfun-api-proxy.plist"
        ),
        secret_file=home / ".macmini-secrets/predictfun.env",
        python=Path(sys.executable),
        rest_proxy_url="http://127.0.0.1:8791",
        relay_url="ws://127.0.0.1:8792/ws",
        uid=501,
    )
    return paths, sha


class LaunchctlRunner(CommandRunner):
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = True,
    ) -> str:
        del cwd, env, check
        command = tuple(str(value) for value in args)
        self.calls.append(command)
        if command[:2] == ("launchctl", "print"):
            return "state = running"
        return ""


class DelayedLaunchctlRunner(LaunchctlRunner):
    def __init__(self, delayed_prints: int) -> None:
        super().__init__()
        self.delayed_prints = delayed_prints

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = True,
    ) -> str:
        command = tuple(str(value) for value in args)
        if command[:2] == ("launchctl", "print") and self.delayed_prints > 0:
            self.calls.append(command)
            self.delayed_prints -= 1
            return "state = waiting"
        return super().run(args, cwd=cwd, env=env, check=check)


def _prepare(tmp_path: Path) -> tuple[RelayDeploymentPaths, str]:
    paths, sha = _paths(tmp_path)
    prepare_release(paths, CommandRunner(), sha)
    paths.secret_file.parent.mkdir(parents=True)
    paths.secret_file.write_text("PREDICTFUN_API_KEY=fixture\n", encoding="utf-8")
    paths.secret_file.chmod(0o600)
    return paths, sha


def test_prepare_builds_minimal_immutable_mac_relay(tmp_path: Path) -> None:
    paths, sha = _paths(tmp_path)
    result = prepare_release(paths, CommandRunner(), sha)
    release = paths.release_root / sha

    assert result["status"] == "prepared"
    assert verify_release(release, sha)["artifact"] == "predictfun-mac-services"
    assert not (release / "platforms/polymarket").exists()
    assert set(ARCHIVE_PATHS) == {
        path.relative_to(release).as_posix()
        for path in release.rglob("*")
        if path.is_file() and path.name != ".release-manifest.json"
    }
    assert all(path.stat().st_mode & 0o222 == 0 for path in release.rglob("*"))


def test_prepare_promotes_before_making_release_immutable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, sha = _paths(tmp_path)
    expected_release = paths.release_root / sha
    immutable_paths: list[Path] = []
    original_make_immutable = relay_deploy._make_immutable

    def record_make_immutable(path: Path) -> None:
        assert path == expected_release
        assert path.is_dir()
        immutable_paths.append(path)
        original_make_immutable(path)

    monkeypatch.setattr(relay_deploy, "_make_immutable", record_make_immutable)

    result = prepare_release(paths, CommandRunner(), sha)

    assert result["status"] == "prepared"
    assert immutable_paths == [expected_release]


def test_activate_renders_launch_agent_and_probes_public_market(
    tmp_path: Path,
) -> None:
    paths, sha = _prepare(tmp_path)
    runner = LaunchctlRunner()

    result = activate_release(
        paths,
        runner,
        target_sha=sha,
        expected_current="none",
        confirm=CONFIRMATION,
        authorization_id="test-authorization",
        api_probe=lambda _url: {
            "ok": True,
            "accounts_ready": 1,
            "release_sha": sha,
        },
        discover_market=lambda _url: 58416,
        relay_probe=lambda _url, market_id: {
            "ok": True,
            "market_id": market_id,
            "source": "subscription_response",
        },
    )

    assert result["status"] == "activated"
    assert paths.current_link.resolve() == paths.release_root / sha
    plist = paths.launch_agent.read_text(encoding="utf-8")
    api_plist = paths.api_launch_agent.read_text(encoding="utf-8")
    assert str(paths.python) in plist
    assert str(paths.current_link) in plist
    assert "__PREDICTFUN_" not in plist
    assert "PREDICTFUN_API_KEY" not in plist
    assert str(paths.python) in api_plist
    assert str(paths.current_link) in api_plist
    assert "__PREDICTFUN_" not in api_plist
    assert "PREDICTFUN_API_KEY" not in api_plist
    assert sha in api_plist
    rendered_calls = "\n".join(" ".join(call) for call in runner.calls)
    assert "launchctl bootstrap" in rendered_calls
    assert "launchctl kickstart -k" in rendered_calls
    assert "polymarket-engine" not in rendered_calls


def test_activate_waits_for_launch_agent_to_reach_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, sha = _prepare(tmp_path)
    runner = DelayedLaunchctlRunner(delayed_prints=2)
    monkeypatch.setattr(
        "platforms.predictfun.deploy_ws_relay.time.sleep",
        lambda _seconds: None,
    )

    result = activate_release(
        paths,
        runner,
        target_sha=sha,
        expected_current="none",
        confirm=CONFIRMATION,
        authorization_id="test-delayed-launch",
        api_probe=lambda _url: {
            "ok": True,
            "accounts_ready": 1,
            "release_sha": sha,
        },
        discover_market=lambda _url: 58416,
        relay_probe=lambda _url, market_id: {
            "ok": True,
            "market_id": market_id,
        },
    )

    assert result["status"] == "activated"
    assert len(
        [call for call in runner.calls if call[:2] == ("launchctl", "print")]
    ) == 4


def test_failed_probe_restores_prior_launch_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, sha = _prepare(tmp_path)
    paths.launch_agent.parent.mkdir(parents=True, exist_ok=True)
    paths.launch_agent.write_text("legacy relay plist\n", encoding="utf-8")
    paths.api_launch_agent.write_text(
        "legacy api plist\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "platforms.predictfun.deploy_ws_relay.time.sleep",
        lambda _seconds: None,
    )

    with pytest.raises(RelayDeploymentError, match="previous state restored"):
        activate_release(
            paths,
            LaunchctlRunner(),
            target_sha=sha,
            expected_current="none",
            confirm=CONFIRMATION,
            authorization_id="test-failure",
            api_probe=lambda _url: {
                "ok": True,
                "accounts_ready": 1,
                "release_sha": sha,
            },
            discover_market=lambda _url: 58416,
            relay_probe=lambda _url, _market_id: (_ for _ in ()).throw(
                RuntimeError("upstream rejected")
            ),
        )

    assert not paths.current_link.exists()
    assert paths.launch_agent.read_text(encoding="utf-8") == "legacy relay plist\n"
    assert (
        paths.api_launch_agent.read_text(encoding="utf-8")
        == "legacy api plist\n"
    )


def test_api_release_mismatch_restores_both_launch_agents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, sha = _prepare(tmp_path)
    paths.launch_agent.parent.mkdir(parents=True, exist_ok=True)
    paths.launch_agent.write_text("legacy relay plist\n", encoding="utf-8")
    paths.api_launch_agent.write_text(
        "legacy api plist\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "platforms.predictfun.deploy_ws_relay.time.sleep",
        lambda _seconds: None,
    )

    with pytest.raises(RelayDeploymentError, match="previous state restored"):
        activate_release(
            paths,
            LaunchctlRunner(),
            target_sha=sha,
            expected_current="none",
            confirm=CONFIRMATION,
            authorization_id="test-api-sha-mismatch",
            api_probe=lambda _url: {
                "ok": True,
                "accounts_ready": 1,
                "release_sha": "f" * 40,
            },
            discover_market=lambda _url: pytest.fail(
                "WS discovery ran after API release mismatch"
            ),
            relay_probe=lambda _url, _market_id: pytest.fail(
                "WS probe ran after API release mismatch"
            ),
        )

    assert not paths.current_link.exists()
    assert paths.launch_agent.read_text(encoding="utf-8") == "legacy relay plist\n"
    assert (
        paths.api_launch_agent.read_text(encoding="utf-8")
        == "legacy api plist\n"
    )


def test_relay_activation_rejects_insecure_secret_permissions(
    tmp_path: Path,
) -> None:
    paths, sha = _prepare(tmp_path)
    paths.secret_file.chmod(0o644)

    with pytest.raises(RelayDeploymentError, match="group/world"):
        activate_release(
            paths,
            LaunchctlRunner(),
            target_sha=sha,
            expected_current="none",
            confirm=CONFIRMATION,
            authorization_id="test-insecure-secret",
            api_probe=lambda _url: {
                "ok": True,
                "accounts_ready": 1,
                "release_sha": sha,
            },
            discover_market=lambda _url: 58416,
            relay_probe=lambda _url, _market_id: {"ok": True},
        )


def test_relay_release_manifest_detects_tampering(tmp_path: Path) -> None:
    paths, sha = _paths(tmp_path)
    prepare_release(paths, CommandRunner(), sha)
    release = paths.release_root / sha
    target = release / "platforms/predictfun/ws_relay.py"
    target.chmod(0o644)
    target.write_text("tampered\n", encoding="utf-8")
    target.chmod(0o444)

    with pytest.raises(RelayDeploymentError, match="hash mismatch"):
        verify_release(release, sha)
