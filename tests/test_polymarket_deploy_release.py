import fcntl
import hashlib
import json
from dataclasses import replace
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Mapping, Optional, Sequence

import pytest


ROOT = Path(__file__).resolve().parents[1]
MAKER_DIR = ROOT / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from deploy_release import (  # noqa: E402
    CommandRunner,
    DeploymentError,
    DeploymentPaths,
    DeploymentRequest,
    _require_drop_in,
    _require_signer_ready,
    _state_file_marker,
    _wait_for_engine_state,
    deployment_paths_for_profile,
    deployment_lock,
    execute,
)


OLD_SHA = "0" * 40


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=str(cwd),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def _write_source_tree(source: Path) -> tuple[str, str]:
    _git(source, "init")
    _git(source, "config", "user.email", "tests@example.com")
    _git(source, "config", "user.name", "Release Tests")
    maker = source / "platforms" / "polymarket" / "maker"
    maker.mkdir(parents=True)
    (maker / "engine.py").write_text("print('maker')\n", encoding="utf-8")
    shutil.copy2(MAKER_DIR / "release_guard.py", maker / "release_guard.py")
    tests = source / "tests"
    tests.mkdir()
    (tests / "test_release_smoke.py").write_text(
        "def test_release_smoke():\n    assert True\n",
        encoding="utf-8",
    )
    _git(source, "add", ".")
    _git(source, "commit", "-m", "old release")
    _git(source, "branch", "-M", "main")
    old_internal_sha = _git(source, "rev-parse", "HEAD")
    (source / "README.md").write_text("new release\n", encoding="utf-8")
    _git(source, "add", "README.md")
    _git(source, "commit", "-m", "new release")
    return old_internal_sha, _git(source, "rev-parse", "HEAD")


def _manifest(release: Path, sha: str) -> None:
    engine = release / "platforms" / "polymarket" / "maker" / "engine.py"
    guard = release / "platforms" / "polymarket" / "maker" / "release_guard.py"
    engine.parent.mkdir(parents=True)
    engine.write_text("print('old maker')\n", encoding="utf-8")
    shutil.copy2(MAKER_DIR / "release_guard.py", guard)
    payload = {
        "source_repository": "ejson8282/polymarket-bot",
        "commit": sha,
        "engine_sha256": hashlib.sha256(engine.read_bytes()).hexdigest(),
    }
    (release / ".release-manifest.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def _paths(tmp_path: Path) -> tuple[DeploymentPaths, Path, str]:
    source = tmp_path / "source"
    source.mkdir()
    old_internal_sha, target_sha = _write_source_tree(source)

    bare = tmp_path / "source.git"
    _git(tmp_path, "clone", "--bare", str(source), str(bare))
    _git(
        tmp_path,
        f"--git-dir={bare}",
        "update-ref",
        "refs/heads/main",
        old_internal_sha,
        target_sha,
    )
    _git(
        tmp_path,
        f"--git-dir={bare}",
        "update-ref",
        f"refs/deploy-candidates/{target_sha}",
        target_sha,
    )

    releases = tmp_path / "releases"
    old_release = releases / OLD_SHA
    _manifest(old_release, OLD_SHA)
    current = releases / "current"
    current.symlink_to(old_release)

    runtime = tmp_path / "runtime"
    runtime.mkdir()
    release_env = runtime / "engine-release.env"
    release_env.write_text(
        f"POLYMARKET_RELEASE_SHA={OLD_SHA}\n",
        encoding="utf-8",
    )

    locks = tmp_path / "locks"
    locks.mkdir()
    lock_file = locks / "vps1-production-deploy.lock"
    lock_file.touch()

    pause_flag = tmp_path / "data" / ".account_1.paused"
    pause_flag.parent.mkdir()
    pause_flag.touch()
    state_file = pause_flag.parent / "engine_state_1.json"
    state_file.write_text(
        json.dumps(
            {
                "release_sha": OLD_SHA,
                "release_required": True,
                "paused": True,
                "quotes_sent": 0,
                "fills_seen": 0,
                "ts": "2099-01-01T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    runtime_config = tmp_path / "config_1.json"
    runtime_config.write_text(
        json.dumps(
            {
                "account": {
                    "signer_server_url": "http://100.64.0.1:8420",
                    "signer_token": "test-token",
                    "funder": "0x1234",
                }
            }
        ),
        encoding="utf-8",
    )

    drop_in = tmp_path / "zz-immutable-release.conf"
    drop_in.write_text(
        "\n".join(
            (
                "[Service]",
                "Environment=POLYMARKET_REQUIRE_RELEASE=1",
                f"EnvironmentFile={release_env}",
                (
                    f"ExecStartPre={sys.executable} "
                    f"{current}/platforms/polymarket/maker/release_guard.py "
                    f"{current}/platforms/polymarket/maker/engine.py"
                ),
                (
                    f"ExecStart={sys.executable} "
                    f"{current}/platforms/polymarket/maker/engine.py "
                    f"{runtime_config}"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    paths = DeploymentPaths(
        bare_repo=bare,
        release_root=releases,
        current_link=current,
        runtime_root=runtime,
        release_env=release_env,
        lock_file=lock_file,
        drop_in=drop_in,
        pause_flag=pause_flag,
        state_file=state_file,
        runtime_config=runtime_config,
        python=Path(sys.executable),
        post_restart_stability_seconds=0.0,
    )
    return paths, source, target_sha


def _as_vps2(paths: DeploymentPaths) -> DeploymentPaths:
    pause_flag = paths.pause_flag.with_name(".account_2.paused")
    state_file = paths.state_file.with_name("engine_state_2.json")
    runtime_config = paths.runtime_config.with_name("config_2.json")
    paths.pause_flag.replace(pause_flag)
    paths.state_file.replace(state_file)
    paths.runtime_config.replace(runtime_config)
    paths.drop_in.write_text(
        paths.drop_in.read_text(encoding="utf-8").replace(
            str(paths.runtime_config),
            str(runtime_config),
        ),
        encoding="utf-8",
    )
    return replace(
        paths,
        profile_name="vps2",
        account_index=2,
        lock_file=paths.lock_file.with_name("vps2-production-deploy.lock"),
        pause_flag=pause_flag,
        state_file=state_file,
        runtime_config=runtime_config,
    )


class SystemctlRunner(CommandRunner):
    def __init__(
        self,
        *,
        paths: DeploymentPaths,
        target_sha: str,
        fail_first_restart: bool = False,
        initially_active: bool = True,
        fail_first_start: bool = False,
    ):
        self.paths = paths
        self.target_sha = target_sha
        self.fail_first_restart = fail_first_restart
        self.fail_first_start = fail_first_start
        self.active = initially_active
        self.restart_count = 0
        self.start_count = 0
        self.stop_count = 0

    def _write_state(self, release_sha: str) -> None:
        self.paths.state_file.write_text(
            json.dumps(
                {
                    "release_sha": release_sha,
                    "release_required": True,
                    "paused": True,
                    "quotes_sent": 0,
                    "fills_seen": 0,
                    "ts": "2099-01-01T00:00:00Z",
                }
            ),
            encoding="utf-8",
        )

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> str:
        normalized = tuple(str(arg) for arg in args)
        if normalized == ("systemctl", "is-active", "polymarket-engine.service"):
            if not self.active:
                raise subprocess.CalledProcessError(3, normalized)
            return "active"
        if normalized == (
            "sudo",
            "-n",
            "systemctl",
            "restart",
            "polymarket-engine.service",
        ):
            self.restart_count += 1
            if self.fail_first_restart and self.restart_count == 1:
                raise subprocess.CalledProcessError(1, normalized)
            self.active = True
            release_sha = self.target_sha if self.restart_count == 1 else OLD_SHA
            self._write_state(release_sha)
            return ""
        if normalized == (
            "sudo",
            "-n",
            "systemctl",
            "start",
            "polymarket-engine.service",
        ):
            self.start_count += 1
            if self.fail_first_start and self.start_count == 1:
                raise subprocess.CalledProcessError(1, normalized)
            self.active = True
            self._write_state(self.target_sha)
            return ""
        if normalized == (
            "sudo",
            "-n",
            "systemctl",
            "stop",
            "polymarket-engine.service",
        ):
            self.stop_count += 1
            self.active = False
            return ""
        return super().run(args, cwd=cwd, env=env)


class FakeSignerResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit: int) -> bytes:
        return json.dumps(
            {
                "api_key": "key",
                "api_secret": "secret",
                "api_passphrase": "passphrase",
                "address": "0xabc",
            }
        ).encode("utf-8")


class FakeSignerOpener:
    def __init__(self):
        self.request = None
        self.timeout = None

    def open(self, request, timeout):
        self.request = request
        self.timeout = timeout
        return FakeSignerResponse()


def _mock_clock_and_signer(monkeypatch):
    import deploy_release

    monkeypatch.setattr(
        deploy_release,
        "_utc_now",
        lambda: deploy_release.datetime(
            2099,
            1,
            1,
            tzinfo=deploy_release.timezone.utc,
        ),
    )
    monkeypatch.setattr(
        deploy_release,
        "_require_signer_ready",
        lambda _paths: {
            "status": "ok",
            "signer_host": "100.64.0.1",
            "funder_configured": True,
        },
    )


def test_global_lock_rejects_a_second_deployment(tmp_path):
    lock_file = tmp_path / "locks" / "vps1-production-deploy.lock"
    lock_file.parent.mkdir()
    lock_file.touch()

    with deployment_lock(lock_file):
        with pytest.raises(DeploymentError, match="owns the global lock"):
            with deployment_lock(lock_file):
                pass


def test_vps2_profile_uses_account_2_runtime_paths():
    paths = deployment_paths_for_profile("vps2")

    assert paths.profile_name == "vps2"
    assert paths.account_index == 2
    assert paths.lock_file.name == "vps2-production-deploy.lock"
    assert paths.pause_flag.name == ".account_2.paused"
    assert paths.state_file.name == "engine_state_2.json"
    assert paths.runtime_config.name == "config_2.json"


def test_vps2_confirmation_is_scoped_to_profile(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)
    paths = _as_vps2(paths)

    with pytest.raises(
        DeploymentError,
        match=f"PREPARE:vps2:{target_sha}",
    ):
        execute(
            DeploymentRequest(
                action="prepare",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"PREPARE:{target_sha}",
                profile_name="vps2",
            ),
            paths=paths,
            tests=("tests/test_release_smoke.py",),
        )


def test_vps2_activate_uses_account_2_profile(tmp_path, monkeypatch):
    paths, _source, target_sha = _paths(tmp_path)
    paths = _as_vps2(paths)
    runner = SystemctlRunner(paths=paths, target_sha=target_sha)
    _mock_clock_and_signer(monkeypatch)

    result = execute(
        DeploymentRequest(
            action="activate",
            target_sha=target_sha,
            expected_current_sha=OLD_SHA,
            confirm=f"ACTIVATE:vps2:{target_sha}",
            authorization_id="test-vps2-authorization",
            profile_name="vps2",
        ),
        paths=paths,
        runner=runner,
        tests=("tests/test_release_smoke.py",),
    )

    assert result["status"] == "succeeded"
    assert result["profile_name"] == "vps2"
    assert result["account_index"] == 2
    assert paths.current_link.resolve().name == target_sha
    assert runner.restart_count == 1


def test_vps1_plan_keeps_backward_compatible_default_profile(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)

    result = execute(
        DeploymentRequest(
            action="plan",
            target_sha=target_sha,
            expected_current_sha=OLD_SHA,
        ),
        paths=paths,
        tests=("tests/test_release_smoke.py",),
    )

    assert result["profile_name"] == "vps1"
    assert result["account_index"] == 1
    assert result["will_restart"] is False


def test_profile_rejects_account_1_paths_for_vps2(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)
    mismatched = replace(paths, profile_name="vps2", account_index=2)

    with pytest.raises(DeploymentError, match="profile paths mismatch"):
        execute(
            DeploymentRequest(
                action="plan",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                profile_name="vps2",
            ),
            paths=mismatched,
            tests=("tests/test_release_smoke.py",),
        )


def test_drop_in_must_name_the_selected_runtime_config(tmp_path):
    paths, _source, _target_sha = _paths(tmp_path)
    paths.drop_in.write_text(
        paths.drop_in.read_text(encoding="utf-8").replace(
            str(paths.runtime_config),
            str(paths.runtime_config.with_name("config_2.json")),
        ),
        encoding="utf-8",
    )

    with pytest.raises(DeploymentError, match=str(paths.runtime_config)):
        _require_drop_in(paths)


def test_signer_preflight_validates_response_without_exposing_credentials(
    tmp_path,
    monkeypatch,
):
    paths, _source, _target_sha = _paths(tmp_path)
    opener = FakeSignerOpener()
    monkeypatch.delenv("POLY_SIGNER_SERVER_URL", raising=False)
    monkeypatch.delenv("SIGNER_TOKEN", raising=False)

    import deploy_release

    monkeypatch.setattr(
        deploy_release.urllib.request,
        "build_opener",
        lambda *_handlers: opener,
    )

    result = _require_signer_ready(paths, timeout_seconds=7.0)

    assert result == {
        "status": "ok",
        "signer_host": "100.64.0.1",
        "funder_configured": True,
    }
    assert opener.timeout == 7.0
    assert opener.request.full_url == "http://100.64.0.1:8420/derive-creds"
    assert json.loads(opener.request.data) == {"funder": "0x1234"}
    assert "api_key" not in result
    assert "api_secret" not in result
    assert "api_passphrase" not in result


def test_signer_preflight_uses_engine_environment_overrides(tmp_path, monkeypatch):
    paths, _source, _target_sha = _paths(tmp_path)
    opener = FakeSignerOpener()
    monkeypatch.setenv("POLY_SIGNER_SERVER_URL", "http://100.64.0.2:9000")
    monkeypatch.setenv("SIGNER_TOKEN", "environment-test-token")

    import deploy_release

    monkeypatch.setattr(
        deploy_release.urllib.request,
        "build_opener",
        lambda *_handlers: opener,
    )

    _require_signer_ready(paths)

    assert opener.request.full_url == "http://100.64.0.2:9000/derive-creds"
    assert opener.request.get_header("Authorization") == (
        "Bearer environment-test-token"
    )


def test_wait_for_engine_state_rejects_pre_restart_state(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(paths=paths, target_sha=target_sha)
    paths.state_file.write_text(
        json.dumps(
            {
                "release_sha": OLD_SHA,
                "release_required": True,
                "paused": True,
                "quotes_sent": 0,
                "fills_seen": 0,
                "ts": "2098-12-31T23:59:00Z",
            }
        ),
        encoding="utf-8",
    )

    import deploy_release

    with pytest.raises(DeploymentError, match="predates this restart"):
        _wait_for_engine_state(
            paths,
            runner,
            OLD_SHA,
            observed_after=deploy_release.datetime(
                2099,
                1,
                1,
                tzinfo=deploy_release.timezone.utc,
            ),
            timeout_seconds=0.01,
            sleep=lambda _seconds: None,
        )


def test_wait_for_engine_state_requires_file_rewrite(tmp_path, monkeypatch):
    paths, _source, _target_sha = _paths(tmp_path)
    runner = SystemctlRunner(paths=paths, target_sha=OLD_SHA)
    previous_marker = _state_file_marker(paths.state_file)

    import deploy_release

    monkeypatch.setattr(
        deploy_release,
        "_utc_now",
        lambda: deploy_release.datetime(
            2099,
            1,
            1,
            tzinfo=deploy_release.timezone.utc,
        ),
    )

    with pytest.raises(DeploymentError, match="has not been rewritten"):
        _wait_for_engine_state(
            paths,
            runner,
            OLD_SHA,
            observed_after=deploy_release.datetime(
                2099,
                1,
                1,
                tzinfo=deploy_release.timezone.utc,
            ),
            previous_state_marker=previous_marker,
            timeout_seconds=0.01,
            sleep=lambda _seconds: None,
        )


def test_prepare_requires_exact_confirmation(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)

    with pytest.raises(DeploymentError, match="confirmation must exactly equal"):
        execute(
            DeploymentRequest(
                action="prepare",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm="yes",
            ),
            paths=paths,
            tests=("tests/test_release_smoke.py",),
        )


def test_prepare_promotes_exact_candidate_and_builds_immutable_release(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)

    result = execute(
        DeploymentRequest(
            action="prepare",
            target_sha=target_sha,
            expected_current_sha=OLD_SHA,
            confirm=f"PREPARE:{target_sha}",
        ),
        paths=paths,
        tests=("tests/test_release_smoke.py",),
    )

    release = paths.release_root / target_sha
    manifest = json.loads(
        (release / ".release-manifest.json").read_text(encoding="utf-8")
    )
    assert result["prepared"] is True
    assert manifest["commit"] == target_sha
    assert manifest["source_repository"] == "ejson8282/polymarket-bot"
    assert paths.current_link.resolve().name == OLD_SHA
    assert (release / "platforms/polymarket/maker/engine.py").stat().st_mode & 0o222 == 0
    assert _git(
        tmp_path,
        f"--git-dir={paths.bare_repo}",
        "rev-parse",
        "refs/heads/main",
    ) == target_sha


def test_prepare_rejects_missing_candidate_ref(tmp_path):
    paths, _source, target_sha = _paths(tmp_path)
    _git(
        tmp_path,
        f"--git-dir={paths.bare_repo}",
        "update-ref",
        "-d",
        f"refs/deploy-candidates/{target_sha}",
    )

    with pytest.raises(DeploymentError, match="candidate ref is missing"):
        execute(
            DeploymentRequest(
                action="prepare",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"PREPARE:{target_sha}",
            ),
            paths=paths,
            tests=("tests/test_release_smoke.py",),
        )


def test_prepare_rejects_non_fast_forward_candidate_downgrade(tmp_path):
    paths, source, target_sha = _paths(tmp_path)
    old_candidate_sha = _git(source, "rev-parse", "HEAD~1")
    _git(
        tmp_path,
        f"--git-dir={paths.bare_repo}",
        "update-ref",
        "refs/heads/main",
        target_sha,
    )
    _git(
        tmp_path,
        f"--git-dir={paths.bare_repo}",
        "update-ref",
        f"refs/deploy-candidates/{old_candidate_sha}",
        old_candidate_sha,
    )

    with pytest.raises(
        DeploymentError,
        match="candidate is not a fast-forward descendant of internal main",
    ):
        execute(
            DeploymentRequest(
                action="prepare",
                target_sha=old_candidate_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"PREPARE:{old_candidate_sha}",
            ),
            paths=paths,
            tests=("tests/test_release_smoke.py",),
        )


def test_activate_switches_exact_release_and_keeps_engine_paused(
    tmp_path,
    monkeypatch,
):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(paths=paths, target_sha=target_sha)
    _mock_clock_and_signer(monkeypatch)

    result = execute(
        DeploymentRequest(
            action="activate",
            target_sha=target_sha,
            expected_current_sha=OLD_SHA,
            confirm=f"ACTIVATE:{target_sha}",
            authorization_id="test-authorization",
        ),
        paths=paths,
        runner=runner,
        tests=("tests/test_release_smoke.py",),
    )

    assert result["status"] == "succeeded"
    assert result["rollback_sha"] == OLD_SHA
    assert paths.current_link.resolve().name == target_sha
    assert paths.release_env.read_text(encoding="utf-8") == (
        f"POLYMARKET_RELEASE_SHA={target_sha}\n"
    )
    assert runner.restart_count == 1
    assert result["signer_preflight"]["status"] == "ok"


def test_inactive_activation_requires_explicit_cold_start_flag(
    tmp_path,
    monkeypatch,
):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(
        paths=paths,
        target_sha=target_sha,
        initially_active=False,
    )
    _mock_clock_and_signer(monkeypatch)

    with pytest.raises(DeploymentError, match="allow-inactive-current"):
        execute(
            DeploymentRequest(
                action="activate",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"ACTIVATE:{target_sha}",
                authorization_id="test-cold-authorization",
            ),
            paths=paths,
            runner=runner,
            tests=("tests/test_release_smoke.py",),
        )

    assert runner.start_count == 0
    assert paths.current_link.resolve().name == OLD_SHA


def test_inactive_activation_starts_target_without_restarting_old_service(
    tmp_path,
    monkeypatch,
):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(
        paths=paths,
        target_sha=target_sha,
        initially_active=False,
    )
    _mock_clock_and_signer(monkeypatch)

    result = execute(
        DeploymentRequest(
            action="activate",
            target_sha=target_sha,
            expected_current_sha=OLD_SHA,
            confirm=f"ACTIVATE:{target_sha}",
            authorization_id="test-cold-authorization",
            allow_inactive_current=True,
        ),
        paths=paths,
        runner=runner,
        tests=("tests/test_release_smoke.py",),
    )

    assert result["status"] == "succeeded"
    assert result["activation_mode"] == "inactive_start"
    assert runner.start_count == 1
    assert runner.restart_count == 0
    assert runner.active is True
    assert paths.current_link.resolve().name == target_sha


def test_failed_inactive_activation_restores_old_release_and_stays_stopped(
    tmp_path,
    monkeypatch,
):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(
        paths=paths,
        target_sha=target_sha,
        initially_active=False,
        fail_first_start=True,
    )
    _mock_clock_and_signer(monkeypatch)

    with pytest.raises(DeploymentError, match="previous release was restored"):
        execute(
            DeploymentRequest(
                action="activate",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"ACTIVATE:{target_sha}",
                authorization_id="test-cold-authorization",
                allow_inactive_current=True,
            ),
            paths=paths,
            runner=runner,
            tests=("tests/test_release_smoke.py",),
        )

    assert runner.restart_count == 0
    assert runner.start_count == 1
    assert runner.stop_count == 1
    assert runner.active is False
    assert paths.current_link.resolve().name == OLD_SHA
    assert paths.release_env.read_text(encoding="utf-8") == (
        f"POLYMARKET_RELEASE_SHA={OLD_SHA}\n"
    )


def test_signer_preflight_failure_leaves_current_service_untouched(
    tmp_path,
    monkeypatch,
):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(paths=paths, target_sha=target_sha)

    import deploy_release

    monkeypatch.setattr(
        deploy_release,
        "_utc_now",
        lambda: deploy_release.datetime(
            2099,
            1,
            1,
            tzinfo=deploy_release.timezone.utc,
        ),
    )

    def fail_preflight(_paths):
        raise DeploymentError("remote signer preflight returned HTTP 500")

    monkeypatch.setattr(deploy_release, "_require_signer_ready", fail_preflight)

    with pytest.raises(DeploymentError, match="current service was left untouched"):
        execute(
            DeploymentRequest(
                action="activate",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"ACTIVATE:{target_sha}",
                authorization_id="test-authorization",
            ),
            paths=paths,
            runner=runner,
            tests=("tests/test_release_smoke.py",),
        )

    assert runner.restart_count == 0
    assert paths.current_link.resolve().name == OLD_SHA
    assert paths.release_env.read_text(encoding="utf-8") == (
        f"POLYMARKET_RELEASE_SHA={OLD_SHA}\n"
    )


def test_failed_activation_restores_previous_release(tmp_path, monkeypatch):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(
        paths=paths,
        target_sha=target_sha,
        fail_first_restart=True,
    )
    _mock_clock_and_signer(monkeypatch)

    with pytest.raises(DeploymentError, match="previous release was restored"):
        execute(
            DeploymentRequest(
                action="activate",
                target_sha=target_sha,
                expected_current_sha=OLD_SHA,
                confirm=f"ACTIVATE:{target_sha}",
                authorization_id="test-authorization",
            ),
            paths=paths,
            runner=runner,
            tests=("tests/test_release_smoke.py",),
        )

    assert paths.current_link.resolve().name == OLD_SHA
    assert paths.release_env.read_text(encoding="utf-8") == (
        f"POLYMARKET_RELEASE_SHA={OLD_SHA}\n"
    )
    assert runner.restart_count == 2


def test_lock_is_advisory_flock_compatible(tmp_path):
    lock_file = tmp_path / "vps1-production-deploy.lock"
    lock_file.touch()
    with deployment_lock(lock_file):
        with lock_file.open("a+", encoding="utf-8") as handle:
            with pytest.raises(BlockingIOError):
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
