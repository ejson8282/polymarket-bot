import fcntl
import hashlib
import json
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
                    f"{current}/platforms/polymarket/maker/engine.py config.json"
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
        python=Path(sys.executable),
    )
    return paths, source, target_sha


class SystemctlRunner(CommandRunner):
    def __init__(
        self,
        *,
        paths: DeploymentPaths,
        target_sha: str,
        fail_first_restart: bool = False,
    ):
        self.paths = paths
        self.target_sha = target_sha
        self.fail_first_restart = fail_first_restart
        self.restart_count = 0

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> str:
        normalized = tuple(str(arg) for arg in args)
        if normalized == ("systemctl", "is-active", "polymarket-engine.service"):
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
            if self.restart_count == 1:
                self.paths.state_file.write_text(
                    json.dumps(
                        {
                            "release_sha": self.target_sha,
                            "release_required": True,
                            "paused": True,
                            "quotes_sent": 0,
                            "fills_seen": 0,
                            "ts": "2099-01-01T00:00:00Z",
                        }
                    ),
                    encoding="utf-8",
                )
            return ""
        return super().run(args, cwd=cwd, env=env)


def test_global_lock_rejects_a_second_deployment(tmp_path):
    lock_file = tmp_path / "locks" / "vps1-production-deploy.lock"
    lock_file.parent.mkdir()
    lock_file.touch()

    with deployment_lock(lock_file):
        with pytest.raises(DeploymentError, match="owns the global lock"):
            with deployment_lock(lock_file):
                pass


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


def test_activate_switches_exact_release_and_keeps_engine_paused(
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


def test_failed_activation_restores_previous_release(tmp_path, monkeypatch):
    paths, _source, target_sha = _paths(tmp_path)
    runner = SystemctlRunner(
        paths=paths,
        target_sha=target_sha,
        fail_first_restart=True,
    )

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
