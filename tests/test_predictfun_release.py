from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Mapping, Optional, Sequence

import pytest

from platforms.predictfun.maker.deploy_release import (
    CONFIRMATION,
    CommandRunner,
    DeploymentError,
    DeploymentPaths,
    activate_release,
    prepare_release,
)
from platforms.predictfun.maker.release_guard import (
    ARTIFACT,
    REQUIRED_FILES,
    SOURCE_REPOSITORY,
    verify_release,
)
from platforms.predictfun.maker.runner import _release_metadata


ROOT = Path(__file__).resolve().parents[1]


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=str(cwd),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def _guard_release(tmp_path: Path, sha: str = "a" * 40) -> Path:
    release = tmp_path / sha
    for relative_name in REQUIRED_FILES:
        path = release / relative_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fixture:{relative_name}\n", encoding="utf-8")
    files = {
        path.relative_to(release).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in release.rglob("*")
        if path.is_file()
    }
    manifest = {
        "source_repository": SOURCE_REPOSITORY,
        "artifact": ARTIFACT,
        "commit": sha,
        "files": files,
    }
    manifest_path = release / ".release-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    for path in release.rglob("*"):
        if path.is_file():
            path.chmod(0o444)
    for path in sorted(
        (path for path in release.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        path.chmod(0o555)
    release.chmod(0o555)
    return release


def _source_repo(tmp_path: Path) -> tuple[Path, str]:
    source = tmp_path / "source"
    (source / "platforms").mkdir(parents=True)
    shutil.copy2(ROOT / "platforms/__init__.py", source / "platforms/__init__.py")
    shutil.copytree(
        ROOT / "platforms/predictfun",
        source / "platforms/predictfun",
    )
    systemd = source / "deploy/systemd"
    systemd.mkdir(parents=True)
    for name in ("predictfun-dryrun.service", "predictfun-dryrun.timer"):
        shutil.copy2(ROOT / "deploy/systemd" / name, systemd / name)

    _git(source, "init")
    _git(source, "config", "user.email", "tests@example.com")
    _git(source, "config", "user.name", "Predict Release Tests")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "predict release fixture")
    _git(source, "branch", "-M", "main")
    return source, _git(source, "rev-parse", "HEAD")


def _paths(tmp_path: Path) -> tuple[DeploymentPaths, str]:
    source, sha = _source_repo(tmp_path)
    bare = tmp_path / "predictfun.git"
    _git(tmp_path, "clone", "--bare", str(source), str(bare))
    runtime = tmp_path / "runtime"
    data = runtime / "data"
    lock = tmp_path / "locks/vps1-production-deploy.lock"
    lock.parent.mkdir()
    lock.touch()
    paths = DeploymentPaths(
        bare_repo=bare,
        release_root=tmp_path / "releases",
        current_link=tmp_path / "releases/current",
        runtime_root=runtime,
        data_root=data,
        runtime_config=runtime / "config.mainnet.json",
        release_env=runtime / "release.env",
        runner_state=data / "predictfun_mainnet_runner_state.json",
        lock_file=lock,
        service_unit=tmp_path / "systemd/predictfun-dryrun.service",
        timer_unit=tmp_path / "systemd/predictfun-dryrun.timer",
        python=Path(sys.executable),
    )
    return paths, sha


class SystemdRunner(CommandRunner):
    def __init__(self, paths: DeploymentPaths, sha: str, *, fail_cycle: bool = False):
        self.paths = paths
        self.sha = sha
        self.fail_cycle = fail_cycle
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[Path] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = True,
    ) -> str:
        command = tuple(str(arg) for arg in args)
        self.calls.append(command)
        if command[:2] == ("systemctl", "is-enabled"):
            return "enabled"
        if command[:2] == ("systemctl", "is-active"):
            return "active"
        if command[:2] == ("systemctl", "start") and command[-1].endswith(
            ".service"
        ):
            self.paths.runner_state.parent.mkdir(parents=True, exist_ok=True)
            now = datetime.now(timezone.utc).isoformat()
            self.paths.runner_state.write_text(
                json.dumps(
                    {
                        "mode": "dry_run",
                        "release_sha": self.sha,
                        "release_required": True,
                        "cycle_count": 0 if self.fail_cycle else 1,
                        "error_count": 1 if self.fail_cycle else 0,
                        "last_error": "upstream failed" if self.fail_cycle else "",
                        "last_cycle_finished_at": now,
                        "running": False,
                    }
                ),
                encoding="utf-8",
            )
        return ""


def test_release_guard_accepts_exact_predict_release(tmp_path: Path) -> None:
    release = _guard_release(tmp_path)
    manifest = verify_release(
        release,
        {
            "PREDICTFUN_REQUIRE_RELEASE": "1",
            "PREDICTFUN_RELEASE_SHA": "a" * 40,
        },
    )
    assert manifest["artifact"] == "predictfun-dryrun"
    current = tmp_path / "current"
    current.symlink_to(release)
    assert verify_release(
        current,
        {
            "PREDICTFUN_REQUIRE_RELEASE": "1",
            "PREDICTFUN_RELEASE_SHA": "a" * 40,
        },
    )["commit"] == "a" * 40


def test_release_guard_rejects_tampering_and_extra_files(tmp_path: Path) -> None:
    release = _guard_release(tmp_path)
    runner = release / "platforms/predictfun/maker/runner.py"
    runner.chmod(0o644)
    runner.write_text("tampered\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="writable|hash mismatch"):
        verify_release(
            release,
            {
                "PREDICTFUN_REQUIRE_RELEASE": "1",
                "PREDICTFUN_RELEASE_SHA": "a" * 40,
            },
        )

    fresh = _guard_release(tmp_path / "fresh")
    extra = fresh / "unexpected.py"
    fresh.chmod(0o755)
    extra.write_text("pass\n", encoding="utf-8")
    extra.chmod(0o444)
    fresh.chmod(0o555)
    with pytest.raises(RuntimeError, match="file set"):
        verify_release(
            fresh,
            {
                "PREDICTFUN_REQUIRE_RELEASE": "1",
                "PREDICTFUN_RELEASE_SHA": "a" * 40,
            },
        )


def test_runner_release_metadata_is_explicit() -> None:
    assert _release_metadata(
        {
            "PREDICTFUN_REQUIRE_RELEASE": "true",
            "PREDICTFUN_RELEASE_SHA": "b" * 40,
        }
    ) == {"release_required": True, "release_sha": "b" * 40}
    assert _release_metadata({}) == {
        "release_required": False,
        "release_sha": "",
    }


def test_prepare_builds_predict_only_immutable_release(tmp_path: Path) -> None:
    paths, sha = _paths(tmp_path)
    result = prepare_release(paths, CommandRunner(), sha)
    release = paths.release_root / sha

    assert result["status"] == "prepared"
    assert (release / "platforms/predictfun/maker/runner.py").is_file()
    assert not (release / "platforms/polymarket").exists()
    assert not (release / "dashboard").exists()
    assert all(path.stat().st_mode & 0o222 == 0 for path in release.rglob("*"))
    assert verify_release(
        release,
        {
            "PREDICTFUN_REQUIRE_RELEASE": "1",
            "PREDICTFUN_RELEASE_SHA": sha,
        },
    )["commit"] == sha


def test_activate_runs_one_dry_cycle_without_polymarket_controls(
    tmp_path: Path,
) -> None:
    paths, sha = _paths(tmp_path)
    prepare_release(paths, CommandRunner(), sha)
    runner = SystemdRunner(paths, sha)

    result = activate_release(
        paths,
        runner,
        target_sha=sha,
        expected_current="none",
        confirm=CONFIRMATION,
        authorization_id="test-authorization",
    )

    assert result["status"] == "activated"
    assert paths.current_link.resolve() == paths.release_root / sha
    config = json.loads(paths.runtime_config.read_text(encoding="utf-8"))
    assert config["environment"] == "mainnet"
    assert paths.runtime_config.stat().st_mode & 0o777 == 0o400
    assert paths.data_root.stat().st_mode & 0o777 == 0o700
    assert all(
        Path(value).parent == paths.data_root
        for value in config["output"].values()
    )
    rendered_calls = "\n".join(" ".join(call) for call in runner.calls)
    assert "polymarket-engine" not in rendered_calls
    assert "polymarket-releases" not in rendered_calls
    assert "predictfun-dryrun.service" in rendered_calls
    service_start = runner.calls.index(
        ("systemctl", "start", "predictfun-dryrun.service")
    )
    timer_enable = runner.calls.index(
        ("systemctl", "enable", "--now", "predictfun-dryrun.timer")
    )
    assert service_start < timer_enable


def test_failed_cycle_restores_legacy_predict_units(tmp_path: Path) -> None:
    paths, sha = _paths(tmp_path)
    prepare_release(paths, CommandRunner(), sha)
    paths.service_unit.parent.mkdir(parents=True)
    paths.service_unit.write_text("legacy predict service\n", encoding="utf-8")
    paths.timer_unit.write_text("legacy predict timer\n", encoding="utf-8")
    runner = SystemdRunner(paths, sha, fail_cycle=True)

    with pytest.raises(DeploymentError, match="previous Predict-only service state"):
        activate_release(
            paths,
            runner,
            target_sha=sha,
            expected_current="none",
            confirm=CONFIRMATION,
            authorization_id="test-authorization",
        )

    assert not paths.current_link.exists()
    assert paths.service_unit.read_text(encoding="utf-8") == "legacy predict service\n"
    assert paths.timer_unit.read_text(encoding="utf-8") == "legacy predict timer\n"


def test_systemd_units_are_predict_only_and_release_guarded() -> None:
    service = (ROOT / "deploy/systemd/predictfun-dryrun.service").read_text(
        encoding="utf-8"
    )
    timer = (ROOT / "deploy/systemd/predictfun-dryrun.timer").read_text(
        encoding="utf-8"
    )
    assert "/home/ubuntu/predictfun-releases/current" in service
    assert "/home/ubuntu/predictfun-runtime" in service
    assert "PREDICTFUN_REQUIRE_RELEASE=1" in service
    assert "release_guard.py" in service
    assert "--once" in service
    assert "Persistent=true" in timer
    combined = service + timer
    assert "/home/ubuntu/polymarket-bot" not in combined
    assert "polymarket-engine" not in combined
    assert "live_order_once" not in combined
