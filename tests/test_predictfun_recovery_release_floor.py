import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from platforms.predictfun.recovery_release_floor import (
    RecoveryReleaseFloorError, check_release_transition, read_release_floor,
)
import platforms.predictfun.maker.deploy_release as vps
import platforms.predictfun.deploy_ws_relay as mini


OLD, NEW = "a" * 40, "b" * 40


@pytest.fixture(autouse=True)
def restore_floor_permissions(tmp_path):
    yield
    directory = tmp_path / "recovery-release-floor"
    if directory.is_dir() and not directory.is_symlink():
        directory.chmod(0o700)


def policy(profile="vps1"):
    return {"version": 1, "repository": "ejson8282/polymarket-bot",
            "profile": profile, "recovery_id": "c" * 64,
            "allowed_releases": [OLD, NEW]}


def write_floor(root, payload):
    directory = root / "recovery-release-floor"
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o700)
    path = directory / "policy.json"
    if path.exists():
        path.chmod(0o600)
    path.write_text(json.dumps(payload) if isinstance(payload, dict) else payload)
    path.chmod(0o400)
    directory.chmod(0o500)
    return path


def test_legacy_absence_is_distinct_from_damaged_enabled_floor(tmp_path):
    snapshot = check_release_transition(tmp_path, "vps1", NEW, None)
    assert snapshot.digest is None
    snapshot.require_unchanged()
    (tmp_path / "recovery-release-floor").mkdir(mode=0o500)
    with pytest.raises(RecoveryReleaseFloorError):
        read_release_floor(tmp_path, "vps1")


@pytest.mark.parametrize("profile", ["vps1", "vps2", "macmini"])
def test_exact_release_and_rollback_allowlist(tmp_path, profile):
    write_floor(tmp_path, policy(profile))
    snapshot = check_release_transition(tmp_path, profile, NEW, OLD)
    assert snapshot.allowed == (OLD, NEW)
    assert len(snapshot.digest) == 64
    snapshot.require_unchanged()
    for target, previous in [("d" * 40, OLD), (NEW, "d" * 40), (NEW, None)]:
        with pytest.raises(RecoveryReleaseFloorError, match="not_approved"):
            check_release_transition(tmp_path, profile, target, previous)


@pytest.mark.parametrize("field,value", [("version", True), ("version", 2),
    ("repository", "another/repo"), ("profile", "vps2"), ("recovery_id", "bad"),
    ("allowed_releases", []), ("allowed_releases", [OLD, OLD]),
    ("allowed_releases", [NEW, OLD]), ("allowed_releases", ["abc"]),
    ("allowed_releases", [None]), ("allowed_releases", "main")])
def test_malformed_policy_blocks(tmp_path, field, value):
    write_floor(tmp_path, {**policy(), field: value})
    with pytest.raises(RecoveryReleaseFloorError, match="invalid"):
        read_release_floor(tmp_path, "vps1")


@pytest.mark.parametrize("payload", ["{", "null", "[]", '{"version":1,"version":1}',
                                     '{"version":NaN}'])
def test_invalid_json_never_becomes_legacy_absence(tmp_path, payload):
    write_floor(tmp_path, payload)
    with pytest.raises(RecoveryReleaseFloorError, match="invalid"):
        read_release_floor(tmp_path, "vps1")


@pytest.mark.parametrize("damage", ["missing", "symlink", "writable_file", "writable_dir", "fifo", "oversize"])
def test_unsafe_storage_blocks_without_reading_special_files(tmp_path, damage):
    path = write_floor(tmp_path, policy())
    path.parent.chmod(0o700)
    if damage in {"missing", "symlink", "fifo"}:
        path.unlink()
    if damage == "symlink":
        path.symlink_to(tmp_path / "missing-target")
    elif damage == "fifo":
        os.mkfifo(path, 0o400)
    elif damage == "writable_file":
        path.chmod(0o600)
    elif damage == "oversize":
        path.chmod(0o600)
        path.write_text("x" * 65537)
        path.chmod(0o400)
    if damage != "writable_dir":
        path.parent.chmod(0o500)
    with pytest.raises(RecoveryReleaseFloorError):
        read_release_floor(tmp_path, "vps1")


def test_floor_directory_symlink_refused(tmp_path):
    (tmp_path / "recovery-release-floor").symlink_to(tmp_path / "elsewhere")
    with pytest.raises(RecoveryReleaseFloorError):
        read_release_floor(tmp_path, "vps1")


@pytest.mark.parametrize("change", ["remove_all", "replace", "damage"])
def test_policy_changes_detected_before_activation_or_rollback(tmp_path, change):
    path = write_floor(tmp_path, policy())
    snapshot = read_release_floor(tmp_path, "vps1")
    if change == "remove_all":
        path.parent.chmod(0o700)
        path.unlink()
        path.parent.rmdir()
    else:
        write_floor(tmp_path, {**policy(), "allowed_releases": [NEW]} if change == "replace" else "{")
    with pytest.raises(RecoveryReleaseFloorError):
        snapshot.require_unchanged()


def test_policy_appearing_mid_deployment_is_also_a_change(tmp_path):
    snapshot = read_release_floor(tmp_path, "vps1")
    write_floor(tmp_path, policy())
    with pytest.raises(RecoveryReleaseFloorError, match="changed"):
        snapshot.require_unchanged()


@pytest.mark.parametrize("profile", ["vps1", "vps2", "macmini"])
@pytest.mark.parametrize("blocked", ["target", "rollback"])
def test_wrappers_check_floor_before_any_service_or_release_mutation(tmp_path, monkeypatch, profile, blocked):
    write_floor(tmp_path, {**policy(profile), "allowed_releases": [OLD if blocked == "target" else NEW]})
    def forbidden(*a, **k):
        pytest.fail("must reject before any service/release action")
    runner = SimpleNamespace(run=forbidden)
    if profile == "macmini":
        paths = mini.RelayDeploymentPaths(runtime_root=tmp_path)
        monkeypatch.setattr(mini, "_current_sha", lambda p: OLD)
        monkeypatch.setattr(mini, "verify_release", forbidden)
        call = mini.activate_release
        confirmation = mini.CONFIRMATION
    else:
        paths = vps.DeploymentPaths(runtime_root=tmp_path, profile=profile,
                                   account_id="account_01" if profile == "vps1" else "account_02")
        monkeypatch.setattr(vps, "_require_expected_current", lambda *a: OLD)
        monkeypatch.setattr(vps, "verify_release", forbidden)
        call = vps.activate_release
        confirmation = vps.CONFIRMATION
    with pytest.raises(RecoveryReleaseFloorError, match="not_approved"):
        call(paths, runner, target_sha=NEW, expected_current=OLD,
             confirm=confirmation, authorization_id="synthetic-test-only")
