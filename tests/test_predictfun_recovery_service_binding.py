import json
import os
import plistlib
import stat
import sys

import pytest

from platforms.predictfun import recovery_service_binding as binding
from platforms.predictfun import recovery_startup_guard as guard
from platforms.predictfun import recovery_bootstrap as bootstrap
from tests.test_predictfun_recovery_bootstrap import fixture_node, install, FakeRunner, SHA, cleanup_permissions


@pytest.mark.parametrize("profile", ["vps1", "vps2", "macmini"])
def test_only_predict_fixed_components_and_no_live_grants(profile):
    files = binding.binding_files(profile)
    assert len(files) == 2
    for path, content in files.items():
        assert "polymarket" not in str(path) and b"PREDICTFUN_LIVE" not in content
        if profile == "macmini":
            document = plistlib.loads(content)
            assert "Program" not in document and "EnvironmentVariables" not in document
            assert document["ProgramArguments"][1:3] == ["-I", "-B"]
        else:
            assert content.count(b"ExecStart=") == 2
            assert b"ExecStartPre=\n" in content and b"ExecStopPost=\n" in content
            assert b"/current/" not in content


@pytest.mark.parametrize("damage", ["missing_anchor", "missing_file", "changed_binding", "writable_binding",
                                    "bad_binding_manifest", "missing_policy", "bad_guard", "anchor_symlink"])
def test_binding_changes_cannot_select_legacy_mode(tmp_path, monkeypatch, damage):
    spec = fixture_node(tmp_path, monkeypatch)
    install(bootstrap.build_plan("vps1", SHA, "d" * 64))
    before = binding.inspect_installed("vps1")
    path = next(iter(binding.binding_files("vps1")))
    anchor = spec.security_root / "anchor.json"
    if damage == "missing_anchor":
        anchor.unlink()
    elif damage == "missing_file":
        path.unlink()
    elif damage == "changed_binding":
        path.chmod(0o600)
        path.write_text("old command")
        path.chmod(0o444)
    elif damage == "writable_binding":
        path.chmod(0o644)
    elif damage == "bad_binding_manifest":
        data = json.loads(anchor.read_bytes())
        data["service_bindings"] = {}
        anchor.chmod(0o600)
        anchor.write_text(json.dumps(data))
        anchor.chmod(0o444)
    elif damage == "missing_policy":
        policy = spec.runtime_root / "recovery-release-floor/policy.json"
        policy.parent.chmod(0o755)
        policy.unlink()
        policy.parent.rmdir()
    elif damage == "bad_guard":
        script = spec.security_root / "guard.py"
        script.chmod(0o600)
        script.write_text("replaced")
        script.chmod(0o444)
    else:
        anchor.rename(spec.security_root / "old-anchor.json")
        anchor.symlink_to(spec.security_root / "old-anchor.json")
    with pytest.raises(binding.BindingError):
        before.require_unchanged()


def test_root_owned_readonly_mac_file_without_immutable_flag_is_not_enough(tmp_path, monkeypatch):
    fixture_node(tmp_path, monkeypatch, "macmini")
    install(bootstrap.build_plan("macmini", SHA, "d" * 64))
    monkeypatch.setattr(binding, "atomic_replace_protected", lambda path: False)
    with pytest.raises(binding.BindingError, match="atomic_replace"):
        binding.inspect_installed("macmini")


def test_unapproved_release_and_wrong_paths_rejected(tmp_path, monkeypatch):
    spec = fixture_node(tmp_path, monkeypatch)
    install(bootstrap.build_plan("vps1", SHA, "d" * 64))
    with pytest.raises(binding.BindingError, match="release_not_approved"):
        binding.check_transition("vps1", spec.runtime_root, spec.release_root, spec.python, "b" * 40, SHA)
    with pytest.raises(binding.BindingError, match="paths_mismatch"):
        binding.check_transition("vps1", tmp_path / "other", spec.release_root, spec.python, SHA, SHA)


def test_approved_sha_with_rewritten_manifest_is_rejected(tmp_path, monkeypatch):
    spec = fixture_node(tmp_path, monkeypatch)
    install(bootstrap.build_plan("vps1", SHA, "d" * 64))
    path = spec.release_root / SHA / ".release-manifest.json"
    path.chmod(0o600)
    path.write_text("{}")
    path.chmod(0o444)
    with pytest.raises(guard.StartupGuardError, match="digest_mismatch"):
        binding.check_transition("vps1", spec.runtime_root, spec.release_root, spec.python, SHA, SHA)


@pytest.mark.parametrize("damage", ["hook", "duplicate_exec", "wrong_user"])
def test_later_systemd_override_is_detected(tmp_path, monkeypatch, damage):
    fixture_node(tmp_path, monkeypatch)
    runner = FakeRunner("vps1")
    original = runner.run
    def overridden(args):
        result = original(args)
        if damage == "hook" and args[-1] == "--property=ExecStartPre":
            return "old release hook"
        if damage == "duplicate_exec" and args[-1] == "--property=ExecStart":
            return result + result
        if damage == "wrong_user" and args[-1] == "--property=User":
            return "root"
        return result
    runner.run = overridden
    with pytest.raises(binding.BindingError):
        binding.verify_effective_vps("vps1", runner)


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS file flags")
def test_real_mac_file_flag_blocks_legacy_wrapper_atomic_replace(tmp_path):
    from platforms.predictfun.deploy_ws_relay import _atomic_write
    target = tmp_path / "only-this-synthetic-launch-agent.plist"
    target.write_bytes(b"protected synthetic plist")
    try:
        os.chflags(target, stat.UF_IMMUTABLE)
        with pytest.raises(PermissionError):
            _atomic_write(target, b"legacy replacement", 0o644)
        assert target.read_bytes() == b"protected synthetic plist"
    finally:
        os.chflags(target, 0)
