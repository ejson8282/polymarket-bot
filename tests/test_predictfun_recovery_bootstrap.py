from dataclasses import replace
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from platforms.predictfun import recovery_bootstrap as bootstrap
from platforms.predictfun import recovery_service_binding as binding
from platforms.predictfun import recovery_startup_guard as guard


SHA = "a" * 40


def fixture_node(tmp_path, monkeypatch, profile="vps1"):
    spec = replace(guard.PROFILES[profile], security_root=tmp_path / "security",
                   runtime_root=tmp_path / "runtime", release_root=tmp_path / "releases",
                   python=Path("/synthetic/python"))
    monkeypatch.setitem(guard.PROFILES, profile, spec)
    monkeypatch.setattr(guard, "TRUSTED_UID", os.getuid())
    monkeypatch.setattr(guard, "TRUST_BOUNDARY", tmp_path)
    monkeypatch.setattr(bootstrap.os, "geteuid", lambda: 0)
    monkeypatch.setattr(binding, "SYSTEMD_ROOT", tmp_path / "systemd")
    monkeypatch.setattr(binding, "MAC_HOME", tmp_path / "mac-home")
    monkeypatch.setitem(bootstrap.LOCKS, profile, tmp_path / "deploy.lock")
    monkeypatch.setitem(bootstrap.BACKUPS, profile, tmp_path / "backups")
    monkeypatch.setattr(guard, "RUNNING_GUARD", spec.security_root / "guard.py")
    release = spec.release_root / SHA
    monkeypatch.setattr(bootstrap, "BOOTSTRAP_SOURCE", release / "platforms/predictfun/recovery_bootstrap.py")
    spec.runtime_root.mkdir()
    binding.SYSTEMD_ROOT.mkdir()
    (binding.MAC_HOME / "Library/LaunchAgents").mkdir(parents=True)
    entries = [value[0] for value in spec.components.values()]
    entries += [bootstrap.GUARD_SOURCE, "platforms/predictfun/recovery_bootstrap.py"]
    files = {}
    for entry in entries:
        path = release / entry
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = b"synthetic reviewed artifact\n"
        path.write_bytes(raw)
        path.chmod(0o444)
        files[entry] = guard._sha(raw)
    manifest = {"source_repository": guard.REPOSITORY, "artifact": spec.artifact,
                "commit": SHA, "files": files}
    (release / ".release-manifest.json").write_text(json.dumps(manifest))
    (release / ".release-manifest.json").chmod(0o444)
    for path in sorted(release.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir():
            path.chmod(0o555)
    release.chmod(0o555)
    (spec.release_root / "current").symlink_to(release)
    flags = set()
    monkeypatch.setattr(bootstrap.os, "chflags", lambda p, *a, **k: flags.add(p), raising=False)
    monkeypatch.setattr(binding, "atomic_replace_protected", lambda p: p in flags)
    return spec


@pytest.fixture(autouse=True)
def cleanup_permissions(tmp_path):
    yield
    for root, directories, _ in os.walk(tmp_path, followlinks=False):
        for name in directories:
            path = Path(root) / name
            if not path.is_symlink():
                path.chmod(0o700)


class FakeRunner(bootstrap.Runner):
    def __init__(self, profile):
        self.profile = profile
        self.calls = []
        self.damage = None

    def result(self, args):
        self.calls.append(tuple(args))
        if args[0] == "pgrep" or args[0] == "/usr/sbin/lsof":
            return SimpleNamespace(returncode=1, stdout="", stderr="")
        if args[:2] == ("launchctl", "print"):
            return SimpleNamespace(returncode=113, stdout="", stderr="Could not find service")
        raise AssertionError(args)

    def run(self, args):
        self.calls.append(tuple(args))
        if args[:3] == ("ip", "-j", "address"):
            return json.dumps([{"addr_info": [{"local": bootstrap.NODE_IPS[self.profile]}]}])
        if args[0].endswith("/Tailscale"):
            return bootstrap.NODE_IPS[self.profile]
        if args[:2] == ("launchctl", "print-disabled"):
            return "\n".join('"' + label + '" => true' for label in binding.LABELS.values())
        if args[:2] == ("systemctl", "daemon-reload"):
            return ""
        if args[:2] == ("systemctl", "show"):
            prop = args[-1].removeprefix("--property=")
            if prop == "LoadState,ActiveState,MainPID,UnitFileState":
                return "LoadState=loaded\nActiveState=inactive\nMainPID=0\nUnitFileState=disabled"
            if prop == "ExecStart":
                component = next(k for k, v in binding.UNITS.items() if v == args[2])
                cmd = " ".join(binding.command(self.profile, component))
                return "{ path=/synthetic/python ; argv[]=" + cmd + " ; ignore_errors=no ; pid=0 ; }"
            return "ubuntu" if prop == "User" else ""
        if "--component" in args:
            component = args[args.index("--component") + 1]
            result = guard.verify_startup(self.profile, component)
            return json.dumps(result)
        raise AssertionError(args)


def install(plan, runner=None):
    return bootstrap.apply_plan(plan, plan_sha256=guard._sha(bootstrap._encode(plan)),
        authorization_id="synthetic-approval", confirmation=f"INSTALL_PREDICT_RECOVERY:{plan['profile']}:{SHA}",
        runner=runner or FakeRunner(plan["profile"]))


@pytest.mark.parametrize("profile", ["vps1", "vps2", "macmini"])
def test_install_then_verify_without_starting_or_changing_current(tmp_path, monkeypatch, profile):
    spec = fixture_node(tmp_path, monkeypatch, profile)
    plan = bootstrap.build_plan(profile, SHA, "d" * 64)
    assert not spec.security_root.exists()
    runner = FakeRunner(profile)
    result = install(plan, runner)
    assert result["status"] == "installed_services_stopped" and result["activation_allowed"] is False
    assert (spec.release_root / "current").resolve() == spec.release_root / SHA
    assert binding.check_transition(profile, spec.runtime_root, spec.release_root, spec.python, SHA, SHA)
    assert not any(any(word in call for word in ("start", "restart", "bootstrap", "kickstart", "enable"))
                   for call in runner.calls)
    assert not any("--exec" in call for call in runner.calls)
    for component in spec.components:
        assert guard.verify_startup(profile, component)["release_sha"] == SHA
    check_calls = [call for call in runner.calls if "--component" in call]
    assert len(check_calls) == 2
    expected_user = "kevinsmacmini" if profile == "macmini" else "ubuntu"
    assert all(call[call.index("-u") + 1] == expected_user for call in check_calls)
    with pytest.raises(bootstrap.BootstrapError, match="partially_installed"):
        bootstrap.build_plan(profile, SHA, "d" * 64)


def test_root_umask_does_not_make_guard_directory_unreadable(tmp_path, monkeypatch):
    spec = fixture_node(tmp_path, monkeypatch)
    plan = bootstrap.build_plan("vps1", SHA, "d" * 64)
    previous_umask = os.umask(0o077)
    try:
        install(plan)
    finally:
        os.umask(previous_umask)
    assert spec.security_root.stat().st_mode & 0o777 == 0o755


def test_failed_unprivileged_guard_check_does_not_record_acceptance(tmp_path, monkeypatch):
    fixture_node(tmp_path, monkeypatch)
    plan = bootstrap.build_plan("vps1", SHA, "d" * 64)
    runner = FakeRunner("vps1")
    original = runner.run
    def cannot_read(args):
        if args[0] == "/usr/sbin/runuser":
            raise bootstrap.BootstrapError("synthetic service user cannot read")
        return original(args)
    runner.run = cannot_read
    with pytest.raises(bootstrap.BootstrapError):
        install(plan, runner)
    assert not list(bootstrap.BACKUPS["vps1"].glob("*/installed.json"))


@pytest.mark.parametrize("damage", ["hash", "authorization", "wrong_confirmation", "not_root", "checkout", "changed_binding"])
def test_apply_rejects_unreviewed_inputs_before_protection_writes(tmp_path, monkeypatch, damage):
    spec = fixture_node(tmp_path, monkeypatch)
    plan = bootstrap.build_plan("vps1", SHA, "d" * 64)
    args = dict(plan_sha256=guard._sha(bootstrap._encode(plan)), authorization_id="test",
                confirmation=f"INSTALL_PREDICT_RECOVERY:vps1:{SHA}", runner=FakeRunner("vps1"))
    if damage == "hash":
        args["plan_sha256"] = "b" * 64
    elif damage == "authorization":
        args["authorization_id"] = ""
    elif damage == "wrong_confirmation":
        args["confirmation"] = f"INSTALL_PREDICT_RECOVERY:vps2:{SHA}"
    elif damage == "not_root":
        monkeypatch.setattr(bootstrap.os, "geteuid", lambda: 501)
    elif damage == "checkout":
        monkeypatch.setattr(bootstrap, "BOOTSTRAP_SOURCE", tmp_path / "checkout/bootstrap.py")
    else:
        path = next(iter(binding.binding_files("vps1")))
        path.parent.mkdir()
        path.write_text("new override")
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.apply_plan(plan, **args)
    assert not spec.security_root.exists()


@pytest.mark.parametrize("damage", ["wrong_host", "active", "enabled", "missing_pid", "manual", "probe_error"])
def test_independent_stopped_checks_are_required(tmp_path, monkeypatch, damage):
    spec = fixture_node(tmp_path, monkeypatch)
    plan = bootstrap.build_plan("vps1", SHA, "d" * 64)
    runner = FakeRunner("vps1")
    original_run, original_result = runner.run, runner.result
    def changed_run(args):
        text = original_run(args)
        if damage == "wrong_host" and args[0] == "ip":
            return "[]"
        if args[-1] == "--property=LoadState,ActiveState,MainPID,UnitFileState":
            if damage == "active":
                return text.replace("inactive", "active")
            if damage == "enabled":
                return text.replace("disabled", "enabled")
            if damage == "missing_pid":
                return text.replace("MainPID=0\n", "")
        return text
    def changed_result(args):
        if args[0] == "pgrep" and damage in {"manual", "probe_error"}:
            return SimpleNamespace(returncode=0 if damage == "manual" else 2, stdout="123", stderr="")
        return original_result(args)
    runner.run, runner.result = changed_run, changed_result
    with pytest.raises(bootstrap.BootstrapError):
        install(plan, runner)
    assert not spec.security_root.exists()


def test_failed_effective_verification_keeps_protection_and_does_not_restart(tmp_path, monkeypatch):
    spec = fixture_node(tmp_path, monkeypatch)
    plan = bootstrap.build_plan("vps1", SHA, "d" * 64)
    runner = FakeRunner("vps1")
    original = runner.run
    runner.run = lambda args: "unverified-old-command" if args[-1] == "--property=ExecStart" else original(args)
    with pytest.raises(binding.BindingError):
        install(plan, runner)
    assert spec.security_root.exists()
    assert binding.inspect_installed("vps1") is not None
    assert not any("start" in call or "restart" in call for call in runner.calls)
    assert list(bootstrap.BACKUPS["vps1"].glob("*/plan.json"))
    assert not list(bootstrap.BACKUPS["vps1"].glob("*/installed.json"))


@pytest.mark.parametrize("damage", ["not_disabled", "loaded", "port_busy", "lsof_error"])
def test_mac_requires_disabled_unloaded_and_drained(tmp_path, monkeypatch, damage):
    fixture_node(tmp_path, monkeypatch, "macmini")
    runner = FakeRunner("macmini")
    run, result = runner.run, runner.result
    runner.run = lambda args: run(args).replace("true", "false") if damage == "not_disabled" else run(args)
    def altered(args):
        if args[:2] == ("launchctl", "print") and damage == "loaded":
            return SimpleNamespace(returncode=0, stdout="state = not running", stderr="")
        if args[0] == "/usr/sbin/lsof" and damage in {"port_busy", "lsof_error"}:
            return SimpleNamespace(returncode=0 if damage == "port_busy" else 2, stdout="", stderr="")
        return result(args)
    runner.result = altered
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.require_stopped("macmini", runner)
