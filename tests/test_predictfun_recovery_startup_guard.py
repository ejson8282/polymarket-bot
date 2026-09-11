from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest

from platforms.predictfun import recovery_startup_guard as guard


SHA = "a" * 40


def frozen(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(raw)
    path.chmod(0o444)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture(autouse=True)
def restore_test_permissions(tmp_path):
    yield
    # Only our own pytest directory; never sweep other runs or production paths.
    tmp_path.chmod(0o700)
    for root, directories, _files in os.walk(tmp_path, followlinks=False):
        for name in directories:
            path = Path(root) / name
            if not path.is_symlink():
                path.chmod(0o700)


def installed(tmp_path, monkeypatch, profile="vps1"):
    spec = replace(guard.PROFILES[profile], security_root=tmp_path / "security",
                   runtime_root=tmp_path / "runtime", release_root=tmp_path / "releases",
                   python=Path("/synthetic/venv/python"))
    monkeypatch.setitem(guard.PROFILES, profile, spec)
    monkeypatch.setattr(guard, "TRUSTED_UID", os.getuid())
    monkeypatch.setattr(guard, "TRUST_BOUNDARY", tmp_path)
    script = spec.security_root / "guard.py"
    frozen(script, b"synthetic installed guard bytes\n")
    monkeypatch.setattr(guard, "RUNNING_GUARD", script)
    release = spec.release_root / SHA
    files = {}
    for entry, *_ in spec.components.values():
        raw = b"synthetic component\n"
        frozen(release / entry, raw)
        files[entry] = digest(raw)
    manifest = {"source_repository": guard.REPOSITORY, "artifact": spec.artifact,
                "commit": SHA, "files": files}
    manifest_raw = json.dumps(manifest).encode()
    frozen(release / ".release-manifest.json", manifest_raw)
    for path in sorted(release.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir():
            path.chmod(0o555)
    release.chmod(0o555)
    (spec.release_root / "current").symlink_to(release)
    policy = {"version": 1, "repository": guard.REPOSITORY, "profile": profile,
              "recovery_id": "d" * 64, "allowed_releases": [SHA]}
    policy_raw = json.dumps(policy).encode()
    frozen(spec.runtime_root / "recovery-release-floor/policy.json", policy_raw)
    anchor = {"version": 1, "repository": guard.REPOSITORY, "profile": profile,
              "guard_sha256": digest(script.read_bytes()), "policy_sha256": digest(policy_raw),
              "approved_manifests": {SHA: digest(manifest_raw)}}
    frozen(spec.security_root / "anchor.json", json.dumps(anchor).encode())
    return spec


@pytest.mark.parametrize("profile,component", [("vps1", "runner"), ("vps2", "runner"),
    ("vps1", "ws"), ("vps2", "ws"), ("macmini", "api"), ("macmini", "relay")])
def test_pinned_external_guard_checks_every_component(tmp_path, monkeypatch, profile, component):
    spec = installed(tmp_path, monkeypatch, profile)
    result = guard.verify_startup(profile, component)
    assert result["release_sha"] == SHA
    assert result["argv"][:4] == [str(spec.python), "-I", "-B",
                                  str(spec.release_root / SHA / spec.components[component][0])]


@pytest.mark.parametrize("damage", ["missing_anchor", "missing_policy", "missing_policy_directory",
    "old_current", "changed_script", "changed_manifest", "changed_component", "extra_file",
    "writable_anchor", "writable_security_dir", "anchor_symlink", "wrong_owner"])
def test_missing_or_replaced_protection_refuses_startup(tmp_path, monkeypatch, damage):
    spec = installed(tmp_path, monkeypatch)
    anchor = spec.security_root / "anchor.json"
    policy = spec.runtime_root / "recovery-release-floor/policy.json"
    release = spec.release_root / SHA
    if damage == "missing_anchor":
        anchor.unlink()
    elif damage in {"missing_policy", "missing_policy_directory"}:
        policy.unlink()
        if damage == "missing_policy_directory":
            policy.parent.rmdir()
    elif damage == "old_current":
        old = spec.release_root / ("b" * 40)
        old.mkdir()
        (spec.release_root / "current").unlink()
        (spec.release_root / "current").symlink_to(old)
    elif damage == "changed_script":
        frozen(spec.security_root / "guard.py", b"old unguarded script")
    elif damage == "changed_manifest":
        frozen(release / ".release-manifest.json", b"{}")
    elif damage == "changed_component":
        frozen(release / spec.components["runner"][0], b"modified code")
    elif damage == "extra_file":
        release.chmod(0o755)
        frozen(release / "extra.py", b"extra code")
        release.chmod(0o555)
    elif damage == "writable_anchor":
        anchor.chmod(0o644)
    elif damage == "writable_security_dir":
        spec.security_root.chmod(0o777)
    elif damage == "anchor_symlink":
        target = spec.security_root / "alternate.json"
        anchor.rename(target)
        anchor.symlink_to(target)
    else:
        monkeypatch.setattr(guard, "TRUSTED_UID", os.getuid() + 1)
    with pytest.raises(guard.StartupGuardError):
        guard.verify_startup("vps1", "runner")


def test_uninstalled_checkout_and_wrong_components_refused(tmp_path, monkeypatch):
    installed(tmp_path, monkeypatch)
    with pytest.raises(guard.StartupGuardError, match="component_not_allowed"):
        guard.verify_startup("vps1", "api")
    monkeypatch.setattr(guard, "RUNNING_GUARD", tmp_path / "checkout/guard.py")
    with pytest.raises(guard.StartupGuardError, match="external_installation"):
        guard.verify_startup("vps1", "runner")


def test_check_mode_never_executes(tmp_path, monkeypatch, capsys):
    installed(tmp_path, monkeypatch)
    monkeypatch.setattr(guard.os, "execve", lambda *a: pytest.fail("no execution in check mode"))
    assert guard.main(["--profile", "vps1", "--component", "runner"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["release_sha"] == SHA and "argv" not in result


def test_exec_only_uses_pinned_command_and_never_grants_live(tmp_path, monkeypatch):
    spec = installed(tmp_path, monkeypatch)
    monkeypatch.setenv("PREDICTFUN_LIVE_RELEASE_SHA", "old-authorization")
    monkeypatch.setenv("PYTHONPATH", "/untrusted/path")
    monkeypatch.delenv("PREDICTFUN_LIVE_TRADING", raising=False)
    monkeypatch.setattr(guard.os, "chdir", lambda path: None)
    calls = []
    monkeypatch.setattr(guard.os, "execve", lambda *args: calls.append(args))
    assert guard.main(["--profile", "vps1", "--component", "runner", "--exec"]) == 0
    binary, arguments, environment = calls[0]
    assert binary == str(spec.python) and arguments[1] == "-I"
    assert environment["PREDICTFUN_RELEASE_SHA"] == SHA
    assert environment["PREDICTFUN_LIVE_RELEASE_SHA"] == "old-authorization"
    assert "PREDICTFUN_LIVE_TRADING" not in environment and "PYTHONPATH" not in environment


def test_guard_has_only_standard_library_imports():
    import ast
    tree = ast.parse(Path(guard.__file__).read_text())
    imports = {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    imports |= {alias.name.split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.Import) for alias in node.names}
    assert imports <= {"__future__", "argparse", "dataclasses", "hashlib", "json", "os", "pathlib", "re", "stat"}
