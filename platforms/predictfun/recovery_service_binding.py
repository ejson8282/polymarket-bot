"""Fixed Predict startup bindings, checked without starting any service."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import plistlib
import stat

from platforms.predictfun import recovery_startup_guard as guard


class BindingError(RuntimeError):
    pass


MAC_HOME = Path("/Users/kevinsmacmini")
SYSTEMD_ROOT = Path("/etc/systemd/system")
LABELS = {"api": "ai.codex.predictfun-api-proxy", "relay": "ai.codex.predictfun-ws-relay"}
UNITS = {"runner": "predictfun-dryrun.service", "ws": "predictfun-ws.service"}
DROPIN = "zzzz-predict-recovery.conf"


def atomic_replace_protected(path: Path) -> bool:
    return bool(getattr(path.lstat(), "st_flags", 0) & stat.UF_IMMUTABLE)


def command(profile: str, component: str) -> list[str]:
    spec = guard.PROFILES[profile]
    if component not in spec.components:
        raise BindingError("binding_component_invalid")
    return [str(spec.python), "-I", "-B", str(spec.security_root / "guard.py"),
            "--profile", profile, "--component", component, "--exec"]


def binding_files(profile: str) -> dict[Path, bytes]:
    spec = guard.PROFILES[profile]
    result = {}
    for component in spec.components:
        if profile == "macmini":
            label = LABELS[component]
            log_name = label.removeprefix("ai.codex.")
            content = {"Label": label, "ProgramArguments": command(profile, component),
                       "WorkingDirectory": str(spec.security_root), "RunAtLoad": True,
                       "KeepAlive": True, "ProcessType": "Background", "ThrottleInterval": 2,
                       "StandardOutPath": str(MAC_HOME / f"Library/Logs/{log_name}.out.log"),
                       "StandardErrorPath": str(MAC_HOME / f"Library/Logs/{log_name}.err.log")}
            result[MAC_HOME / f"Library/LaunchAgents/{label}.plist"] = plistlib.dumps(content)
        else:
            # Clear every legacy executable hook, not just the old main command.
            content = "[Service]\nUser=ubuntu\nWorkingDirectory=" + str(spec.security_root) + "\n"
            content += "ExecCondition=\nExecStartPre=\nExecStartPost=\nExecStop=\nExecStopPost=\nExecStart=\n"
            content += "ExecStart=" + " ".join(command(profile, component)) + "\n"
            result[SYSTEMD_ROOT / (UNITS[component] + ".d") / DROPIN] = content.encode()
    return result


@dataclass(frozen=True)
class InstalledBindings:
    profile: str
    anchor_digest: str
    manifests: tuple[tuple[str, str], ...]

    def require_unchanged(self) -> None:
        if inspect_installed(self.profile) != self:
            raise BindingError("installed_bindings_changed")


def inspect_installed(profile: str) -> InstalledBindings | None:
    """Only absence of the entire root-owned installation is legacy mode."""
    spec = guard.PROFILES[profile]
    try:
        spec.security_root.lstat()
    except FileNotFoundError:
        return None
    try:
        raw = guard._read(spec.security_root / "anchor.json", protected=True, limit=65536)
        anchor = guard._json(raw)
        guard_raw = guard._read(spec.security_root / "guard.py", protected=True)
        if (type(anchor.get("version")) is not int or anchor["version"] != 1
                or anchor.get("repository") != guard.REPOSITORY or anchor.get("profile") != profile
                or anchor.get("guard_sha256") != guard._sha(guard_raw)):
            raise BindingError("binding_anchor_invalid")
        policy = guard._read(spec.runtime_root / "recovery-release-floor/policy.json", limit=65536)
        if anchor.get("policy_sha256") != guard._sha(policy):
            raise BindingError("binding_policy_changed")
        approved = anchor.get("approved_manifests")
        if (not isinstance(approved, dict) or not 1 <= len(approved) <= 100
                or any(not guard._hex(k, 40) or not guard._hex(v, 64) for k, v in approved.items())):
            raise BindingError("binding_approval_invalid")
        expected = binding_files(profile)
        hashes = {str(path): guard._sha(content) for path, content in expected.items()}
        if anchor.get("service_bindings") != hashes:
            raise BindingError("binding_manifest_invalid")
        for path, content in expected.items():
            actual = guard._read(path, protected=profile != "macmini")
            info = path.lstat()
            if info.st_uid != guard.TRUSTED_UID or actual != content:
                raise BindingError("binding_content_changed")
            if profile == "macmini" and not atomic_replace_protected(path):
                raise BindingError("binding_atomic_replace_not_protected")
        return InstalledBindings(profile, guard._sha(raw), tuple(sorted(approved.items())))
    except (OSError, ValueError, TypeError, guard.StartupGuardError) as exc:
        raise BindingError("binding_evidence_unavailable") from exc


def check_transition(profile: str, runtime_root: Path, release_root: Path, python: Path,
                     target: str, previous: str | None) -> InstalledBindings | None:
    installed = inspect_installed(profile)
    if installed is not None:
        spec = guard.PROFILES[profile]
        if (runtime_root, release_root, python) != (spec.runtime_root, spec.release_root, spec.python):
            raise BindingError("binding_deployment_paths_mismatch")
        manifests = dict(installed.manifests)
        if target not in manifests or previous not in manifests:
            raise BindingError("binding_release_not_approved")
        for sha in {target, previous}:
            guard.verify_artifact(profile, release_root / sha, manifests[sha])
        installed.require_unchanged()
    return installed


def verify_effective_vps(profile: str, runner) -> None:
    """Validate systemd's effective command, including later override files."""
    for component, unit in UNITS.items():
        for hook in ("ExecCondition", "ExecStartPre", "ExecStartPost", "ExecStop", "ExecStopPost"):
            if runner.run(("systemctl", "show", unit, "--value", "--property=" + hook)).strip():
                raise BindingError("binding_unexpected_effective_hook")
        value = runner.run(("systemctl", "show", unit, "--value", "--property=ExecStart"))
        fields = value.split("argv[]=")
        expected = " ".join(command(profile, component))
        if len(fields) != 2 or fields[1].split(" ;", 1)[0].strip() != expected:
            raise BindingError("binding_effective_command_mismatch")
        if runner.run(("systemctl", "show", unit, "--value", "--property=User")) != "ubuntu":
            raise BindingError("binding_effective_user_mismatch")
