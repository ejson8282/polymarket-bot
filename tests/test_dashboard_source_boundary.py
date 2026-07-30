from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FROZEN_DIR = ROOT / "deploy" / "latitude-console"


def test_legacy_dashboard_copy_is_explicitly_frozen() -> None:
    readme = (FROZEN_DIR / "README.md").read_text(encoding="utf-8")

    assert "禁止开发、禁止部署" in readme
    assert "ejson8282/latitude-alpha" in readme
    assert "唯一源码" in readme


def test_legacy_dashboard_deployer_fails_closed() -> None:
    script = FROZEN_DIR / "deploy_from_internal_main.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)

    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 64
    assert "DISABLED" in result.stderr
    assert "latitude-alpha" in result.stderr
