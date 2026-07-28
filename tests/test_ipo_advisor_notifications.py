from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load(script_name: str):
    path = ROOT / "deploy" / "latitude-console" / script_name
    spec = importlib.util.spec_from_file_location("latitude_" + script_name.replace(".", "_"), path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("script_name", ["ipo_advisor.py", "ipo_advisor_win.py"])
def test_brief_does_not_push_a_separate_discord_message_by_default(
    monkeypatch, capsys, script_name: str,
) -> None:
    module = _load(script_name)
    monkeypatch.setattr(module, "build_pack", lambda: {"stocks": [], "active_count": 0})

    module.brief()

    assert "当前无「申购中」新股" in capsys.readouterr().out


@pytest.mark.parametrize("script_name", ["ipo_advisor.py", "ipo_advisor_win.py"])
def test_ipo_has_no_standalone_discord_route(script_name: str) -> None:
    source = (
        ROOT / "deploy" / "latitude-console" / script_name
    ).read_text(encoding="utf-8")

    assert "IPO_STANDALONE_DISCORD" not in source
    assert "discord_webhook.txt" not in source
    assert "def _discord(" not in source
