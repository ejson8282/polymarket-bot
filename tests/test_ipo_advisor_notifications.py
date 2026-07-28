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
    monkeypatch.delenv("IPO_STANDALONE_DISCORD", raising=False)
    monkeypatch.setattr(module, "build_pack", lambda: {"stocks": [], "active_count": 0})
    monkeypatch.setattr(
        module,
        "_discord",
        lambda _text: (_ for _ in ()).throw(AssertionError("standalone push must be disabled")),
    )

    module.brief()

    assert "当前无「申购中」新股" in capsys.readouterr().out


def test_legacy_standalone_push_can_be_enabled_explicitly(monkeypatch) -> None:
    module = _load("ipo_advisor_win.py")
    sent = []
    monkeypatch.setenv("IPO_STANDALONE_DISCORD", "1")
    monkeypatch.setattr(module, "build_pack", lambda: {"stocks": [], "active_count": 0})
    monkeypatch.setattr(module, "_discord", lambda text: sent.append(text) or True)

    module.brief()

    assert len(sent) == 1


@pytest.mark.parametrize("script_name", ["ipo_advisor.py", "ipo_advisor_win.py"])
def test_discord_prefers_normal_channel_and_falls_back_to_legacy(
    monkeypatch, tmp_path: Path, script_name: str,
) -> None:
    module = _load(script_name)
    normal = tmp_path / "discord_normal_webhook.txt"
    legacy = tmp_path / "discord_webhook.txt"
    legacy.write_text("legacy", encoding="utf-8")
    monkeypatch.setattr(module, "DISCORD_NORMAL_FILE", normal)
    monkeypatch.setattr(module, "DISCORD_LEGACY_FILE", legacy)

    assert module._discord_webhook_file() == legacy

    normal.write_text("normal", encoding="utf-8")

    assert module._discord_webhook_file() == normal
