from __future__ import annotations

import importlib.util
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "deploy" / "latitude-console" / "console_app.py"
HTML_PATH = ROOT / "deploy" / "latitude-console" / "console.html"

spec = importlib.util.spec_from_file_location("latitude_console_app", APP_PATH)
console = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(console)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _venue(*, ok: bool, side: Optional[str] = None, size: str = "0") -> dict:
    symbols = {}
    if side is not None:
        symbols["SOL"] = {
            "position": {"side": side, "size": size, "entry_price": "100"}
        }
    return {"ok": ok, "balance": {"total_equity": "100"}, "symbols": symbols}


def _state(host: str, generated_at: str, decibel: dict, variational: dict) -> dict:
    return {
        "host_id": host,
        "generated_at": generated_at,
        "exchanges": {"decibel": decibel, "variational": variational},
    }


def _patch_varia_dependencies(monkeypatch, data_dir: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "_varia_trades_today", lambda: {"present": False})
    monkeypatch.setattr(console, "_varia_budget", lambda _: {"present": False})
    monkeypatch.setattr(console, "_equity_history", lambda: {"present": False})


def test_var_decibel_only_classifies_fresh_complete_sources(monkeypatch, tmp_path: Path) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc)
    _write_json(
        tmp_path / "ops_state.json",
        _state("vps1", now.isoformat(), _venue(ok=True), _venue(ok=True)),
    )
    _write_json(
        tmp_path / "ops_peer_state" / "vps2.json",
        _state(
            "vps2",
            (now - timedelta(hours=1)).isoformat(),
            _venue(ok=True, side="sell", size="-0.473"),
            _venue(ok=True, side="buy", size="0.473"),
        ),
    )

    result = console._var_decibel()

    assert result["pairs"] == []
    assert result["single_leg"] == []
    assert result["position_sources"]["verified_hosts"] == ["vps1"]
    assert result["position_sources"]["unverified"] == [
        {
            "host": "vps2",
            "age": "60m 前",
            "reason": "快照过期",
            "last_seen_symbols": ["SOL"],
        }
    ]


def test_var_decibel_does_not_report_single_leg_when_one_venue_failed(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    _write_json(
        tmp_path / "ops_state.json",
        _state(
            "vps1",
            datetime.now(timezone.utc).isoformat(),
            _venue(ok=True, side="sell", size="-0.5"),
            _venue(ok=False),
        ),
    )

    result = console._var_decibel()

    assert result["pairs"] == []
    assert result["single_leg"] == []
    assert result["position_sources"]["unverified"][0]["reason"] == "交易所读取不完整"
    assert result["position_sources"]["unverified"][0]["last_seen_symbols"] == ["SOL"]


def test_stopped_polymarket_engine_does_not_claim_historical_orders(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)
    monkeypatch.setattr(console, "PM_PEER_DIR", tmp_path / "pm_peer")
    monkeypatch.setattr(console, "_load_pm_remotes", lambda: {})
    state_path = tmp_path / "engine_state_1.json"
    _write_json(
        state_path,
        {"markets": {"m1": {"live_orders": [{"id": "old"}]}}, "balance": 100},
    )
    old = time.time() - 3600
    os.utime(state_path, (old, old))

    result = console._polymarket()

    assert result["live_orders"] is None
    assert result["orders_unknown"] is True
    assert result["accounts"][0]["orders"] is None
    assert result["accounts"][0]["orders_last_seen"] == 1
    assert result["accounts"][0]["state_stale"] is True
    assert result["accounts"][0]["balance"] is None
    assert result["accounts"][0]["volume_today"] is None


def test_running_polymarket_engine_uses_fresh_order_count(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)
    monkeypatch.setattr(console, "PM_PEER_DIR", tmp_path / "pm_peer")
    monkeypatch.setattr(console, "_load_pm_remotes", lambda: {})
    _write_json(
        tmp_path / "engine_state_1.json",
        {"markets": {"m1": {"live_orders": [{"id": "current"}]}}, "balance": 100},
    )
    (tmp_path / ".engine_1.pid").write_text(str(os.getpid()), encoding="utf-8")

    result = console._polymarket()

    assert result["live_orders"] == 1
    assert result["orders_unknown"] is False
    assert result["accounts"][0]["orders"] == 1
    assert result["accounts"][0]["orders_verified"] is True


def test_stale_polymarket_pid_file_is_not_treated_as_running(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)
    monkeypatch.setattr(console, "PM_PEER_DIR", tmp_path / "pm_peer")
    monkeypatch.setattr(console, "_load_pm_remotes", lambda: {})
    _write_json(
        tmp_path / "engine_state_1.json",
        {"markets": {"m1": {"live_orders": [{"id": "last-seen"}]}}},
    )
    (tmp_path / ".engine_1.pid").write_text("999999999", encoding="utf-8")

    result = console._polymarket()

    assert result["running"] == 0
    assert result["accounts"][0]["status"] == "已停止"
    assert result["accounts"][0]["orders"] is None


def test_console_html_contains_no_trading_status_samples_or_dead_buttons() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    for fake in (
        "SOL 单腿:Decibel 空腿裸露 $88",
        "ETH 开仓成功,双腿对齐",
        "−$88.40",
        "暂停 VPS1 自动化",
        "暂停 VPS2 自动化",
        "一键平仓…",
        "示例数据(真数据接入中)",
        "下方为模板样例",
        "5 · 今日 12 笔",
        "NBA · LAL vs BOS",
        "HK-02 · 84ms",
        "#2 BUY 120@0.42",
        "VPS2 · SOL 单腿",
        "$2,418.62",
        "2/2 运行",
        "34 · $1,240",
        "US Election 子盘 A",
    ):
        assert fake not in html
    assert "无真数据宁可显示未知" in html
    assert "旧 Var/Decibel worker 已退出生产" in html
    assert 'id="alertbar" style="display:none"' in html
