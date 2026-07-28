from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUSHER_PATH = ROOT / "deploy" / "latitude-console" / "alert_pusher.py"

spec = importlib.util.spec_from_file_location("latitude_alert_pusher", PUSHER_PATH)
pusher = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(pusher)


def test_digest_uses_reconciled_capital_not_equity_history_last() -> None:
    line = pusher._equity_digest_line({
        "capital": {
            "complete": True,
            "current_equity": 2023.28,
            "pnl": 25.91,
            "pnl_pct": 1.30,
        },
        "equity_history": {"present": True, "valid": True, "last": 9999, "change": 5},
    })

    assert line == "总权益 $2,023.28 · 相对投入 ▲+$25.91 (+1.30%)"
    assert "9999" not in line


def test_digest_does_not_report_a_curve_when_capital_is_incomplete() -> None:
    line = pusher._equity_digest_line({
        "capital": {"complete": False, "reason": "待对账"},
        "equity_history": {"present": True, "valid": True, "last": 25.91},
    })

    assert line == "权益对账暂不可用(本金账本未完成)"


def test_ipo_digest_is_part_of_the_shared_report() -> None:
    lines = pusher._ipo_digest_lines({
        "present": True,
        "active_stocks": 1,
        "stocks": [{
            "status": "申购中",
            "code": "2523",
            "name_zh": "永康控股有限公司",
            "fee": 1000,
            "lockup_cost_hkd": 0.45,
            "ai_verdict": "观望",
        }],
    })

    assert lines == [
        "港股打新 · 1 只申购中",
        "· 2523 永康控股有限公司 · HK$1,000 · 锁资磨损~HK$0 · 判研：观望",
    ]


def test_ipo_digest_handles_empty_and_unreachable_sources() -> None:
    assert pusher._ipo_digest_lines({"present": True, "stocks": [], "active_stocks": 0}) == [
        "港股打新：当前无申购中新股"
    ]
    assert pusher._ipo_digest_lines({"present": False}) == ["港股打新：数据暂不可用"]


def test_persistent_alert_is_written_to_cross_project_event_log(
    monkeypatch, tmp_path: Path
) -> None:
    alert = {
        "sev": "warn",
        "tag": "GRID",
        "msg": "<b>网格 BTC 停机</b>:行情超时",
        "page": "grid",
    }
    text = "[GRID]🟡 网格 BTC 停机:行情超时"
    fingerprint = hashlib.sha1(text.encode()).hexdigest()[:16]
    state_path = tmp_path / "push-state.json"
    event_path = tmp_path / "system-events.jsonl"
    audit_path = tmp_path / "audit.jsonl"
    state_path.write_text(
        json.dumps(
            {
                "alerts": {
                    fingerprint: {
                        "first_seen": 1,
                        "pushed": False,
                        "text": text,
                        "sev": "warn",
                    }
                },
                "audit_offset": 0,
            }
        ),
        encoding="utf-8",
    )

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return json.dumps({"alerts": [alert]}).encode()

    class _Opener:
        def open(self, *_args, **_kwargs):
            return _Response()

    monkeypatch.setattr(pusher, "_opener", _Opener())
    monkeypatch.setattr(pusher, "PUSH_STATE", state_path)
    monkeypatch.setattr(pusher, "SYSTEM_EVENT_LOG", event_path)
    monkeypatch.setattr(pusher, "AUDIT_LOG", audit_path)
    monkeypatch.setattr(pusher.time, "time", lambda: 1000)
    monkeypatch.setattr(pusher, "send_routed", lambda *_args, **_kwargs: None)

    pusher.run_alerts()

    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert event["project"] == "grid"
    assert event["page"] == "grid"
    assert event["sev"] == "warn"
    assert "网格 BTC 停机" in event["msg"]


def test_legacy_alert_state_recovers_project_tag() -> None:
    assert pusher._legacy_alert_tag({"text": "[VAR/DEC]🔴 单腿风险"}) == "VAR/DEC"
    assert pusher._legacy_alert_tag({"tag": "GRID", "text": "[OLD] text"}) == "GRID"


def test_discord_channels_use_separate_files_with_legacy_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    legacy = tmp_path / "legacy.txt"
    normal = tmp_path / "normal.txt"
    important = tmp_path / "important.txt"
    legacy.write_text("https://discord.com/api/webhooks/legacy/token", encoding="utf-8")
    normal.write_text("https://discord.com/api/webhooks/normal/token", encoding="utf-8")
    important.write_text("https://discord.com/api/webhooks/important/token", encoding="utf-8")
    monkeypatch.setattr(pusher, "DISCORD_LEGACY_FILE", legacy)
    monkeypatch.setattr(pusher, "DISCORD_NORMAL_FILE", normal)
    monkeypatch.setattr(pusher, "DISCORD_IMPORTANT_FILE", important)

    assert pusher._discord_webhook("normal") == normal.read_text(encoding="utf-8")
    assert pusher._discord_webhook("important") == important.read_text(encoding="utf-8")

    normal.unlink()
    important.unlink()
    assert pusher._discord_webhook("normal") == legacy.read_text(encoding="utf-8")
    assert pusher._discord_webhook("important") == legacy.read_text(encoding="utf-8")


def test_alerts_route_only_to_important_discord(monkeypatch) -> None:
    discord_calls: list[tuple[str, str]] = []
    feishu_calls: list[str] = []
    monkeypatch.setattr(
        pusher,
        "send_discord",
        lambda text, *, channel="normal": discord_calls.append((channel, text)),
    )
    monkeypatch.setattr(pusher, "send_feishu", lambda text: feishu_calls.append(text))

    pusher.send_routed("warn", "warning")
    pusher.send_routed("crit", "critical")

    assert discord_calls == [
        ("important", "warning"),
        ("important", "critical"),
    ]
    assert feishu_calls == ["critical"]
