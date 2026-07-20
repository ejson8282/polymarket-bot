from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
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
    monkeypatch.setattr(console, "VARIA_CAPITAL_LEDGER", data_dir / "home_equity_principal.json")
    monkeypatch.setattr(console, "VARIA_RECONCILED_PNL_HISTORY", data_dir / "reconciled_pnl_history.json")
    monkeypatch.setattr(console, "_varia_trades_today", lambda: {"present": False})
    monkeypatch.setattr(console, "_varia_budget", lambda _: {"present": False})
    monkeypatch.setattr(console, "_equity_history", lambda: {"present": False})


def test_account_ops_uses_persistent_last_good_snapshot(monkeypatch, tmp_path: Path) -> None:
    snapshot = tmp_path / "account_ops_last_good.json"
    payload = {
        "meta": {"as_of": "2026-07-20T15:00:00+08:00"},
        "accounts": [{"id": "HK-001"}],
    }
    _write_json(snapshot, payload)
    monkeypatch.setattr(console, "ACCOUNT_OPS_SNAPSHOT_PATH", snapshot)
    console._HTTP_CACHE.pop(console.ACCOUNT_OPS_URL, None)
    monkeypatch.setattr(
        console,
        "_do_fetch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("disk snapshot should avoid a cold Windows fetch")
        ),
    )

    assert console._fetch_json(console.ACCOUNT_OPS_URL) == payload


def test_account_ops_successful_write_merges_cache_without_refetch(
    monkeypatch, tmp_path: Path
) -> None:
    snapshot = tmp_path / "account_ops_last_good.json"
    monkeypatch.setattr(console, "ACCOUNT_OPS_SNAPSHOT_PATH", snapshot)
    console._HTTP_CACHE[console.ACCOUNT_OPS_URL] = (
        {"accounts": [{"id": "HK-001"}], "onboarding": {"profiles": []}},
        time.time(),
    )

    updated = {"profiles": [{"id": "profile-1"}]}
    console._merge_account_ops_cache("onboarding", updated)

    cached = console._HTTP_CACHE[console.ACCOUNT_OPS_URL][0]
    assert cached["accounts"] == [{"id": "HK-001"}]
    assert cached["onboarding"] == updated
    assert json.loads(snapshot.read_text(encoding="utf-8"))["onboarding"] == updated


def test_account_ops_rejects_late_older_snapshot(monkeypatch, tmp_path: Path) -> None:
    snapshot = tmp_path / "account_ops_last_good.json"
    newer = {
        "meta": {"as_of": "2026-07-20T19:57:00+08:00"},
        "onboarding": {"profiles": [{"id": "profile-1"}]},
    }
    older = {
        "meta": {"as_of": "2026-07-20T19:49:00+08:00"},
        "onboarding": {"profiles": []},
    }
    _write_json(snapshot, newer)
    monkeypatch.setattr(console, "ACCOUNT_OPS_SNAPSHOT_PATH", snapshot)
    console._HTTP_CACHE[console.ACCOUNT_OPS_URL] = (newer, time.time())

    console._store_http_cache(console.ACCOUNT_OPS_URL, older)

    assert console._HTTP_CACHE[console.ACCOUNT_OPS_URL][0] == newer
    assert json.loads(snapshot.read_text(encoding="utf-8")) == newer


def test_account_ops_exposes_real_alpha_accounts_and_booster_tasks(monkeypatch) -> None:
    monkeypatch.setattr(
        console,
        "_fetch_json",
        lambda _: {
            "meta": {"as_of": "2026-07-17T20:00:00+08:00"},
            "accounts": [
                {
                    "id": "BN-001",
                    "platform": "Binance Alpha",
                    "owner": "张三",
                    "currency": "USDT",
                    "capital": 1000,
                    "wear": 10,
                    "income": 120,
                    "status": "运行中",
                },
                {
                    "id": "HK-001",
                    "platform": "港股",
                    "owner": "张三",
                    "capital": 5000,
                    "wear": 0,
                    "income": 0,
                },
            ],
            "ledger": [
                {"account": "BN-001", "type": "本金 / 入金", "amount": 1000},
                {"account": "BN-001", "type": "本金 / 出金", "amount": -200},
                {"account": "BN-001", "type": "奖励 / Alpha 奖励", "amount": 70},
            ],
            "people": [{"name": "张三", "share": 0.2}],
            "reminders": {"summary": {}},
            "risks": [],
            "alpha_booster": {
                "updated_at": "2026-07-17T20:05:00+08:00",
                "tasks": [
                    {
                        "id": "alpha-test",
                        "title": "完成 Booster",
                        "accounts": [{"accountId": "BN-001", "status": "可领取"}],
                    }
                ],
            },
        },
    )

    result = console._account_ops()
    alpha = result["alpha"]

    assert alpha["account_count"] == 1
    assert alpha["accounts"][0]["deposits"] == 1000
    assert alpha["accounts"][0]["withdrawals"] == 200
    assert alpha["accounts"][0]["rewards"] == 70
    assert alpha["accounts"][0]["profit"] == 50
    assert alpha["accounts"][0]["net"] == 110
    assert alpha["active_tasks"] == 1
    assert alpha["claimable"] == 1


def test_console_contains_alpha_booster_workflow() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert "Binance Alpha · Booster" in html
    assert "待完成 → 已完成/等发奖 → 可领取 → 已领取" in html
    assert "/api/alpha/action" in html
    assert "领取后仍需通过飞书流水确认实际奖励" in html


def test_account_ops_exposes_onboarding_deadlines_and_capital(monkeypatch) -> None:
    monkeypatch.setattr(
        console,
        "_fetch_json",
        lambda _: {
            "meta": {"as_of": datetime.now().astimezone().isoformat()},
            "accounts": [{"id": "HK-001", "owner": "蒋星晨", "platform": "港股"}],
            "people": [{"name": "蒋星晨"}],
            "reminders": {"summary": {}},
            "risks": [],
            "onboarding": {
                "profiles": [
                    {
                        "id": "profile-1",
                        "person": "蒋星晨",
                        "institution": "富途证券",
                        "maskedEmail": "j***@example.com",
                    }
                ],
                "fundingPlans": [
                    {
                        "id": "fund-1",
                        "person": "蒋星晨",
                        "batchId": "batch-working-capital",
                        "batchName": "HK$10,000 周转金",
                        "sequence": 1,
                        "toInstitution": "富途证券",
                        "nextInstitution": "有鱼证券",
                        "amount": 10000,
                        "currency": "HKD",
                        "status": "锁资中",
                    },
                    {
                        "id": "fund-2",
                        "person": "蒋星晨",
                        "batchId": "batch-working-capital",
                        "batchName": "HK$10,000 周转金",
                        "sequence": 2,
                        "fromInstitution": "富途证券",
                        "toInstitution": "众安银行",
                        "nextInstitution": "有鱼证券",
                        "amount": 10000,
                        "currency": "HKD",
                        "status": "计划中",
                    }
                ],
                "records": [
                    {
                        "id": "onb-test",
                        "person": "蒋星晨",
                        "accountId": "HK-001",
                        "institution": "富途证券",
                        "institutionType": "券商",
                        "status": "锁资中",
                        "deadline": (datetime.now().astimezone() + timedelta(days=5)).date().isoformat(),
                        "depositAmount": 80000,
                        "currency": "HKD",
                        "holdDays": 60,
                        "rewardValue": 1200,
                        "rewardCurrency": "HKD",
                        "rewardTiers": [
                            {
                                "id": "reward-1",
                                "requirements": [
                                    {
                                        "id": "trades",
                                        "label": "交易 3 笔",
                                        "target": 3,
                                        "current": 1,
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
        },
    )

    onboarding = console._account_ops()["onboarding"]

    assert onboarding["active"] == 1
    assert onboarding["expiring_7d"] == 1
    assert onboarding["locked_capital"] == 10000
    assert onboarding["locked_capital_by_currency"] == {"HKD": 10000}
    assert onboarding["capital_batches"][0]["id"] == "batch-working-capital"
    assert onboarding["expected_rewards"] == 1200
    assert onboarding["expected_rewards_by_currency"] == {"HKD": 1200}
    assert onboarding["accounts"][0]["id"] == "HK-001"
    assert onboarding["profiles"][0]["maskedEmail"] == "j***@example.com"
    assert onboarding["funding_plans"][0]["nextInstitution"] == "有鱼证券"
    assert onboarding["records"][0]["reward_tiers"][0]["requirements"][0]["target"] == 3


def test_console_contains_onboarding_and_reward_workflow() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert 'data-page="onboarding"' not in html
    assert 'data-ops-page="onboarding"' in html
    assert "function goOps(p)" in html
    assert "opsNav.className='ops-subnav'" in html
    assert "page.replaceChildren(opsNav,kpis,head,workbench,alphaPanel,realGrid)" in html
    assert "onclick=\"goOps('onboarding')\">开户与奖励 →" in html
    assert 'id="page-onboarding"' in html
    assert "开户与奖励" in html
    assert "资金路径" in html
    onboarding_section = html.split('<section class="page" id="page-onboarding">', 1)[1]
    assert onboarding_section.index('class="kpis onb-overview-kpis"') < onboarding_section.index('class="onb-head"')
    assert 'data-onb-view="profiles"' in html
    assert 'data-onb-view="journey"' in html
    assert 'data-onb-view="activities"' not in html
    assert 'data-onb-view="pipeline"' not in html
    assert 'id="onb-journeys"' in html
    assert 'data-onb-group="journey"' in html
    assert "同一资金批次可经过银行与券商多个站点；本金只计算一次" in html
    assert 'id="onb-plan-batch-id"' in html
    assert 'id="onb-plan-sequence"' in html
    assert "upsert_profile" in html
    assert "账号编号与登录名完整展示" in html
    assert "填写完整账号编号" in html
    assert "填写完整登录名" in html
    assert "profile.accountId||'—'" not in html
    assert "profile.loginId?esc(profile.loginId)" in html
    assert "String(left.person||'').localeCompare(String(right.person||''),'zh-CN'" in html
    assert "String(left.institution||'').localeCompare(String(right.institution||''),'zh-CN'" in html
    assert "profileForm.querySelectorAll('input').forEach(input=>{input.value='';})" in html
    assert "再次点击才会永久删除" in html
    assert "正在删除并同步 Windows 数据源" in html
    assert "window.confirm('确认删除这份账号档案？')" not in html
    assert "upsert_funding_plan" in html
    assert "/api/onboarding/action" in html
    assert "不保存密码、身份证、银行卡完整号码等敏感信息" in html


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
    assert result["equity_total"] is None
    assert result["equity_complete"] is False
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
    assert result["equity_total"] is None
    assert result["equity_complete"] is False
    assert result["hosts"]["vps1"]["equity_dec"] == 100.0
    assert result["hosts"]["vps1"]["equity_var"] is None
    assert result["position_sources"]["unverified"][0]["reason"] == "交易所读取不完整"
    assert result["position_sources"]["unverified"][0]["last_seen_symbols"] == ["SOL"]


def test_var_decibel_reports_total_equity_only_when_all_sources_are_complete(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    states = []
    for host, dec_points, var_points, volume in (
        ("vps1", 1.0, 0.1, 10.0),
        ("vps2", 2.0, 0.2, 20.0),
    ):
        state = _state(host, now, _venue(ok=True), _venue(ok=True))
        state["exchanges"]["decibel"]["points"] = {"total_points": dec_points}
        state["exchanges"]["variational"]["points"] = {"total_points": var_points}
        state["trade_volume"] = {
            "ok": True,
            "venues": {
                "decibel": {
                    "weekly_notional_usdc": volume,
                    "total_notional_usdc": volume * 10,
                },
                "variational": {
                    "weekly_notional_usdc": volume,
                    "total_notional_usdc": volume * 10,
                },
            },
        }
        states.append(state)
    _write_json(
        tmp_path / "ops_state.json",
        states[0],
    )
    _write_json(
        tmp_path / "ops_peer_state" / "vps2.json",
        states[1],
    )

    result = console._var_decibel()

    assert result["equity_complete"] is True
    assert result["equity_total"] == 400.0
    assert result["points_complete"] == {"decibel": True, "variational": True}
    assert result["points_decibel"] == 3.0
    assert result["points_variational"] == 0.3
    assert result["points_by_venue"] == {
        "decibel": {"total": 3.0, "hosts": {"vps1": 1.0, "vps2": 2.0}, "complete": True},
        "variational": {"total": 0.3, "hosts": {"vps1": 0.1, "vps2": 0.2}, "complete": True},
    }
    assert result["volume_complete"] == {"weekly": True, "total": True}
    assert result["volume_weekly"] == 60.0
    assert result["volume_total"] == 600.0


def test_capital_accounting_separates_reconciled_cashflows_from_pnl(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc)
    for host in ("vps1", "vps2"):
        state = _state(host, now.isoformat(), _venue(ok=True), _venue(ok=True))
        for venue in ("decibel", "variational"):
            state["exchanges"][venue]["points"] = {"total_points": "1"}
        target = tmp_path / ("ops_state.json" if host == "vps1" else "ops_peer_state/vps2.json")
        _write_json(target, state)
    _write_json(
        tmp_path / "home_equity_principal.json",
        {
            "vps1": {
                "decibel": {"initial": 80, "cashflows": [{"type": "deposit", "amount": 10}], "reconciled": True},
                "variational": {"initial": 100, "cashflows": [], "reconciled": True},
            },
            "vps2": {
                "decibel": {"initial": 90, "cashflows": [], "reconciled": True},
                "variational": {"initial": 100, "cashflows": [], "reconciled": True},
            },
        },
    )

    capital = console._var_decibel()["capital"]

    assert capital["complete"] is True
    assert capital["initial_principal"] == 370.0
    assert capital["net_cashflow"] == 10.0
    assert capital["principal_total"] == 380.0
    assert capital["current_equity"] == 400.0
    assert capital["pnl"] == 20.0
    assert capital["pnl_pct"] == 5.26


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
    assert 'id="alertbar" style="display:none"' in html
    assert "查看全部 '+alerts.length+' 条" in html
    assert 'class="alert-more"' in html
    assert "hosts[h].age_sec??999999" in html
    assert "四源权益不完整" in html
    assert "四源交易量不完整" in html
    assert "const score=vd.points_by_venue||{}" in html
    assert "旧自动化" not in html
    assert "<span class=\"on\">自动运行</span><span>手动交易</span><span>成交记录</span><span>统计汇总</span>" in html
    for duplicate_tab in (">Trade <", ">Statistics <", ">Research <", ">Execution <", ">Advanced <"):
        assert duplicate_tab not in html
    assert "进入 Var/Decibel 操作面板" not in html
    assert "window.open('/varia/'" not in html
    assert 'src="/varia/?embed=true"' not in html
    assert "operation-frame" not in html
    assert 'id="vdctl-host"' in html
    assert 'id="vdctl-symbol"' in html
    assert 'id="vdctl-open"' in html
    assert 'id="vdctl-close"' in html
    assert 'id="vdauto-root"' in html
    assert 'id="vdauto-start"' in html
    assert 'id="vdauto-stop"' in html
    assert 'id="vdauto-vps1-strategy"' in html
    assert 'id="vdauto-vps2-strategy"' in html
    assert 'id="vdauto-major-symbols"' in html
    assert 'id="vdauto-opportunity-symbols"' in html
    assert "自动策略只会从绿色币种中下单" in html
    assert 'id="vdauto-spread"' in html
    assert "/api/varia/control/open" in html
    assert "/api/varia/control/close-all" in html
    assert "'/api/varia/automation/'+action" in html
    assert "保存配置不会启动后台" in html
    assert "自动运行的补充入口" in html
    assert "打开旧只读详情" not in html


def test_varia_auto_state_normalizes_hosts_and_preserves_zero_budget() -> None:
    result = console._normalize_varia_auto_state({
        "enabled": True,
        "mode": "full_auto",
        "weekly_loss_cap_usdc": 0,
        "max_auto_spread_bps": 5,
        "major_ratio": 0,
        "pressure_test": {
            "enabled": True,
            "min_open_interval_minutes": 30,
            "max_open_interval_minutes": 180,
        },
        "hosts": {
            "vps1": {"enabled": True, "strategy": "b"},
            "vps2": {"enabled": False, "strategy": "invalid"},
        },
    })

    assert result["enabled"] is True
    assert result["weekly_loss_cap_usdc"] == "0.0"
    assert result["max_auto_spread_bps"] == "5.0"
    assert result["major_ratio"] == "0.0"
    assert result["hosts"] == {
        "vps1": {"enabled": True, "strategy": "B"},
        "vps2": {"enabled": False, "strategy": "A"},
    }


def test_varia_auto_payload_keeps_vps_assignments_independent() -> None:
    result = console._varia_auto_payload({
        "mode": "full_auto",
        "weekly_loss_cap_usdc": 15,
        "max_auto_spread_bps": 5,
        "major_ratio": 0.8,
        "pressure_test": {
            "enabled": True,
            "min_open_interval_minutes": 30,
            "max_open_interval_minutes": 180,
        },
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })

    assert result["hosts"]["vps1"] == {"enabled": True, "strategy": "A"}
    assert result["hosts"]["vps2"] == {"enabled": True, "strategy": "B"}
    assert result["max_auto_spread_bps"] == "5.0"
    assert result["pressure_test"]["max_open_interval_minutes"] == 180


def test_varia_automation_status_requires_config_and_live_worker(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": True,
        "mode": "full_auto",
        "weekly_loss_cap_usdc": 15,
        "major_ratio": 0.8,
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })
    monkeypatch.setattr(console, "_varia_worker_status", lambda host: "active" if host == "vps1" else "inactive")

    result = console._varia_automation_state({"budget": {"hosts": {}}})

    assert result["status"] == "partial"
    assert result["running_hosts"] == ["vps1"]
    assert result["hosts"]["vps1"]["running"] is True
    assert result["hosts"]["vps2"]["running"] is False


def test_varia_strategy_pools_show_all_configured_symbols_and_readiness(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("BTC", "ETH", "HYPE", "SOL", "XAU"))
    (tmp_path / "config.yaml").write_text(
        'strategy:\n  major_symbols: ["BTC", "ETH"]\n'
        '  opportunity_symbols: ["XAU", "SOL"]\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [
        {"symbol": "BTC", "age_sec": 30, "var_bid": 99.99, "var_ask": 100.01,
         "decibel_bid": 99.99, "decibel_ask": 100.01, "costs": {"var_buy": 2, "var_sell": 2}},
        {"symbol": "SOL", "age_sec": 601},
    ])

    result = console._varia_strategy_pools()

    assert result["major"] == ["BTC", "ETH"]
    assert result["opportunity"] == ["XAU", "SOL", "HYPE"]
    assert result["strategy_a"]["eligible"] == ["BTC", "ETH", "HYPE", "SOL", "XAU"]
    assert result["strategy_b"]["priority"] == ["XAU", "SOL", "HYPE"]
    assert result["strategy_b"]["fallback"] == ["BTC", "ETH"]
    assert result["quote_ready"] == ["BTC"]
    assert result["allowed"] == ["BTC"]
    assert result["blocked"] == []
    assert result["metrics"]["BTC"]["display_bps"] == 2.0


def test_varia_strategy_pools_marks_wide_spread_blocked(monkeypatch) -> None:
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("SOL",))
    monkeypatch.setattr(console, "_varia_strategy_symbol_config", lambda: {
        "major_symbols": [], "opportunity_symbols": ["SOL"],
    })
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [{
        "symbol": "SOL", "age_sec": 20,
        "var_bid": 99.9, "var_ask": 100.1,
        "decibel_bid": 99.5, "decibel_ask": 100.5,
        "costs": {"var_buy": 6, "var_sell": 6},
    }])

    result = console._varia_strategy_pools(2)

    assert result["allowed"] == []
    assert result["blocked"] == ["SOL"]
    assert result["metrics"]["SOL"]["display_bps"] > 5


def test_varia_worker_actions_use_each_hosts_own_service(monkeypatch) -> None:
    calls = []

    def fake_run(cmd, timeout, cwd=None, env=None):
        calls.append((cmd, timeout, env))
        return {"rc": 0, "out": "inactive", "err": ""}

    monkeypatch.setattr(console, "_run_cmd", fake_run)

    console._varia_worker_action("vps1", "is-active")
    console._varia_worker_action("vps2", "is-active")

    assert calls[0][0] == [
        "systemctl", "--user", "is-active", "var-decibel-worker@vps1.service"
    ]
    assert calls[0][2]["XDG_RUNTIME_DIR"].startswith("/run/user/")
    assert calls[1][0][-1] == "systemctl --user is-active var-decibel-worker@vps2.service || true"
    assert any("100.101.50.40" in part for part in calls[1][0])


def test_write_auto_strategy_preserves_future_fields(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    monkeypatch.setattr(console, "AUDIT_LOG", tmp_path / "audit.jsonl")
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": False,
        "mode": "semi_auto",
        "weekly_loss_cap_usdc": "15",
        "major_ratio": "0.8",
        "hosts": {},
        "future_worker_setting": {"keep": True},
    })

    result = console._write_auto_strategy({"enabled": True})
    saved = json.loads((tmp_path / "auto_strategy_state.json").read_text())

    assert result["ok"] is True
    assert saved["enabled"] is True
    assert saved["future_worker_setting"] == {"keep": True}


def test_start_automation_reconciles_selected_hosts_without_trading(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    monkeypatch.setattr(console, "AUDIT_LOG", tmp_path / "audit.jsonl")
    monkeypatch.setattr(console, "WRITES_ENABLED", True)
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": False,
        "mode": "full_auto",
        "weekly_loss_cap_usdc": 15,
        "major_ratio": 0.8,
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": False, "strategy": "B"},
        },
    })
    monkeypatch.setattr(console, "_sync_varia_auto_state_to_vps2", lambda: {"ok": True})
    monkeypatch.setattr(console, "_varia_automation_state", lambda vd=None: {"status": "running"})
    actions = []

    def fake_action(host, action):
        actions.append((host, action))
        return {"rc": 0, "out": "active" if host == "vps1" else "inactive", "err": ""}

    monkeypatch.setattr(console, "_varia_worker_action", fake_action)
    request = type("RequestStub", (), {"headers": {}})()

    response = asyncio.run(console.varia_automation_start(request))
    saved = json.loads((tmp_path / "auto_strategy_state.json").read_text())

    assert response.status_code == 200
    assert saved["enabled"] is True
    assert ("vps1", "start") in actions
    assert ("vps2", "stop") in actions
    assert not any("open" in action or "close" in action for _, action in actions)


def test_stop_automation_only_stops_workers_and_leaves_positions_alone(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    monkeypatch.setattr(console, "AUDIT_LOG", tmp_path / "audit.jsonl")
    monkeypatch.setattr(console, "WRITES_ENABLED", True)
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": True,
        "mode": "full_auto",
        "weekly_loss_cap_usdc": 15,
        "major_ratio": 0.8,
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })
    monkeypatch.setattr(console, "_sync_varia_auto_state_to_vps2", lambda: {"ok": True})
    monkeypatch.setattr(console, "_varia_automation_state", lambda vd=None: {"status": "stopped"})
    actions = []

    def fake_action(host, action):
        actions.append((host, action))
        return {"rc": 0, "out": "inactive", "err": ""}

    monkeypatch.setattr(console, "_varia_worker_action", fake_action)
    request = type("RequestStub", (), {"headers": {}})()

    response = asyncio.run(console.varia_automation_stop(request))
    saved = json.loads((tmp_path / "auto_strategy_state.json").read_text())

    assert response.status_code == 200
    assert saved["enabled"] is False
    assert actions == [("vps1", "stop"), ("vps2", "stop")]


def test_varia_quote_direction_uses_lower_cross_venue_entry_cost() -> None:
    result = console._varia_quote_direction({
        "var_bid": 99.0, "var_ask": 100.0,
        "decibel_bid": 101.0, "decibel_ask": 102.0,
    })

    assert result["recommended"] == "var_buy"
    assert result["costs"]["var_buy"] < result["costs"]["var_sell"]


def test_varia_control_lists_full_ranked_market_candidates(monkeypatch) -> None:
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [])
    monkeypatch.setattr(console, "_varia_recent_jobs", lambda: [])
    monkeypatch.setattr(console, "_varia_active_job", lambda: None)

    state = console._varia_control_state({"pairs": [], "single_leg": [], "hosts": {}})

    assert len(state["symbols"]) == 34
    assert state["symbols"][:6] == ["BTC", "ETH", "HYPE", "XAU", "SPCX", "SOL"]
    assert state["symbols"][-3:] == ["CBRS", "ZRO", "CHIP"]


def test_varia_vps2_command_routes_to_peer_without_secrets(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path / "data")
    command = console._varia_live_command(
        host="vps2", symbol="SOL", var_side="buy", quantity=1.25,
        leverage=6, notional=100,
    )

    assert command[0] == "ssh"
    assert console.VARIA_VPS2_SSH in command
    assert "ubuntu@100.101.50.40" in command
    assert "--symbol SOL" in command[-1]
    assert "--leverage 6" in command[-1]
    assert "--leverage-cap 40" in command[-1]
    assert "private" not in " ".join(command).lower()


def test_varia_close_commands_require_fresh_verified_two_leg_positions(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    state = _state(
        "vps1", datetime.now(timezone.utc).isoformat(),
        _venue(ok=True, side="sell", size="-2"),
        _venue(ok=True, side="buy", size="2"),
    )
    _write_json(tmp_path / "ops_state.json", state)

    commands, blocks = console._varia_close_commands()

    assert blocks == []
    assert len(commands) == 1
    assert commands[0]["symbol"] == "SOL"
    assert commands[0]["planned_var_side"] == "sell"
    assert "--reduce-only" in commands[0]["command"]


def test_varia_close_commands_block_single_leg(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    state = _state(
        "vps1", datetime.now(timezone.utc).isoformat(),
        _venue(ok=True, side="sell", size="-2"), _venue(ok=True),
    )
    _write_json(tmp_path / "ops_state.json", state)

    commands, blocks = console._varia_close_commands()

    assert commands == []
    assert blocks == ["VPS1·SOL 是单腿仓位"]


def test_varia_manual_queue_allows_only_one_active_job(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    with sqlite3.connect(tmp_path / "hedge_bot.sqlite3") as connection:
        connection.execute(
            "CREATE TABLE dashboard_jobs ("
            "id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "started_at TEXT, finished_at TEXT, kind TEXT NOT NULL, status TEXT NOT NULL, "
            "payload_json TEXT, command_json TEXT, result_json TEXT, error_message TEXT, "
            "worker_id TEXT, attempts INTEGER DEFAULT 0)"
        )

    job_id = console._enqueue_varia_job(
        kind="manual_live", command={"mode": "single", "commands": []},
        payload={"host": "vps1", "symbol": "BTC"},
    )

    assert job_id == 1
    with sqlite3.connect(tmp_path / "hedge_bot.sqlite3") as connection:
        row = connection.execute(
            "SELECT status, payload_json FROM dashboard_jobs WHERE id=1"
        ).fetchone()
    assert row is not None and row[0] == "queued"
    assert json.loads(row[1])["symbol"] == "BTC"
    try:
        console._enqueue_varia_job(
            kind="manual_live", command={"mode": "single", "commands": []}, payload={},
        )
    except RuntimeError as exc:
        assert "已有任务 #1" in str(exc)
    else:
        raise AssertionError("second active dashboard job should be rejected")


def test_ipo_prefers_official_chinese_name_and_keeps_english_label(monkeypatch) -> None:
    monkeypatch.setattr(
        console,
        "_fetch_json",
        lambda *_args, **_kwargs: {
            "ipo": {
                "stocks": [
                    {
                        "code": "2523",
                        "name": "永康控股有限公司",
                        "nameZh": "永康控股有限公司",
                        "nameEn": "EKH LIMITED",
                        "status": "申购中",
                    }
                ]
            }
        },
    )

    result = console._ipo()

    assert result["stocks"][0]["name"] == "永康控股有限公司"
    assert result["stocks"][0]["name_zh"] == "永康控股有限公司"
    assert result["stocks"][0]["name_en"] == "EKH LIMITED"


def test_ipo_only_returns_currently_subscribing_stocks(monkeypatch) -> None:
    monkeypatch.setattr(
        console,
        "_fetch_json",
        lambda url, **_kwargs: {
            "ipo": {
                "stocks": [
                    {"code": "1001", "name": "可申购", "status": "申购中"},
                    {"code": "1002", "name": "等待结果", "status": "待结果"},
                    {"code": "1003", "name": "已经上市", "status": "已上市"},
                    {"code": "IPO-A", "name": "演示占位", "status": "申购中"},
                    {"code": "1004", "name": "上游状态滞后", "status": "申购中"},
                ]
            }
        } if url == console.IPO_STATE_URL else {
            "stocks": [{"code": "1004", "verdict": "跳(已过期)"}]
        },
    )

    result = console._ipo()

    assert [stock["code"] for stock in result["stocks"]] == ["1001"]
    assert result["stocks_total"] == 1
    assert result["active_stocks"] == 1


def test_ipo_entries_read_router_stock_name_and_code(monkeypatch) -> None:
    monkeypatch.setattr(
        console,
        "_fetch_json",
        lambda url, **_kwargs: {
            "ipo": {
                "stocks": [{"code": "9001", "name": "模拟新股", "status": "申购中"}],
                "entries": [
                    {"accountId": "Hk-001", "owner": "测试员", "status": "待申购",
                     "stockCode": "9001", "stockName": "模拟新股"},
                    {"accountId": "Hk-002", "owner": "测试员", "status": "已申购",
                     "stockCode": "9002"},
                ],
            }
        } if url == console.IPO_STATE_URL else {},
    )

    result = console._ipo()

    assert result["entries"][0]["stock"] == "模拟新股"
    assert result["entries"][1]["stock"] == "9002"


def test_ipo_console_uses_contextual_account_actions() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert "function ipoActionButtons(entry)" in html
    assert "if(entry.status==='待申购')" in html
    assert "if(entry.status==='已申购')" in html
    assert "当前可申购新股" in html
    assert "每个账号只显示当前可执行的下一步" in html
    assert "一键申购(活跃)" not in html
    assert "结束本轮" not in html


def test_ipo_console_shows_live_summary_and_safe_action_feedback() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    for key in ("ipo-active-count", "ipo-pending-count", "ipo-subscribed-count", "ipo-updated-age"):
        assert f'data-k="{key}"' in html
    assert "const hasActiveStocks=activeCount>0" in html
    assert "当前无申购中新股，账户状态操作已暂停" in html
    assert "等待申购中新股" in html
    assert "function showIpoToast(text,kind)" in html
    assert "function showIpoRowStatus(button,text,kind)" in html
    assert "window.confirm('确认将 '" in html


def test_console_renders_chinese_stock_name_before_english() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert "const primary=x.name_zh||x.name||x.name_en||'—'" in html
    assert 'class="stock-name-en"' in html


def test_stale_equity_alert_only_matters_while_varia_is_active(monkeypatch) -> None:
    monkeypatch.setattr(console, "_mtime_age", lambda _: None)
    old_equity = {
        "present": True,
        "points": [{"t": "2026-01-01", "v": 100.0}, {"t": "2026-01-02", "v": 99.0}],
    }
    base = {"equity_history": old_equity, "auto": {"enabled": False}, "pairs": []}

    quiet = console._alerts(base, {}, {}, {"present": True}, {"present": True}, {})
    assert not any("权益曲线断更" in item["msg"] for item in quiet)

    active = console._alerts(
        {**base, "auto": {"enabled": True}},
        {}, {}, {"present": True}, {"present": True}, {},
    )
    assert any("权益曲线断更" in item["msg"] for item in active)


def _shadow_database(path: Path, *, safety_matched: int = 1, actions_matched: int = 1) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE shadow_samples (
                id INTEGER PRIMARY KEY, observed_at TEXT, venue TEXT,
                source_fingerprint TEXT, source_timestamps_json TEXT,
                desired_orders INTEGER, actual_orders INTEGER, books INTEGER,
                fresh INTEGER, matched INTEGER, safety_matched INTEGER,
                actions_matched INTEGER, python_can_execute INTEGER,
                rust_can_execute INTEGER, snapshot_path TEXT,
                python_result_json TEXT, rust_result_json TEXT
            );
            CREATE TABLE shadow_collector_status (
                venue TEXT PRIMARY KEY, last_poll_at TEXT,
                last_new_state_at TEXT, last_fingerprint TEXT, last_error TEXT
            );
            CREATE TABLE shadow_errors (
                id INTEGER PRIMARY KEY, observed_at TEXT, venue TEXT, error TEXT
            );
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        for venue in ("polymarket", "predictfun"):
            matched = int(bool(safety_matched and actions_matched))
            connection.execute(
                """
                INSERT INTO shadow_samples VALUES
                (NULL, ?, ?, 'fingerprint', '{}', 1, 0, 1, 1, ?, ?, ?, 1, 1, 'snapshot', '{}', '{}')
                """,
                (now, venue, matched, safety_matched, actions_matched),
            )
            connection.execute(
                "INSERT INTO shadow_collector_status VALUES (?, ?, ?, 'fingerprint', '')",
                (venue, now, now),
            )


def _observer_files(path: Path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    _write_json(
        path / "polymarket_observer_status.json",
        {
            "last_poll_at": now,
            "healthy": True,
            "summary": {"accounts": 2, "markets": 2, "ready_markets": 2, "plans": 2, "errors": 0},
        },
    )
    _write_json(
        path / "polymarket_observer_state_1.json",
        {
            "ts": now,
            "account_index": 1,
            "markets": {
                "very-long-public-token-identifier": {
                    "display_name": "World Cup market",
                    "best_bid": "0.48",
                    "best_ask": "0.52",
                    "mid": "0.5",
                    "reference_plan": [{"price": "0.47", "quantity": "20"}],
                    "status": "ready",
                }
            },
        },
    )


def test_maker_shadow_is_unknown_without_observer_or_database(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)

    result = console._maker_shadow()

    assert result["present"] is False
    assert result["overall_tier"] == "danger"
    assert result["observer"]["status"] == "公共行情不可用"


def test_maker_shadow_reports_fresh_public_books_and_matching_core(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)
    _observer_files(tmp_path)
    _shadow_database(tmp_path / "maker_shadow.sqlite3")

    result = console._maker_shadow()

    assert result["present"] is True
    assert result["overall_tier"] == "ok"
    assert result["observer"]["ready_markets"] == 2
    assert [row["tier"] for row in result["venues"]] == ["ok", "ok"]
    assert result["markets"][0]["reference_plan"] == "0.47 x 20"
    assert result["markets"][0]["actual_orders_available"] is False


def test_maker_shadow_escalates_safety_differences(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "DATA_DIR", tmp_path)
    _observer_files(tmp_path)
    _shadow_database(tmp_path / "maker_shadow.sqlite3", safety_matched=0)

    result = console._maker_shadow()

    assert result["overall_tier"] == "danger"
    assert all(row["safety_mismatches"] == 1 for row in result["venues"])
    assert all(row["tier"] == "danger" for row in result["venues"])


def test_console_contains_compact_shadow_status_and_native_observer_view() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert 'data-k="shadow.home"' in html
    assert "只读观察" in html
    assert "公共盘口与参考报价计划" in html
    assert "不连接 signer" in html
    assert "polymarket_observer_state_1.json" not in html


def test_ipo_open_filter_rejects_expired_listed_and_synthetic_rows() -> None:
    now = datetime.fromisoformat("2026-07-17T15:00:00+08:00")

    assert not console._ipo_stock_is_open(
        {
            "code": "2523",
            "status": "申购中",
            "closeAt": "12:00 noon on Wednesday, 8 July 2026",
        },
        now,
    )
    assert not console._ipo_stock_is_open(
        {"code": "SIM20260716", "status": "申购中", "closeAt": "2026-07-17 23:29"},
        now,
    )
    assert console._ipo_stock_is_open(
        {"code": "9999", "status": "申购中", "closeAt": "2026-07-18 12:00"},
        now,
    )


def test_console_has_gpt_judgment_and_real_account_readiness() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert 'id="ipo-judge-btn"' in html
    assert 'id="ipo-research-input"' in html
    assert "账户准备度" in html
    assert "不生成虚假申购方案" in html
