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


def _state(
    host: str, generated_at: str, decibel: dict, variational: dict,
    *, ondo: Optional[dict] = None,
) -> dict:
    state = {
        "host_id": host,
        "generated_at": generated_at,
        "exchanges": {"decibel": decibel, "variational": variational},
    }
    if ondo is not None:
        state["exchanges"]["ondo"] = ondo
    return state


def _patch_varia_dependencies(monkeypatch, data_dir: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "VARIA_CAPITAL_LEDGER", data_dir / "home_equity_principal.json")
    monkeypatch.setattr(console, "VARIA_RECONCILED_PNL_HISTORY", data_dir / "reconciled_pnl_history.json")
    monkeypatch.setattr(console, "_varia_trades_today", lambda: {"present": False})
    monkeypatch.setattr(console, "_varia_budget", lambda _: {"present": False})
    monkeypatch.setattr(console, "_equity_history", lambda: {"present": False})


def test_parse_ts_treats_naive_recorder_timestamp_as_utc() -> None:
    expected = datetime(2026, 7, 22, 18, 34, 8, tzinfo=timezone.utc).timestamp()

    assert console._parse_ts("2026-07-22 18:34:08") == expected
    assert console._parse_ts("2026-07-22T18:34:08Z") == expected


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


def test_macmini_uses_persistent_last_good_snapshot(monkeypatch, tmp_path: Path) -> None:
    snapshot = tmp_path / "macmini_status_last_good.json"
    payload = {"ts": time.time() - 30, "services": {}}
    _write_json(snapshot, payload)
    monkeypatch.setattr(console, "MACMINI_STATUS_SNAPSHOT_PATH", snapshot)
    console._HTTP_CACHE.pop(console.MACMINI_STATUS_URL, None)
    monkeypatch.setattr(
        console,
        "_do_fetch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("disk snapshot should avoid a cold mac-mini fetch")
        ),
    )

    assert console._fetch_json(console.MACMINI_STATUS_URL) == payload


def test_macmini_alert_waits_for_grace_period(monkeypatch) -> None:
    monkeypatch.setattr(console, "MACMINI_ALERT_GRACE_SECONDS", 600)
    monkeypatch.setattr(console, "PROCESS_STARTED_AT", time.time() - 60)

    startup_alerts = console._alerts(
        {}, {}, {}, {"present": True}, {"present": False}, {}, {}
    )
    assert not any(item.get("tag") == "INFRA" for item in startup_alerts)

    monkeypatch.setattr(console, "PROCESS_STARTED_AT", time.time() - 601)
    missing_alerts = console._alerts(
        {}, {}, {}, {"present": True}, {"present": False}, {}, {}
    )
    assert any(item.get("tag") == "INFRA" for item in missing_alerts)

    stale_alerts = console._alerts(
        {}, {}, {}, {"present": True},
        {"present": True, "age_sec": 601}, {}, {},
    )
    assert any(
        item.get("tag") == "INFRA" and "超过" in item.get("msg", "")
        for item in stale_alerts
    )


def test_prefetch_refreshes_ipo_judgment_pack() -> None:
    """A completed GPT run must replace the judgment pack cached at startup."""

    assert console.IPO_PACK_URL in console._PREFETCH_URLS


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
    assert "onclick=\"goOps('onboarding')\">开户与奖励 →" not in html
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
            _venue(ok=True),
            _venue(ok=True, side="buy", size="0.473"),
            ondo=_venue(ok=True, side="sell", size="-0.473"),
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
            "summary": "快照过期",
            "failed_venues": [],
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


def test_var_decibel_exposes_redacted_venue_error_details(monkeypatch, tmp_path: Path) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    variational = _venue(ok=False)
    variational["error"] = {"type": "TimeoutError", "message": "Mac signer timeout"}
    _write_json(
        tmp_path / "ops_state.json",
        _state(
            "vps1",
            datetime.now(timezone.utc).isoformat(),
            _venue(ok=True),
            variational,
        ),
    )

    result = console._var_decibel()

    assert result["hosts"]["vps1"]["venue_reads"] == {
        "decibel": {"ok": True, "error": None},
        "variational": {"ok": False, "error": "Mac signer timeout"},
    }
    assert result["position_sources"]["unverified"][0]["failed_venues"][0]["error"] == "Mac signer timeout"


def test_vps2_ignores_legacy_decibel_and_uses_ondo_as_hedge(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    _write_json(
        tmp_path / "ops_peer_state" / "vps2.json",
        _state(
            "vps2", datetime.now(timezone.utc).isoformat(),
            _venue(ok=True, side="sell", size="-9"),
            _venue(ok=True), ondo=_venue(ok=True),
        ),
    )

    result = console._var_decibel()

    assert result["pairs"] == []
    assert result["single_leg"] == []
    assert result["hosts"]["vps2"]["positions_verified"] is True
    assert result["hosts"]["vps2"]["hedge_venue"] == "ondo"
    assert result["hosts"]["vps2"]["equity_hedge"] == 100.0
    assert set(result["hosts"]["vps2"]["venue_reads"]) == {"variational", "ondo"}


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
        state = _state(
            host, now, _venue(ok=True), _venue(ok=True),
            ondo=_venue(ok=True) if host == "vps2" else None,
        )
        state["exchanges"]["decibel"]["points"] = {"total_points": dec_points}
        state["exchanges"]["variational"]["points"] = {"total_points": var_points}
        hedge_venue = "decibel" if host == "vps1" else "ondo"
        state["trade_volume"] = {
            "ok": True,
            "venues": {
                hedge_venue: {
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
    assert result["points_decibel"] == 1.0
    assert result["points_variational"] == 0.3
    assert result["points_by_venue"] == {
        "decibel": {"total": 1.0, "hosts": {"vps1": 1.0}, "complete": True},
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
        state = _state(
            host, now.isoformat(), _venue(ok=True), _venue(ok=True),
            ondo=_venue(ok=True) if host == "vps2" else None,
        )
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
                "ondo": {"initial": 90, "cashflows": [], "reconciled": True},
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


def test_settled_incentives_are_attributed_without_double_counting_equity(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    vps1 = _state("vps1", now, _venue(ok=True), _venue(ok=True))
    vps2 = _state(
        "vps2",
        now,
        _venue(ok=True),
        _venue(ok=True),
        ondo=_venue(ok=True),
    )
    vps1["exchanges"]["variational"]["loss_refunds"] = {
        "settled_total_usdc": 1,
        "settled_week_usdc": 1,
        "settled_24h_usdc": 0,
        "own_refunds_total_usdc": 1,
        "other_rewards_total_usdc": 0,
        "estimated_refund_usdc": 0,
        "pool_usdc": 0,
        "complete": True,
    }
    vps2["exchanges"]["variational"]["loss_refunds"] = {
        "settled_total_usdc": 2,
        "settled_week_usdc": 2,
        "settled_24h_usdc": 0,
        "own_refunds_total_usdc": 1.5,
        "other_rewards_total_usdc": 0.5,
        "estimated_refund_usdc": 0.2,
        "pool_usdc": 6000,
        "complete": True,
    }
    vps2["exchanges"]["ondo"]["rewards"] = {
        "settled_total_usdc": 5,
        "settled_week_usdc": 4,
        "settled_24h_usdc": 1,
        "current_week": 8,
        "current_week_pool_usdc": 175000,
        "current_week_status": "pending",
        "pending_account_reward_usdc": None,
        "complete": True,
    }
    _write_json(tmp_path / "ops_state.json", vps1)
    _write_json(tmp_path / "ops_peer_state/vps2.json", vps2)
    _write_json(
        tmp_path / "home_equity_principal.json",
        {
            "vps1": {
                "decibel": {"initial": 90, "cashflows": [], "reconciled": True},
                "variational": {"initial": 100, "cashflows": [], "reconciled": True},
            },
            "vps2": {
                "ondo": {"initial": 90, "cashflows": [], "reconciled": True},
                "variational": {"initial": 100, "cashflows": [], "reconciled": True},
            },
        },
    )

    result = console._var_decibel()

    assert result["capital"]["current_equity"] == 400.0
    assert result["capital"]["pnl"] == 20.0
    assert result["incentives"]["complete"] is True
    assert result["incentives"]["settled_total_usdc"] == 8.0
    assert result["incentives"]["settled_week_usdc"] == 7.0
    assert result["incentives"]["ondo"]["current_week_pool_usdc"] == 175000
    assert result["incentives"]["variational"]["estimated_refund_usdc"] == 0.2
    assert result["pnl_attribution"] == {
        "complete": True,
        "net_pnl_usdc": 20.0,
        "settled_incentives_usdc": 8.0,
        "trading_funding_fees_usdc": 12.0,
        "note": "Settled incentives are already inside equity and are not added twice.",
    }


def test_weekly_budget_adds_profit_and_subtracts_loss(monkeypatch) -> None:
    monkeypatch.setattr(console, "_budget_cap_for_host", lambda: 15.0)

    result = console._varia_budget({"vps1": 3.0, "vps2": -2.0})

    assert result["hosts"]["vps1"]["remaining"] == 18.0
    assert result["hosts"]["vps2"]["remaining"] == 13.0
    assert result["total_remaining"] == 31.0
    assert result["basis"] == "friday_0800_settled_net_including_incentives"


def test_vps2_decibel_to_ondo_transfer_preserves_principal_and_counts_ondo_once(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_varia_dependencies(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    vps1 = _state("vps1", now, _venue(ok=True), _venue(ok=True))
    vps2 = _state(
        "vps2",
        now,
        _venue(ok=True),
        _venue(ok=True),
        ondo=_venue(ok=True),
    )
    vps1["exchanges"]["decibel"]["balance"]["total_equity"] = "520.443289"
    vps1["exchanges"]["variational"]["balance"]["total_equity"] = "499.778932"
    # The legacy Decibel account stays in the audit data but is no longer an
    # active VPS2 leg after the internal venue migration.
    vps2["exchanges"]["decibel"]["balance"]["total_equity"] = "0"
    vps2["exchanges"]["variational"]["balance"]["total_equity"] = "533.659805"
    vps2["exchanges"]["ondo"]["balance"]["total_equity"] = "463.4"
    _write_json(tmp_path / "ops_state.json", vps1)
    _write_json(tmp_path / "ops_peer_state" / "vps2.json", vps2)
    _write_json(
        tmp_path / "home_equity_principal.json",
        {
            "vps1": {
                "decibel": {"initial": 499.894658, "cashflows": [], "reconciled": True},
                "variational": {"initial": 497.718961, "cashflows": [], "reconciled": True},
            },
            "vps2": {
                "decibel": {
                    "initial": 499.85787,
                    "cashflows": [],
                    "reconciled": True,
                    "migrated_to": "ondo",
                },
                "ondo": {
                    "initial": 499.85787,
                    "cashflows": [],
                    "reconciled": True,
                    "capital_source": "vps2.decibel",
                },
                "variational": {"initial": 499.9, "cashflows": [], "reconciled": True},
            },
        },
    )

    result = console._var_decibel()
    capital = result["capital"]

    assert result["equity_total"] == 2017.28
    assert result["hosts"]["vps2"]["equity_hedge"] == 463.4
    assert capital["principal_total"] == 1997.37
    assert capital["current_equity"] == 2017.28
    assert capital["pnl"] == 19.91
    assert capital["pnl_pct"] == 1.0
    assert not any(
        row["host"] == "vps2" and row["venue"] == "decibel"
        for row in capital["sources"]
    )


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


def test_polymarket_page_is_native_and_has_complete_workspaces() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    page = html.split('<section class="page" id="page-pm">', 1)[1].split(
        '<section class="page" id="page-pf">', 1
    )[0]

    for view in ("overview", "markets", "fills", "scan", "settings"):
        assert f'data-pm-view="{view}"' in page
        assert f'data-pm-view-panel="{view}"' in page
    assert "<iframe" not in page
    assert "打开完整面板" not in page
    assert "window.open('/alpha/'" not in html


def test_pm_detail_separates_live_orders_from_stale_engine_state(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    maker_dir = tmp_path / "maker"
    data_dir.mkdir()
    maker_dir.mkdir()
    token = "123456789012345678901234567890"
    secret = "must-never-reach-dashboard"
    _write_json(
        maker_dir / "config_1.json",
        {
            "account": {
                "signer_server_url": "http://macmini/signer",
                "signer_token": secret,
                "private_key": secret,
            },
            "markets": [
                {
                    "token_id": token,
                    "side": "YES",
                    "enabled": True,
                    "quote_size": 200,
                    "risk": "low",
                }
            ],
            "night_markets": [],
            "strategy": {
                "post_only": True,
                "requote_interval_ms": 500,
                "dual_side": {"enabled": True, "max_mid": 0.1},
            },
            "risk": {
                "max_quote_shares_per_market": 200,
                "max_notional_usdc_per_order": 510,
            },
            "execution": {"min_front_bid_notional_usdc": 10000},
            "exit_strategy": {
                "exit_delay_sec": 5,
                "exit_timeout_sec": 300,
                "retry_count": 2,
            },
        },
    )
    _write_json(
        data_dir / "engine_state_1.json",
        {
            "markets": {
                token: {
                    "orders": [{"id": "old-order"}],
                    "event_state": "ACTIVE",
                }
            },
            "fills": [],
            "pending_unwinds": [],
            "exit_records": [],
        },
    )
    _write_json(
        data_dir / "polymarket_observer_state_1.json",
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "markets": {
                token: {
                    "display_name": "Fed decision",
                    "best_bid": "0.41",
                    "best_ask": "0.42",
                    "mid": "0.415",
                    "reference_plan": [{"price": "0.40", "quantity": "200"}],
                }
            },
        },
    )
    _write_json(
        data_dir / "polymarket_observer_status.json",
        {
            "last_poll_at": datetime.now(timezone.utc).isoformat(),
            "summary": {"accounts": 1, "markets": 1, "ready_markets": 1, "plans": 1},
        },
    )
    monkeypatch.setattr(console, "DATA_DIR", data_dir)
    monkeypatch.setattr(console, "PM_PEER_DIR", data_dir / "pm_peer")
    monkeypatch.setattr(console, "MAKER_DIR", maker_dir)
    monkeypatch.setattr(console, "_load_pm_remotes", lambda: {})
    monkeypatch.setattr(console, "_pid_file_alive", lambda *_: False)

    result = console._pm_detail()

    assert result["markets"][0]["bid"] == 0.41
    assert result["markets"][0]["ask"] == 0.42
    assert result["markets"][0]["orders"] is None
    assert result["markets"][0]["orders_last_seen"] == 1
    assert result["accounts"][0]["rules"]["post_only"] is True
    assert result["accounts"][0]["rules"]["max_quote_shares"] == 200
    assert result["accounts"][0]["signer_mode"] == "Mac mini"
    assert secret not in json.dumps(result)


def test_pm_detail_reports_orders_only_for_running_fresh_engine(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    maker_dir = tmp_path / "maker"
    data_dir.mkdir()
    maker_dir.mkdir()
    token = "987654321"
    _write_json(
        maker_dir / "config_1.json",
        {"markets": [{"token_id": token, "enabled": True}], "night_markets": []},
    )
    _write_json(
        data_dir / "engine_state_1.json",
        {"markets": {token: {"orders": [{"id": "live-order"}]}}},
    )
    monkeypatch.setattr(console, "DATA_DIR", data_dir)
    monkeypatch.setattr(console, "PM_PEER_DIR", data_dir / "pm_peer")
    monkeypatch.setattr(console, "MAKER_DIR", maker_dir)
    monkeypatch.setattr(console, "_load_pm_remotes", lambda: {})
    monkeypatch.setattr(console, "_pid_file_alive", lambda *_: True)

    result = console._pm_detail()

    assert result["markets"][0]["orders"] == 1
    assert result["markets"][0]["orders_verified"] is True


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
    assert "双边权益不完整" in html
    assert "双边交易量不完整" in html
    assert "const score=vd.points_by_venue||{}" in html
    assert "旧自动化" not in html
    assert 'data-vd-tab="自动运行"' in html
    assert 'data-vd-tab="策略设置"' in html
    assert 'data-vd-tab="仓位"' in html
    assert 'data-vd-tab="行情机会"' in html
    assert 'data-vd-tab="统计与奖励"' in html
    assert 'data-vd-tab="手动交易"' in html
    assert 'data-vd-tab="成交记录"' in html
    for duplicate_tab in (">Trade <", ">Statistics <", ">Research <", ">Execution <", ">Advanced <"):
        assert duplicate_tab not in html
    assert "进入 Var/Decibel 操作面板" not in html
    assert "window.open('/varia/'" not in html
    assert 'src="/varia/?embed=true"' not in html
    assert "operation-frame" not in html
    assert 'id="vdctl-host"' in html
    assert 'id="vdctl-symbol"' in html
    assert "state.symbols_by_host" in html
    assert "selectedHost+'|'+symbols.join('|')" in html
    assert "addEventListener('change',()=>vdUpdateControl())" in html
    assert "addEventListener('change',vdUpdateControl)" not in html
    assert 'id="vdctl-open"' in html
    assert 'id="vdctl-close"' in html
    assert 'id="vdauto-root"' in html
    assert 'id="vdauto-start"' in html
    assert 'id="vdauto-stop"' in html
    assert 'id="vdauto-vps1-strategy"' in html
    assert 'id="vdauto-vps2-strategy"' in html
    assert 'id="vdauto-vps1-plan"' in html
    assert 'id="vdauto-vps2-plan"' in html
    assert 'id="vdauto-vps1-common"' in html
    assert 'id="vdauto-vps1-crypto"' in html
    assert 'id="vdauto-vps1-rwa"' in html
    assert 'id="vdauto-vps2-common"' in html
    assert 'id="vdauto-vps2-crypto"' in html
    assert 'id="vdauto-vps2-rwa"' in html
    assert "_vdAutoRenderFunnel('vps1',dec)" in html
    assert "_vdAutoRenderFunnel('vps2',ondo)" in html
    assert "_vdAutoRenderNextPlan(host,h)" in html
    assert "只读，未下单" in html
    assert "计划已过期，等待只读行情刷新" in html
    assert 'id="vdauto-major-symbols"' in html
    assert 'id="vdauto-opportunity-symbols"' in html
    assert 'id="vdauto-ondo-acceptance"' in html
    assert 'id="vdauto-ondo-detail"' in html
    assert 'id="vdauto-route-comparison"' in html
    assert 'id="vdauto-symbol-route-comparison"' in html
    assert 'id="vdauto-route-wrap"' in html
    assert 'id="vdauto-compare-dec-markets"' in html
    assert 'id="vdauto-compare-ondo-markets"' in html
    assert 'id="vdauto-compare-dec-status"' in html
    assert 'id="vdauto-compare-ondo-status"' in html
    assert "开仓前同步重算双腿价格" in html
    assert "实时价差优先，接近时再比较净资金费" in html
    assert "同币种跨平台价差参考" in html
    assert "跨平台扫描价差（非点差）" in html
    assert "entry_signal_unconfirmed:'等待下一轮稳定确认'" in html
    assert "entry_signal_unstable:'两轮报价变化过大'" in html
    assert "扫描参考路线" in html
    assert "负扫描价差只代表当时报价有利" in html
    assert "扫描价差参考" in html
    assert "当前资金费外推24小时" in html
    assert "_vdFundingPercent" in html
    assert "平台单边点差" in html
    assert "跨平台扫描价差" in html
    assert "点差 '+platformBp.toFixed(2)+'bp" in html
    assert "预计入场成本" not in html
    assert "VPS2 · Var/Ondo" in html
    assert "VPS / 路线" in html
    assert "_table(['时间','VPS','路线','SYMBOL'" in html
    assert "Ondo 正式环境验收" in html
    assert "部分成交待验收" in html
    assert "微量双腿待验收" in html
    assert "提高金额必须另行确认" in html
    assert "两列各读对应 VPS 的真实来源" in html
    assert "VPS2 Var/Ondo 不复用这些报价" not in html
    assert "普通币 2bp、RWA 3bp" in html
    assert "VPS2 · Var/Ondo 共同币" in html
    assert "当前资金费外推 24h" in html
    assert "资金费按当前费率外推 24 小时并用百分比显示" in html
    assert "不是平台点差，也不是收益预测" in html
    assert "净资金费方向不合格" in html
    assert "资金费率异常" in html
    assert 'id="vdauto-spread"' in html
    assert "/api/varia/control/open" in html
    assert "/api/varia/control/close-all" in html
    assert "'/api/varia/automation/'+action" in html
    assert "保存配置不会启动后台" in html
    assert "自动运行的补充入口" in html
    assert "acceptance.present===true?mutationKeys.filter" in html
    assert "if(selectedBlocked.length&&!msg.textContent)" not in html
    assert "window.__vdAutoRequestActive" in html
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


def test_varia_automation_state_exposes_execution_freeze_as_hard_block(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": True,
        "execution_frozen": True,
        "execution_frozen_reason": "leverage_readback_guard_deploy",
        "mode": "full_auto",
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })
    monkeypatch.setattr(console, "_varia_worker_status", lambda host: "active")

    result = console._varia_automation_state({"budget": {"hosts": {}}})

    assert result["status"] == "frozen"
    assert result["execution_frozen"] is True
    assert result["running_hosts"] == []
    assert result["start_blocked"] is True
    assert all(item["start_blocked"] is True for item in result["hosts"].values())
    assert all(
        "只读维护" in item["start_block_reason"]
        for item in result["hosts"].values()
    )


def test_varia_auto_runtime_exposes_worker_next_open_plan(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    _write_json(tmp_path / "auto_strategy_runtime.json", {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "status": "plan_ready_read_only",
        "message": "只读计划已生成",
        "next_open_plan": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_scan_generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "ready",
            "host": "vps1",
            "strategy": "A",
            "hedge_venue": "decibel",
            "symbol": "BTC",
            "direction": "Var buy / Decibel sell",
            "leverage": "6",
            "notional_usdc": "100",
            "var_spread_bps": "4.2",
            "hedge_spread_bps": "3.4",
            "ready_for_live": False,
            "mutations_sent": False,
        },
    })

    result = console._varia_auto_runtime("vps1")

    assert result["status"] == "plan_ready_read_only"
    assert result["next_open_plan"]["symbol"] == "BTC"
    assert result["next_open_plan"]["ready_for_live"] is False
    assert result["next_open_plan"]["stale"] is False
    assert result["next_open_plan"]["market_data_age_sec"] is not None
    assert result["next_open_plan"]["var_one_way_spread_bps"] == 2.1
    assert result["next_open_plan"]["hedge_one_way_spread_bps"] == 1.7


def test_varia_auto_runtime_prefers_fresh_embedded_vps2_runtime(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    old = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    fresh = datetime.now(timezone.utc).isoformat()
    peer_dir = tmp_path / "auto_strategy_peer_runtime"
    peer_dir.mkdir()
    _write_json(peer_dir / "vps2.json", {
        "updated_at": old,
        "next_open_plan": {"status": "ready", "symbol": "OLD", "generated_at": old},
    })
    monkeypatch.setattr(console, "_varia_raw_states", lambda: {
        "vps2": {
            "auto_strategy_runtime": {
                "updated_at": fresh,
                "status": "plan_ready_read_only",
                "next_open_plan": {
                    "status": "ready", "symbol": "XAG", "generated_at": fresh,
                    "ready_for_live": False,
                },
            }
        }
    })

    result = console._varia_auto_runtime("vps2")

    assert result["next_open_plan"]["symbol"] == "XAG"
    assert result["next_open_plan"]["stale"] is False


def test_varia_auto_runtime_marks_stale_market_scan_plan(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    fresh = datetime.now(timezone.utc).isoformat()
    stale_scan = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    _write_json(tmp_path / "auto_strategy_runtime.json", {
        "updated_at": fresh,
        "next_open_plan": {
            "status": "ready",
            "symbol": "XAG",
            "generated_at": fresh,
            "market_scan_generated_at": stale_scan,
        },
    })

    result = console._varia_auto_runtime("vps1")

    assert result["next_open_plan"]["stale"] is True
    assert result["next_open_plan"]["market_data_age_sec"] > console.STALE_SEC


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
    monkeypatch.setattr(console, "_varia_decibel_scan_state", lambda: {
        "present": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": [{"symbol": "BTC"}, {"symbol": "XAU"}],
    })

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
    assert result["metrics"]["BTC"]["platform_spread_bps"] == 1.0
    assert result["venues"]["decibel"]["common"] == ["BTC", "XAU"]
    assert result["venues"]["decibel"]["categories"]["BTC"] == "crypto"
    assert result["venues"]["decibel"]["categories"]["XAU"] == "rwa"


def test_varia_latest_quotes_prefers_independent_readonly_scan(monkeypatch) -> None:
    now = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(console, "_varia_raw_states", lambda: {
        "vps1": {
            "var_decibel_market_scan": {
                "present": True,
                "generated_at": now,
                "rows": [{
                    "timestamp": now,
                    "symbol": "btc",
                    "var_bid_1k": "99.90",
                    "var_ask_1k": "100.00",
                    "decibel_bid": "100.10",
                    "decibel_ask": "100.20",
                }],
            }
        }
    })

    result = console._varia_latest_quotes()

    assert len(result) == 1
    assert result[0]["symbol"] == "BTC"
    assert result[0]["source"] == "read_only_market_scan"
    assert result[0]["recommended"] == "var_buy"
    assert result[0]["age_sec"] <= 2


def test_varia_decibel_scan_state_uses_newer_direct_scan(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "_varia_raw_states", lambda: {
        "vps1": {
            "var_decibel_market_scan": {
                "generated_at": "2026-07-22T19:25:00+00:00",
                "rows": [{"symbol": "BTC"}],
            }
        }
    })
    _write_json(data_dir / "var_decibel_market_scan.json", {
        "generated_at": "2026-07-22T19:29:00+00:00",
        "read_only": True,
        "mutations_sent": False,
        "rows": [{"symbol": "ETH"}],
    })

    result = console._varia_decibel_scan_state()

    assert result["rows"] == [{"symbol": "ETH"}]
    assert result["read_only"] is True


def test_varia_decibel_scan_state_keeps_newer_embedded_scan(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "_varia_raw_states", lambda: {
        "vps1": {
            "var_decibel_market_scan": {
                "generated_at": "2026-07-22T19:30:00+00:00",
                "rows": [{"symbol": "BTC"}],
            }
        }
    })
    _write_json(data_dir / "var_decibel_market_scan.json", {
        "generated_at": "2026-07-22T19:29:00+00:00",
        "rows": [{"symbol": "ETH"}],
    })

    result = console._varia_decibel_scan_state()

    assert result["rows"] == [{"symbol": "BTC"}]


def test_varia_readonly_scan_ignores_incomplete_double_sided_quote() -> None:
    result = console._varia_quotes_from_readonly_scan({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": [{
            "symbol": "XAG",
            "var_bid_1k": "29.9",
            "var_ask_1k": "30.1",
            "decibel_bid": None,
            "decibel_ask": None,
        }],
    })

    assert result == []


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


def test_varia_strategy_pools_preserve_favorable_signed_entry_difference(
    monkeypatch,
) -> None:
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("BTC",))
    monkeypatch.setattr(console, "_varia_strategy_symbol_config", lambda: {
        "major_symbols": ["BTC"], "opportunity_symbols": [],
    })
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [{
        "symbol": "BTC", "age_sec": 20,
        "var_bid": 100.02, "var_ask": 100.03,
        "decibel_bid": 100.00, "decibel_ask": 100.01,
        "costs": {"var_buy": 0.3, "var_sell": -0.1},
        "recommended": "var_sell",
    }])

    result = console._varia_strategy_pools()

    assert result["metrics"]["BTC"]["entry_cost_bps"] == -0.1
    assert result["metrics"]["BTC"]["allowed"] is True


def test_varia_strategy_pools_do_not_invent_direction_without_quote_recommendation(
    monkeypatch,
) -> None:
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("BTC",))
    monkeypatch.setattr(console, "_varia_strategy_symbol_config", lambda: {
        "major_symbols": ["BTC"], "opportunity_symbols": [],
    })
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [{
        "symbol": "BTC", "age_sec": 20,
        "var_bid": 99.99, "var_ask": 100.01,
        "decibel_bid": 99.99, "decibel_ask": 100.01,
        "costs": {"var_buy": 2, "var_sell": 2},
        "recommended": None,
    }])

    result = console._varia_strategy_pools()

    assert result["metrics"]["BTC"]["recommended"] == "方向待定"


def test_varia_strategy_pools_use_vps2_ondo_scan_without_reusing_vps1_quotes(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    peer_dir = data_dir / "ops_peer_state"
    peer_dir.mkdir(parents=True)
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("BTC", "XAU"))
    monkeypatch.setattr(console, "_varia_strategy_symbol_config", lambda: {
        "major_symbols": ["BTC"], "opportunity_symbols": ["XAU"],
    })
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [{
        "symbol": "BTC", "age_sec": 20,
        "var_bid": 99.99, "var_ask": 100.01,
        "decibel_bid": 99.99, "decibel_ask": 100.01,
        "costs": {"var_buy": 2, "var_sell": 2},
    }])
    _write_json(peer_dir / "vps2.json", {
        "host_id": "vps2",
        "var_ondo_market_scan": {
            "present": True,
            "ok": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {"common_markets": 1},
            "thresholds_bps": {"standard": "2", "rwa": "3"},
            "rows": [{
                "symbol": "XAU", "category": "rwa", "eligible": True,
                "var_half_spread_bps": "0.8", "ondo_half_spread_bps": "1.2",
                "recommended_entry_cost_bps": "0.4",
                "recommended_net_funding_24h_bps": "1.5",
                "recommended_expected_24h_cost_bps": "-1.1",
                "funding_projection_note": "current_rate_24h_equivalent_not_forecast",
                "direction_selection_policy": "entry_cost_first_funding_within_tolerance",
                "minimum_net_funding_24h_bps": "0",
                "recommended": "Var buy / Ondo sell",
                "quote_age_seconds": "2", "volume_24h": "100000",
                "max_spread_bps": "3", "block_reasons": [],
                "entry_signal_confirmed": True,
                "entry_signal_confirmation_count": 2,
                "entry_signal_confirmation_required": 2,
            }],
        },
    })

    result = console._varia_strategy_pools()
    decibel = result["venues"]["decibel"]
    ondo = result["venues"]["ondo"]

    assert decibel["allowed"] == ["BTC"]
    assert ondo["allowed"] == ["XAU"]
    assert ondo["metrics"]["XAU"]["recommended"] == "Var 买 / Ondo 卖"
    assert ondo["metrics"]["XAU"]["funding_projection_note"] == (
        "current_rate_24h_equivalent_not_forecast"
    )
    assert ondo["metrics"]["XAU"]["minimum_net_funding_24h_bps"] == 0
    assert ondo["common"] == ["XAU"]
    assert ondo["categories"] == {"XAU": "rwa"}
    assert ondo["quote_ready"] == ["XAU"]
    assert ondo["confirmed"] == ["XAU"]
    assert ondo["pending_confirmation"] == []
    assert ondo["unstable"] == []
    assert "BTC" not in ondo["metrics"]


def test_varia_ondo_strategy_pool_exposes_confirmation_and_fresh_quote_states(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    peer_dir = data_dir / "ops_peer_state"
    peer_dir.mkdir(parents=True)
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    now = datetime.now(timezone.utc).isoformat()
    _write_json(peer_dir / "vps2.json", {
        "host_id": "vps2",
        "var_ondo_market_scan": {
            "present": True,
            "ok": True,
            "generated_at": now,
            "summary": {"common_markets": 3, "confirmed_markets": 1},
            "rows": [
                {
                    "symbol": "BTC", "category": "major", "eligible": False,
                    "quote_age_seconds": "5",
                    "entry_signal_confirmation_count": 1,
                    "entry_signal_confirmation_required": 2,
                    "pre_confirmation_eligible": True,
                    "block_reasons": ["entry_signal_unconfirmed"],
                },
                {
                    "symbol": "XAU", "category": "rwa", "eligible": False,
                    "quote_age_seconds": "6",
                    "entry_signal_confirmation_count": 1,
                    "entry_signal_confirmation_required": 2,
                    "pre_confirmation_eligible": True,
                    "block_reasons": ["entry_signal_unstable"],
                },
                {
                    "symbol": "ETH", "category": "major", "eligible": False,
                    "quote_age_seconds": "300",
                    "entry_signal_confirmation_count": 0,
                    "entry_signal_confirmation_required": 2,
                    "block_reasons": ["stale_quote"],
                },
            ],
        },
    })

    ondo = console._varia_ondo_strategy_pool()

    assert ondo["quote_ready"] == ["BTC", "XAU"]
    assert ondo["confirmed"] == []
    assert ondo["pending_confirmation"] == ["BTC"]
    assert ondo["unstable"] == ["XAU"]
    assert ondo["metrics"]["BTC"]["entry_signal_confirmation_count"] == 1
    assert ondo["metrics"]["BTC"]["entry_signal_confirmation_required"] == 2


def test_varia_strategy_pools_compare_same_symbol_and_prefer_lower_allowed_cost(
    monkeypatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    peer_dir = data_dir / "ops_peer_state"
    peer_dir.mkdir(parents=True)
    monkeypatch.setattr(console, "VARIA_DIR", data_dir)
    monkeypatch.setattr(console, "VARIA_MARKET_CANDIDATES", ("BTC",))
    monkeypatch.setattr(console, "_varia_strategy_symbol_config", lambda: {
        "major_symbols": ["BTC"], "opportunity_symbols": [],
    })
    monkeypatch.setattr(console, "_varia_latest_quotes", lambda: [{
        "symbol": "BTC", "age_sec": 20,
        "var_bid": 99.99, "var_ask": 100.01,
        "decibel_bid": 99.99, "decibel_ask": 100.01,
        "costs": {"var_buy": 1.4, "var_sell": 1.8},
        "recommended": "var_buy",
    }])
    _write_json(peer_dir / "vps2.json", {
        "host_id": "vps2",
        "var_ondo_market_scan": {
            "present": True,
            "ok": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {"common_markets": 1},
            "thresholds_bps": {"standard": "2", "rwa": "3"},
            "rows": [{
                "symbol": "BTC", "category": "major", "eligible": True,
                "var_half_spread_bps": "0.7", "ondo_half_spread_bps": "0.9",
                "ondo_maker_fee_bps": "1",
                "recommended_entry_cost_bps": "0.6",
                "recommended_net_funding_24h_bps": "2.5",
                "recommended_expected_24h_cost_bps": "-1.9",
                "recommended": "Var sell / Ondo buy",
                "quote_age_seconds": "2", "volume_24h": "100000",
                "max_spread_bps": "2", "block_reasons": [],
            }],
        },
    })

    comparison = console._varia_strategy_pools()["route_comparison"]

    assert comparison == [{
        "symbol": "BTC",
        "preferred": "ondo",
        "preferred_label": "Var/Ondo",
        "reason": "两边均通过，Ondo 入场价差更低",
        "entry_savings_bps": 0.8,
        "expected_24h_savings_bps": None,
        "decibel": {
            "allowed": True,
            "direction": "Var 买 / Decibel 卖",
            "entry_cost_bps": 1.4,
            "net_funding_24h_bps": None,
            "expected_24h_cost_bps": None,
            "var_spread_bps": 1.0,
            "hedge_spread_bps": 1.0,
            "spread_bps": 1.0,
            "maker_fee_bps": 0.0,
        },
        "ondo": {
            "allowed": True,
            "direction": "Var 卖 / Ondo 买",
            "entry_cost_bps": 0.6,
            "net_funding_24h_bps": 2.5,
            "expected_24h_cost_bps": -1.9,
            "var_spread_bps": 0.7,
            "hedge_spread_bps": 0.9,
            "maker_fee_bps": 1.0,
            "spread_bps": 0.9,
        },
    }]


def test_varia_decibel_direction_uses_lowest_expected_24h_cost_within_tolerance() -> None:
    result = console._varia_quote_direction({
        "var_bid": 100.00,
        "var_ask": 100.01,
        "decibel_bid": 100.00,
        "decibel_ask": 100.02,
        # Var is +0.001% per 8h (0.3 bp/day), Decibel is flat.
        "var_funding": 0.001,
        "decibel_funding": 0,
    })

    assert result["costs"]["var_buy"] < result["costs"]["var_sell"]
    assert result["net_funding_24h_bps"] == {
        "var_buy": -0.3,
        "var_sell": 0.3,
    }
    # The funding-positive direction costs ~1 bp more to enter, so its
    # current-rate 24h total is still worse than the cheaper entry direction.
    assert result["recommended"] == "var_buy"
    assert (
        result["expected_24h_cost_bps"]["var_buy"]
        < result["expected_24h_cost_bps"]["var_sell"]
    )


def test_varia_route_comparison_prefers_lower_expected_24h_cost() -> None:
    rows = console._varia_route_comparison(
        {
            "symbols": ["BTC"],
            "metrics": {
                "BTC": {
                    "allowed": True,
                    "recommended": "Var 买 / Decibel 卖",
                    "entry_cost_bps": 0.2,
                    "net_funding_24h_bps": -1.0,
                    "expected_24h_cost_bps": 1.2,
                    "var_spread_bps": 0.4,
                    "decibel_spread_bps": 0.2,
                    "platform_spread_bps": 0.4,
                },
            },
        },
        {
            "metrics": {
                "BTC": {
                    "allowed": True,
                    "recommended": "Var 卖 / Ondo 买",
                    "entry_cost_bps": 0.8,
                    "net_funding_24h_bps": 2.0,
                    "expected_24h_cost_bps": -1.2,
                    "var_spread_bps": 0.4,
                    "ondo_spread_bps": 0.3,
                    "maker_fee_bps": 0,
                },
            },
        },
    )

    assert rows[0]["preferred"] == "ondo"
    assert rows[0]["reason"] == "两边均通过，按当前费率折算的 24h 净成本更低"
    assert rows[0]["entry_savings_bps"] == 0.6
    assert rows[0]["expected_24h_savings_bps"] == 2.4


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


def test_start_automation_rejects_execution_freeze_without_touching_workers(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    monkeypatch.setattr(console, "AUDIT_LOG", tmp_path / "audit.jsonl")
    monkeypatch.setattr(console, "WRITES_ENABLED", True)
    _write_json(tmp_path / "auto_strategy_state.json", {
        "enabled": True,
        "execution_frozen": True,
        "execution_frozen_reason": "leverage_readback_guard_deploy",
        "mode": "full_auto",
        "hosts": {
            "vps1": {"enabled": True, "strategy": "A"},
            "vps2": {"enabled": False, "strategy": "B"},
        },
    })
    actions = []
    monkeypatch.setattr(
        console, "_varia_worker_action",
        lambda host, action: actions.append((host, action)),
    )
    monkeypatch.setattr(
        console, "_varia_automation_state",
        lambda vd=None: {"status": "frozen", "execution_frozen": True},
    )
    request = type("RequestStub", (), {"headers": {}})()

    response = asyncio.run(console.varia_automation_start(request))
    body = json.loads(response.body)

    assert response.status_code == 409
    assert "只读维护" in body["error"]
    assert "未启动任何 worker" in body["error"]
    assert actions == []


def test_start_automation_blocks_vps2_until_ondo_mutations_are_verified(
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
            "vps1": {"enabled": False, "strategy": "A"},
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })
    _write_json(tmp_path / "ops_peer_state" / "vps2.json", {
        "host_id": "vps2",
        "ondo_acceptance": {
            "present": True,
            "environment": "production",
            "live_ready": False,
            "read_only": {"passed": True, "mutations_sent": False},
            "mutation": {
                "leverage_sync": False,
                "post_only_cancel": False,
                "partial_fill_reconcile": False,
                "reduce_only_close": False,
                "paired_micro_hedge": False,
            },
        },
    })
    monkeypatch.setattr(console, "_varia_automation_state", lambda vd=None: {"status": "attention"})
    actions = []
    monkeypatch.setattr(console, "_varia_worker_action", lambda host, action: actions.append((host, action)))
    request = type("RequestStub", (), {"headers": {}})()

    response = asyncio.run(console.varia_automation_start(request))
    body = json.loads(response.body)

    assert response.status_code == 409
    assert "Ondo 真实交易验收待完成" in body["error"]
    assert "未启动任何 worker" in body["error"]
    assert actions == []
    assert json.loads((tmp_path / "auto_strategy_state.json").read_text())["enabled"] is False


def test_start_automation_starts_ready_vps1_and_keeps_blocked_vps2_stopped(
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
            "vps2": {"enabled": True, "strategy": "B"},
        },
    })
    _write_json(tmp_path / "ops_peer_state" / "vps2.json", {
        "host_id": "vps2",
        "ondo_acceptance": {
            "present": True,
            "environment": "production",
            "live_ready": False,
            "read_only": {"passed": True, "mutations_sent": False},
            "mutation": {},
        },
    })
    monkeypatch.setattr(console, "_sync_varia_auto_state_to_vps2", lambda: {"ok": True})
    monkeypatch.setattr(console, "_varia_automation_state", lambda vd=None: {"status": "partial"})
    actions = []

    def fake_action(host, action):
        actions.append((host, action))
        return {"rc": 0, "out": "active" if host == "vps1" else "inactive", "err": ""}

    monkeypatch.setattr(console, "_varia_worker_action", fake_action)
    request = type("RequestStub", (), {"headers": {}})()

    response = asyncio.run(console.varia_automation_start(request))
    body = json.loads(response.body)

    assert response.status_code == 200
    assert body["started_hosts"] == ["vps1"]
    assert any("VPS2" in reason for reason in body["blocked_hosts"])
    assert ("vps1", "start") in actions
    assert ("vps2", "stop") in actions
    saved = json.loads((tmp_path / "auto_strategy_state.json").read_text())
    assert saved["enabled"] is True
    assert saved["hosts"]["vps1"]["enabled"] is True
    assert saved["hosts"]["vps2"]["enabled"] is False


def test_ondo_live_readiness_requires_correct_strategy_and_all_mutations() -> None:
    state = {
        "ondo_acceptance": {
            "present": True,
            "environment": "production",
            "live_ready": True,
            "read_only": {"passed": True, "mutations_sent": False},
            "policy": {"variational_automated_trading_authorized": True},
            "mutation": {
                "leverage_sync": True,
                "post_only_cancel": True,
                "partial_fill_reconcile": True,
                "reduce_only_close": True,
                "paired_micro_hedge": True,
            },
        }
    }

    assert console._varia_host_live_readiness("vps2", state, "B")["ready"] is True
    wrong = console._varia_host_live_readiness("vps2", state, "A")
    assert wrong["ready"] is False
    assert "策略 B" in wrong["reason"]


def test_ondo_live_readiness_keeps_variational_authorization_as_a_hard_gate() -> None:
    state = {
        "ondo_acceptance": {
            "present": True,
            "environment": "production",
            "live_ready": False,
            "read_only": {"passed": True, "mutations_sent": False},
            "policy": {"variational_automated_trading_authorized": False},
            "mutation": {
                "leverage_sync": True,
                "post_only_cancel": True,
                "partial_fill_reconcile": True,
                "reduce_only_close": True,
                "paired_micro_hedge": True,
            },
        }
    }

    readiness = console._varia_host_live_readiness("vps2", state, "B")

    assert readiness["ready"] is False
    assert readiness["variational_policy_ready"] is False
    assert "Variational" in readiness["reason"]


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
    monkeypatch.setattr(
        console,
        "_varia_ondo_strategy_pool",
        lambda: {"common": ["BTC", "XAU", "AAPL"]},
    )

    state = console._varia_control_state({"pairs": [], "single_leg": [], "hosts": {}})

    assert len(state["symbols"]) == 34
    assert state["symbols"][:6] == ["BTC", "ETH", "HYPE", "XAU", "SPCX", "SOL"]
    assert state["symbols"][-3:] == ["CBRS", "ZRO", "CHIP"]
    assert state["symbols_by_host"]["vps1"] == state["symbols"]
    assert state["symbols_by_host"]["vps2"] == ["BTC", "XAU", "AAPL"]
    assert state["host_controls"]["vps2"]["symbols"] == ["BTC", "XAU", "AAPL"]


def test_varia_detail_labels_routes_and_does_not_present_unattributed_rows_as_all(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path)
    with sqlite3.connect(tmp_path / "hedge_bot.sqlite3") as connection:
        connection.execute(
            "CREATE TABLE trades ("
            "id INTEGER PRIMARY KEY, host TEXT, symbol TEXT, timestamp_open TEXT, "
            "timestamp_close TEXT, target_notional REAL, var_side TEXT, "
            "decibel_side TEXT, basis_open_bp REAL, basis_close_bp REAL, "
            "realized_pnl_usdc REAL, realized_cost_bp REAL, status TEXT, strategy TEXT)"
        )
        rows = [
            (1, "vps1", "BTC", "2026-07-23 00:00", "2026-07-23 01:00",
             100, "buy", "sell", 1, 0, 0.1, 1, "executed", "A"),
            (2, "vps2", "XAU", "2026-07-23 00:00", "2026-07-23 01:00",
             120, "sell", "buy", 1, 0, -0.1, 1, "executed", "B"),
            (3, "all", "ETH", "2026-07-23 00:00", "2026-07-23 01:00",
             80, "sell", "buy", 1, 0, 0, 1, "executed", None),
        ]
        connection.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    detail = console._varia_detail()

    assert [row["host"] for row in detail["trades"]] == [
        "VPS1", "VPS2", "未标记",
    ]
    assert detail["trades"][0]["route"] == "VPS1 · Var/Decibel"
    assert detail["trades"][0]["side"] == "Var 买 / Decibel 卖"
    assert detail["trades"][1]["route"] == "VPS2 · Var/Ondo"
    assert detail["trades"][1]["side"] == "Var 卖 / Ondo 买"
    assert detail["trades"][2]["route"] == "未标记 · 历史记录"
    assert detail["trades"][2]["side"] == "Var 卖 / 对冲腿 买"
    assert {row["name"] for row in detail["by_host"]} == {
        "VPS1 · Var/Decibel",
        "VPS2 · Var/Ondo",
        "未标记 · 历史记录",
    }


def test_varia_vps2_command_routes_to_peer_without_secrets(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(console, "VARIA_DIR", tmp_path / "data")
    command = console._varia_live_command(
        host="vps2", symbol="SOL", var_side="buy", quantity=1.25,
        leverage=6, notional=100, hedge_venue="ondo",
    )

    assert command[0] == "ssh"
    assert console.VARIA_VPS2_SSH in command
    assert "ubuntu@100.101.50.40" in command
    assert "--symbol SOL" in command[-1]
    assert "--leverage 6" in command[-1]
    assert "--leverage-cap 40" in command[-1]
    assert "--hedge-venue ondo" in command[-1]
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
                    {"accountId": "Hk-002", "owner": "测试员", "status": "中签",
                     "stockCode": "9002", "broker": "富途", "method": "融资",
                     "financingCost": 88, "tradePnl": 1200, "netPnl": 1112,
                     "settledAt": "2026-07-21T12:00:00+08:00"},
                ],
            }
        } if url == console.IPO_STATE_URL else {},
    )

    result = console._ipo()

    assert result["entries"][0]["stock"] == "模拟新股"
    assert result["entries"][1]["stock"] == "9002"
    assert result["entries"][1]["broker"] == "富途"
    assert result["entries"][1]["method"] == "融资"
    assert result["entries"][1]["financing_cost"] == 88
    assert result["entries"][1]["net_pnl"] == 1112


def test_ipo_console_uses_contextual_account_actions() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    assert "function ipoActionButtons(entry,hasActiveStocks)" in html
    assert "if(entry.status==='待申购')" in html
    assert "if(entry.status==='已申购')" in html
    assert "当前可申购新股" in html
    assert "系统已按判研、资金和账号状态生成建议" in html
    assert 'id="ipo-entry-dialog"' in html
    assert "action:mode==='settlement'?'settle_result':(mode==='batch'?'apply_round_strategy':'set_strategy')" in html
    assert 'id="ipo-apply-suggestion"' in html
    assert "suggested_action" in html
    assert "批量确认 / 调整方案" in html
    assert "function openIpoBatchDialog()" in html
    assert "apply_round_strategy" in html
    assert "人工调整过的账号会保留" in html
    assert "一键申购(活跃)" not in html
    assert "结束本轮" not in html
    assert 'id="ipo-auto-mode"' in html
    assert "GPT 自动策略" in html
    assert "由 GPT 结合 PDF" in html
    assert 'data-mode="conservative"' not in html
    assert "action:'set_mode'" not in html


def test_ipo_console_shows_live_summary_and_safe_action_feedback() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")

    for key in ("ipo-active-count", "ipo-pending-count", "ipo-subscribed-count", "ipo-updated-age"):
        assert f'data-k="{key}"' in html
    assert "const hasActiveStocks=activeCount>0" in html
    assert "已申购及历史结算仍可继续处理" in html
    assert "等待新股" in html
    assert "function showIpoToast(text,kind)" in html
    assert "function showIpoRowStatus(button,text,kind)" in html
    assert "window.confirm('确认将 '" in html
    assert 'class="ipo-stock-card"' in html
    assert "建议：" in html
    assert "资料完整度" in html
    assert "基本面" in html and "估值" in html and "热度" in html and "首日" in html
    assert "include_pdf_details:true" in html


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


def test_ipo_import_alert_uses_verified_success_stamp(monkeypatch, tmp_path: Path) -> None:
    stamp = tmp_path / "ipo_import.success"
    stamp.write_text("2026-07-19T01:00:00+08:00\n", encoding="utf-8")
    monkeypatch.setattr(console, "IPO_IMPORT_SUCCESS_STAMP", stamp)
    monkeypatch.setattr(
        console,
        "_mtime_age",
        lambda path: 27 * 3600 if path == stamp else None,
    )

    alerts = console._alerts({}, {}, {}, {"present": True}, {"present": True}, {})
    assert any("每日新股导入超期" in item["msg"] for item in alerts)

    monkeypatch.setattr(
        console,
        "_mtime_age",
        lambda path: 60 if path == stamp else None,
    )
    alerts = console._alerts({}, {}, {}, {"present": True}, {"present": True}, {})
    assert not any("每日新股导入超期" in item["msg"] for item in alerts)


def test_ipo_import_timer_retries_until_success() -> None:
    script = (ROOT / "deploy" / "latitude-console" / "ipo_import_daily.sh").read_text(
        encoding="utf-8"
    )
    timer = (ROOT / "deploy" / "systemd" / "latitude-ipo-import.timer").read_text(
        encoding="utf-8"
    )

    assert "ipo_import.success" in script
    assert "--connect-timeout 8" in script
    assert "--max-time 180" in script
    assert "timer will retry" in script
    assert "OnCalendar=*-*-* 01:00:00" in timer
    assert "OnUnitInactiveSec=15min" in timer
    assert "Persistent=true" in timer


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
    assert 'id="ipo-pdf-input"' in html
    assert 'id="ipo-pdf-pages"' in html
    assert 'id="ipo-pdf-read"' not in html
    assert "视觉读取 PDF</button>" not in html
    assert "/api/ipo/research-pdf" in html
    assert "PDF 视觉分析" in html
    assert "X-Page-Range" in html
    assert "账户准备度" in html
    assert "不生成虚假申购方案" in html
