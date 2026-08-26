from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from platforms.polymarket.maker import rewards_live


BJT = ZoneInfo("Asia/Shanghai")


def test_reward_day_rolls_at_0800_beijing() -> None:
    before = datetime(2026, 7, 28, 7, 59, tzinfo=BJT)
    after = datetime(2026, 7, 28, 8, 0, tzinfo=BJT)

    assert rewards_live.reward_day_utc(before).isoformat() == "2026-07-27"
    assert rewards_live.reward_day_utc(after).isoformat() == "2026-07-28"
    start, end = rewards_live.reward_window_bjt(before)
    assert start.isoformat() == "2026-07-27T08:00:00+08:00"
    assert end.isoformat() == "2026-07-28T08:00:00+08:00"


def test_refresh_separates_live_day_and_finalized_history(tmp_path: Path) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    for idx in (1, 2):
        (config_dir / f"config_{idx}.json").write_text(
            json.dumps(
                {"account": {"funder": "0x" + str(idx) * 40}}
            ),
            encoding="utf-8",
        )
    data_dir.mkdir()
    (data_dir / "rewards_cumulative.json").write_text(
        json.dumps(
            {
                "version": 1,
                "accounts": {
                    "0": {
                        "daily": {"2026-04-23": 99},
                        "cumulative_usd": 99,
                    },
                    "1": {
                        "daily": {"2026-07-25": 1.25},
                        "cumulative_usd": 1.25,
                        "last_snapshot_date": "2026-07-25",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    def build_client(path: Path) -> tuple:
        idx = int(path.stem.rsplit("_", 1)[1])
        return idx, f"address-{idx}", 2, None

    amounts = {
        (1, "2026-07-26"): 0.5,
        (1, "2026-07-27"): 0.75,
        (2, "2026-07-26"): 1.0,
        (2, "2026-07-27"): 1.4,
    }

    def fetch_daily(client: int, _signature_type: int, day: str) -> float:
        return amounts[(client, day)]

    rebate_amounts = {
        ("0x" + "1" * 40, "2026-07-26"): 0.2,
        ("0x" + "1" * 40, "2026-07-27"): 0.3,
        ("0x" + "2" * 40, "2026-07-26"): 0.4,
        ("0x" + "2" * 40, "2026-07-27"): 0.5,
    }

    def fetch_rebate(maker_address: str, day: str) -> float:
        return rebate_amounts[(maker_address, day)]

    percentages = {
        1: {"condition-a": 62.5},
        2: {"condition-b": 18.25},
    }

    def fetch_percentages(client: int, _signature_type: int) -> dict:
        return percentages[client]

    def fetch_pnl(client: int, _addresses: list, *, now: datetime) -> dict:
        return {
            "status": "ok",
            "complete": True,
            "realized_pnl_usd": float(client),
            "updated_at": now.isoformat(),
        }

    state = asyncio.run(
        rewards_live.refresh_rewards(
            rewards_live.discover_configs(config_dir),
            data_dir,
            now=datetime(2026, 7, 28, 7, 59, tzinfo=BJT),
            build_client=build_client,
            fetch_daily=fetch_daily,
            fetch_rebate=fetch_rebate,
            fetch_percentages=fetch_percentages,
            fetch_pnl=fetch_pnl,
        )
    )

    assert state["reward_date_utc"] == "2026-07-27"
    assert state["window_label_bjt"] == "07-27 08:00 - 07-28 08:00"
    assert state["total_today_usd"] == 2.15
    assert state["total_previous_usd"] == 1.5
    assert state["total_today_rebates_usd"] == 0.8
    assert state["total_previous_rebates_usd"] == 0.6
    assert state["total_today_income_usd"] == 2.95
    assert state["accounts"]["1"]["today_usd"] == 0.75
    assert state["accounts"]["1"]["today_rebates_usd"] == 0.3
    assert state["accounts"]["1"]["today_total_income_usd"] == 1.05
    assert state["accounts"]["2"]["today_usd"] == 1.4
    assert state["accounts"]["2"]["today_rebates_usd"] == 0.5
    assert state["accounts"]["1"]["reward_percentages"] == {
        "condition-a": 62.5
    }
    assert state["successful_percentage_accounts"] == 2
    assert state["successful_pnl_accounts"] == 2
    assert state["accounts"]["1"]["pnl"]["realized_pnl_usd"] == 1.0

    cumulative = json.loads(
        (data_dir / "rewards_cumulative.json").read_text(encoding="utf-8")
    )
    assert cumulative["accounts"]["0"]["cumulative_usd"] == 99
    assert cumulative["accounts"]["1"]["daily"]["2026-07-26"] == 0.5
    assert cumulative["accounts"]["1"]["cumulative_usd"] == 1.75
    assert cumulative["accounts"]["1"]["rebates_daily"]["2026-07-26"] == 0.2
    assert cumulative["accounts"]["1"]["rebates_cumulative_usd"] == 0.2
    assert cumulative["accounts"]["1"]["income_cumulative_usd"] == 1.95
    assert cumulative["accounts"]["2"]["daily"]["2026-07-26"] == 1.0
    assert cumulative["accounts"]["2"]["rebates_daily"]["2026-07-26"] == 0.4
    assert "2026-07-27" not in cumulative["accounts"]["1"]["daily"]
    assert "2026-07-27" not in cumulative["accounts"]["1"]["rebates_daily"]


def test_finalized_reward_never_decreases_on_empty_rollover_response(
    tmp_path: Path,
) -> None:
    config = tmp_path / "config_1.json"
    config.write_text("{}", encoding="utf-8")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "rewards_cumulative.json").write_text(
        json.dumps(
            {
                "accounts": {
                    "1": {
                        "daily": {"2026-07-27": 3.25},
                        "cumulative_usd": 3.25,
                        "last_snapshot_date": "2026-07-27",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def build_client(_path: Path) -> tuple:
        return object(), "address-1", 2, None

    def fetch_daily(_client: object, _signature_type: int, day: str) -> float:
        return 0.0 if day == "2026-07-27" else 0.2

    asyncio.run(
        rewards_live.refresh_rewards(
            [(1, config)],
            data_dir,
            now=datetime(2026, 7, 28, 8, 1, tzinfo=BJT),
            build_client=build_client,
            fetch_daily=fetch_daily,
            fetch_percentages=lambda *_args: {},
        )
    )
    cumulative = json.loads(
        (data_dir / "rewards_cumulative.json").read_text(encoding="utf-8")
    )
    assert cumulative["accounts"]["1"]["daily"]["2026-07-27"] == 3.25


def test_systemd_timer_is_aligned_to_five_minute_boundaries() -> None:
    root = Path(__file__).resolve().parents[1]
    timer = (
        root / "deploy" / "systemd" / "polymarket-rewards-live.timer"
    ).read_text(encoding="utf-8")
    service = (
        root / "deploy" / "systemd" / "polymarket-rewards-live.service"
    ).read_text(encoding="utf-8")

    assert "OnCalendar=*-*-* *:00/5:00" in timer
    assert "Persistent=true" in timer
    assert "rewards_live" in service
    assert "/home/ubuntu/.venv2/bin/python" in service


def test_rebate_endpoint_sums_official_market_rows(monkeypatch) -> None:
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return json.dumps(
                [
                    {"rebated_fees_usdc": "1.25"},
                    {"rebated_fees_usdc": "0.65"},
                ]
            ).encode("utf-8")

    def fake_open(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(rewards_live, "urlopen", fake_open)
    total = rewards_live._fetch_daily_rebate_usd(
        "0x" + "a" * 40,
        "2026-07-28",
    )

    assert total == 1.9
    assert "date=2026-07-28" in captured["url"]
    assert "maker_address=0x" in captured["url"]
    assert captured["timeout"] == 20


def test_detailed_ledger_deduplicates_same_maker_and_tracks_stale(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    data_dir.mkdir()
    maker = "0x" + "a" * 40
    for idx in (1, 2):
        (config_dir / f"config_{idx}.json").write_text(
            json.dumps(
                {
                    "account": {
                        "funder": maker,
                        "chain_id": 137,
                        "signature_type": 2,
                    }
                }
            ),
            encoding="utf-8",
        )

    def build_client(path: Path) -> tuple:
        idx = int(path.stem.rsplit("_", 1)[1])
        return idx, maker, 2, None

    def fetch_market(
        _client: int,
        _signature_type: int,
        day: str,
        _maker: str,
        sponsored: bool,
    ) -> list:
        return [
            {
                "condition_id": "condition-a",
                "asset_address": "asset-a",
                "earnings": "2" if sponsored else "1",
                "asset_rate": "1",
                "day": day,
            }
        ]

    def fetch_rebate_rows(_maker: str, day: str) -> list:
        return [
            {
                "condition_id": "condition-a",
                "asset_address": "usdc",
                "rebated_fees_usdc": "0.4",
                "day": day,
            }
        ]

    def fetch_pnl(_client: int, _addresses: list, *, now: datetime) -> dict:
        return {
            "status": "ok",
            "complete": True,
            "realized_exits": [
                {
                    "complete": True,
                    "market": "condition-a",
                    "asset_id": "asset-a",
                    "epoch": int(now.timestamp()),
                    "net_pnl_usd": 0.25,
                }
            ],
        }

    kwargs = {
        "now": datetime(2026, 8, 26, 10, 0, tzinfo=BJT),
        "build_client": build_client,
        "fetch_daily": lambda *_args: 3.0,
        "fetch_rebate": lambda *_args: 0.4,
        "fetch_percentages": lambda *_args: {"condition-a": 50.0},
        "fetch_pnl": fetch_pnl,
        "fetch_market_earnings": fetch_market,
        "fetch_rebate_rows": fetch_rebate_rows,
    }
    state = asyncio.run(
        rewards_live.refresh_rewards(
            rewards_live.discover_configs(config_dir),
            data_dir,
            **kwargs,
        )
    )

    assert state["configured_accounts"] == 2
    assert state["canonical_accounts"] == 1
    assert state["duplicate_account_aliases"] == 1
    assert state["total_today_usd"] == 3.0
    assert state["total_today_rebates_usd"] == 0.4
    summary = state["reward_ledger"]["summary"]
    assert summary["record_count"] == 4
    assert summary["current_usd_by_type"] == {
        "maker_rebate": 0.4,
        "native_lp": 1.0,
        "sponsored_lp": 2.0,
        "trading_pnl": 0.25,
    }

    second = asyncio.run(
        rewards_live.refresh_rewards(
            rewards_live.discover_configs(config_dir),
            data_dir,
            **kwargs,
        )
    )
    assert second["reward_ledger"]["summary"]["record_count"] == 4

    def failing_market(*_args) -> list:
        raise TimeoutError("market earnings unavailable")

    def failing_rebates(*_args) -> list:
        raise TimeoutError("rebates unavailable")

    def failing_pnl(*_args, **_kwargs) -> dict:
        raise TimeoutError("trades unavailable")

    stale_kwargs = dict(kwargs)
    stale_kwargs.update(
        {
            "fetch_market_earnings": failing_market,
            "fetch_rebate_rows": failing_rebates,
            "fetch_pnl": failing_pnl,
        }
    )
    stale = asyncio.run(
        rewards_live.refresh_rewards(
            rewards_live.discover_configs(config_dir),
            data_dir,
            **stale_kwargs,
        )
    )
    stale_summary = stale["reward_ledger"]["summary"]
    assert stale_summary["current_total_usd"] == 0.0
    assert stale_summary["last_known_total_usd"] == 3.65
    assert stale_summary["stale_record_count"] == 4
