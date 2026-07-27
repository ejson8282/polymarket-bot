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
        (config_dir / f"config_{idx}.json").write_text("{}", encoding="utf-8")
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

    state = asyncio.run(
        rewards_live.refresh_rewards(
            rewards_live.discover_configs(config_dir),
            data_dir,
            now=datetime(2026, 7, 28, 7, 59, tzinfo=BJT),
            build_client=build_client,
            fetch_daily=fetch_daily,
        )
    )

    assert state["reward_date_utc"] == "2026-07-27"
    assert state["window_label_bjt"] == "07-27 08:00 - 07-28 08:00"
    assert state["total_today_usd"] == 2.15
    assert state["total_previous_usd"] == 1.5
    assert state["accounts"]["1"]["today_usd"] == 0.75
    assert state["accounts"]["2"]["today_usd"] == 1.4

    cumulative = json.loads(
        (data_dir / "rewards_cumulative.json").read_text(encoding="utf-8")
    )
    assert cumulative["accounts"]["0"]["cumulative_usd"] == 99
    assert cumulative["accounts"]["1"]["daily"]["2026-07-26"] == 0.5
    assert cumulative["accounts"]["1"]["cumulative_usd"] == 1.75
    assert cumulative["accounts"]["2"]["daily"]["2026-07-26"] == 1.0
    assert "2026-07-27" not in cumulative["accounts"]["1"]["daily"]


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
