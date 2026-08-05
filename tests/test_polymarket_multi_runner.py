import asyncio
import json
import sys
import types
from pathlib import Path

import pytest

from platforms.polymarket.maker.account_roster import parse_runtime_roster

try:
    import py_clob_client_v2  # noqa: F401
except ModuleNotFoundError:
    # The orchestration helpers are pure. Keep this test runnable in a minimal
    # local environment without pretending to exercise the exchange client.
    engine_stub = types.ModuleType("engine")
    engine_stub.PolyLPSMulti = object
    engine_stub.log = lambda _message: None
    sys.modules.setdefault("engine", engine_stub)
    sys.modules.setdefault("platforms.polymarket.maker.engine", engine_stub)

from platforms.polymarket.maker.multi_runner import (
    _cancel_accounts_preserving_exits,
    _require_pause_flags,
    _resolve_host_id,
    _roster_config_files,
    _verify_roster_config,
    multi_run,
)
from scripts.generate_configs import _render


def _row(index: int, host_id: str, port: int) -> dict:
    return {
        "account_index": index,
        "host_id": host_id,
        "funder": "0x" + f"{index:040x}",
        "clash_port": port,
        "lp_account": {
            "account_id": f"lp_{index}",
            "profile_type": "aggressive",
            "strategy_group": "aggressive",
            "target_principal_usdc": 100,
            "allocation_mode": "exclusive",
        },
    }


def test_host_id_must_be_explicit_for_multi_host_roster() -> None:
    accounts = parse_runtime_roster([_row(1, "vps1", 7901), _row(6, "vps2", 7901)])

    with pytest.raises(ValueError, match="--host-id is required"):
        _resolve_host_id(accounts, "")
    assert _resolve_host_id(accounts, "VPS2") == "vps2"


def test_roster_config_selection_is_host_local_and_requires_every_config(tmp_path: Path) -> None:
    accounts = parse_runtime_roster(
        [_row(1, "vps1", 7901), _row(2, "vps1", 7902), _row(6, "vps2", 7901)]
    )
    (tmp_path / "config_1.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="config_2.json"):
        _roster_config_files(tmp_path, accounts, "vps1")

    (tmp_path / "config_2.json").write_text("{}", encoding="utf-8")
    selected = _roster_config_files(tmp_path, accounts, "vps1")
    assert [(account.account_index, path.name) for account, path in selected] == [
        (1, "config_1.json"),
        (2, "config_2.json"),
    ]


def test_generated_config_must_match_roster_and_digest(tmp_path: Path) -> None:
    from platforms.polymarket.maker.account_roster import (
        market_universe_sha256,
        routing_roster_sha256,
    )

    accounts = parse_runtime_roster([_row(1, "vps1", 7901)])
    digest = routing_roster_sha256(accounts)
    account = accounts[0]
    rendered = _render(
        {
            "account": {
                "signer_server_url": "http://signer.invalid",
                "signer_token": "removed",
            },
            "markets": [{"token_id": "1", "paired_token_id": "2"}],
        },
        account.generation_entry(),
        "127.0.0.1",
        roster_sha256=digest,
    )
    path = tmp_path / "config_1.json"
    path.write_text(json.dumps(rendered), encoding="utf-8")

    _verify_roster_config(account, path, digest)
    assert "signer_token" not in rendered["account"]
    assert rendered["account"]["private_key"] == "REDACTED"
    assert rendered["runtime_account"]["clash_port"] == 7901
    assert rendered["runtime_account"]["market_universe_sha256"] == market_universe_sha256(
        rendered
    )

    rendered["runtime_account"]["routing_roster_sha256"] = "stale"
    path.write_text(json.dumps(rendered), encoding="utf-8")
    with pytest.raises(ValueError, match="metadata is stale"):
        _verify_roster_config(account, path, digest)

    rendered = _render(
        {"account": {}, "markets": [{"token_id": "1"}]},
        account.generation_entry(),
        "127.0.0.1",
        roster_sha256=digest,
    )
    rendered["proxy_pool"]["items"][0]["url"] = "http://127.0.0.1:7999"
    path.write_text(json.dumps(rendered), encoding="utf-8")
    with pytest.raises(ValueError, match="proxy port does not match"):
        _verify_roster_config(account, path, digest)

    rendered = _render(
        {"account": {}, "markets": [{"token_id": "1"}]},
        account.generation_entry(),
        "127.0.0.1",
        roster_sha256=digest,
    )
    rendered["markets"].append({"token_id": "3"})
    path.write_text(json.dumps(rendered), encoding="utf-8")
    with pytest.raises(ValueError, match="metadata is stale"):
        _verify_roster_config(account, path, digest)


def test_require_paused_checks_every_local_account(tmp_path: Path) -> None:
    (tmp_path / ".account_1.paused").touch()

    with pytest.raises(ValueError, match=r"\.account_2\.paused"):
        _require_pause_flags(tmp_path, [1, 2])


def test_roster_mode_rejects_different_local_market_universes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from platforms.polymarket.maker.account_roster import routing_roster_sha256

    rows = [_row(1, "vps1", 7901), _row(2, "vps1", 7902)]
    accounts = parse_runtime_roster(rows)
    digest = routing_roster_sha256(accounts)
    roster_path = tmp_path / "accounts.runtime.json"
    roster_path.write_text(
        json.dumps({"schema_version": 1, "accounts": rows}),
        encoding="utf-8",
    )
    for account in accounts:
        rendered = _render(
            {
                "account": {},
                "markets": [{"token_id": str(account.account_index)}],
            },
            account.generation_entry(),
            "127.0.0.1",
            roster_sha256=digest,
        )
        (tmp_path / f"config_{account.account_index}.json").write_text(
            json.dumps(rendered),
            encoding="utf-8",
        )

    constructed = []
    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.PolyLPSMulti",
        lambda config_path: constructed.append(config_path),
    )

    with pytest.raises(ValueError, match="different market universes"):
        asyncio.run(
            multi_run(
                tmp_path,
                roster_path=roster_path,
                host_id="vps1",
                data_dir=tmp_path / "data",
            )
        )
    assert constructed == []


def test_shutdown_cancels_maker_orders_and_preserves_exits() -> None:
    class Engine:
        def __init__(self, result: bool):
            self.result = result
            self.called = 0

        async def _cancel_all_except_exit(self) -> bool:
            self.called += 1
            return self.result

    first = Engine(True)
    second = Engine(True)

    assert asyncio.run(_cancel_accounts_preserving_exits([(1, first), (2, second)])) is True
    assert first.called == second.called == 1


def test_one_initialization_failure_prevents_all_workers(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "config_1.json").write_text("{}", encoding="utf-8")
    (tmp_path / "config_2.json").write_text("{}", encoding="utf-8")
    constructed = []

    class Engine:
        def __init__(self, config_path: str):
            index = int(Path(config_path).stem.split("_")[-1])
            constructed.append(index)
            if index == 2:
                raise RuntimeError("signer unavailable")
            self._account_idx = index
            self.market_cfg = {}
            self._night_market_cfg = {}
            self.lp_account_profile = None
            self._state_path = tmp_path / "data" / f"engine_state_{index}.json"

    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.PolyLPSMulti",
        Engine,
    )

    with pytest.raises(RuntimeError, match="signer unavailable"):
        asyncio.run(multi_run(tmp_path, data_dir=tmp_path / "data"))
    assert constructed == [1, 2]


def test_worker_failure_stops_all_local_accounts_with_global_routing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from platforms.polymarket.maker.account_profiles import parse_lp_account_profile
    from platforms.polymarket.maker.account_roster import routing_roster_sha256

    rows = [_row(1, "vps1", 7901), _row(2, "vps1", 7902), _row(6, "vps2", 7901)]
    accounts = parse_runtime_roster(rows)
    digest = routing_roster_sha256(accounts)
    roster_path = tmp_path / "accounts.runtime.json"
    roster_path.write_text(
        json.dumps({"schema_version": 1, "accounts": rows}),
        encoding="utf-8",
    )
    for account in accounts[:2]:
        rendered = _render(
            {"account": {}},
            account.generation_entry(),
            "127.0.0.1",
            roster_sha256=digest,
        )
        (tmp_path / f"config_{account.account_index}.json").write_text(
            json.dumps(rendered),
            encoding="utf-8",
        )

    built = {}

    class Bus:
        def __init__(self):
            self.namespace = ""

        def set_state_namespace(self, namespace: str) -> None:
            self.namespace = namespace

    class Engine:
        def __init__(self, config_path: str):
            self._account_idx = int(Path(config_path).stem.split("_")[-1])
            self.cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            self.lp_account_profile = parse_lp_account_profile(self.cfg, self._account_idx)
            self.market_cfg = {}
            self._night_market_cfg = {}
            self._event_bus = Bus()
            self._running = True
            self.cancelled = False
            self._state_path = tmp_path / "data" / f"engine_state_{self._account_idx}.json"
            built[self._account_idx] = self

        async def run(self) -> None:
            if self._account_idx == 2:
                raise RuntimeError("worker exploded")
            while self._running:
                await asyncio.sleep(0)

        async def _cancel_all_except_exit(self) -> bool:
            self.cancelled = True
            return True

    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.PolyLPSMulti",
        Engine,
    )
    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.random.uniform",
        lambda _low, _high: 0.0,
    )

    with pytest.raises(RuntimeError, match="worker account_2 failed"):
        asyncio.run(
            multi_run(
                tmp_path,
                roster_path=roster_path,
                host_id="vps1",
                data_dir=tmp_path / "data",
            )
        )

    assert set(built[1]._shared_account_profiles) == {1, 2, 6}
    assert built[1]._event_bus.namespace == "account:1"
    assert built[2]._event_bus.namespace == "account:2"
    assert built[1]._runtime_market_updates_enabled is False
    assert built[2]._runtime_market_updates_enabled is False
    assert built[1].cancelled is True
    assert built[2].cancelled is True


def test_validate_only_initializes_accounts_without_starting_workers(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from platforms.polymarket.maker.account_profiles import parse_lp_account_profile
    from platforms.polymarket.maker.account_roster import routing_roster_sha256

    rows = [_row(1, "vps1", 7901), _row(2, "vps1", 7902)]
    accounts = parse_runtime_roster(rows)
    digest = routing_roster_sha256(accounts)
    roster_path = tmp_path / "accounts.runtime.json"
    roster_path.write_text(
        json.dumps({"schema_version": 1, "accounts": rows}),
        encoding="utf-8",
    )
    for account in accounts:
        rendered = _render(
            {"account": {}, "markets": [{"token_id": "1"}]},
            account.generation_entry(),
            "127.0.0.1",
            roster_sha256=digest,
        )
        (tmp_path / f"config_{account.account_index}.json").write_text(
            json.dumps(rendered),
            encoding="utf-8",
        )

    built = []

    class Bus:
        def set_state_namespace(self, _namespace: str) -> None:
            return None

    class Engine:
        def __init__(self, config_path: str):
            self._account_idx = int(Path(config_path).stem.split("_")[-1])
            self.cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            self.lp_account_profile = parse_lp_account_profile(self.cfg, self._account_idx)
            self.market_cfg = {"1": {}}
            self._night_market_cfg = {}
            self._event_bus = Bus()
            self._state_path = tmp_path / "data" / f"engine_state_{self._account_idx}.json"
            built.append(self)

        async def run(self) -> None:
            raise AssertionError("validate-only must not start account workers")

    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.PolyLPSMulti",
        Engine,
    )

    asyncio.run(
        multi_run(
            tmp_path,
            roster_path=roster_path,
            host_id="vps1",
            data_dir=tmp_path / "data",
            validate_only=True,
        )
    )

    assert len(built) == 2
    assert all(engine._runtime_market_updates_enabled is False for engine in built)
