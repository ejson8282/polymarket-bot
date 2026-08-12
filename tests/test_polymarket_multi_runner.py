import asyncio
import json
import sys
import types
from pathlib import Path

import pytest

from platforms.polymarket.maker.account_roster import (
    market_universe_sha256,
    parse_runtime_roster,
)

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
    SharedBookCache,
    _aggressive_reward_observer_loop,
    _cancel_accounts_preserving_exits,
    _require_pause_flags,
    _resolve_host_id,
    _roster_config_files,
    _shared_book_fetcher,
    _verify_roster_config,
    _verify_expected_digest,
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
    from platforms.polymarket.maker.account_roster import routing_roster_sha256

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


def test_expected_digest_is_strict_and_fail_closed() -> None:
    digest = "a" * 64
    _verify_expected_digest("market universe", digest, digest)

    with pytest.raises(ValueError, match="must be 64 hex"):
        _verify_expected_digest("market universe", "abc", digest)
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        _verify_expected_digest("market universe", "b" * 64, digest)


def test_shared_book_cache_pins_a_complete_quote_cycle(monkeypatch) -> None:
    now = {"value": 100.0}
    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.time.time",
        lambda: now["value"],
    )
    cache = SharedBookCache(ttl_sec=0.5)
    books = [
        types.SimpleNamespace(asset_id=token, bids=[1], asks=[1])
        for token in ("101", "102")
    ]

    assert cache.put_many(books) == 2
    cycle = cache.snapshot(["101", "102", "missing"])
    now["value"] = 101.0

    assert sorted(cycle) == ["101", "102"]
    assert cycle["101"].book is books[0]
    assert cycle["101"].fetched_at == 100.0
    assert cache.get("101") is None
    stats = cache.stats()
    assert stats["generation"] == 1
    assert stats["cache_hits"] == 2
    assert stats["cache_misses"] == 2


def test_shared_book_cache_bounds_direct_rest_fallbacks(monkeypatch) -> None:
    now = {"value": 200.0}
    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.time.time",
        lambda: now["value"],
    )
    cache = SharedBookCache(
        direct_rest_burst=2,
        direct_rest_window_sec=1.0,
    )

    assert cache.allow_direct_rest() is True
    assert cache.allow_direct_rest() is True
    assert cache.allow_direct_rest() is False
    now["value"] = 201.1
    assert cache.allow_direct_rest() is True

    stats = cache.stats()
    assert stats["direct_rest_fallbacks"] == 3
    assert stats["direct_rest_suppressed"] == 1


def test_shared_book_fetcher_uses_bounded_chunk_batches_after_failure() -> None:
    class Client:
        def __init__(self) -> None:
            self.batch_sizes = []
            self.per_token_calls = 0

        def get_order_books(self, payload):
            self.batch_sizes.append(len(payload))
            if len(self.batch_sizes) == 1:
                raise RuntimeError("batch timeout")
            if len(self.batch_sizes) == 4:
                engine._running = False
            return [
                types.SimpleNamespace(
                    asset_id=str(row["token_id"]),
                    bids=[1],
                    asks=[1],
                )
                for row in payload
            ]

        def get_order_book(self, _token_id):
            self.per_token_calls += 1
            raise AssertionError("per-token fallback must not run")

    client = Client()
    engine = types.SimpleNamespace(
        _running=True,
        client=client,
        _shared_book_chunk_size=10,
        _shared_book_chunk_concurrency=2,
    )
    cache = SharedBookCache()
    token_ids = [str(index) for index in range(25)]

    asyncio.run(
        _shared_book_fetcher(
            engine,
            lambda: token_ids,
            cache,
            fetch_interval_sec=0.0,
        )
    )

    assert client.batch_sizes[0] == 25
    assert sorted(client.batch_sizes[1:]) == [5, 10, 10]
    assert client.per_token_calls == 0
    stats = cache.stats()
    assert stats["full_batch_failures"] == 1
    assert stats["chunk_batch_requests"] == 3
    assert stats["chunk_batch_successes"] == 3
    assert stats["books_stored"] == 25


def test_shared_book_fetcher_rate_limit_backs_off_without_chunk_burst() -> None:
    class Client:
        def __init__(self) -> None:
            self.batch_calls = 0

        def get_order_books(self, _payload):
            self.batch_calls += 1
            engine._running = False
            raise RuntimeError("429 too many requests")

        def get_order_book(self, _token_id):
            raise AssertionError("rate limits must never trigger per-token fallback")

    client = Client()
    engine = types.SimpleNamespace(
        _running=True,
        client=client,
        _shared_book_chunk_size=10,
        _shared_book_chunk_concurrency=2,
    )
    cache = SharedBookCache()

    asyncio.run(
        _shared_book_fetcher(
            engine,
            lambda: ["101", "102"],
            cache,
            fetch_interval_sec=0.0,
        )
    )

    assert client.batch_calls == 1
    stats = cache.stats()
    assert stats["full_batch_failures"] == 1
    assert stats["chunk_batch_requests"] == 0
    assert stats["backoff_sec"] == 1.0


def test_shared_book_fetcher_stops_queued_chunks_after_chunk_rate_limit() -> None:
    class Client:
        def __init__(self) -> None:
            self.batch_sizes = []

        def get_order_books(self, payload):
            self.batch_sizes.append(len(payload))
            if len(self.batch_sizes) == 1:
                raise RuntimeError("batch timeout")
            engine._running = False
            raise RuntimeError("429 too many requests")

        def get_order_book(self, _token_id):
            raise AssertionError("chunk rate limits must not trigger per-token fallback")

    client = Client()
    engine = types.SimpleNamespace(
        _running=True,
        client=client,
        _shared_book_chunk_size=10,
        _shared_book_chunk_concurrency=1,
    )
    cache = SharedBookCache()

    asyncio.run(
        _shared_book_fetcher(
            engine,
            lambda: [str(index) for index in range(50)],
            cache,
            fetch_interval_sec=0.0,
        )
    )

    assert client.batch_sizes == [50, 10]
    stats = cache.stats()
    assert stats["full_batch_failures"] == 1
    assert stats["chunk_batch_requests"] == 1
    assert stats["chunk_batch_failures"] == 1
    assert stats["backoff_sec"] == 1.0


def test_multi_host_roster_requires_both_reviewed_digests(tmp_path: Path) -> None:
    rows = [_row(1, "vps1", 7901), _row(2, "vps2", 7901)]
    roster_path = tmp_path / "accounts.runtime.json"
    roster_path.write_text(
        json.dumps({"schema_version": 1, "accounts": rows}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requires --expected-roster-sha256"):
        asyncio.run(
            multi_run(
                tmp_path,
                roster_path=roster_path,
                host_id="vps1",
                data_dir=tmp_path / "data",
            )
        )


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

        def set_runtime_namespace(self, _namespace: str) -> None:
            return None

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
                expected_roster_sha256=digest,
                expected_market_sha256=market_universe_sha256({}),
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
        def set_runtime_namespace(self, _namespace: str) -> None:
            return None

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


def test_aggressive_runtime_is_fully_isolated_and_starts_paused(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from platforms.polymarket.maker.account_profiles import parse_lp_account_profile
    from platforms.polymarket.maker.account_roster import routing_roster_sha256

    root = tmp_path / "aggressive-runtime"
    config_dir = root / "platforms" / "polymarket" / "maker"
    data_dir = root / "data"
    config_dir.mkdir(parents=True)
    data_dir.mkdir()
    rows = [_row(1, "aggressive-a", 7901), _row(2, "aggressive-b", 7901)]
    roster_payload = {
        "schema_version": 1,
        "runtime_scope": "aggressive",
        "accounts": rows,
    }
    roster_path = root / "accounts.runtime.json"
    roster_path.write_text(json.dumps(roster_payload), encoding="utf-8")
    accounts = parse_runtime_roster(roster_payload)
    digest = routing_roster_sha256(accounts, "aggressive")
    signer_url = "http://100.91.159.54:8421"
    rendered = _render(
        {
            "account": {"signer_server_url": signer_url},
            "markets": [{"token_id": "1"}],
        },
        accounts[0].generation_entry(),
        "127.0.0.1",
        roster_sha256=digest,
        runtime_scope="aggressive",
    )
    (config_dir / "config_1.json").write_text(
        json.dumps(rendered),
        encoding="utf-8",
    )
    (data_dir / ".account_1.paused").touch()
    monkeypatch.setenv("POLY_SIGNER_SERVER_URL", signer_url)

    built = []

    class Bus:
        def __init__(self):
            self.runtime_namespace = ""
            self.state_namespace = ""

        def set_runtime_namespace(self, namespace: str) -> None:
            self.runtime_namespace = namespace

        def set_state_namespace(self, namespace: str) -> None:
            self.state_namespace = namespace

    class Engine:
        def __init__(self, config_path: str):
            self._account_idx = 1
            self.cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            self.lp_account_profile = parse_lp_account_profile(self.cfg, 1)
            self.market_cfg = {"1": {}}
            self._night_market_cfg = {}
            self._event_bus = Bus()
            self._state_path = data_dir / "engine_state_1.json"
            built.append(self)

        async def run(self) -> None:
            raise AssertionError("validate-only must not start quote workers")

    monkeypatch.setattr(
        "platforms.polymarket.maker.multi_runner.PolyLPSMulti",
        Engine,
    )

    asyncio.run(
        multi_run(
            config_dir,
            roster_path=roster_path,
            host_id="aggressive-a",
            data_dir=data_dir,
            require_paused=True,
            validate_only=True,
            expected_roster_sha256=digest,
            expected_market_sha256=market_universe_sha256(rendered),
            runtime_scope="aggressive",
            runtime_root=root,
            expected_signer_url=signer_url,
        )
    )

    assert len(built) == 1
    assert built[0]._runtime_scope == "aggressive"
    assert built[0]._runtime_host_id == "aggressive-a"
    assert built[0]._event_bus.runtime_namespace == "aggressive"
    assert built[0]._event_bus.state_namespace == "account:1"


def test_aggressive_runtime_rejects_normal_host_or_signer_environment(
    monkeypatch,
    tmp_path: Path,
) -> None:
    roster_path = tmp_path / "accounts.runtime.json"
    roster_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "runtime_scope": "aggressive",
                "accounts": [_row(1, "vps1", 7901)],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("POLY_SIGNER_SERVER_URL", "http://normal-signer:8420")

    with pytest.raises(ValueError, match="POLY_SIGNER_SERVER_URL"):
        asyncio.run(
            multi_run(
                tmp_path,
                roster_path=roster_path,
                host_id="vps1",
                data_dir=tmp_path / "data",
                require_paused=True,
                runtime_scope="aggressive",
                runtime_root=tmp_path,
                expected_signer_url="http://aggressive-signer:8421",
            )
        )

    monkeypatch.setenv("POLY_SIGNER_SERVER_URL", "http://aggressive-signer:8421")
    with pytest.raises(ValueError, match="host ids must start with 'aggressive-'"):
        asyncio.run(
            multi_run(
                tmp_path,
                roster_path=roster_path,
                host_id="vps1",
                data_dir=tmp_path / "data",
                require_paused=True,
                runtime_scope="aggressive",
                runtime_root=tmp_path,
                expected_signer_url="http://aggressive-signer:8421",
            )
        )


def test_aggressive_systemd_template_never_reuses_normal_runtime() -> None:
    root = Path(__file__).resolve().parents[1]
    unit = (
        root / "deploy" / "systemd" / "polymarket-aggressive-engine.service.example"
    ).read_text(encoding="utf-8")

    assert "polymarket-engine.service" not in unit
    assert "/home/ubuntu/polymarket-aggressive-runtime" in unit
    assert "/home/ubuntu/polymarket-aggressive-releases/current" in unit
    assert "/home/ubuntu/polymarket-aggressive-venv/bin/python" in unit
    assert "--runtime-scope aggressive" in unit
    assert "--require-paused" in unit
    assert "--expected-signer-url" in unit
    assert "polymarket-aggressive-redis.service" in unit
    assert (
        "release_guard.py "
        "/home/ubuntu/polymarket-aggressive-releases/current/"
        "platforms/polymarket/maker/engine.py"
    ) in unit
    assert "/home/ubuntu/polymarket-bot" not in unit
    assert "/home/ubuntu/polymarket-runtime" not in unit
    assert "/home/ubuntu/.venv2" not in unit


def test_aggressive_reward_observer_uses_isolated_runtime_data(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "aggressive-runtime" / "data"
    config_dir = tmp_path / "aggressive-runtime" / "maker"
    calls = []

    async def refresh(path: Path, config_dir: Path) -> str:
        calls.append((path, config_dir))
        path.mkdir(parents=True, exist_ok=True)
        (path / "reward_observer_state.json").write_text(
            json.dumps({"generated_at": 1, "candidates": []}),
            encoding="utf-8",
        )
        return "markets=12 ready=0"

    async def exercise() -> None:
        task = asyncio.create_task(
            _aggressive_reward_observer_loop(
                data_dir,
                config_dir,
                refresh_once=refresh,
            )
        )
        for _ in range(100):
            if calls:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())

    assert calls == [(data_dir, config_dir)]
    assert (data_dir / "reward_observer_state.json").is_file()
