import json
from dataclasses import replace
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
MAKER_DIR = ROOT / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from account_roster import (  # noqa: E402
    market_universe_sha256,
    parse_runtime_roster,
    routing_roster_sha256,
)
from deploy_aggressive_runtime import (  # noqa: E402
    AggressivePaths,
    AggressiveRequest,
    DeploymentError,
    _normalize_request,
    _load_sanitized_base_config,
    _parse_env_file,
    _runtime_contract,
    _restore_optional_unit,
    _restore_service_state,
    ServiceState,
    _unit_contract,
    _validate_host_dependencies,
    _validate_paths,
)
from market_universe import apply_market_universe  # noqa: E402
from scripts.generate_configs import _render, _validate_signer_url  # noqa: E402


SHA = "a" * 40


def _account() -> dict:
    return {
        "account_index": 1,
        "host_id": "aggressive-a",
        "funder": "0x" + "1" * 40,
        "clash_port": 7901,
        "lp_account": {
            "account_id": "aggressive_200",
            "profile_type": "aggressive",
            "strategy_group": "aggressive",
            "target_principal_usdc": 200,
            "allocation_mode": "exclusive",
        },
    }


def _paths(tmp_path: Path) -> AggressivePaths:
    runtime = tmp_path / "polymarket-aggressive-runtime"
    releases = tmp_path / "polymarket-aggressive-releases"
    return AggressivePaths(
        profile_name="aggressive-a",
        bare_repo=tmp_path / "source.git",
        release_root=releases,
        current_link=releases / "current",
        runtime_root=runtime,
        python=tmp_path / "polymarket-aggressive-venv" / "bin" / "python",
        unit_file=tmp_path / "polymarket-aggressive-engine.service",
        redis_unit_file=tmp_path / "polymarket-aggressive-redis.service",
        lock_root=tmp_path / "locks",
    )


def test_confirmation_is_profile_and_sha_scoped() -> None:
    with pytest.raises(DeploymentError, match="confirmation must exactly equal"):
        _normalize_request(
            AggressiveRequest(
                action="activate",
                target_sha=SHA,
                expected_current="none",
                profile_name="aggressive-a",
                confirm=f"ACTIVATE-AGGRESSIVE:aggressive-b:{SHA}",
                authorization_id="approved-1",
            )
        )

    request = _normalize_request(
        AggressiveRequest(
            action="activate",
            target_sha=SHA,
            expected_current="none",
            profile_name="aggressive-a",
            confirm=f"ACTIVATE-AGGRESSIVE:aggressive-a:{SHA}",
            authorization_id="approved-1",
        )
    )
    assert request.expected_current == "none"


def test_aggressive_paths_reject_normal_lp_reuse(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _validate_paths(paths)

    with pytest.raises(DeploymentError, match="isolated domain"):
        _validate_paths(replace(paths, runtime_root=Path("/home/ubuntu/polymarket-runtime")))


def test_runtime_env_parser_never_requires_shell_evaluation(tmp_path: Path) -> None:
    env_path = tmp_path / "runtime.env"
    env_path.write_text(
        "\n".join(
            (
                "POLYMARKET_HOST_ID=aggressive-a",
                "POLYMARKET_EXPECTED_SIGNER_URL=http://100.91.159.54:8421",
                "POLY_SIGNER_SERVER_URL=http://100.91.159.54:8421",
                "SIGNER_TOKEN=test-only",
                "POLY_REDIS_URL=redis://127.0.0.1:6380/0",
                f"POLYMARKET_EXPECTED_ROSTER_SHA256={'1' * 64}",
                f"POLYMARKET_EXPECTED_MARKET_SHA256={'2' * 64}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    values = _parse_env_file(env_path)
    assert values["POLYMARKET_HOST_ID"] == "aggressive-a"
    assert values["SIGNER_TOKEN"] == "test-only"


def test_generated_aggressive_config_pins_dedicated_signer_without_secret() -> None:
    rendered = _render(
        {"account": {"signer_server_url": "http://normal:8420", "signer_token": "no"}},
        _account(),
        "127.0.0.1",
        roster_sha256="1" * 64,
        runtime_scope="aggressive",
        signer_url="http://100.91.159.54:8421",
    )
    assert rendered["account"]["signer_server_url"].endswith(":8421")
    assert "signer_token" not in rendered["account"]
    assert rendered["runtime_account"]["runtime_scope"] == "aggressive"


def test_generated_config_signer_url_validation_is_strict() -> None:
    assert _validate_signer_url("http://100.91.159.54:8421/") == (
        "http://100.91.159.54:8421"
    )
    for invalid in (
        "100.91.159.54:8421",
        "ftp://100.91.159.54:8421",
        "http://user:secret@100.91.159.54:8421",
        "http://100.91.159.54:70000",
        "http://100.91.159.54:8421/?token=secret",
    ):
        with pytest.raises(ValueError, match="signer-url"):
            _validate_signer_url(invalid)


def test_runtime_contract_binds_roster_market_signer_and_redis(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    release = paths.release_root / SHA
    release.mkdir(parents=True)
    paths.base_config.parent.mkdir(parents=True)
    base = {
        "account": {},
        "markets": [],
        "night_markets": [],
    }
    paths.base_config.write_text(json.dumps(base), encoding="utf-8")

    paths.runtime_env.parent.mkdir(parents=True)
    roster = {
        "schema_version": 1,
        "runtime_scope": "aggressive",
        "accounts": [_account()],
    }
    paths.roster.write_text(json.dumps(roster), encoding="utf-8")
    market = {
        "markets": [
            {
                "token_id": "1",
                "paired_token_id": "2",
                "slug": "test-market",
            }
        ],
        "night_markets": [],
    }
    paths.market_universe.write_text(json.dumps(market), encoding="utf-8")
    roster_sha = routing_roster_sha256(parse_runtime_roster(roster), "aggressive")
    market_sha = market_universe_sha256(apply_market_universe(base, market))
    paths.runtime_env.write_text(
        "\n".join(
            (
                "POLYMARKET_HOST_ID=aggressive-a",
                "POLYMARKET_EXPECTED_SIGNER_URL=http://100.91.159.54:8421",
                "POLY_SIGNER_SERVER_URL=http://100.91.159.54:8421",
                "SIGNER_TOKEN=test-only",
                "POLY_REDIS_URL=redis://127.0.0.1:6380/0",
                f"POLYMARKET_EXPECTED_ROSTER_SHA256={roster_sha}",
                f"POLYMARKET_EXPECTED_MARKET_SHA256={market_sha}",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    contract = _runtime_contract(paths, release)
    assert contract["signer_url"].endswith(":8421")
    assert contract["roster_sha256"] == roster_sha
    assert contract["market_sha256"] == market_sha
    assert [row.account_index for row in contract["local_accounts"]] == [1]


def test_sanitized_base_config_rejects_secrets_and_account_routes(tmp_path: Path) -> None:
    path = tmp_path / "base.config.json"
    for field, value in (
        ("private_key", "0xsecret"),
        ("signer_token", "secret"),
        ("signer_server_url", "http://normal.invalid:8420"),
        ("api_key", "secret"),
        ("funder", "0x" + "1" * 40),
    ):
        path.write_text(
            json.dumps({"account": {field: value}, "markets": [], "night_markets": []}),
            encoding="utf-8",
        )
        with pytest.raises(DeploymentError, match="routing or secrets|must not pin"):
            _load_sanitized_base_config(path)


def test_sanitized_base_config_accepts_non_secret_strategy(tmp_path: Path) -> None:
    path = tmp_path / "base.config.json"
    payload = {
        "account": {},
        "markets": [],
        "night_markets": [],
        "strategy": {"post_only": True, "dual_side": True},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert _load_sanitized_base_config(path) == payload


def test_runtime_contract_rejects_normal_signer_port(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.runtime_env.parent.mkdir(parents=True)
    paths.runtime_env.write_text(
        "\n".join(
            (
                "POLYMARKET_HOST_ID=aggressive-a",
                "POLYMARKET_EXPECTED_SIGNER_URL=http://100.91.159.54:8420",
                "POLY_SIGNER_SERVER_URL=http://100.91.159.54:8420",
                "SIGNER_TOKEN=test-only",
                "POLY_REDIS_URL=redis://127.0.0.1:6380/0",
                f"POLYMARKET_EXPECTED_ROSTER_SHA256={'1' * 64}",
                f"POLYMARKET_EXPECTED_MARKET_SHA256={'2' * 64}",
            )
        ),
        encoding="utf-8",
    )
    with pytest.raises(DeploymentError, match="normal signer port 8420"):
        _runtime_contract(paths, tmp_path / "release")


def test_reviewed_units_are_isolated_and_guard_the_engine(tmp_path: Path) -> None:
    release = tmp_path / SHA
    unit_dir = release / "deploy" / "systemd"
    unit_dir.mkdir(parents=True)
    for name in (
        "polymarket-aggressive-engine.service.example",
        "polymarket-aggressive-redis.service.example",
    ):
        (unit_dir / name).write_text(
            (ROOT / "deploy" / "systemd" / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    _unit_contract(AggressivePaths(profile_name="aggressive-a"), release)

    engine_unit = (unit_dir / "polymarket-aggressive-engine.service.example").read_text()
    assert "release_guard.py" in engine_unit
    assert "maker/engine.py" in engine_unit
    assert "Requires=polymarket-aggressive-redis.service" in engine_unit


class _RecordingRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []

    def run(self, command, **_kwargs):
        self.commands.append(tuple(command))
        return ""


def test_restore_optional_unit_removes_new_unit_when_none_existed(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    destination = tmp_path / "polymarket-aggressive-engine.service"
    _restore_optional_unit(
        runner,
        destination=destination,
        previous=None,
        staging_root=tmp_path,
    )
    assert runner.commands == [("sudo", "-n", "rm", "-f", str(destination))]


def test_restore_optional_unit_reinstalls_previous_bytes(tmp_path: Path) -> None:
    runner = _RecordingRunner()
    destination = tmp_path / "polymarket-aggressive-engine.service"
    _restore_optional_unit(
        runner,
        destination=destination,
        previous=b"[Unit]\nDescription=old\n",
        staging_root=tmp_path,
    )
    assert runner.commands[0][:5] == ("sudo", "-n", "install", "-m", "0644")
    assert runner.commands[0][-1] == str(destination)
    assert not list(tmp_path.glob(".*.rollback-*"))


def test_restore_service_state_preserves_enablement_and_activity() -> None:
    runner = _RecordingRunner()
    _restore_service_state(
        runner,
        "polymarket-aggressive-engine.service",
        ServiceState(active=True, enabled=False),
    )
    assert runner.commands == [
        (
            "sudo",
            "-n",
            "systemctl",
            "disable",
            "polymarket-aggressive-engine.service",
        ),
        (
            "sudo",
            "-n",
            "systemctl",
            "start",
            "polymarket-aggressive-engine.service",
        ),
    ]

    active_runner = _RecordingRunner()
    _restore_service_state(
        active_runner,
        "polymarket-aggressive-engine.service",
        ServiceState(active=True, enabled=True),
    )
    assert active_runner.commands == [
        (
            "sudo",
            "-n",
            "systemctl",
            "enable",
            "polymarket-aggressive-engine.service",
        ),
        (
            "sudo",
            "-n",
            "systemctl",
            "start",
            "polymarket-aggressive-engine.service",
        ),
    ]


def test_host_dependencies_are_dedicated_and_executable(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.python.parent.mkdir(parents=True)
    paths.python.write_text("#!/bin/sh\n", encoding="utf-8")
    paths.python.chmod(0o700)
    redis_server = tmp_path / "redis-server"
    redis_server.write_text("#!/bin/sh\n", encoding="utf-8")
    redis_server.chmod(0o700)

    _validate_host_dependencies(paths, redis_server=redis_server)

    redis_server.chmod(0o600)
    with pytest.raises(DeploymentError, match="Redis dependency is missing"):
        _validate_host_dependencies(paths, redis_server=redis_server)
