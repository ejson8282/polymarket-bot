import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
MAKER_DIR = ROOT / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from account_roster import (  # noqa: E402
    market_universe_sha256,
    parse_runtime_roster,
    routing_profiles,
    routing_roster_sha256,
)
from account_profiles import shared_event_owner  # noqa: E402
from deploy_aggressive_runtime import AggressivePaths  # noqa: E402
from deploy_release import DeploymentError  # noqa: E402
from market_universe import apply_market_universe  # noqa: E402
import stage_aggressive_market as stage  # noqa: E402


SHA = "a" * 40
NOW_TS = 2_000_000_000.0
NOW = datetime.fromtimestamp(NOW_TS, tz=timezone.utc)


class ActiveRunner:
    def run(self, args, **_kwargs):
        if tuple(args) == (
            "systemctl",
            "is-active",
            "polymarket-aggressive-engine.service",
        ):
            return "active"
        raise AssertionError(f"unexpected command: {args}")


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
        proxy_unit_file=tmp_path / "polymarket-aggressive-proxy.service",
        lock_root=tmp_path / "locks",
    )


def _account(account_index: int = 1, principal: int = 200) -> dict:
    return {
        "account_index": account_index,
        "host_id": "aggressive-a",
        "funder": "0x" + str(account_index) * 40,
        "clash_port": 7900 + account_index,
        "lp_account": {
            "account_id": f"aggressive_{principal}_{account_index}",
            "profile_type": "aggressive",
            "strategy_group": "aggressive",
            "target_principal_usdc": principal,
            "allocation_mode": "exclusive",
        },
    }


def _market(token: str, paired: str, slug: str, *, source: str) -> dict:
    return {
        "token_id": token,
        "paired_token_id": paired,
        "side": "YES",
        "max_incentive_spread": 0.03,
        "price_tick": 0.01,
        "min_distance_from_best_bid": 0.01,
        "min_distance_ticks": 1,
        "quote_size": 200,
        "risk": "mid",
        "enabled": True,
        "source": source,
        "eligibility_managed": True,
        "eligibility_base_risk": "mid",
        "condition_id": "0x" + token.zfill(64)[-64:],
        "slug": slug,
        "question": slug,
        "game_start_ts": 0,
        "market_end_ts": NOW_TS + 86_400,
    }


def _candidate() -> dict:
    return {
        "schema_version": 1,
        "markets": [
            _market("3", "4", "new-market", source="aggressive_observer_selected")
        ],
        "night_markets": [],
        "build": {
            "source": "reward_observer_state.json",
            "observer_generated_at": NOW_TS - 30,
            "observer_age_sec": 30,
            "principal_usdc": 200,
            "selection_limit": 1,
            "selection_mode": "review_only",
            "min_front_bid_notional_usdc": 5000,
            "max_depth_age_sec": 300,
            "quote_budget_pct": 1,
            "max_sponsored_age_sec": 180,
        },
    }


def _setup(
    tmp_path: Path,
    *,
    accounts: list[dict] | None = None,
) -> tuple[AggressivePaths, Path, str]:
    paths = _paths(tmp_path)
    paths.lock_root.mkdir(parents=True)
    release = paths.release_root / SHA
    release.mkdir(parents=True)
    paths.current_link.symlink_to(release)
    paths.data_dir.mkdir(parents=True)
    paths.runtime_env.parent.mkdir(parents=True)

    base = {
        "account": {},
        "execution": {"min_front_bid_notional_usdc": 5000},
        "markets": [],
        "night_markets": [],
    }
    paths.base_config.write_text(json.dumps(base), encoding="utf-8")
    roster_accounts = accounts or [_account()]
    roster = {
        "schema_version": 1,
        "runtime_scope": "aggressive",
        "accounts": roster_accounts,
    }
    paths.roster.write_text(json.dumps(roster), encoding="utf-8")
    current_market = {
        "schema_version": 1,
        "markets": [
            _market("1", "2", "old-market", source="aggressive_observer_selected")
        ],
        "night_markets": [],
    }
    paths.market_universe.write_text(json.dumps(current_market), encoding="utf-8")
    roster_sha = routing_roster_sha256(parse_runtime_roster(roster), "aggressive")
    market_sha = market_universe_sha256(apply_market_universe(base, current_market))
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
    for account in roster_accounts:
        account_index = account["account_index"]
        (paths.data_dir / f".account_{account_index}.paused").touch()
        state_payload = {
            "ts": NOW.isoformat(),
            "account_index": account_index,
            "release_sha": SHA,
            "paused": True,
            "runtime": {
                "scope": "aggressive",
                "host_id": "aggressive-a",
                "routing_roster_sha256": roster_sha,
                "market_universe_sha256": market_sha,
            },
            "markets": {"1": {"orders": []}, "2": {"orders": []}},
            "pending_unwinds": [],
            "aggressive_guardrails": {
                "enabled": True,
                "active": True,
                "state": {
                    "last_success_ts": NOW_TS,
                    "last_position_value_usdc": "0",
                    "latched": False,
                },
            },
        }
        (paths.data_dir / f"engine_state_{account_index}.json").write_text(
            json.dumps(state_payload), encoding="utf-8"
        )
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(json.dumps(_candidate()), encoding="utf-8")
    return paths, candidate_path, market_sha


def _request(action: str, candidate: Path, **kwargs) -> stage.StageRequest:
    return stage.StageRequest(
        action=action,
        candidate_path=candidate,
        profile_name="aggressive-a",
        **kwargs,
    )


def test_plan_is_read_only_and_reports_exact_confirmation(tmp_path: Path) -> None:
    paths, candidate, current_sha = _setup(tmp_path)
    market_before = paths.market_universe.read_bytes()
    env_before = paths.runtime_env.read_bytes()

    result = stage.execute(
        _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
    )

    assert result["current_market_sha256"] == current_sha
    assert result["required_confirmation"] == (
        f"STAGE-AGGRESSIVE-MARKET:{result['candidate_market_sha256']}"
    )
    assert result["will_restart"] is False
    assert result["will_resume"] is False
    assert result["will_sign"] is False
    assert result["will_post_or_cancel"] is False
    assert paths.market_universe.read_bytes() == market_before
    assert paths.runtime_env.read_bytes() == env_before


def test_apply_atomically_stages_candidate_and_audit(tmp_path: Path) -> None:
    paths, candidate, _current_sha = _setup(tmp_path)
    plan = stage.execute(
        _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
    )
    result = stage.execute(
        _request(
            "apply",
            candidate,
            confirm=plan["required_confirmation"],
            authorization_id="approved-market-stage-1",
        ),
        paths=paths,
        runner=ActiveRunner(),
        now=NOW,
    )

    assert result["applied"] is True
    assert (
        json.loads(paths.market_universe.read_text())["markets"][0]["slug"]
        == "new-market"
    )
    assert (
        f"POLYMARKET_EXPECTED_MARKET_SHA256={result['candidate_market_sha256']}"
        in paths.runtime_env.read_text()
    )
    audit = json.loads(Path(result["audit_path"]).read_text())
    assert audit["authorization_id"] == "approved-market-stage-1"
    assert audit["service_restarted"] is False
    assert audit["orders_touched"] is False


def test_apply_requires_digest_bound_confirmation(tmp_path: Path) -> None:
    paths, candidate, _current_sha = _setup(tmp_path)
    with pytest.raises(DeploymentError, match="confirmation must exactly equal"):
        stage.execute(
            _request(
                "apply",
                candidate,
                confirm="STAGE-AGGRESSIVE-MARKET:" + "0" * 64,
                authorization_id="approved-market-stage-1",
            ),
            paths=paths,
            runner=ActiveRunner(),
            now=NOW,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda paths, state: (paths.data_dir / ".account_1.paused").unlink(), "pause flag"),
        (lambda _paths, state: state.update(paused=False), "not paused"),
        (
            lambda _paths, state: state["markets"]["1"]["orders"].append({"id": "live"}),
            "active orders",
        ),
        (
            lambda _paths, state: state["pending_unwinds"].append({"id": "exit"}),
            "pending unwinds",
        ),
        (
            lambda _paths, state: state["aggressive_guardrails"]["state"].update(
                last_position_value_usdc="1"
            ),
            "position value",
        ),
        (
            lambda _paths, state: state["aggressive_guardrails"]["state"].update(
                latched=True
            ),
            "guardrail is latched",
        ),
    ),
)
def test_staging_fails_closed_on_nonflat_or_unprotected_state(
    tmp_path: Path, mutation, message: str
) -> None:
    paths, candidate, _current_sha = _setup(tmp_path)
    state_path = paths.data_dir / "engine_state_1.json"
    state = json.loads(state_path.read_text())
    mutation(paths, state)
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(DeploymentError, match=message):
        stage.execute(
            _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
        )


def test_staging_rejects_stale_candidate_and_weaker_depth_gate(tmp_path: Path) -> None:
    paths, candidate_path, _current_sha = _setup(tmp_path)
    candidate = json.loads(candidate_path.read_text())
    candidate["build"]["observer_generated_at"] = NOW_TS - 301
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    with pytest.raises(DeploymentError, match="candidate observer snapshot is stale"):
        stage.execute(
            _request("plan", candidate_path), paths=paths, runner=ActiveRunner(), now=NOW
        )

    candidate["build"]["observer_generated_at"] = NOW_TS - 30
    candidate["build"]["min_front_bid_notional_usdc"] = 4999
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    with pytest.raises(DeploymentError, match="below the engine gate"):
        stage.execute(
            _request("plan", candidate_path), paths=paths, runner=ActiveRunner(), now=NOW
        )


def _set_candidate_for_owner(
    candidate_path: Path,
    accounts: list[dict],
    owner_index: int,
) -> None:
    profiles = routing_profiles(parse_runtime_roster({"accounts": accounts}))
    candidate = json.loads(candidate_path.read_text())
    for token_number in range(10, 10_000, 2):
        row = _market(
            str(token_number),
            str(token_number + 1),
            f"owner-{owner_index}",
            source="aggressive_observer_selected",
        )
        owner = shared_event_owner(
            next(iter(profiles)), row["token_id"], row, profiles
        )
        if owner == owner_index:
            candidate["markets"] = [row]
            candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
            return
    raise AssertionError(f"could not find deterministic market for owner {owner_index}")


def test_multi_account_candidate_checks_actual_shared_owner_principal(
    tmp_path: Path,
) -> None:
    accounts = [_account(1, 50), _account(2, 200)]
    paths, candidate, _current_sha = _setup(tmp_path, accounts=accounts)
    _set_candidate_for_owner(candidate, accounts, owner_index=2)

    result = stage.execute(
        _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
    )

    assert [row["account_index"] for row in result["accounts"]] == [1, 2]
    assert result["markets"][0]["quoting_accounts"] == [2]
    assert result["markets"][0]["candidate_principal_usdc"] == "200"


def test_multi_account_candidate_rejects_when_shared_owner_is_too_small(
    tmp_path: Path,
) -> None:
    accounts = [_account(1, 50), _account(2, 200)]
    paths, candidate, _current_sha = _setup(tmp_path, accounts=accounts)
    _set_candidate_for_owner(candidate, accounts, owner_index=1)

    with pytest.raises(
        DeploymentError,
        match="candidate principal exceeds quoting account target: 1",
    ):
        stage.execute(
            _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
        )


def test_multi_account_staging_requires_every_account_to_be_paused(
    tmp_path: Path,
) -> None:
    accounts = [_account(1, 100), _account(2, 200)]
    paths, candidate, _current_sha = _setup(tmp_path, accounts=accounts)
    (paths.data_dir / ".account_2.paused").unlink()

    with pytest.raises(DeploymentError, match="account 2 pause flag is missing"):
        stage.execute(
            _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
        )


def test_apply_rolls_back_both_runtime_inputs_when_audit_write_fails(
    tmp_path: Path, monkeypatch
) -> None:
    paths, candidate, _current_sha = _setup(tmp_path)
    plan = stage.execute(
        _request("plan", candidate), paths=paths, runner=ActiveRunner(), now=NOW
    )
    market_before = paths.market_universe.read_bytes()
    env_before = paths.runtime_env.read_bytes()
    real_atomic_write = stage._atomic_write

    def fail_audit(path, content, mode=0o600):
        if path.parent == paths.audit_root:
            raise OSError("audit disk failure")
        return real_atomic_write(path, content, mode=mode)

    monkeypatch.setattr(stage, "_atomic_write", fail_audit)
    with pytest.raises(DeploymentError, match="market staging failed"):
        stage.execute(
            _request(
                "apply",
                candidate,
                confirm=plan["required_confirmation"],
                authorization_id="approved-market-stage-1",
            ),
            paths=paths,
            runner=ActiveRunner(),
            now=NOW,
        )

    assert paths.market_universe.read_bytes() == market_before
    assert paths.runtime_env.read_bytes() == env_before
    assert not list(paths.audit_root.glob("*-market-stage-*.json"))
