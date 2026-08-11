"""Stage a reviewed aggressive-LP market while every local account is flat.

This operator tool only updates the host-local market universe and its reviewed
digest. It never signs, posts, cancels, resumes, or restarts a service. The
normal exact-SHA aggressive deployment workflow must be run afterwards, and it
will activate the refreshed configuration in paused mode.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import re
from typing import Any, Dict, Mapping, Optional, Sequence

try:
    from .account_profiles import shared_event_owner
    from .account_roster import market_universe_sha256, routing_profiles
    from .deploy_aggressive_runtime import (
        AggressivePaths,
        SERVICE_NAME,
        _current_release,
        _runtime_contract,
        _service_is_active,
        _validate_paths,
        aggressive_paths_for_profile,
    )
    from .deploy_release import (
        CommandRunner,
        DeploymentError,
        _atomic_write,
        _load_json,
        _parse_timestamp,
        deployment_lock,
    )
    from .market_universe import apply_market_universe
except ImportError:  # pragma: no cover - direct script execution
    from account_profiles import shared_event_owner
    from account_roster import market_universe_sha256, routing_profiles
    from deploy_aggressive_runtime import (
        AggressivePaths,
        SERVICE_NAME,
        _current_release,
        _runtime_contract,
        _service_is_active,
        _validate_paths,
        aggressive_paths_for_profile,
    )
    from deploy_release import (
        CommandRunner,
        DeploymentError,
        _atomic_write,
        _load_json,
        _parse_timestamp,
        deployment_lock,
    )
    from market_universe import apply_market_universe


_AUTHORIZATION_RE = re.compile(r"^[A-Za-z0-9._:-]{3,120}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_MARKET_KEY = "POLYMARKET_EXPECTED_MARKET_SHA256"


@dataclass(frozen=True)
class StageRequest:
    action: str
    candidate_path: Path
    profile_name: str
    confirm: str = ""
    authorization_id: str = ""
    max_state_age_seconds: float = 120.0
    max_candidate_age_seconds: float = 300.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DeploymentError(f"{label} is invalid") from exc
    if not parsed.is_finite():
        raise DeploymentError(f"{label} is invalid")
    return parsed


def _normalize_request(request: StageRequest) -> StageRequest:
    action = str(request.action or "").strip().lower()
    if action not in {"plan", "apply"}:
        raise DeploymentError(f"unsupported market staging action: {action}")
    profile = str(request.profile_name or "").strip().lower()
    if (
        not math.isfinite(request.max_state_age_seconds)
        or request.max_state_age_seconds <= 0
    ):
        raise DeploymentError("max_state_age_seconds must be positive")
    if (
        not math.isfinite(request.max_candidate_age_seconds)
        or request.max_candidate_age_seconds <= 0
    ):
        raise DeploymentError("max_candidate_age_seconds must be positive")
    authorization = str(request.authorization_id or "").strip()
    if action == "apply" and not authorization:
        raise DeploymentError("apply requires an explicit authorization ID")
    if authorization and not _AUTHORIZATION_RE.fullmatch(authorization):
        raise DeploymentError("authorization ID contains unsupported characters")
    return StageRequest(
        action=action,
        candidate_path=Path(request.candidate_path),
        profile_name=profile,
        confirm=str(request.confirm or "").strip(),
        authorization_id=authorization,
        max_state_age_seconds=float(request.max_state_age_seconds),
        max_candidate_age_seconds=float(request.max_candidate_age_seconds),
    )


def _active_order_count(state: Mapping[str, Any]) -> int:
    markets = state.get("markets")
    if not isinstance(markets, Mapping):
        raise DeploymentError("engine state markets must be an object")
    count = 0
    for token_id, market in markets.items():
        if not isinstance(market, Mapping):
            raise DeploymentError(f"engine state market {token_id} is invalid")
        orders = market.get("orders")
        if not isinstance(orders, list):
            raise DeploymentError(f"engine state market {token_id} orders are invalid")
        count += len(orders)
    return count


def _require_flat_paused_accounts(
    paths: AggressivePaths,
    contract: Mapping[str, Any],
    release_sha: str,
    *,
    now: datetime,
    max_age_seconds: float,
) -> list[dict[str, Any]]:
    verified: list[dict[str, Any]] = []
    now_ts = now.timestamp()
    for account in contract["local_accounts"]:
        account_index = account.account_index
        pause_flag = paths.data_dir / f".account_{account_index}.paused"
        state_path = paths.data_dir / f"engine_state_{account_index}.json"
        if not pause_flag.is_file():
            raise DeploymentError(f"account {account_index} pause flag is missing")
        state = _load_json(state_path, f"aggressive engine state {account_index}")
        if state.get("account_index") != account_index:
            raise DeploymentError(f"account {account_index} state identity mismatch")
        if state.get("paused") is not True:
            raise DeploymentError(f"account {account_index} is not paused")
        if state.get("release_sha") != release_sha:
            raise DeploymentError(f"account {account_index} release mismatch")
        observed_at = _parse_timestamp(state.get("ts"))
        age = (now - observed_at).total_seconds()
        if age < -5 or age > max_age_seconds:
            raise DeploymentError(
                f"account {account_index} state is stale ({age:.1f}s old)"
            )

        runtime = state.get("runtime")
        if not isinstance(runtime, Mapping):
            raise DeploymentError(f"account {account_index} runtime state is invalid")
        if runtime.get("scope") != "aggressive":
            raise DeploymentError(f"account {account_index} scope is not aggressive")
        if runtime.get("host_id") != paths.profile_name:
            raise DeploymentError(f"account {account_index} host identity mismatch")
        if runtime.get("routing_roster_sha256") != contract["roster_sha256"]:
            raise DeploymentError(f"account {account_index} roster digest mismatch")
        if runtime.get("market_universe_sha256") != contract["market_sha256"]:
            raise DeploymentError(f"account {account_index} market digest mismatch")

        active_orders = _active_order_count(state)
        if active_orders:
            raise DeploymentError(
                f"account {account_index} still has {active_orders} active orders"
            )
        pending = state.get("pending_unwinds")
        if not isinstance(pending, list):
            raise DeploymentError(f"account {account_index} pending unwinds are invalid")
        if pending:
            raise DeploymentError(f"account {account_index} still has pending unwinds")

        guardrails = state.get("aggressive_guardrails")
        if not isinstance(guardrails, Mapping):
            raise DeploymentError(f"account {account_index} guardrail state is invalid")
        if (
            guardrails.get("enabled") is not True
            or guardrails.get("active") is not True
        ):
            raise DeploymentError(f"account {account_index} guardrails are not active")
        guard_state = guardrails.get("state")
        if not isinstance(guard_state, Mapping):
            raise DeploymentError(f"account {account_index} guardrail details are invalid")
        if guard_state.get("latched") is not False:
            raise DeploymentError(f"account {account_index} guardrail is latched")
        success_ts = float(
            _decimal(
                guard_state.get("last_success_ts"),
                f"account {account_index} guardrail last_success_ts",
            )
        )
        success_age = now_ts - success_ts
        if success_ts <= 0 or success_age < -5 or success_age > max_age_seconds:
            raise DeploymentError(
                f"account {account_index} equity state is stale ({success_age:.1f}s old)"
            )
        position_value = _decimal(
            guard_state.get("last_position_value_usdc"),
            f"account {account_index} position value",
        )
        if abs(position_value) > Decimal("0.000001"):
            raise DeploymentError(
                f"account {account_index} position value is not zero"
            )
        verified.append(
            {
                "account_index": account_index,
                "state_age_sec": round(age, 1),
                "equity_age_sec": round(success_age, 1),
                "active_orders": 0,
                "pending_unwinds": 0,
                "position_value_usdc": "0",
            }
        )
    return verified


def _candidate_contract(
    candidate: Mapping[str, Any],
    base: Mapping[str, Any],
    accounts: Sequence[Any],
    *,
    now: datetime,
    max_age_seconds: float,
) -> tuple[str, list[dict[str, Any]]]:
    build = candidate.get("build")
    if not isinstance(build, Mapping):
        raise DeploymentError("candidate build metadata is missing")
    if build.get("selection_mode") != "review_only":
        raise DeploymentError("candidate was not produced in review-only mode")
    if build.get("source") != "reward_observer_state.json":
        raise DeploymentError("candidate source is not the reward observer")
    generated_at = float(
        _decimal(build.get("observer_generated_at"), "observer timestamp")
    )
    age = now.timestamp() - generated_at
    if generated_at <= 0 or age < -30 or age > max_age_seconds:
        raise DeploymentError(f"candidate observer snapshot is stale ({age:.1f}s old)")

    selection_limit = _decimal(build.get("selection_limit"), "selection limit")
    if selection_limit != Decimal("1"):
        raise DeploymentError("candidate selection limit must be exactly one")
    markets = candidate.get("markets")
    night_markets = candidate.get("night_markets")
    if not isinstance(markets, list) or not isinstance(night_markets, list):
        raise DeploymentError("candidate market sections are invalid")
    if len(markets) != 1 or night_markets:
        raise DeploymentError("candidate must contain exactly one day market")

    execution = base.get("execution")
    if not isinstance(execution, Mapping):
        raise DeploymentError("aggressive base execution config is missing")
    engine_depth = _decimal(
        execution.get("min_front_bid_notional_usdc", 2000),
        "engine front-depth threshold",
    )
    candidate_depth = _decimal(
        build.get("min_front_bid_notional_usdc"),
        "candidate front-depth threshold",
    )
    if candidate_depth < engine_depth:
        raise DeploymentError("candidate front-depth threshold is below the engine gate")

    principal = _decimal(build.get("principal_usdc"), "candidate principal")
    if principal <= 0:
        raise DeploymentError("candidate principal must be positive")
    profiles = routing_profiles(accounts)
    if not profiles:
        raise DeploymentError("aggressive roster has no enabled account profiles")

    public_rows: list[dict[str, Any]] = []
    for row in markets:
        if not isinstance(row, Mapping):
            raise DeploymentError("candidate market row is invalid")
        if row.get("enabled") is not True:
            raise DeploymentError("candidate market is not enabled")
        if row.get("source") != "aggressive_observer_selected":
            raise DeploymentError("candidate market source is not observer-selected")
        if row.get("eligibility_managed") is not True:
            raise DeploymentError("candidate market is not eligibility-managed")
        token_id = str(row.get("token_id") or "").strip()
        paired_token_id = str(row.get("paired_token_id") or "").strip()
        if (
            not token_id.isdigit()
            or not paired_token_id.isdigit()
            or token_id == paired_token_id
        ):
            raise DeploymentError("candidate token pair is invalid")
        quote_owners = sorted(
            account_index
            for account_index in profiles
            if shared_event_owner(account_index, token_id, row, profiles)
            == account_index
        )
        if not quote_owners:
            raise DeploymentError("candidate market has no quoting account")
        insufficient = [
            account_index
            for account_index in quote_owners
            if principal > profiles[account_index].target_principal_usdc
        ]
        if insufficient:
            raise DeploymentError(
                "candidate principal exceeds quoting account target: "
                + ", ".join(str(account_index) for account_index in insufficient)
            )
        end_ts = float(_decimal(row.get("market_end_ts"), "candidate market end"))
        if end_ts <= now.timestamp():
            raise DeploymentError("candidate market has expired")
        public_rows.append(
            {
                "slug": str(row.get("slug") or "").strip(),
                "token_tail": token_id[-10:],
                "paired_token_tail": paired_token_id[-10:],
                "quoting_accounts": quote_owners,
                "candidate_principal_usdc": str(principal),
            }
        )

    try:
        rendered = apply_market_universe(base, candidate)
        digest = market_universe_sha256(rendered)
    except (TypeError, ValueError) as exc:
        raise DeploymentError(f"candidate market universe is invalid: {exc}") from exc
    if not _DIGEST_RE.fullmatch(digest):
        raise DeploymentError("candidate market digest is invalid")
    return digest, public_rows


def _replace_market_digest(env_bytes: bytes, digest: str) -> bytes:
    try:
        lines = env_bytes.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise DeploymentError("aggressive runtime env is not UTF-8") from exc
    found = 0
    output: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key == _EXPECTED_MARKET_KEY:
                found += 1
                output.append(f"{_EXPECTED_MARKET_KEY}={digest}")
                continue
        output.append(raw)
    if found != 1:
        raise DeploymentError(
            f"runtime env must contain exactly one {_EXPECTED_MARKET_KEY}"
        )
    return ("\n".join(output) + "\n").encode("utf-8")


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def _rollback_files(
    paths: AggressivePaths,
    *,
    market_bytes: bytes,
    market_mode: int,
    env_bytes: bytes,
    env_mode: int,
) -> None:
    errors: list[str] = []
    for path, content, mode in (
        (paths.market_universe, market_bytes, market_mode),
        (paths.runtime_env, env_bytes, env_mode),
    ):
        try:
            _atomic_write(path, content, mode=mode)
        except Exception as exc:  # pragma: no cover - catastrophic filesystem failure
            errors.append(f"{path}: {exc}")
    if errors:
        raise DeploymentError("market staging rollback failed: " + "; ".join(errors))


def execute(
    request: StageRequest,
    *,
    paths: Optional[AggressivePaths] = None,
    runner: Optional[CommandRunner] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized = _normalize_request(request)
    selected = paths or aggressive_paths_for_profile(normalized.profile_name)
    _validate_paths(selected)
    if selected.profile_name != normalized.profile_name:
        raise DeploymentError("market staging profile does not match paths")
    commands = runner or CommandRunner()
    reference = (now or _utc_now()).astimezone(timezone.utc)

    with deployment_lock(selected.lock_file):
        if not _service_is_active(commands, SERVICE_NAME):
            raise DeploymentError(f"{SERVICE_NAME} must be active and paused")
        release_sha = _current_release(selected)
        if release_sha == "none":
            raise DeploymentError("aggressive runtime has no current release")
        release_dir = selected.release_root / release_sha
        contract = _runtime_contract(selected, release_dir)
        accounts = _require_flat_paused_accounts(
            selected,
            contract,
            release_sha,
            now=reference,
            max_age_seconds=normalized.max_state_age_seconds,
        )
        candidate = _load_json(normalized.candidate_path, "aggressive market candidate")
        base = _load_json(selected.base_config, "aggressive base config")
        candidate_sha, public_markets = _candidate_contract(
            candidate,
            base,
            contract["accounts"],
            now=reference,
            max_age_seconds=normalized.max_candidate_age_seconds,
        )
        if candidate_sha == contract["market_sha256"]:
            raise DeploymentError("candidate market is already staged")

        required_confirmation = f"STAGE-AGGRESSIVE-MARKET:{candidate_sha}"
        plan: Dict[str, Any] = {
            "ok": True,
            "action": normalized.action,
            "profile_name": selected.profile_name,
            "release_sha": release_sha,
            "current_market_sha256": contract["market_sha256"],
            "candidate_market_sha256": candidate_sha,
            "required_confirmation": required_confirmation,
            "accounts": accounts,
            "markets": public_markets,
            "service": SERVICE_NAME,
            "will_restart": False,
            "will_resume": False,
            "will_sign": False,
            "will_post_or_cancel": False,
            "next_step": "run the exact-SHA aggressive plan/prepare/activate workflow; activation stays paused",
        }
        if normalized.action == "plan":
            return plan
        if normalized.confirm != required_confirmation:
            raise DeploymentError(
                f"confirmation must exactly equal {required_confirmation}"
            )

        original_market = selected.market_universe.read_bytes()
        original_env = selected.runtime_env.read_bytes()
        market_mode = _mode(selected.market_universe)
        env_mode = _mode(selected.runtime_env)
        candidate_bytes = (
            json.dumps(candidate, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
        updated_env = _replace_market_digest(original_env, candidate_sha)
        audit_path = selected.audit_root / (
            f"{reference.strftime('%Y%m%dT%H%M%SZ')}-market-stage-{candidate_sha[:12]}.json"
        )
        if audit_path.exists():
            raise DeploymentError(f"market staging audit already exists: {audit_path}")

        changed = False
        try:
            _atomic_write(selected.market_universe, candidate_bytes, mode=market_mode)
            changed = True
            _atomic_write(selected.runtime_env, updated_env, mode=env_mode)
            after_contract = _runtime_contract(selected, release_dir)
            if after_contract["market_sha256"] != candidate_sha:
                raise DeploymentError("staged market digest verification failed")
            for account in contract["local_accounts"]:
                pause_flag = selected.data_dir / f".account_{account.account_index}.paused"
                if not pause_flag.is_file():
                    raise DeploymentError(
                        f"account {account.account_index} pause flag changed during staging"
                    )
            audit = {
                "ok": True,
                "action": "apply",
                "authorization_id": normalized.authorization_id,
                "staged_at": reference.isoformat(),
                "profile_name": selected.profile_name,
                "release_sha": release_sha,
                "previous_market_sha256": contract["market_sha256"],
                "candidate_market_sha256": candidate_sha,
                "accounts": accounts,
                "markets": public_markets,
                "service_restarted": False,
                "accounts_resumed": False,
                "orders_touched": False,
            }
            _atomic_write(
                audit_path,
                (json.dumps(audit, ensure_ascii=True, indent=2) + "\n").encode("utf-8"),
                mode=0o600,
            )
        except Exception as exc:
            try:
                if changed:
                    _rollback_files(
                        selected,
                        market_bytes=original_market,
                        market_mode=market_mode,
                        env_bytes=original_env,
                        env_mode=env_mode,
                    )
            finally:
                audit_path.unlink(missing_ok=True)
            if isinstance(exc, DeploymentError):
                raise
            raise DeploymentError(f"market staging failed: {exc}") from exc

        plan.update(
            {
                "applied": True,
                "authorization_id": normalized.authorization_id,
                "audit_path": str(audit_path),
            }
        )
        return plan


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage one reviewed aggressive-LP market while fully paused"
    )
    parser.add_argument("action", choices=("plan", "apply"))
    parser.add_argument("candidate", type=Path)
    parser.add_argument(
        "--profile",
        choices=("aggressive-a", "aggressive-b"),
        required=True,
    )
    parser.add_argument("--confirm", default="")
    parser.add_argument("--authorization-id", default="")
    parser.add_argument("--max-state-age-seconds", type=float, default=120.0)
    parser.add_argument("--max-candidate-age-seconds", type=float, default=300.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = execute(
            StageRequest(
                action=args.action,
                candidate_path=args.candidate,
                profile_name=args.profile,
                confirm=args.confirm,
                authorization_id=args.authorization_id,
                max_state_age_seconds=args.max_state_age_seconds,
                max_candidate_age_seconds=args.max_candidate_age_seconds,
            )
        )
    except DeploymentError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
