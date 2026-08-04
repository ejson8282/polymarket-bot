from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


LIVE_CONFIRMATION = "ENABLE_PREDICTFUN_LIVE"


@dataclass(frozen=True)
class ExecutionGate:
    requested_mode: str
    effective_mode: str
    allowed: bool
    blocks: tuple[str, ...]
    account_ids: tuple[str, ...]

    def to_state(self) -> dict[str, Any]:
        data = asdict(self)
        data["blocks"] = list(self.blocks)
        data["account_ids"] = list(self.account_ids)
        return data


def resolve_execution_gate(
    cfg: dict[str, Any],
    *,
    environ: Mapping[str, str],
    release: Mapping[str, Any],
) -> ExecutionGate:
    execution = cfg.get("execution")
    execution = execution if isinstance(execution, dict) else {}
    requested = str(execution.get("mode") or "dry_run").strip().lower()
    account_ids = tuple(_configured_account_ids(cfg))
    if requested == "dry_run":
        return ExecutionGate(
            requested_mode=requested,
            effective_mode="dry_run",
            allowed=True,
            blocks=(),
            account_ids=account_ids,
        )
    if requested != "live":
        return ExecutionGate(
            requested_mode=requested,
            effective_mode="blocked",
            allowed=False,
            blocks=("execution_mode_invalid",),
            account_ids=account_ids,
        )

    blocks: list[str] = []
    if str(cfg.get("environment") or "").lower() != "mainnet":
        blocks.append("live_requires_mainnet")
    if not _truthy(str(environ.get("PREDICTFUN_LIVE_TRADING") or "")):
        blocks.append("live_environment_disabled")
    if str(environ.get("PREDICTFUN_LIVE_CONFIRM") or "") != LIVE_CONFIRMATION:
        blocks.append("live_confirmation_missing")
    release_sha = str(release.get("release_sha") or "").strip().lower()
    if release.get("release_required") is not True or len(release_sha) != 40:
        blocks.append("immutable_release_required")
    if str(environ.get("PREDICTFUN_LIVE_RELEASE_SHA") or "").strip().lower() != release_sha:
        blocks.append("live_release_sha_mismatch")
    configured_env_accounts = tuple(
        value.strip()
        for value in str(
            environ.get("PREDICTFUN_LIVE_ACCOUNT_IDS") or ""
        ).split(",")
        if value.strip()
    )
    if not account_ids or configured_env_accounts != account_ids:
        blocks.append("live_account_set_mismatch")
    simulation = cfg.get("simulation")
    simulation = simulation if isinstance(simulation, dict) else {}
    if simulation.get("enabled") is not False:
        blocks.append("live_requires_simulation_disabled")
    return ExecutionGate(
        requested_mode=requested,
        effective_mode="live" if not blocks else "blocked",
        allowed=not blocks,
        blocks=tuple(blocks),
        account_ids=account_ids,
    )


def _configured_account_ids(cfg: dict[str, Any]) -> list[str]:
    accounts = cfg.get("accounts")
    accounts = accounts if isinstance(accounts, dict) else {}
    rows = accounts.get("ids") or accounts.get("account_ids") or []
    return [str(value).strip() for value in rows if str(value).strip()]


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
