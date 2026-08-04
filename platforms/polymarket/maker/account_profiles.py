"""Config and deterministic routing helpers for multi-account Polymarket LP.

This module deliberately has no exchange or signer dependencies. It describes
how much principal an account is allowed to use and which account in a shared
strategy group owns each event. Orders, balances, positions, and exits remain
account-local.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


PROFILE_TYPES = frozenset({"standard", "aggressive"})
ALLOCATION_MODES = frozenset({"disabled", "exclusive"})


def _decimal(value: object, *, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _boolean(raw: Mapping[str, Any], field: str, default: bool) -> bool:
    value = raw.get(field, default)
    if not isinstance(value, bool):
        raise ValueError(f"lp_account.{field} must be a boolean")
    return value


@dataclass(frozen=True)
class LPAccountProfile:
    account_index: int
    account_id: str
    enabled: bool
    profile_type: str
    strategy_group: str
    target_principal_usdc: Decimal
    pause_equity_usdc: Decimal
    daily_loss_limit_usdc: Decimal
    allocation_mode: str
    auto_top_up: bool = False
    auto_sweep: bool = False

    @property
    def managed(self) -> bool:
        return self.enabled and self.target_principal_usdc > 0

    @property
    def sweep_above_usdc(self) -> Decimal:
        return self.target_principal_usdc

    def effective_available(self, available: Decimal) -> Decimal:
        """Cap quoting capital without changing the venue balance."""
        available = max(Decimal("0"), available)
        if not self.managed:
            return available
        return min(available, self.target_principal_usdc)

    def public_dict(self) -> dict[str, object]:
        return {
            "account_index": self.account_index,
            "account_id": self.account_id,
            "enabled": self.enabled,
            "managed": self.managed,
            "profile_type": self.profile_type,
            "strategy_group": self.strategy_group,
            "target_principal_usdc": str(self.target_principal_usdc),
            "pause_equity_usdc": str(self.pause_equity_usdc),
            "daily_loss_limit_usdc": str(self.daily_loss_limit_usdc),
            "allocation_mode": self.allocation_mode,
            "auto_top_up": self.auto_top_up,
            "auto_sweep": self.auto_sweep,
            "sweep_policy": "manual",
            "guardrails_enforced": False,
        }


def parse_lp_account_profile(config: Mapping[str, Any], account_index: int) -> LPAccountProfile:
    """Parse optional ``lp_account`` config while preserving legacy behavior."""
    raw = config.get("lp_account")
    if raw is None:
        return LPAccountProfile(
            account_index=account_index,
            account_id=f"account_{account_index:02d}",
            enabled=False,
            profile_type="standard",
            strategy_group="legacy",
            target_principal_usdc=Decimal("0"),
            pause_equity_usdc=Decimal("0"),
            daily_loss_limit_usdc=Decimal("0"),
            allocation_mode="disabled",
        )
    if not isinstance(raw, Mapping):
        raise ValueError("lp_account must be an object")

    enabled = _boolean(raw, "enabled", True)
    account_id = str(raw.get("account_id") or f"account_{account_index:02d}").strip()
    if not account_id:
        raise ValueError("lp_account.account_id must not be blank")

    profile_type = str(raw.get("profile_type") or "standard").strip().lower()
    if profile_type not in PROFILE_TYPES:
        raise ValueError(
            "lp_account.profile_type must be one of " + ", ".join(sorted(PROFILE_TYPES))
        )

    strategy_group = str(raw.get("strategy_group") or profile_type).strip()
    if not strategy_group:
        raise ValueError("lp_account.strategy_group must not be blank")

    principal = _money(
        _decimal(raw.get("target_principal_usdc", 0), field="target_principal_usdc")
    )
    if enabled and principal <= 0:
        raise ValueError("lp_account.target_principal_usdc must be greater than zero")
    if principal < 0:
        raise ValueError("lp_account.target_principal_usdc must not be negative")

    default_pause = principal * Decimal("0.85") if profile_type == "aggressive" else Decimal("0")
    default_daily_loss = (
        principal * Decimal("0.05") if profile_type == "aggressive" else Decimal("0")
    )
    pause_equity = _money(
        _decimal(raw.get("pause_equity_usdc", default_pause), field="pause_equity_usdc")
    )
    daily_loss = _money(
        _decimal(
            raw.get("daily_loss_limit_usdc", default_daily_loss),
            field="daily_loss_limit_usdc",
        )
    )
    if pause_equity < 0 or pause_equity > principal:
        raise ValueError("lp_account.pause_equity_usdc must be between zero and principal")
    if daily_loss < 0 or daily_loss > principal:
        raise ValueError("lp_account.daily_loss_limit_usdc must be between zero and principal")

    allocation_mode = str(raw.get("allocation_mode") or "disabled").strip().lower()
    if allocation_mode not in ALLOCATION_MODES:
        raise ValueError(
            "lp_account.allocation_mode must be one of "
            + ", ".join(sorted(ALLOCATION_MODES))
        )

    auto_top_up = _boolean(raw, "auto_top_up", False)
    auto_sweep = _boolean(raw, "auto_sweep", False)
    if auto_top_up:
        raise ValueError("lp_account.auto_top_up is not supported; replenishment is manual")
    if auto_sweep:
        raise ValueError("lp_account.auto_sweep is not supported; withdrawals are manual")

    return LPAccountProfile(
        account_index=account_index,
        account_id=account_id,
        enabled=enabled,
        profile_type=profile_type,
        strategy_group=strategy_group,
        target_principal_usdc=principal,
        pause_equity_usdc=pause_equity,
        daily_loss_limit_usdc=daily_loss,
        allocation_mode=allocation_mode,
        auto_top_up=auto_top_up,
        auto_sweep=auto_sweep,
    )


def market_event_key(token_id: str, market: Mapping[str, Any]) -> str:
    """Return one stable key for both YES/NO tokens of an event."""
    paired = str(market.get("paired_token_id") or "").strip()
    token = str(token_id).strip()
    if paired:
        return "pair:" + ":".join(sorted({token, paired}))
    condition = str(market.get("condition_id") or "").strip().lower()
    if condition:
        return f"condition:{condition}"
    return f"token:{token}"


def _weighted_score(group: str, event_key: str, profile: LPAccountProfile) -> float:
    """Weighted rendezvous score; lower wins and assignment is deterministic."""
    material = f"{group}\0{event_key}\0{profile.account_id}".encode("utf-8")
    digest = int.from_bytes(hashlib.sha256(material).digest(), "big")
    uniform = (digest + 1) / ((1 << 256) + 1)
    weight = max(float(profile.target_principal_usdc), 0.01)
    return -math.log(uniform) / weight


def choose_event_owner(
    event_key: str,
    profiles: list[LPAccountProfile],
) -> LPAccountProfile:
    if not profiles:
        raise ValueError("at least one account profile is required")
    groups = {profile.strategy_group for profile in profiles}
    if len(groups) != 1:
        raise ValueError("event owner candidates must belong to one strategy group")
    group = next(iter(groups))
    return min(
        profiles,
        key=lambda profile: (
            _weighted_score(group, event_key, profile),
            profile.account_index,
        ),
    )


def shared_event_owner(
    account_index: int,
    token_id: str,
    market: Mapping[str, Any],
    profiles: Mapping[int, LPAccountProfile],
) -> int:
    """Resolve the owning account, or return the caller when sharing is off."""
    profile = profiles.get(account_index)
    if (
        profile is None
        or not profile.managed
        or profile.allocation_mode != "exclusive"
    ):
        return account_index
    candidates = [
        candidate
        for candidate in profiles.values()
        if candidate.managed
        and candidate.allocation_mode == "exclusive"
        and candidate.strategy_group == profile.strategy_group
    ]
    return choose_event_owner(market_event_key(token_id, market), candidates).account_index


def validate_shared_allocation(
    profiles: Mapping[int, LPAccountProfile],
    markets_by_account: Mapping[int, Mapping[str, Mapping[str, Any]]],
) -> None:
    """Fail closed when an exclusive strategy group cannot route consistently."""
    managed_ids: dict[str, int] = {}
    grouped_accounts: dict[str, list[int]] = {}
    for account_index, profile in profiles.items():
        if not profile.managed or profile.allocation_mode != "exclusive":
            continue
        account_key = profile.account_id.casefold()
        if account_key in managed_ids:
            raise ValueError(
                f"duplicate LP account_id {profile.account_id!r} for accounts "
                f"{managed_ids[account_key]} and {account_index}"
            )
        managed_ids[account_key] = account_index
        grouped_accounts.setdefault(profile.strategy_group, []).append(account_index)

    for group, account_indexes in grouped_accounts.items():
        if len(account_indexes) < 2:
            continue
        event_sets = {
            account_index: {
                market_event_key(token_id, market)
                for token_id, market in markets_by_account.get(
                    account_index,
                    {},
                ).items()
            }
            for account_index in account_indexes
        }
        baseline_index = min(account_indexes)
        baseline = event_sets[baseline_index]
        for account_index in sorted(account_indexes):
            if event_sets[account_index] == baseline:
                continue
            missing = len(baseline - event_sets[account_index])
            extra = len(event_sets[account_index] - baseline)
            raise ValueError(
                f"exclusive strategy group {group!r} has different market "
                f"universes: account {account_index} vs {baseline_index} "
                f"(missing={missing}, extra={extra})"
            )
