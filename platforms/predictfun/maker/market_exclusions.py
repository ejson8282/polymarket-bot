"""Explicit account/market automation exclusions, not inventory provenance."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MarketExclusions:
    pairs: frozenset[tuple[str, int]] = frozenset()
    inventory_budget_pairs: frozenset[tuple[str, int]] = frozenset()

    @classmethod
    def from_config(cls, cfg: dict[str, Any]) -> "MarketExclusions":
        if not isinstance(cfg, dict):
            raise ValueError("invalid_predict_config")
        pairs = cls._parse_pairs(cfg.get("manual_market_exclusions", {}))
        budget_pairs = cls._parse_pairs(cfg.get("manual_inventory_budget_exclusions", {}))
        if not budget_pairs.issubset(pairs):
            raise ValueError("inventory_budget_exclusion_requires_market_exclusion")
        return cls(pairs, budget_pairs)

    @staticmethod
    def _parse_pairs(raw: Any) -> frozenset[tuple[str, int]]:
        if not isinstance(raw, dict):
            raise ValueError("invalid_manual_market_exclusions")
        pairs = set()
        for account, markets in raw.items():
            if (not isinstance(account, str) or not account.strip()
                    or account != account.strip() or not isinstance(markets, list)):
                raise ValueError("invalid_manual_market_exclusions")
            for market in markets:
                if type(market) is not int or market <= 0:
                    raise ValueError("invalid_manual_market_exclusion_market")
                pairs.add((account, market))
        return frozenset(pairs)

    def excludes_inventory_budget(self, account: str, market: object) -> bool:
        # Unlike operation blocking, malformed identity must never remove risk.
        if isinstance(market, str) and market.isascii() and market.isdigit():
            market = int(market)
        return type(market) is int and (account, market) in self.inventory_budget_pairs

    def blocks(self, account: str, market: object) -> bool:
        if not any(a == account for a, _ in self.pairs):
            return False
        # A malformed market identifier must not bypass an account's protection.
        if isinstance(market, str) and market.isascii() and market.isdigit():
            market = int(market)
        if type(market) is not int or market <= 0:
            return True
        return (account, market) in self.pairs

    def as_dict(self) -> dict[str, list[int]]:
        out: dict[str, list[int]] = {}
        for account, market in sorted(self.pairs):
            out.setdefault(account, []).append(market)
        return out

    def summary(self, active_orders: list[Any]) -> dict[str, Any]:
        active = [row for row in active_orders if self.blocks(row.account_id, row.market_id)]
        return {
            "markets_by_account": self.as_dict(),
            "excluded_managed_active_orders": len(active),
            "requires_manual_order_review": bool(active),
        }
