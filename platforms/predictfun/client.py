from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import time
from typing import Any

import requests


PREDICT_MAINNET_BASE = "https://api.predict.fun"
PREDICT_TESTNET_BASE = "https://api-testnet.predict.fun"
PREDICT_USER_AGENT = "predictfun-maker/0.1"


class PredictFunError(RuntimeError):
    pass


@dataclass(frozen=True)
class PredictFunClient:
    base_url: str = PREDICT_TESTNET_BASE
    api_key: str = ""
    timeout: float = 15.0
    retries: int = 3
    user_agent: str = PREDICT_USER_AGENT
    session: requests.Session = field(
        default_factory=requests.Session, compare=False, repr=False
    )

    def _headers(self, *, jwt: str = "") -> dict[str, str]:
        headers = {"accept": "application/json", "user-agent": self.user_agent}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if jwt:
            headers["Authorization"] = f"Bearer {jwt}"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        jwt: str = "",
    ) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(max(1, self.retries)):
            try:
                resp = self.session.request(
                    method,
                    url,
                    params={k: v for k, v in (params or {}).items() if v is not None},
                    json=json_body,
                    headers=self._headers(jwt=jwt),
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as exc:
                last_exc = exc
                if attempt + 1 >= max(1, self.retries):
                    raise PredictFunError(f"{method} {url} failed: {exc}") from exc
                time.sleep(0.4 * (attempt + 1))
        else:
            raise PredictFunError(f"{method} {url} failed: {last_exc}")

        if isinstance(data, dict) and data.get("success") is False:
            raise PredictFunError(f"{method} {url} returned success=false: {data}")
        if not isinstance(data, dict):
            raise PredictFunError(f"{method} {url} returned non-object JSON")
        return data

    def list_markets(
        self,
        *,
        first: int = 50,
        after: str | None = None,
        status: str | None = "OPEN",
        has_active_rewards: bool | None = None,
        market_variant: str | None = None,
        is_boosted: bool | None = None,
        sort: str | None = None,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            "/v1/markets",
            params={
                "first": first,
                "after": after,
                "status": status,
                "hasActiveRewards": _bool_param(has_active_rewards),
                "marketVariant": market_variant,
                "isBoosted": _bool_param(is_boosted),
                "sort": sort,
            },
        )

    def get_market(self, market_id: int | str) -> dict[str, Any]:
        return self._request("GET", f"/v1/markets/{market_id}")

    def get_orderbook(self, market_id: int | str) -> dict[str, Any]:
        return self._request("GET", f"/v1/markets/{market_id}/orderbook")


def _bool_param(value: bool | None) -> str | None:
    if value is None:
        return None
    return "true" if value else "false"


def as_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def decimal_tick(decimal_precision: int) -> Decimal:
    precision = max(0, int(decimal_precision))
    return Decimal(1).scaleb(-precision)
