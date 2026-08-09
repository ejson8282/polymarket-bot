import asyncio
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
MAKER_DIR = ROOT / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from aggressive_proxy import parse_connect_target, proxy_ports, public_addresses  # noqa: E402


def _roster() -> dict:
    return {
        "schema_version": 1,
        "runtime_scope": "aggressive",
        "accounts": [
            {
                "account_index": 1,
                "host_id": "aggressive-a",
                "funder": "0x" + "1" * 40,
                "clash_port": 7901,
                "lp_account": {
                    "account_id": "aggressive_200",
                    "profile_type": "aggressive",
                    "target_principal_usdc": 200,
                    "allocation_mode": "exclusive",
                },
            }
        ],
    }


def test_proxy_ports_follow_enabled_local_roster() -> None:
    assert proxy_ports(_roster(), "aggressive-a") == (7901,)
    with pytest.raises(ValueError, match="no enabled account"):
        proxy_ports(_roster(), "aggressive-b")


def test_connect_target_allows_https_only() -> None:
    assert parse_connect_target(b"CONNECT clob.polymarket.com:443 HTTP/1.1") == (
        "clob.polymarket.com",
        443,
    )
    for request in (
        b"GET https://clob.polymarket.com HTTP/1.1",
        b"CONNECT clob.polymarket.com:80 HTTP/1.1",
    ):
        with pytest.raises(ValueError):
            parse_connect_target(request)


def test_proxy_rejects_non_public_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLoop:
        async def getaddrinfo(self, *_args, **_kwargs):
            return [(2, 1, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: FakeLoop())

    async def check() -> None:
        with pytest.raises(ValueError, match="no public address"):
            await public_addresses("example.invalid", 443)

    asyncio.run(check())
