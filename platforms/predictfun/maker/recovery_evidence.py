"""Read-only account and chain evidence collection for recovery review.

No signing keys, transaction methods, ledger writes or service controls belong
here. Collector output is evidence for independent review, not authorization.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import re
import time
from typing import Any, Callable
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from platforms.predictfun.maker.recovery_plan import EXCHANGES, _digest, _uint


NONCE_ABI = [
    {"type": "function", "name": "nonces", "stateMutability": "view",
     "inputs": [{"name": "user", "type": "address"}],
     "outputs": [{"name": "", "type": "uint256"}]},
    {"type": "function", "name": "isValidNonce", "stateMutability": "view",
     "inputs": [{"name": "user", "type": "address"}, {"name": "nonce", "type": "uint256"}],
     "outputs": [{"name": "", "type": "bool"}]},
    {"type": "event", "name": "NonceIncremented", "anonymous": False,
     "inputs": [{"name": "user", "type": "address", "indexed": True},
                {"name": "newNonce", "type": "uint256", "indexed": False}]},
]


def _address(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", value) or int(value, 16) == 0:
        raise ValueError("invalid_maker_or_exchange")
    return value.lower()


def _hash(value: object) -> str:
    if isinstance(value, bytes):
        value = "0x" + value.hex()
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError("invalid_chain_hash")
    return value.lower()


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def make_bsc_reader(rpc_url: str):
    from web3 import Web3
    try:
        from web3.middleware import ExtraDataToPOAMiddleware as poa
    except ImportError:
        from web3.middleware import geth_poa_middleware as poa
    client = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    client.middleware_onion.inject(poa, layer=0)
    return client


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("recovery_redirect_refused")


class ProxyAccountReader:
    """A GET-only client pinned to one account and three known read routes."""

    def __init__(self, base_url: str, account_id: str):
        parsed = urlsplit(base_url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in {"", "/"}):
            raise ValueError("invalid_proxy_origin")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", account_id):
            raise ValueError("invalid_account_id")
        self.base = base_url.rstrip("/")
        self.account_id = account_id
        self.opener = build_opener(_NoRedirect())

    def __call__(self, resource: str, query: dict[str, object]) -> dict[str, Any]:
        if resource not in {"orders", "positions", "allowances"}:
            raise ValueError("recovery_read_route_not_allowed")
        allowed = {"orders": {"first", "after", "status"},
                   "positions": {"first", "after", "isResolved"}, "allowances": set()}
        if set(query) - allowed[resource]:
            raise ValueError("recovery_read_query_not_allowed")
        url = f"{self.base}/predictfun/accounts/{self.account_id}/{resource}"
        if query:
            url += "?" + urlencode(query)
        try:
            request = Request(url, headers={"Accept": "application/json"}, method="GET")
            with self.opener.open(request, timeout=10) as response:
                raw = response.read(2_000_001)
            if len(raw) > 2_000_000:
                raise ValueError("response_too_large")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("invalid_response")
            return payload
        except Exception:
            raise ValueError("recovery_account_read_failed") from None


def collect_account_baseline(
    read: Callable, *, account_id: str, maker: str,
    clock: Callable[[], float] = time.time, max_pages: int = 20,
) -> dict[str, Any]:
    """Complete, bounded account reads; failures never produce empty evidence."""
    owner = _address(maker)
    if type(max_pages) is not int or not 1 <= max_pages <= 100:
        raise ValueError("invalid_page_limit")
    started = clock()
    output: dict[str, Any] = {"account_id": account_id, "maker": owner}
    for resource, filters in (("orders", {"status": "OPEN"}),
                              ("positions", {"isResolved": "false"})):
        cursor = None
        seen: set[str] = set()
        rows: list[dict[str, Any]] = []
        for page in range(max_pages):
            if not 0 <= clock() - started <= 60:
                raise ValueError("account_snapshot_too_slow")
            query = {"first": 100, **filters}
            if cursor is not None:
                query["after"] = cursor
            result = read(resource, query)
            response = result.get("response") if isinstance(result, dict) else None
            if (not isinstance(response, dict) or result.get("ok") is not True
                    or type(result.get("status")) is not int or not 200 <= result["status"] < 300
                    or result.get("alias") != account_id or response.get("success") is not True
                    or not isinstance(response.get("data"), list) or "cursor" not in response):
                raise ValueError("account_page_invalid")
            for row in response["data"]:
                if not isinstance(row, dict) or not row:
                    raise ValueError("account_row_invalid")
                # Preserve nonempty evidence without exporting signed orders.
                rows.append({"record_sha256": _digest(row)})
            cursor = response["cursor"]
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise ValueError("account_cursor_invalid")
            seen.add(cursor)
        else:
            raise ValueError("account_pagination_incomplete")
        output["open_orders" if resource == "orders" else "positions"] = rows
        output[f"{resource}_pages"] = page + 1
    balance = read("allowances", {})
    if (not isinstance(balance, dict) or balance.get("ok") is not True
            or balance.get("alias") != account_id or balance.get("chain_id") != 56
            or balance.get("collateral") != "USDT" or _address(balance.get("owner")) != owner):
        raise ValueError("account_balance_identity_invalid")
    try:
        amount = Decimal(str(balance["balance"]))
        if not amount.is_finite() or amount < 0:
            raise ValueError("invalid_amount")
    except (KeyError, InvalidOperation, ValueError):
        raise ValueError("account_balance_invalid") from None
    finished = clock()
    if not 0 <= finished - started <= 60:
        raise ValueError("account_snapshot_too_slow")
    return {**output, "ok": True, "pagination_complete": True,
            "balance": str(amount), "started_at": _iso(started), "observed_at": _iso(finished)}


def collect_nonce_barrier(
    w3: Any, pending: dict[str, Any], *, account_id: str, maker: str,
    tx_hash: str, old_nonce_upper_bound: int, signing_audit_sha256: str,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Verify a supplied receipt with reads only; never broadcasts a barrier."""
    started = clock()
    owner = _address(maker)
    transaction = _hash(tx_hash)
    if (pending.get("account_id") != account_id
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", str(pending.get("idempotency_key", ""))) is None
            or type(pending.get("market_id")) is not int or pending["market_id"] <= 0
            or type(pending.get("is_neg_risk")) is not bool
            or type(pending.get("is_yield_bearing")) is not bool
            or not _uint(old_nonce_upper_bound)
            or re.fullmatch(r"[0-9a-f]{64}", str(signing_audit_sha256)) is None):
        raise ValueError("nonce_evidence_input_invalid")
    exchange = EXCHANGES[(pending["is_neg_risk"], pending["is_yield_bearing"])]
    if w3.eth.chain_id != 56:
        raise ValueError("nonce_chain_mismatch")
    head = w3.eth.get_block("latest")
    if (not _uint(head["number"]) or not _uint(head["timestamp"])
            or not -5 <= started - head["timestamp"] <= 30):
        raise ValueError("nonce_head_stale")
    head_hash = _hash(head["hash"])
    receipt = w3.eth.get_transaction_receipt(transaction)
    if (type(receipt.get("status")) is not int or receipt["status"] != 1
            or _hash(receipt.get("transactionHash")) != transaction
            or not _uint(receipt.get("blockNumber"))):
        raise ValueError("nonce_receipt_invalid")
    confirmations = head["number"] - receipt["blockNumber"] + 1
    if confirmations < 12:
        raise ValueError("nonce_receipt_unconfirmed")
    mined = w3.eth.get_block(receipt["blockNumber"])
    if (mined.get("number") != receipt["blockNumber"]
            or _hash(mined["hash"]) != _hash(receipt.get("blockHash"))
            or not _uint(mined["timestamp"]) or mined["timestamp"] > head["timestamp"]):
        raise ValueError("nonce_receipt_not_canonical")
    contract = w3.eth.contract(address=w3.to_checksum_address(exchange), abi=NONCE_ABI)
    nonce = contract.functions.nonces(w3.to_checksum_address(owner)).call(block_identifier=head["number"])
    if not _uint(nonce) or nonce <= old_nonce_upper_bound:
        raise ValueError("nonce_barrier_not_advanced")
    old_valid = contract.functions.isValidNonce(w3.to_checksum_address(owner), old_nonce_upper_bound).call(block_identifier=head["number"])
    new_valid = contract.functions.isValidNonce(w3.to_checksum_address(owner), nonce).call(block_identifier=head["number"])
    if old_valid is not False or new_valid is not True:
        raise ValueError("nonce_barrier_not_invalidating")
    events = contract.events.NonceIncremented().process_receipt(receipt)
    matches = [e for e in events if _address(e.get("address")) == exchange
               and e.get("removed", False) is False
               and _address(e["args"].get("user")) == owner
               and type(e["args"].get("newNonce")) is int and e["args"]["newNonce"] == nonce
               and _hash(e.get("transactionHash")) == transaction
               and _hash(e.get("blockHash")) == _hash(mined["hash"])]
    if len(matches) != 1:
        raise ValueError("nonce_event_not_bound_to_maker")
    if _hash(w3.eth.get_block(head["number"])["hash"]) != head_hash:
        raise ValueError("nonce_snapshot_reorg")
    finished = clock()
    if not 0 <= finished - started <= 30:
        raise ValueError("nonce_snapshot_too_slow")
    return {
        "account_id": account_id, "idempotency_key": pending["idempotency_key"],
        "market_id": pending["market_id"], "is_neg_risk": pending["is_neg_risk"],
        "is_yield_bearing": pending["is_yield_bearing"], "maker": owner, "exchange": exchange,
        "chain_id": 56, "old_nonce_upper_bound": old_nonce_upper_bound, "current_nonce": nonce,
        "old_nonce_valid": old_valid, "current_nonce_valid": new_valid,
        "receipt_success": True, "confirmations": confirmations, "tx_hash": transaction,
        "block_number": head["number"], "block_hash": head_hash,
        "receipt_block_number": receipt["blockNumber"], "receipt_block_hash": _hash(mined["hash"]),
        "mined_at": _iso(mined["timestamp"]), "confirmed_at": _iso(head["timestamp"]),
        "observed_at": _iso(finished), "signing_audit_sha256": signing_audit_sha256,
        "nonce_event_verified": True,
    }
