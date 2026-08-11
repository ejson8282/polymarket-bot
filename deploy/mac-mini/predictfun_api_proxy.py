#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time
from decimal import Decimal, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

SECRET_FILE = Path.home() / ".macmini-secrets" / "predictfun.env"
ORDER_LEDGER_FILE = (
    Path.home() / ".macmini-secrets" / "predictfun-order-ledger.json"
)
USER_AGENT = "predictfun-maker/0.1"
ALLOWED_CLIENTS = {
    "127.0.0.1",
    "::1",
    "100.91.159.54",
    "100.122.255.98",
    "100.101.50.40",
}
ALLOWED_PATHS = (
    re.compile(r"^/v1/markets/?$"),
    re.compile(r"^/v1/markets/[^/]+/?$"),
    re.compile(r"^/v1/markets/[^/]+/orderbook/?$"),
)
AUTH_CHECK_RE = re.compile(r"^/predictfun/accounts/([^/]+)/auth-check/?$")
ORDER_PREVIEW_RE = re.compile(r"^/predictfun/accounts/([^/]+)/preview-order/?$")
ORDER_SUBMIT_RE = re.compile(r"^/predictfun/accounts/([^/]+)/submit-order/?$")
ORDER_REMOVE_HASH_RE = re.compile(r"^/predictfun/accounts/([^/]+)/remove-order-by-hash/?$")
ALLOWANCES_RE = re.compile(r"^/predictfun/accounts/([^/]+)/allowances/?$")
CAPABILITIES_RE = re.compile(r"^/predictfun/accounts/([^/]+)/capabilities/?$")
ACCOUNT_RE = re.compile(r"^/predictfun/accounts/([^/]+)/account/?$")
ACCOUNT_STATE_RE = re.compile(r"^/predictfun/accounts/([^/]+)/state/?$")
ACCOUNT_ORDERS_RE = re.compile(r"^/predictfun/accounts/([^/]+)/orders/?$")
ACCOUNT_ORDER_RE = re.compile(
    r"^/predictfun/accounts/([^/]+)/orders/([^/]+)/?$"
)
ACCOUNT_POSITIONS_RE = re.compile(
    r"^/predictfun/accounts/([^/]+)/positions/?$"
)
ACCOUNT_ACTIVITY_RE = re.compile(
    r"^/predictfun/accounts/([^/]+)/activity/?$"
)
ORDER_CANCEL_RE = re.compile(
    r"^/predictfun/accounts/([^/]+)/cancel-orders/?$"
)

_TOKEN_TTL_SEC = 8 * 60
_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
_TOKEN_CACHE_LOCK = threading.Lock()
_ACCOUNT_LOCKS: dict[str, threading.RLock] = {}
_ACCOUNT_LOCKS_LOCK = threading.Lock()
_LEDGER_LOCK = threading.Lock()


class PredictFunHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key.strip()] = value
    return values


def is_allowed_path(path: str) -> bool:
    return any(pattern.match(path) for pattern in ALLOWED_PATHS)


def _account_lock(alias: str) -> threading.RLock:
    with _ACCOUNT_LOCKS_LOCK:
        lock = _ACCOUNT_LOCKS.get(alias)
        if lock is None:
            lock = threading.RLock()
            _ACCOUNT_LOCKS[alias] = lock
        return lock


def _load_order_ledger() -> dict[str, object]:
    try:
        payload = json.loads(ORDER_LEDGER_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"version": 1, "orders": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "orders": {}}
    orders = payload.get("orders")
    if not isinstance(orders, dict):
        payload["orders"] = {}
    return payload


def _write_order_ledger(payload: dict[str, object]) -> None:
    ORDER_LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = ORDER_LEDGER_FILE.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.chmod(0o600)
    temporary.replace(ORDER_LEDGER_FILE)
    ORDER_LEDGER_FILE.chmod(0o600)


def _idempotency_key(alias: str, body: dict[str, object]) -> str:
    raw = str(body.get("idempotency_key") or body.get("intent_id") or "").strip()
    if not raw:
        raise ValueError("missing_idempotency_key")
    if len(raw) > 160 or not re.fullmatch(r"[A-Za-z0-9_.:-]+", raw):
        raise ValueError("idempotency_key_invalid")
    return f"{alias}:{raw}"


def _idempotent_salt(key: str) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 2_147_483_648


def _order_request_fingerprint(body: dict[str, object]) -> str:
    post_only_value = (
        body.get("is_post_only")
        if body.get("is_post_only") is not None
        else body.get("isPostOnly")
    )
    payload = {
        "market_id": str(body.get("market_id") or ""),
        "token_id": str(body.get("token_id") or body.get("tokenId") or ""),
        "side": str(body.get("side") or "").strip().upper(),
        "price": _canonical_decimal(
            body.get("price")
            or body.get("price_per_share")
            or body.get("pricePerShare")
        ),
        "size": _canonical_decimal(
            body.get("size") or body.get("quantity") or body.get("amount")
        ),
        "fee_rate_bps": str(
            int(
                str(
                    body.get("fee_rate_bps")
                    if body.get("fee_rate_bps") is not None
                    else body.get("feeRateBps") or "0"
                )
            )
        ),
        "is_neg_risk": _bool_value(
            body.get("is_neg_risk")
            if body.get("is_neg_risk") is not None
            else body.get("isNegRisk")
        ),
        "is_yield_bearing": _bool_value(
            body.get("is_yield_bearing")
            if body.get("is_yield_bearing") is not None
            else body.get("isYieldBearing")
        ),
        "is_post_only": True if post_only_value is None else _bool_value(post_only_value),
        "reserved_balance_policy": str(
            body.get("reserved_balance_policy")
            or body.get("reservedBalancePolicy")
            or ""
        ),
        "self_trade_prevention": str(
            body.get("self_trade_prevention")
            or body.get("selfTradePrevention")
            or ""
        ),
        "expiration": str(
            body.get("expiration")
            or body.get("expiration_secs")
            or body.get("expirationTimestamp")
            or ""
        ),
        "maker": str(body.get("maker") or "").strip().lower(),
        "signer": str(body.get("signer") or "").strip().lower(),
        "taker": str(body.get("taker") or "").strip().lower(),
        "nonce": str(int(str(body.get("nonce") or "0"))),
        "signature_type": str(
            int(
                str(
                    body.get("signature_type")
                    if body.get("signature_type") is not None
                    else body.get("signatureType") or "0"
                )
            )
        ),
        "salt": str(body.get("salt") or ""),
        "max_notional_usdc": _canonical_decimal(
            body.get("max_notional_usdc")
            or body.get("max_notional")
            or "0"
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_maker_order_safety(body: dict[str, object]) -> str:
    post_only_value = (
        body.get("is_post_only")
        if body.get("is_post_only") is not None
        else body.get("isPostOnly")
    )
    if post_only_value is None or not _bool_value(post_only_value):
        raise ValueError("post_only_required")

    if "reserved_balance_policy" in body or "reservedBalancePolicy" in body:
        raise ValueError("reserved_balance_policy_not_allowed_for_limit")

    self_trade_prevention = str(
        body.get("self_trade_prevention")
        or body.get("selfTradePrevention")
        or ""
    )
    if self_trade_prevention != "CANCEL_MAKER":
        raise ValueError("self_trade_prevention_required")
    return self_trade_prevention


def _json_obj(raw: str) -> dict[str, object]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _account_row(env: dict[str, str], alias: str) -> dict[str, object]:
    accounts = _json_obj(env.get("PREDICTFUN_ACCOUNT_KEYS_JSON", ""))
    value = accounts.get(alias)
    return value if isinstance(value, dict) else {}


def _account_api_key(env: dict[str, str], row: dict[str, object]) -> str:
    return str(row.get("api_key") or env.get("PREDICTFUN_API_KEY") or "").strip()


def account_summary(env: dict[str, str]) -> dict[str, object]:
    accounts = _json_obj(env.get("PREDICTFUN_ACCOUNT_KEYS_JSON", ""))
    rows: list[dict[str, object]] = []
    ready = 0
    for alias, value in sorted(accounts.items()):
        row = value if isinstance(value, dict) else {}
        wallet_present = bool(str(row.get("wallet_address") or ""))
        api_key_present = bool(_account_api_key(env, row))
        private_key_present = bool(str(row.get("private_key") or ""))
        is_ready = api_key_present and private_key_present
        ready += 1 if is_ready else 0
        rows.append(
            {
                "alias": str(alias),
                "ready": is_ready,
                "wallet_present": wallet_present,
                "api_key_present": api_key_present,
                "private_key_present": private_key_present,
            }
        )
    return {"configured": len(rows), "ready": ready, "aliases": rows}


def mask_address(address: str) -> str:
    if len(address) < 14:
        return "invalid"
    return address[:8] + "..." + address[-6:]


def normalize_private_key(value: object) -> str:
    private_key = str(value or "").strip()
    if private_key and not private_key.startswith("0x"):
        private_key = "0x" + private_key
    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", private_key):
        raise ValueError("private_key_format_invalid")
    return private_key


def _hex_body(value: str) -> str:
    return value[2:] if value.startswith("0x") else value


def signature_v_to_0_1(signature_hex: str) -> str:
    data = bytearray.fromhex(_hex_body(signature_hex))
    if data[-1] in (27, 28):
        data[-1] -= 27
    return "0x" + data.hex()


def _read_json_response(resp) -> dict[str, object]:
    body = resp.read()
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {"text": body[:180].decode("utf-8", errors="replace")}
    return payload if isinstance(payload, dict) else {"data": payload}


def _extract_token(payload: dict[str, object]) -> str | None:
    for key in ("token", "access_token", "jwt", "bearer", "accessToken"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    for value in payload.values():
        if isinstance(value, dict):
            found = _extract_token(value)
            if found:
                return found
    return None


def _safe_upstream_error(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {"body": str(payload)[:180]}
    nested = payload.get("upstream")
    if isinstance(nested, dict):
        nested_safe = _safe_upstream_error(nested)
        if nested_safe:
            return nested_safe
    return {
        key: value
        for key, value in payload.items()
        if key in {"success", "code", "error", "message"}
    }


def _format_token_amount(raw: int, decimals: int) -> str:
    scale = Decimal(10) ** Decimal(decimals)
    return format((Decimal(raw) / scale).normalize(), "f")


def collateral_allowances(env: dict[str, str], alias: str) -> dict[str, object]:
    from eth_account import Account
    from web3 import Web3

    row = _account_row(env, alias)
    if not row:
        return {"ok": False, "error": "account_alias_not_found", "alias": alias}
    owner = str(row.get("wallet_address") or row.get("address") or "").strip()
    if not owner.lower().startswith("0x"):
        private_key = normalize_private_key(row.get("private_key"))
        owner = Account.from_key(private_key).address

    rpc_url = str(env.get("PREDICTFUN_BSC_RPC_URL") or env.get("BSC_RPC_URL") or env.get("BNB_RPC_URL") or PREDICT_BSC_RPC_URL)
    token_address = str(env.get("PREDICTFUN_COLLATERAL_TOKEN") or PREDICT_USDT_ADDRESS)
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        return {"ok": False, "error": "bsc_rpc_unavailable", "alias": alias}

    abi = [
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
        {"constant": True, "inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
        {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    ]
    token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=abi)
    owner_addr = Web3.to_checksum_address(owner)
    decimals = int(token.functions.decimals().call())
    balance_raw = int(token.functions.balanceOf(owner_addr).call())
    modes: dict[str, object] = {}
    for mode, exchange in PREDICT_EXCHANGES.items():
        allowance_raw = int(token.functions.allowance(owner_addr, Web3.to_checksum_address(exchange)).call())
        mode_name = PREDICT_EXCHANGE_MODE_NAMES[mode]
        modes[mode_name] = {
            "neg_risk": mode[0],
            "yield_bearing": mode[1],
            "spender": exchange,
            "allowance_raw": str(allowance_raw),
            "allowance": _format_token_amount(allowance_raw, decimals),
        }
    return {
        "ok": True,
        "alias": alias,
        "chain_id": PREDICT_CHAIN_ID,
        "owner": owner,
        "owner_masked": mask_address(owner),
        "collateral": "USDT",
        "token": token_address,
        "decimals": decimals,
        "balance_raw": str(balance_raw),
        "balance": _format_token_amount(balance_raw, decimals),
        "modes": modes,
    }


def _account_predict_address(row: dict[str, object], eoa_address: str) -> str:
    configured = str(row.get("wallet_address") or row.get("address") or "").strip()
    if configured.lower().startswith("0x") and configured.lower() != eoa_address.lower():
        return configured
    return ""


PREDICT_CHAIN_ID = 56
PREDICT_PROTOCOL_NAME = "predict.fun CTF Exchange"
PREDICT_PROTOCOL_VERSION = "1"
PREDICT_KERNEL_NAME = "Kernel"
PREDICT_KERNEL_VERSION = "0.3.1"
PREDICT_ECDSA_VALIDATOR = "0x845ADb2C711129d4f3966735eD98a9F09fC4cE57"
PREDICT_EXCHANGES = {
    (False, False): "0x8BC070BEdAB741406F4B1Eb65A72bee27894B689",
    (True, False): "0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A",
    (False, True): "0x6bEb5a40C032AFc305961162d8204CDA16DECFa5",
    (True, True): "0x8A289d458f5a134bA40015085A8F50Ffb681B41d",
}
PREDICT_EXCHANGE_MODE_NAMES = {
    (False, False): "standard",
    (True, False): "neg_risk",
    (False, True): "yield_bearing",
    (True, True): "neg_risk_yield_bearing",
}
PREDICT_USDT_ADDRESS = "0x55d398326f99059fF775485246999027B3197955"
PREDICT_BSC_RPC_URL = "https://bsc-dataseed.binance.org"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
WEI = Decimal("1000000000000000000")
ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ]
}


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _decimal_to_wei(value: object) -> int:
    dec = Decimal(str(value))
    if not dec.is_finite():
        raise ValueError("decimal_must_be_finite")
    return int((dec * WEI).to_integral_value(rounding=ROUND_DOWN))


def _canonical_decimal(value: object) -> str:
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise ValueError("decimal_invalid") from exc
    if not number.is_finite():
        raise ValueError("decimal_must_be_finite")
    return format(number.normalize(), "f")


def _retain_significant_digits(num: int, significant_digits: int) -> int:
    if num == 0:
        return 0
    sign = -1 if num < 0 else 1
    raw = str(abs(num))
    excess = len(raw) - significant_digits
    if excess <= 0:
        return num
    divisor = 10 ** excess
    return sign * ((abs(num) // divisor) * divisor)


def _build_limit_amounts(side: int, price_wei: int, quantity_wei: int) -> dict[str, int]:
    if quantity_wei < 10**16:
        raise ValueError("quantity_too_small")
    price = _retain_significant_digits(price_wei, 3)
    qty = _retain_significant_digits(quantity_wei, 5)
    if side == 0:
        maker_amount = price * qty // 10**18
        taker_amount = qty
    else:
        maker_amount = qty
        taker_amount = price * qty // 10**18
    return {
        "pricePerShare": price,
        "makerAmount": maker_amount,
        "takerAmount": taker_amount,
        "amount": qty,
        "slippageBps": 0,
        "isMinAmountOut": False,
    }


def _build_order(row: dict[str, object], signer_address: str, body: dict[str, object]) -> tuple[dict[str, object], dict[str, int | bool], dict[str, object]]:
    side_raw = str(body.get("side") or "").strip().upper()
    if side_raw not in {"BUY", "SELL"}:
        raise ValueError("side_must_be_buy_or_sell")
    side = 0 if side_raw == "BUY" else 1
    token_id = str(body.get("token_id") or body.get("tokenId") or "").strip()
    if not token_id:
        raise ValueError("missing_token_id")
    fee_rate_bps = int(str(body.get("fee_rate_bps") if body.get("fee_rate_bps") is not None else body.get("feeRateBps") or "0"))
    if fee_rate_bps < 0 or fee_rate_bps > 10_000:
        raise ValueError("fee_rate_bps_out_of_range")
    price = Decimal(
        _canonical_decimal(
            body.get("price")
            or body.get("price_per_share")
            or body.get("pricePerShare")
        )
    )
    quantity = Decimal(
        _canonical_decimal(
            body.get("size") or body.get("quantity") or body.get("amount")
        )
    )
    if price <= 0 or price >= 1:
        raise ValueError("price_out_of_range")
    if quantity <= 0:
        raise ValueError("quantity_must_be_positive")
    price_wei = _decimal_to_wei(price)
    quantity_wei = _decimal_to_wei(quantity)
    amounts = _build_limit_amounts(side, price_wei, quantity_wei)
    now = int(__import__("time").time())
    expiration = int(body.get("expiration") or body.get("expiration_secs") or body.get("expirationTimestamp") or (now + 24 * 3600))
    if expiration <= now:
        raise ValueError("expiration_must_be_future")
    configured_account = str(row.get("wallet_address") or row.get("address") or "").strip()
    predict_account = configured_account if configured_account.lower().startswith("0x") and configured_account.lower() != signer_address.lower() else ""
    maker = predict_account or str(body.get("maker") or signer_address)
    signer = predict_account or str(body.get("signer") or signer_address)
    order = {
        "salt": str(body.get("salt") or secrets.randbelow(2_147_483_648)),
        "maker": maker,
        "signer": signer,
        "taker": str(body.get("taker") or ZERO_ADDRESS),
        "tokenId": str(token_id),
        "makerAmount": str(amounts["makerAmount"]),
        "takerAmount": str(amounts["takerAmount"]),
        "expiration": str(expiration),
        "nonce": str(body.get("nonce") or 0),
        "feeRateBps": str(fee_rate_bps),
        "side": side,
        "signatureType": int(body.get("signatureType") or body.get("signature_type") or 0),
    }
    flags = {
        "isNegRisk": _bool_value(body.get("is_neg_risk") if body.get("is_neg_risk") is not None else body.get("isNegRisk")),
        "isYieldBearing": _bool_value(body.get("is_yield_bearing") if body.get("is_yield_bearing") is not None else body.get("isYieldBearing")),
        "predictAccount": bool(predict_account),
    }
    return order, amounts, flags


def _typed_data(order: dict[str, object], *, is_neg_risk: bool, is_yield_bearing: bool) -> dict[str, object]:
    verifying_contract = PREDICT_EXCHANGES[(is_neg_risk, is_yield_bearing)]
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            **ORDER_TYPES,
        },
        "primaryType": "Order",
        "domain": {
            "name": PREDICT_PROTOCOL_NAME,
            "version": PREDICT_PROTOCOL_VERSION,
            "chainId": PREDICT_CHAIN_ID,
            "verifyingContract": verifying_contract,
        },
        "message": {
            **order,
            "salt": int(order["salt"]),
            "tokenId": int(order["tokenId"]),
            "makerAmount": int(order["makerAmount"]),
            "takerAmount": int(order["takerAmount"]),
            "expiration": int(order["expiration"]),
            "nonce": int(order["nonce"]),
            "feeRateBps": int(order["feeRateBps"]),
            "side": int(order["side"]),
            "signatureType": int(order["signatureType"]),
        },
    }


def _hash_kernel_message(message_hash: bytes) -> bytes:
    from eth_abi import encode
    from eth_utils import keccak

    type_hash = keccak(text="Kernel(bytes32 hash)")
    return keccak(encode(["bytes32", "bytes32"], [type_hash, message_hash]))


def _predict_account_digest(order_hash: bytes, predict_account: str) -> bytes:
    from eth_account._utils.encode_typed_data.encoding_and_hashing import hash_domain
    from eth_utils import keccak

    domain_hash = hash_domain({
        "name": PREDICT_KERNEL_NAME,
        "version": PREDICT_KERNEL_VERSION,
        "chainId": PREDICT_CHAIN_ID,
        "verifyingContract": predict_account,
    })
    return keccak(b"\x19\x01" + domain_hash + _hash_kernel_message(order_hash))



def _eoa_auth_signature(private_key: str, message: str) -> str:
    from eth_account import Account
    from eth_account.messages import encode_defunct

    signature = Account.sign_message(encode_defunct(text=message), private_key=private_key).signature.hex()
    return signature_v_to_0_1(signature)


def _predict_account_auth_signature(private_key: str, predict_account: str, message: str) -> str:
    from eth_account import Account
    from eth_account.messages import encode_defunct, _hash_eip191_message

    message_hash = _hash_eip191_message(encode_defunct(text=message))
    digest = _predict_account_digest(message_hash, predict_account)
    inner = Account.sign_message(encode_defunct(primitive=digest), private_key=private_key).signature.hex()
    return "0x01" + PREDICT_ECDSA_VALIDATOR[2:] + _hex_body(inner)


def _auth_token(
    env: dict[str, str],
    row: dict[str, object],
    private_key: str,
    address: str,
    *,
    auth_signer: str | None = None,
    auth_mode: str = "eoa",
) -> tuple[str, int]:
    api_key = _account_api_key(env, row)
    if not api_key:
        raise ValueError("missing_api_key")
    base_url = (env.get("PREDICTFUN_BASE_URL") or "https://api.predict.fun").rstrip("/")
    headers = {"accept": "application/json", "user-agent": USER_AGENT, "x-api-key": api_key}
    message_req = Request(base_url + "/v1/auth/message", headers=headers)
    with urlopen(message_req, timeout=20) as resp:
        message_payload = _read_json_response(resp)
    data = message_payload.get("data")
    message = data.get("message") if isinstance(data, dict) else None
    if not isinstance(message, str) or not message:
        raise ValueError("auth_message_missing")
    signer = str(auth_signer or address).strip()
    if auth_mode == "predict_account":
        signature = _predict_account_auth_signature(private_key, signer, message)
    else:
        signature = _eoa_auth_signature(private_key, message)
    auth_body = json.dumps({"signer": signer, "signature": signature, "message": message}).encode("utf-8")
    auth_req = Request(
        base_url + "/v1/auth",
        data=auth_body,
        headers={**headers, "content-type": "application/json"},
        method="POST",
    )
    with urlopen(auth_req, timeout=20) as resp:
        auth_payload = _read_json_response(resp)
        status = resp.status
    token = _extract_token(auth_payload)
    if not token:
        raise ValueError("auth_token_missing")
    return token, status


def _account_auth_context(
    env: dict[str, str], alias: str
) -> dict[str, object]:
    from eth_account import Account

    row = _account_row(env, alias)
    if not row:
        raise ValueError("account_alias_not_found")
    private_key = normalize_private_key(row.get("private_key"))
    account = Account.from_key(private_key)
    predict_account = _account_predict_address(row, account.address)
    return {
        "row": row,
        "private_key": private_key,
        "eoa_address": account.address,
        "predict_account": predict_account,
        "auth_signer": predict_account or account.address,
        "auth_mode": "predict_account" if predict_account else "eoa",
    }


def _cached_auth_token(
    env: dict[str, str], alias: str, *, force_refresh: bool = False
) -> tuple[str, dict[str, object]]:
    context = _account_auth_context(env, alias)
    now = time.monotonic()
    if not force_refresh:
        with _TOKEN_CACHE_LOCK:
            cached = _TOKEN_CACHE.get(alias)
            if cached and cached[1] > now:
                return cached[0], context
    token, _status = _auth_token(
        env,
        context["row"],
        str(context["private_key"]),
        str(context["eoa_address"]),
        auth_signer=str(context["auth_signer"]),
        auth_mode=str(context["auth_mode"]),
    )
    with _TOKEN_CACHE_LOCK:
        _TOKEN_CACHE[alias] = (token, now + _TOKEN_TTL_SEC)
    return token, context


def _authenticated_request(
    env: dict[str, str],
    alias: str,
    path: str,
    *,
    query: dict[str, object] | None = None,
    method: str = "GET",
    body: dict[str, object] | None = None,
) -> tuple[int, dict[str, object]]:
    for attempt in range(2):
        token, context = _cached_auth_token(
            env, alias, force_refresh=attempt > 0
        )
        row = context["row"]
        api_key = _account_api_key(env, row)
        base_url = (
            env.get("PREDICTFUN_BASE_URL") or "https://api.predict.fun"
        ).rstrip("/")
        target = base_url + path
        if query:
            encoded = urlencode(
                {
                    key: value
                    for key, value in query.items()
                    if value is not None and str(value) != ""
                }
            )
            if encoded:
                target += "?" + encoded
        request_body = None
        headers = {
            "accept": "application/json",
            "user-agent": USER_AGENT,
            "x-api-key": api_key,
            "authorization": "Bearer " + token,
        }
        if body is not None:
            request_body = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"
        req = Request(
            target,
            data=request_body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(req, timeout=20) as resp:
                return resp.status, _read_json_response(resp)
        except HTTPError as exc:
            payload = _read_json_response(exc)
            if exc.code == 401 and attempt == 0:
                with _TOKEN_CACHE_LOCK:
                    _TOKEN_CACHE.pop(alias, None)
                continue
            return exc.code, {
                "success": False,
                "error": "upstream_rejected",
                "upstream": _safe_upstream_error(payload),
            }
    return 401, {"success": False, "error": "auth_rejected"}


def _authenticated_get(
    env: dict[str, str],
    alias: str,
    path: str,
    *,
    query: dict[str, object] | None = None,
) -> dict[str, object]:
    status, payload = _authenticated_request(
        env, alias, path, query=query
    )
    return {
        "ok": status < 400 and payload.get("success") is not False,
        "alias": alias,
        "status": status,
        "response": payload,
    }


def account_capabilities(
    env: dict[str, str], alias: str
) -> dict[str, object]:
    import importlib.util

    row = _account_row(env, alias)
    if not row:
        return {"ok": False, "error": "account_alias_not_found", "alias": alias}
    sdk_present = importlib.util.find_spec("predict_sdk") is not None
    try:
        _token, context = _cached_auth_token(env, alias)
        auth_ok = True
        auth_error = ""
    except Exception as exc:
        context = {}
        auth_ok = False
        auth_error = type(exc).__name__
    return {
        "ok": auth_ok,
        "alias": alias,
        "auth": auth_ok,
        "auth_error": auth_error,
        "predict_account": bool(context.get("predict_account")),
        "sdk_present": sdk_present,
        "live_order_submit": auth_ok,
        "live_order_read": auth_ok,
        "live_position_read": auth_ok,
        "live_balance_read": True,
        "live_order_cancel": auth_ok and sdk_present,
        "off_book_remove": auth_ok,
    }


def account_state(env: dict[str, str], alias: str) -> dict[str, object]:
    account = _authenticated_get(env, alias, "/v1/account")
    orders = _authenticated_get(
        env, alias, "/v1/orders", query={"first": 100, "status": "OPEN"}
    )
    positions = _authenticated_get(
        env,
        alias,
        "/v1/positions",
        query={"first": 100, "isResolved": "false"},
    )
    allowances = collateral_allowances(env, alias)
    capabilities = account_capabilities(env, alias)
    return {
        "ok": all(
            item.get("ok") is True
            for item in (account, orders, positions, allowances, capabilities)
        ),
        "alias": alias,
        "account": account,
        "orders": orders,
        "positions": positions,
        "balance": allowances,
        "capabilities": capabilities,
    }


def _auth_attempt(env: dict[str, str], row: dict[str, object], private_key: str, address: str, *, auth_signer: str, auth_mode: str) -> dict[str, object]:
    try:
        token, status = _auth_token(
            env,
            row,
            private_key,
            address,
            auth_signer=auth_signer,
            auth_mode=auth_mode,
        )
        return {"mode": auth_mode, "status": status, "signer": mask_address(auth_signer), "token_present": bool(token)}
    except HTTPError as exc:
        try:
            body = _read_json_response(exc)
        except Exception:
            body = {}
        return {"mode": auth_mode, "status": exc.code, "signer": mask_address(auth_signer), "error": _safe_upstream_error(body)}


def run_auth_check(env: dict[str, str], alias: str) -> dict[str, object]:
    from eth_account import Account

    row = _account_row(env, alias)
    if not row:
        return {"ok": False, "error": "account_alias_not_found", "alias": alias}
    private_key = normalize_private_key(row.get("private_key"))
    account = Account.from_key(private_key)
    modes: list[dict[str, object]] = [
        _auth_attempt(env, row, private_key, account.address, auth_signer=account.address, auth_mode="eoa")
    ]
    predict_account = _account_predict_address(row, account.address)
    if predict_account:
        modes.append(
            _auth_attempt(env, row, private_key, account.address, auth_signer=predict_account, auth_mode="predict_account")
        )
    return {"ok": all("error" not in item for item in modes), "alias": alias, "modes": modes}


def _signed_order_payload(env: dict[str, str], alias: str, body: dict[str, object]) -> dict[str, object]:
    from eth_account import Account
    from eth_account.messages import encode_defunct, encode_typed_data, _hash_eip191_message

    row = _account_row(env, alias)
    if not row:
        raise ValueError("account_alias_not_found")
    private_key = normalize_private_key(row.get("private_key"))
    account = Account.from_key(private_key)
    order, amounts, flags = _build_order(row, account.address, body)
    typed = _typed_data(order, is_neg_risk=bool(flags["isNegRisk"]), is_yield_bearing=bool(flags["isYieldBearing"]))
    signable = encode_typed_data(full_message=typed)
    order_hash = _hash_eip191_message(signable)
    if flags["predictAccount"]:
        digest = _predict_account_digest(order_hash, str(order["maker"]))
        sig = Account.sign_message(encode_defunct(primitive=digest), private_key=private_key).signature.hex()
        signature = "0x01" + PREDICT_ECDSA_VALIDATOR[2:] + _hex_body(sig)
        signer_mode = "predict_account"
    else:
        signature = Account.sign_message(signable, private_key=private_key).signature.hex()
        signer_mode = "eoa"
    signed_order = {**order, "hash": "0x" + order_hash.hex(), "signature": signature}
    return {
        "row": row,
        "private_key": private_key,
        "account_address": account.address,
        "order": order,
        "signed_order": signed_order,
        "amounts": amounts,
        "flags": flags,
        "typed": typed,
        "order_hash": "0x" + order_hash.hex(),
        "signer_mode": signer_mode,
    }


def preview_order(env: dict[str, str], alias: str, body: dict[str, object]) -> dict[str, object]:
    payload = _signed_order_payload(env, alias, body)
    order = payload["order"]
    amounts = payload["amounts"]
    flags = payload["flags"]
    typed = payload["typed"]
    return {
        "ok": True,
        "alias": alias,
        "signer": mask_address(payload["account_address"]),
        "maker": mask_address(str(order["maker"])),
        "signer_mode": payload["signer_mode"],
        "strategy": "LIMIT",
        "side": "BUY" if int(order["side"]) == 0 else "SELL",
        "token_id_tail": str(order["tokenId"])[-10:],
        "price_per_share_wei": str(amounts["pricePerShare"]),
        "maker_amount": str(order["makerAmount"]),
        "taker_amount": str(order["takerAmount"]),
        "fee_rate_bps": str(order["feeRateBps"]),
        "is_neg_risk": bool(flags["isNegRisk"]),
        "is_yield_bearing": bool(flags["isYieldBearing"]),
        "exchange": mask_address(str(typed["domain"]["verifyingContract"])),
        "order_hash": payload["order_hash"],
        "signature_present": bool(payload["signed_order"].get("signature")),
    }


def _notional_usdc(order: dict[str, object]) -> Decimal:
    side = int(order["side"])
    raw = Decimal(str(order["makerAmount"] if side == 0 else order["takerAmount"]))
    return raw / WEI


def submit_order(env: dict[str, str], alias: str, body: dict[str, object]) -> dict[str, object]:
    if body.get("submit") is not True or str(body.get("confirm") or "") != "SUBMIT_PREDICTFUN_ORDER":
        return {"ok": False, "error": "explicit_submit_confirmation_required", "alias": alias}
    account_row = _account_row(env, alias)
    if not account_row:
        return {"ok": False, "error": "account_alias_not_found", "alias": alias}
    self_trade_prevention = _require_maker_order_safety(body)
    ledger_key = _idempotency_key(alias, body)
    request_fingerprint = _order_request_fingerprint(body)
    previous: object = None
    with _account_lock(alias):
        with _LEDGER_LOCK:
            ledger = _load_order_ledger()
            rows = ledger.get("orders")
            rows = rows if isinstance(rows, dict) else {}
            previous = rows.get(ledger_key)
            if isinstance(previous, dict):
                if previous.get("request_fingerprint") != request_fingerprint:
                    return {
                        "ok": False,
                        "error": "idempotency_key_payload_mismatch",
                        "alias": alias,
                    }
                if previous.get("ok") is True:
                    return {**previous, "idempotent_replay": True}

        signed_body = dict(body)
        signed_body.setdefault("salt", _idempotent_salt(ledger_key))
        if not any(
            signed_body.get(key)
            for key in ("expiration", "expiration_secs", "expirationTimestamp")
        ):
            previous_expiration = (
                previous.get("order_expiration")
                if isinstance(previous, dict)
                else None
            )
            signed_body["expiration"] = int(
                previous_expiration or (time.time() + 24 * 3600)
            )
        payload = _signed_order_payload(env, alias, signed_body)
        order = payload["order"]
        notional = _notional_usdc(order)
        requested_max_notional = Decimal(
            str(
                body.get("max_notional_usdc")
                or body.get("max_notional")
                or "0"
            )
        )
        server_max_notional = Decimal(
            str(
                account_row.get("max_order_notional_usdc")
                or env.get("PREDICTFUN_MAX_ORDER_NOTIONAL_USDC")
                or "20"
            )
        )
        if server_max_notional <= 0 or not server_max_notional.is_finite():
            return {
                "ok": False,
                "error": "server_order_notional_limit_invalid",
                "alias": alias,
            }
        max_notional = (
            min(requested_max_notional, server_max_notional)
            if requested_max_notional > 0
            else server_max_notional
        )
        if notional > max_notional:
            return {
                "ok": False,
                "error": "max_notional_exceeded",
                "notional_usdc": str(notional),
                "max_notional_usdc": str(max_notional),
                "alias": alias,
            }
        data = {
            "order": payload["signed_order"],
            "pricePerShare": str(payload["amounts"]["pricePerShare"]),
            "strategy": "LIMIT",
        }
        data["isPostOnly"] = True
        data["selfTradePrevention"] = self_trade_prevention
        resolved_expiration = str(
            order.get("expiration")
            or signed_body.get("expiration")
            or signed_body.get("expiration_secs")
            or signed_body.get("expirationTimestamp")
            or ""
        )
        with _LEDGER_LOCK:
            ledger = _load_order_ledger()
            rows = ledger.get("orders")
            rows = rows if isinstance(rows, dict) else {}
            rows[ledger_key] = {
                "ok": False,
                "alias": alias,
                "status": 0,
                "order_hash": str(payload["order_hash"]),
                "notional_usdc": str(notional),
                "maker": mask_address(str(order["maker"])),
                "signer_mode": payload["signer_mode"],
                "error": "submission_pending",
                "updated_at": int(time.time()),
                "request_fingerprint": request_fingerprint,
                "order_expiration": resolved_expiration,
            }
            ledger["orders"] = rows
            _write_order_ledger(ledger)
        status, response = _authenticated_request(
            env,
            alias,
            "/v1/orders",
            method="POST",
            body={"data": data},
        )
        data_resp = (
            response.get("data")
            if isinstance(response.get("data"), dict)
            else {}
        )
        ok = status < 400 and response.get("success") is not False
        order_hash = str(
            data_resp.get("orderHash") or payload["order_hash"]
        )
        if not ok and re.fullmatch(r"0x[0-9a-fA-F]{64}", order_hash):
            lookup_status, lookup = _authenticated_request(
                env, alias, f"/v1/orders/{order_hash}"
            )
            if lookup_status < 400 and lookup.get("success") is not False:
                ok = True
                lookup_data = (
                    lookup.get("data")
                    if isinstance(lookup.get("data"), dict)
                    else {}
                )
                data_resp = {**lookup_data, **data_resp}
                status = lookup_status
        result = {
            "ok": ok,
            "alias": alias,
            "status": status,
            "order_id": data_resp.get("orderId") or data_resp.get("id"),
            "order_hash": order_hash,
            "code": data_resp.get("code"),
            "removal_locked_until": data_resp.get("removalLockedUntil"),
            "notional_usdc": str(notional),
            "maker": mask_address(str(order["maker"])),
            "signer_mode": payload["signer_mode"],
            "error": "" if ok else "order_rejected",
        }
        if not ok:
            result["upstream"] = _safe_upstream_error(response)
        with _LEDGER_LOCK:
            ledger = _load_order_ledger()
            rows = ledger.get("orders")
            rows = rows if isinstance(rows, dict) else {}
            rows[ledger_key] = {
                key: value
                for key, value in result.items()
                if key not in {"upstream", "idempotent_replay"}
            }
            rows[ledger_key]["updated_at"] = int(time.time())
            rows[ledger_key]["request_fingerprint"] = request_fingerprint
            rows[ledger_key]["order_expiration"] = resolved_expiration
            ledger["orders"] = rows
            _write_order_ledger(ledger)
        return result


def remove_order_by_hash(env: dict[str, str], alias: str, body: dict[str, object]) -> dict[str, object]:
    if body.get("remove") is not True or str(body.get("confirm") or "") != "REMOVE_PREDICTFUN_ORDER":
        return {"ok": False, "error": "explicit_remove_confirmation_required", "alias": alias}
    row = _account_row(env, alias)
    if not row:
        return {"ok": False, "error": "account_alias_not_found", "alias": alias}
    hashes = body.get("hashes") or ([body.get("hash")] if body.get("hash") else [])
    hashes = [str(x) for x in hashes if x]
    if not hashes:
        return {"ok": False, "error": "missing_hashes", "alias": alias}
    if any(not re.fullmatch(r"0x[0-9a-fA-F]{64}", value) for value in hashes):
        return {"ok": False, "error": "invalid_order_hash", "alias": alias}
    with _account_lock(alias):
        status, response = _authenticated_request(
            env,
            alias,
            "/v1/orders/remove-by-hash",
            method="POST",
            body={"data": {"hashes": hashes[:100]}},
        )
    ok = status < 400 and response.get("success") is not False
    return {
        "ok": ok,
        "alias": alias,
        "status": status,
        "hash_count": len(hashes),
        "off_book_only": True,
        "error": "" if ok else "remove_rejected",
    }


def _order_status(payload: dict[str, object]) -> str:
    data = payload.get("data")
    if not isinstance(data, dict):
        return ""
    return str(data.get("status") or "").strip().upper()


def _receipt_summary(receipt: object) -> dict[str, object]:
    if not isinstance(receipt, Mapping):
        return {}
    tx_hash = receipt.get("transactionHash")
    if isinstance(tx_hash, (bytes, bytearray)):
        tx_hash = "0x" + bytes(tx_hash).hex()
    elif hasattr(tx_hash, "hex"):
        tx_hash = tx_hash.hex()
        if tx_hash and not str(tx_hash).startswith("0x"):
            tx_hash = "0x" + str(tx_hash)
    return {
        "tx_hash": str(tx_hash or ""),
        "block_number": int(receipt.get("blockNumber") or 0),
        "receipt_status": int(receipt.get("status") or 0),
    }


def _cancel_gas_context(
    env: dict[str, str], alias: str, min_gas_bnb_raw: object
) -> tuple[dict[str, object] | None, dict[str, object]]:
    context = _account_auth_context(env, alias)
    rpc_url = str(
        env.get("PREDICTFUN_BSC_RPC_URL")
        or env.get("BSC_RPC_URL")
        or env.get("BNB_RPC_URL")
        or PREDICT_BSC_RPC_URL
    )
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        return None, {"ok": False, "error": "bsc_rpc_unavailable", "alias": alias}
    gas_balance_wei = int(
        w3.eth.get_balance(
            Web3.to_checksum_address(str(context["eoa_address"]))
        )
    )
    min_gas_bnb = Decimal(_canonical_decimal(min_gas_bnb_raw or "0.0001"))
    if min_gas_bnb <= 0:
        raise ValueError("min_gas_bnb_must_be_positive")
    gas_balance_bnb = Decimal(gas_balance_wei) / Decimal(10**18)
    result = {
        "ok": gas_balance_bnb >= min_gas_bnb,
        "alias": alias,
        "gas_balance_bnb": _format_token_amount(gas_balance_wei, 18),
        "min_gas_bnb": str(min_gas_bnb),
        "error": (
            "" if gas_balance_bnb >= min_gas_bnb else "insufficient_bnb_for_cancel"
        ),
    }
    return (context if result["ok"] else None), result


def _cancel_receipt_verified(summary: dict[str, object]) -> bool:
    return (
        summary.get("success") is True
        and int(summary.get("receipt_status") or 0) == 1
        and bool(
            re.fullmatch(
                r"0x[0-9a-fA-F]{64}", str(summary.get("tx_hash") or "")
            )
        )
    )


def cancel_orders_on_chain(
    env: dict[str, str], alias: str, body: dict[str, object]
) -> dict[str, object]:
    if (
        body.get("cancel") is not True
        or str(body.get("confirm") or "") != "CANCEL_PREDICTFUN_ORDERS"
    ):
        return {
            "ok": False,
            "error": "explicit_cancel_confirmation_required",
            "alias": alias,
        }
    preflight_only = body.get("preflight_only") is True
    hashes_raw = body.get("hashes") or (
        [body.get("hash")] if body.get("hash") else []
    )
    hashes = [str(value) for value in hashes_raw if value]
    if preflight_only and hashes:
        return {
            "ok": False,
            "error": "preflight_must_not_include_hashes",
            "alias": alias,
        }
    if not preflight_only and not hashes:
        return {"ok": False, "error": "missing_hashes", "alias": alias}
    if len(hashes) > 100 or any(
        not re.fullmatch(r"0x[0-9a-fA-F]{64}", value) for value in hashes
    ):
        return {"ok": False, "error": "invalid_order_hash", "alias": alias}

    with _account_lock(alias):
        context, gas_preflight = _cancel_gas_context(
            env, alias, body.get("min_gas_bnb") or "0.0001"
        )
        if not gas_preflight.get("ok") or context is None:
            return gas_preflight
        if preflight_only:
            return {
                **gas_preflight,
                "preflight_only": True,
                "on_chain_action": False,
            }

        try:
            from predict_sdk import (
                CancelOrdersOptions,
                ChainId,
                Order,
                OrderBuilder,
                OrderBuilderOptions,
                Side,
                SignatureType,
            )
        except ImportError:
            return {
                "ok": False,
                "error": "predict_sdk_not_installed",
                "alias": alias,
            }

        expected_maker = str(
            context.get("predict_account") or context.get("eoa_address") or ""
        ).lower()

        groups: dict[tuple[bool, bool], list[object]] = {}
        for order_hash in hashes:
            status, payload = _authenticated_request(
                env, alias, f"/v1/orders/{order_hash}"
            )
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            order_data = (
                data.get("order") if isinstance(data.get("order"), dict) else {}
            )
            if status >= 400 or not order_data:
                return {
                    "ok": False,
                    "error": "order_lookup_failed",
                    "alias": alias,
                    "order_hash": order_hash,
                    "status": status,
                }
            maker = str(order_data.get("maker") or "").lower()
            if not expected_maker or maker != expected_maker:
                return {
                    "ok": False,
                    "error": "order_account_mismatch",
                    "alias": alias,
                    "order_hash": order_hash,
                }
            sdk_order = Order(
                salt=str(order_data.get("salt") or ""),
                maker=str(order_data.get("maker") or ""),
                signer=str(order_data.get("signer") or ""),
                taker=str(order_data.get("taker") or ZERO_ADDRESS),
                token_id=str(order_data.get("tokenId") or ""),
                maker_amount=str(order_data.get("makerAmount") or ""),
                taker_amount=str(order_data.get("takerAmount") or ""),
                expiration=str(order_data.get("expiration") or ""),
                nonce=str(order_data.get("nonce") or "0"),
                fee_rate_bps=str(order_data.get("feeRateBps") or "0"),
                side=Side(int(order_data.get("side") or 0)),
                signature_type=SignatureType(
                    int(order_data.get("signatureType") or 0)
                ),
            )
            group = (
                bool(data.get("isNegRisk")),
                bool(data.get("isYieldBearing")),
            )
            groups.setdefault(group, []).append(sdk_order)

        remove_status, remove_payload = _authenticated_request(
            env,
            alias,
            "/v1/orders/remove-by-hash",
            method="POST",
            body={"data": {"hashes": hashes}},
        )
        off_book_removed = (
            remove_status < 400 and remove_payload.get("success") is not False
        )
        if not off_book_removed:
            return {
                "ok": False,
                "error": "off_book_remove_failed",
                "alias": alias,
                "status": remove_status,
            }

        builder = OrderBuilder.make(
            ChainId.BNB_MAINNET,
            str(context["private_key"]),
            OrderBuilderOptions(
                predict_account=str(context.get("predict_account") or "") or None
            ),
        )
        transactions: list[dict[str, object]] = []
        for (is_neg_risk, is_yield_bearing), orders in groups.items():
            result = builder.cancel_orders(
                orders,
                CancelOrdersOptions(
                    is_neg_risk=is_neg_risk,
                    is_yield_bearing=is_yield_bearing,
                ),
            )
            summary = {
                "is_neg_risk": is_neg_risk,
                "is_yield_bearing": is_yield_bearing,
                "order_count": len(orders),
                "success": result.success is True,
                **_receipt_summary(result.receipt),
            }
            transactions.append(summary)
            if not _cancel_receipt_verified(summary):
                return {
                    "ok": False,
                    "error": "on_chain_cancel_receipt_unverified",
                    "alias": alias,
                    "off_book_removed": True,
                    "transactions": transactions,
                }

        timeout_sec = min(20.0, max(0.0, float(body.get("verify_timeout_sec") or 8)))
        deadline = time.monotonic() + timeout_sec
        remaining = list(hashes)
        while remaining:
            open_hashes: list[str] = []
            for order_hash in remaining:
                status, payload = _authenticated_request(
                    env, alias, f"/v1/orders/{order_hash}"
                )
                if status < 400 and _order_status(payload) in {
                    "OPEN",
                    "PENDING",
                    "MATCHING",
                }:
                    open_hashes.append(order_hash)
            remaining = open_hashes
            if not remaining or time.monotonic() >= deadline:
                break
            time.sleep(1)

        ok = not remaining
        result_payload = {
            "ok": ok,
            "alias": alias,
            "hash_count": len(hashes),
            "off_book_removed": True,
            "on_chain_cancelled": True,
            "verified": ok,
            "open_hashes": remaining,
            "transactions": transactions,
            "error": "" if ok else "cancel_verification_timeout",
        }
        if ok:
            with _LEDGER_LOCK:
                ledger = _load_order_ledger()
                rows = ledger.get("orders")
                rows = rows if isinstance(rows, dict) else {}
                hash_set = {value.lower() for value in hashes}
                for value in rows.values():
                    if (
                        isinstance(value, dict)
                        and str(value.get("order_hash") or "").lower() in hash_set
                    ):
                        value["status"] = "cancelled"
                        value["updated_at"] = int(time.time())
                ledger["orders"] = rows
                _write_order_ledger(ledger)
        return result_payload


class Handler(BaseHTTPRequestHandler):
    server_version = "PredictFunAccountProxy/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        logging.info(
            "client=%s method=%s path=%s status_log=%s",
            self.client_address[0],
            self.command,
            urlsplit(self.path).path,
            fmt % args,
        )

    def _write_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _client_allowed(self) -> bool:
        return self.client_address[0] in ALLOWED_CLIENTS

    def do_GET(self) -> None:
        client_ip = self.client_address[0]
        parsed = urlsplit(self.path)
        if not self._client_allowed():
            self._write_json(403, {"ok": False, "error": "client_not_allowed"})
            return
        if parsed.path == "/health":
            env = load_env(SECRET_FILE)
            accounts = account_summary(env)
            aliases = accounts.get("aliases")
            aliases = aliases if isinstance(aliases, list) else []
            self._write_json(
                200,
                {
                    "ok": int(accounts.get("ready") or 0) > 0,
                    "project": "predictfun",
                    "mode": "account_read_submit_full_cancel",
                    "release_sha": str(
                        os.environ.get("PREDICTFUN_RELEASE_SHA") or ""
                    ),
                    "base_url": env.get("PREDICTFUN_BASE_URL") or "https://api.predict.fun",
                    "key_present": any(
                        isinstance(row, dict) and row.get("api_key_present") is True
                        for row in aliases
                    ),
                    "accounts": accounts,
                    "client_ip": client_ip,
                    "allowed_paths": [
                        "/v1/markets",
                        "/v1/markets/{id}",
                        "/v1/markets/{id}/orderbook",
                        "/predictfun/accounts/{alias}/auth-check",
                        "/predictfun/accounts/{alias}/preview-order",
                        "/predictfun/accounts/{alias}/submit-order",
                        "/predictfun/accounts/{alias}/remove-order-by-hash",
                        "/predictfun/accounts/{alias}/cancel-orders",
                        "/predictfun/accounts/{alias}/allowances",
                        "/predictfun/accounts/{alias}/capabilities",
                        "/predictfun/accounts/{alias}/state",
                        "/predictfun/accounts/{alias}/account",
                        "/predictfun/accounts/{alias}/orders",
                        "/predictfun/accounts/{alias}/orders/{hash}",
                        "/predictfun/accounts/{alias}/positions",
                        "/predictfun/accounts/{alias}/activity",
                    ],
                },
            )
            return
        env = load_env(SECRET_FILE)
        capabilities_match = CAPABILITIES_RE.match(parsed.path)
        state_match = ACCOUNT_STATE_RE.match(parsed.path)
        account_match = ACCOUNT_RE.match(parsed.path)
        orders_match = ACCOUNT_ORDERS_RE.match(parsed.path)
        order_match = ACCOUNT_ORDER_RE.match(parsed.path)
        positions_match = ACCOUNT_POSITIONS_RE.match(parsed.path)
        activity_match = ACCOUNT_ACTIVITY_RE.match(parsed.path)
        allowances_match = ALLOWANCES_RE.match(parsed.path)
        account_route = (
            capabilities_match
            or state_match
            or account_match
            or orders_match
            or order_match
            or positions_match
            or activity_match
            or allowances_match
        )
        if account_route:
            alias = account_route.group(1)
            try:
                if capabilities_match:
                    result = account_capabilities(env, alias)
                elif state_match:
                    result = account_state(env, alias)
                elif allowances_match:
                    result = collateral_allowances(env, alias)
                elif account_match:
                    result = _authenticated_get(env, alias, "/v1/account")
                elif order_match:
                    order_hash = order_match.group(2)
                    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", order_hash):
                        raise ValueError("invalid_order_hash")
                    result = _authenticated_get(
                        env, alias, f"/v1/orders/{order_hash}"
                    )
                else:
                    query_values = parse_qs(parsed.query, keep_blank_values=False)
                    if orders_match:
                        allowed = {"first", "after", "status"}
                        upstream_path = "/v1/orders"
                    elif positions_match:
                        allowed = {
                            "first",
                            "after",
                            "marketId",
                            "isResolved",
                            "sort",
                        }
                        upstream_path = "/v1/positions"
                    else:
                        allowed = {"first", "after", "marketId", "type"}
                        upstream_path = "/v1/account/activity"
                    unknown = set(query_values) - allowed
                    if unknown:
                        raise ValueError("unsupported_query_parameter")
                    query = {
                        key: values[-1]
                        for key, values in query_values.items()
                        if values
                    }
                    result = _authenticated_get(
                        env, alias, upstream_path, query=query
                    )
            except ImportError:
                result = {
                    "ok": False,
                    "error": "missing_account_dependency",
                    "alias": alias,
                }
            except ValueError as exc:
                result = {"ok": False, "error": str(exc), "alias": alias}
            except Exception as exc:
                logging.exception(
                    "predictfun_get_failed alias=%s path=%s", alias, parsed.path
                )
                result = {
                    "ok": False,
                    "error": "predictfun_get_failed",
                    "detail": type(exc).__name__,
                    "alias": alias,
                }
            self._write_json(200 if result.get("ok") else 502, result)
            return
        if not is_allowed_path(parsed.path):
            self._write_json(403, {"ok": False, "error": "path_not_allowed"})
            return
        api_key = env.get("PREDICTFUN_API_KEY") or ""
        if not api_key:
            self._write_json(503, {"ok": False, "error": "missing_api_key"})
            return
        base_url = (env.get("PREDICTFUN_BASE_URL") or "https://api.predict.fun").rstrip("/")
        target = base_url + parsed.path
        if parsed.query:
            target += "?" + parsed.query
        req = Request(
            target,
            headers={"accept": "application/json", "user-agent": USER_AGENT, "x-api-key": api_key},
        )
        try:
            with urlopen(req, timeout=20) as resp:
                body = resp.read()
                status = resp.status
                content_type = resp.headers.get("content-type") or "application/json; charset=utf-8"
        except HTTPError as exc:
            body = exc.read()
            status = exc.code
            content_type = exc.headers.get("content-type") or "application/json; charset=utf-8"
        except URLError as exc:
            logging.warning("upstream_error type=%s", type(exc.reason).__name__)
            self._write_json(502, {"ok": False, "error": "upstream_error", "detail": type(exc.reason).__name__})
            return
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        if not self._client_allowed():
            self._write_json(403, {"ok": False, "error": "client_not_allowed"})
            return
        auth_match = AUTH_CHECK_RE.match(parsed.path)
        preview_match = ORDER_PREVIEW_RE.match(parsed.path)
        submit_match = ORDER_SUBMIT_RE.match(parsed.path)
        remove_hash_match = ORDER_REMOVE_HASH_RE.match(parsed.path)
        allowances_match = ALLOWANCES_RE.match(parsed.path)
        cancel_match = ORDER_CANCEL_RE.match(parsed.path)
        if not auth_match and not preview_match and not submit_match and not remove_hash_match and not allowances_match and not cancel_match:
            self._write_json(403, {"ok": False, "error": "path_not_allowed"})
            return
        alias = (auth_match or preview_match or submit_match or remove_hash_match or allowances_match or cancel_match).group(1)
        env = load_env(SECRET_FILE)
        try:
            if auth_match:
                result = run_auth_check(env, alias)
            elif allowances_match:
                result = collateral_allowances(env, alias)
            else:
                length = int(self.headers.get("content-length") or "0")
                raw = self.rfile.read(min(length, 65536)) if length else b"{}"
                body = json.loads(raw.decode("utf-8")) if raw else {}
                if not isinstance(body, dict):
                    raise ValueError("request_body_must_be_object")
                if preview_match:
                    result = preview_order(env, alias, body)
                elif submit_match:
                    result = submit_order(env, alias, body)
                elif remove_hash_match:
                    result = remove_order_by_hash(env, alias, body)
                else:
                    result = cancel_orders_on_chain(env, alias, body)
        except ImportError:
            result = {"ok": False, "error": "missing_eth_account_dependency", "alias": alias}
        except ValueError as exc:
            result = {"ok": False, "error": str(exc), "alias": alias}
        except Exception as exc:
            logging.exception("predictfun_post_failed alias=%s path=%s", alias, parsed.path)
            result = {"ok": False, "error": "predictfun_post_failed", "detail": type(exc).__name__, "alias": alias}
        self._write_json(200 if result.get("ok") else 500, result)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict.fun read proxy with Mac-mini-only auth check.")
    parser.add_argument("--host", default="100.91.159.54")
    parser.add_argument("--port", type=int, default=8791)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = PredictFunHTTPServer((args.host, args.port), Handler)
    logging.info("PredictFun proxy listening on %s:%s", args.host, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
