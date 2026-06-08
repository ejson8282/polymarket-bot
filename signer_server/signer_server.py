"""
Signer Server — runs on Mac Mini, holds the private keys.
Exposes /derive-creds, /sign-order, /health via FastAPI.
Only accepts requests from Tailscale IP whitelist + Bearer token.

Multi-key mode
--------------
Set SIGNER_KEYS_JSON to a JSON object mapping checksummed funder address
→ private key, for example:

    SIGNER_KEYS_JSON='{"0xFUNDER1":"0xKEY1","0xFUNDER2":"0xKEY2"}'

Requests must then include a `funder` field so we can route to the
correct key. A `{funder: ClobClient}` cache is lazily populated on the
first request for each funder.

Single-key backward compatibility
---------------------------------
If SIGNER_KEYS_JSON is unset, we fall back to POLY_PRIVATE_KEY +
POLY_FUNDER like before. Incoming requests without a `funder` field
keep working unchanged.
"""

import json
import os
import time
import threading
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import uvicorn

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
CHAIN_ID = int(os.environ.get("POLY_CHAIN_ID", "137"))
SIGNATURE_TYPE = int(os.environ.get("POLY_SIGNATURE_TYPE", "2"))
HOST = os.environ.get("POLY_CLOB_HOST", "https://clob.polymarket.com")
BEARER_TOKEN = os.environ.get("SIGNER_TOKEN", "").strip()
ALLOWED_IPS = [ip.strip() for ip in os.environ.get("SIGNER_ALLOWED_IPS", "").split(",") if ip.strip()]

# Rate-limit / circuit-breaker
MAX_AMOUNT_PER_ORDER = float(os.environ.get("SIGNER_MAX_AMOUNT", "2000"))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get("SIGNER_MAX_RPM", "100"))


def _load_key_map() -> dict[str, str]:
    """Build the funder→private_key map.

    Priority:
        1. SIGNER_KEYS_JSON (multi-key)
        2. POLY_PRIVATE_KEY + POLY_FUNDER (single-key backward compat)
    Funder addresses are lowercased for lookup consistency.
    """
    raw = os.environ.get("SIGNER_KEYS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"SIGNER_KEYS_JSON is not valid JSON: {e}")
        if not isinstance(parsed, dict) or not parsed:
            raise RuntimeError("SIGNER_KEYS_JSON must be a non-empty JSON object")
        out: dict[str, str] = {}
        for funder, key in parsed.items():
            f = str(funder).strip().lower()
            k = str(key).strip()
            if not f.startswith("0x") or not k.startswith("0x"):
                raise RuntimeError(f"SIGNER_KEYS_JSON entry malformed: funder={funder!r}")
            out[f] = k
        return out

    legacy_key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    legacy_funder = os.environ.get("POLY_FUNDER", "").strip().lower()
    if legacy_key:
        if not legacy_funder:
            # single-key mode without explicit funder — use a sentinel so
            # requests omitting `funder` still resolve
            return {"": legacy_key}
        return {legacy_funder: legacy_key}
    return {}


KEY_MAP: dict[str, str] = _load_key_map()

# ---------------------------------------------------------------------------
# Logging — audit log (sanitized: time | funder | amount | asset | result only)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("signer_server")

# ---------------------------------------------------------------------------
# Rate limiter state (global across all funders)
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_request_timestamps: list[float] = []
_locked = False
_lock_reason = ""


def _check_rate_limit():
    """Reject if manually locked or over rpm. Never auto-locks."""
    if _locked:
        raise HTTPException(status_code=423, detail=f"Service locked: {_lock_reason}. Manual unlock required.")
    now = time.time()
    with _lock:
        cutoff = now - 60
        _request_timestamps[:] = [t for t in _request_timestamps if t > cutoff]
        if len(_request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
            raise HTTPException(status_code=429, detail=f"Rate limit exceeded ({MAX_REQUESTS_PER_MINUTE}/min)")
        _request_timestamps.append(now)


def _check_amount(amount: float):
    """Reject single order over max amount. Never auto-locks."""
    if amount > MAX_AMOUNT_PER_ORDER:
        raise HTTPException(status_code=403, detail=f"Order amount ${amount} exceeds limit ${MAX_AMOUNT_PER_ORDER}")


# ---------------------------------------------------------------------------
# Auth + IP whitelist
# ---------------------------------------------------------------------------
security = HTTPBearer()


async def verify_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    if not BEARER_TOKEN:
        raise HTTPException(status_code=500, detail="SIGNER_TOKEN not configured")
    if credentials.credentials != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    if ALLOWED_IPS:
        client_ip = request.client.host if request.client else ""
        if client_ip not in ALLOWED_IPS:
            logger.warning(f"Rejected request from IP {client_ip}")
            raise HTTPException(status_code=403, detail="IP not allowed")


# ---------------------------------------------------------------------------
# Per-funder ClobClient cache (lazy, thread-safe)
# ---------------------------------------------------------------------------
_client_cache: dict[str, ClobClient] = {}
_client_lock = threading.Lock()


def _resolve_funder(requested: str | None) -> str:
    """Map a requested funder to a key-map entry.

    - Empty request + single-key legacy mode → use the sole entry
    - Otherwise the funder must be present in KEY_MAP (case-insensitive)
    """
    if not KEY_MAP:
        raise HTTPException(status_code=500, detail="Signer has no keys configured")
    if requested is None or requested == "":
        if len(KEY_MAP) == 1:
            return next(iter(KEY_MAP.keys()))
        raise HTTPException(status_code=400, detail="funder required (multi-key mode)")
    key = requested.strip().lower()
    if key in KEY_MAP:
        return key
    # If server is in legacy sentinel mode ("" → key) and caller sent a
    # funder, just use the only key — the caller is the authority.
    if "" in KEY_MAP and len(KEY_MAP) == 1:
        return ""
    raise HTTPException(status_code=404, detail=f"funder {requested} not configured")


def _get_client(funder_key: str) -> ClobClient:
    """Return (and lazily create) a ClobClient for the given funder_key.

    funder_key is the lookup key into KEY_MAP (lowercased address, or ""
    for legacy single-key-without-funder mode).
    """
    cached = _client_cache.get(funder_key)
    if cached is not None:
        return cached
    with _client_lock:
        cached = _client_cache.get(funder_key)
        if cached is not None:
            return cached
        priv = KEY_MAP[funder_key]
        kwargs: dict = {
            "host": HOST,
            "chain_id": CHAIN_ID,
            "key": priv,
            "signature_type": SIGNATURE_TYPE,
        }
        if funder_key:
            kwargs["funder"] = funder_key
        client = ClobClient(**kwargs)
        _client_cache[funder_key] = client
        return client


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SignOrderRequest(BaseModel):
    token_id: str
    price: float
    size: float
    side: str = Field(..., pattern="^(BUY|SELL)$")
    funder: str | None = None  # required in multi-key mode


class SignOrderResponse(BaseModel):
    signed_order: dict


class DeriveCredsRequest(BaseModel):
    funder: str | None = None


class DeriveCredsResponse(BaseModel):
    api_key: str
    api_secret: str
    api_passphrase: str
    address: str


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not KEY_MAP:
        logger.error("No signer keys loaded — set SIGNER_KEYS_JSON or POLY_PRIVATE_KEY")
    if not BEARER_TOKEN:
        logger.error("SIGNER_TOKEN not set — all requests will be rejected")
    funder_preview = [
        (f[:10] + "…") if f else "<legacy-no-funder>"
        for f in KEY_MAP.keys()
    ]
    logger.info(f"Signer server starting. Funders loaded: {len(KEY_MAP)} ({funder_preview})")
    logger.info(f"Allowed IPs: {ALLOWED_IPS or 'any'}")
    logger.info(f"Limits: max_amount=${MAX_AMOUNT_PER_ORDER}, max_rpm={MAX_REQUESTS_PER_MINUTE}")
    yield


app = FastAPI(title="Polymarket Signer Server", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "locked": _locked,
        "funders_configured": len(KEY_MAP),
        "clients_cached": len(_client_cache),
        "time": datetime.utcnow().isoformat(),
    }


@app.post("/unlock")
async def unlock(request: Request, _=Depends(verify_auth)):
    global _locked, _lock_reason
    _locked = False
    _lock_reason = ""
    logger.info("Service unlocked manually")
    return {"status": "unlocked"}


@app.post("/derive-creds", response_model=DeriveCredsResponse)
async def derive_creds(request: Request, body: DeriveCredsRequest | None = None, _=Depends(verify_auth)):
    _check_rate_limit()
    requested_funder = body.funder if body else None
    funder_key = _resolve_funder(requested_funder)
    client = _get_client(funder_key)
    try:
        creds = await run_in_threadpool(client.create_or_derive_api_creds)
        address = await run_in_threadpool(client.get_address)
        logger.info(f"derive-creds | funder={funder_key[:10] or 'legacy'}… | address={address[:8]}… | result=ok")
        return DeriveCredsResponse(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
            address=address,
        )
    except Exception as e:
        logger.error(f"derive-creds | funder={funder_key[:10] or 'legacy'}… | result=error | {type(e).__name__}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sign-order", response_model=SignOrderResponse)
async def sign_order(req: SignOrderRequest, request: Request, _=Depends(verify_auth)):
    _check_rate_limit()
    notional = req.price * req.size
    _check_amount(notional)

    funder_key = _resolve_funder(req.funder)
    client = _get_client(funder_key)
    side = BUY if req.side == "BUY" else SELL
    args = OrderArgs(
        token_id=req.token_id,
        price=req.price,
        size=req.size,
        side=side,
    )
    try:
        signed = await run_in_threadpool(client.create_order, args)
        signed_dict = signed.dict() if hasattr(signed, "dict") else signed
        logger.info(
            f"sign-order | funder={funder_key[:10] or 'legacy'}… "
            f"| asset={req.token_id[:16]}… | side={req.side} | size={req.size} "
            f"| price={req.price} | notional=${notional:.2f} | result=ok"
        )
        return SignOrderResponse(signed_order=signed_dict)
    except Exception as e:
        logger.error(
            f"sign-order | funder={funder_key[:10] or 'legacy'}… "
            f"| asset={req.token_id[:16]}… | result=error | {type(e).__name__}"
        )
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("SIGNER_PORT", "8420"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
