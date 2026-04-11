"""
Signer Server — runs on Mac Mini, holds the private key.
Exposes /derive-creds, /sign-order, /health via FastAPI.
Only accepts requests from Tailscale IP whitelist + Bearer token.
"""

import os
import time
import threading
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import uvicorn

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
CHAIN_ID = int(os.environ.get("POLY_CHAIN_ID", "137"))
SIGNATURE_TYPE = int(os.environ.get("POLY_SIGNATURE_TYPE", "2"))
FUNDER = os.environ.get("POLY_FUNDER", "").strip()
HOST = os.environ.get("POLY_CLOB_HOST", "https://clob.polymarket.com")
BEARER_TOKEN = os.environ.get("SIGNER_TOKEN", "").strip()
ALLOWED_IPS = [ip.strip() for ip in os.environ.get("SIGNER_ALLOWED_IPS", "").split(",") if ip.strip()]

# Rate-limit / circuit-breaker
MAX_AMOUNT_PER_ORDER = float(os.environ.get("SIGNER_MAX_AMOUNT", "500"))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get("SIGNER_MAX_RPM", "100"))

# ---------------------------------------------------------------------------
# Logging — audit log (sanitized: time | amount | asset | result only)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("signer_server")

# ---------------------------------------------------------------------------
# Rate limiter state
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_request_timestamps: list[float] = []
_locked = False
_lock_reason = ""


def _check_rate_limit():
    """Raise if rate-limited or locked."""
    global _locked
    if _locked:
        raise HTTPException(status_code=423, detail=f"Service locked: {_lock_reason}. Manual unlock required.")
    now = time.time()
    with _lock:
        cutoff = now - 60
        _request_timestamps[:] = [t for t in _request_timestamps if t > cutoff]
        if len(_request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
            _locked = True
            raise HTTPException(status_code=429, detail=f"Rate limit exceeded ({MAX_REQUESTS_PER_MINUTE}/min). Service locked.")
        _request_timestamps.append(now)


def _check_amount(amount: float):
    """Lock service if single order exceeds max amount."""
    global _locked, _lock_reason
    if amount > MAX_AMOUNT_PER_ORDER:
        _locked = True
        _lock_reason = f"Order amount ${amount} exceeds limit ${MAX_AMOUNT_PER_ORDER}"
        raise HTTPException(status_code=403, detail=_lock_reason)

# ---------------------------------------------------------------------------
# Auth + IP whitelist
# ---------------------------------------------------------------------------
security = HTTPBearer()


async def verify_auth(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    # Bearer token check
    if not BEARER_TOKEN:
        raise HTTPException(status_code=500, detail="SIGNER_TOKEN not configured")
    if credentials.credentials != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    # IP whitelist check
    if ALLOWED_IPS:
        client_ip = request.client.host if request.client else ""
        if client_ip not in ALLOWED_IPS:
            logger.warning(f"Rejected request from IP {client_ip}")
            raise HTTPException(status_code=403, detail="IP not allowed")

# ---------------------------------------------------------------------------
# ClobClient singleton
# ---------------------------------------------------------------------------
_clob_client: ClobClient | None = None


def _get_client() -> ClobClient:
    global _clob_client
    if _clob_client is None:
        if not PRIVATE_KEY:
            raise RuntimeError("POLY_PRIVATE_KEY not set")
        kwargs = {
            "host": HOST,
            "chain_id": CHAIN_ID,
            "key": PRIVATE_KEY,
            "signature_type": SIGNATURE_TYPE,
        }
        if FUNDER:
            kwargs["funder"] = FUNDER
        _clob_client = ClobClient(**kwargs)
    return _clob_client

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SignOrderRequest(BaseModel):
    token_id: str
    price: float
    size: float
    side: str = Field(..., pattern="^(BUY|SELL)$")


class SignOrderResponse(BaseModel):
    signed_order: dict


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
    if not PRIVATE_KEY:
        logger.error("POLY_PRIVATE_KEY not set — server will reject sign requests")
    if not BEARER_TOKEN:
        logger.error("SIGNER_TOKEN not set — all requests will be rejected")
    logger.info(f"Signer server starting. Allowed IPs: {ALLOWED_IPS or 'any'}")
    logger.info(f"Limits: max_amount=${MAX_AMOUNT_PER_ORDER}, max_rpm={MAX_REQUESTS_PER_MINUTE}")
    yield

app = FastAPI(title="Polymarket Signer Server", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "locked": _locked, "time": datetime.utcnow().isoformat()}


@app.post("/unlock")
async def unlock(request: Request, _=Depends(verify_auth)):
    global _locked, _lock_reason
    _locked = False
    _lock_reason = ""
    logger.info("Service unlocked manually")
    return {"status": "unlocked"}


@app.post("/derive-creds", response_model=DeriveCredsResponse)
async def derive_creds(request: Request, _=Depends(verify_auth)):
    _check_rate_limit()
    client = _get_client()
    try:
        creds = client.create_or_derive_api_creds()
        address = client.get_address()
        logger.info(f"derive-creds | address={address[:8]}... | result=ok")
        return DeriveCredsResponse(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            api_passphrase=creds.api_passphrase,
            address=address,
        )
    except Exception as e:
        logger.error(f"derive-creds | result=error | {type(e).__name__}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sign-order", response_model=SignOrderResponse)
async def sign_order(req: SignOrderRequest, request: Request, _=Depends(verify_auth)):
    _check_rate_limit()
    notional = req.price * req.size
    _check_amount(notional)

    client = _get_client()
    side = BUY if req.side == "BUY" else SELL
    args = OrderArgs(
        token_id=req.token_id,
        price=req.price,
        size=req.size,
        side=side,
    )
    try:
        signed = client.create_order(args)
        signed_dict = signed.dict() if hasattr(signed, "dict") else signed
        logger.info(f"sign-order | asset={req.token_id[:16]}... | side={req.side} | size={req.size} | price={req.price} | notional=${notional:.2f} | result=ok")
        return SignOrderResponse(signed_order=signed_dict)
    except Exception as e:
        logger.error(f"sign-order | asset={req.token_id[:16]}... | result=error | {type(e).__name__}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("SIGNER_PORT", "8420"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
