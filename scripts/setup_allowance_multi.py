"""
Batch USDC + CTF allowance setup for all accounts.

For every account in a roster, runs the equivalent of `approve_usdc.py` +
`setup_allowance.py`:

  1. Polygon on-chain: ERC-20 `approve(MAX)` USDC → 3 Polymarket spenders
     (Exchange, CTF Exchange, Neg-Risk Exchange). Needs ~0.1 MATIC per
     account for gas. Skips spenders already approved.
  2. CLOB API:        update_balance_allowance for COLLATERAL + CONDITIONAL.
     Signed locally — does NOT go through the remote signer.

Input
-----
`allowance_roster.json` (default) — one entry per account:

    [
      { "funder": "0xFUNDER", "private_key": "0xKEY" },
      ...
    ]

Private keys live only on the machine running this script (Mac Mini or
wherever you hold the cold wallet material). Safe to run once at bring-up
and again whenever a new account is added — approved spenders are skipped.

Usage
-----
    python scripts/setup_allowance_multi.py
    python scripts/setup_allowance_multi.py --roster /path/to/roster.json
    python scripts/setup_allowance_multi.py --only 3        # just account #3
    python scripts/setup_allowance_multi.py --skip-onchain  # CLOB allowance only
    python scripts/setup_allowance_multi.py --skip-clob     # on-chain only
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROSTER = REPO_ROOT / "scripts" / "allowance_roster.json"

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
SPENDERS = [
    ("Exchange",         "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
    ("CTF Exchange",     "0xC5d563A36AE78145C45a50134d48A1215220f80a"),
    ("Neg-Risk Exchange","0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
]
MAX_UINT = 2 ** 256 - 1
APPROVED_THRESHOLD = MAX_UINT // 2

POLYGON_RPCS = [
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://rpc.ankr.com/polygon",
    "https://1rpc.io/matic",
]

ERC20_ABI = [
    {"name": "approve", "type": "function",
     "inputs":  [{"name": "spender", "type": "address"},
                 {"name": "amount",  "type": "uint256"}],
     "outputs": [{"type": "bool"}],
     "stateMutability": "nonpayable"},
    {"name": "allowance", "type": "function",
     "inputs":  [{"name": "owner",   "type": "address"},
                 {"name": "spender", "type": "address"}],
     "outputs": [{"type": "uint256"}],
     "stateMutability": "view"},
]


def _load_roster(path: Path) -> list[dict]:
    if not path.exists():
        sys.exit(f"ERROR: roster file not found: {path}")
    try:
        roster = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.exit(f"ERROR: {path} invalid JSON — {e}")
    if not isinstance(roster, list) or not roster:
        sys.exit("ERROR: roster must be a non-empty array")
    seen: set[str] = set()
    for i, entry in enumerate(roster, start=1):
        if not isinstance(entry, dict):
            sys.exit(f"ERROR: entry {i} must be an object")
        funder = str(entry.get("funder", "")).strip()
        pk = str(entry.get("private_key", "")).strip()
        if not funder.startswith("0x") or len(funder) != 42:
            sys.exit(f"ERROR: entry {i}: bad funder {funder!r}")
        if not pk.startswith("0x") or len(pk) < 60:
            sys.exit(f"ERROR: entry {i}: bad private_key")
        key = funder.lower()
        if key in seen:
            sys.exit(f"ERROR: duplicate funder at entry {i}: {funder}")
        seen.add(key)
    return roster


def _connect_rpc():
    from web3 import Web3
    for url in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
            if w3.is_connected():
                print(f"[rpc] connected via {url}")
                return w3
        except Exception:
            pass
    sys.exit("ERROR: no Polygon RPC reachable")


def _run_onchain(w3, idx: int, entry: dict) -> bool:
    """Approve USDC for the 3 Polymarket spenders. Returns True on success."""
    from web3 import Web3

    account = w3.eth.account.from_key(entry["private_key"])
    addr = account.address
    matic_wei = w3.eth.get_balance(addr)
    matic = w3.from_wei(matic_wei, "ether")
    print(f"  wallet: {addr}  MATIC balance: {matic:.4f}")
    if matic_wei < w3.to_wei(0.02, "ether"):
        print(f"  SKIP on-chain: MATIC balance < 0.02 (need gas). Top up then re-run.")
        return False

    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC), abi=ERC20_ABI)
    nonce = w3.eth.get_transaction_count(addr)
    ok = True

    for name, spender_raw in SPENDERS:
        spender = Web3.to_checksum_address(spender_raw)
        current = usdc.functions.allowance(addr, spender).call()
        if current >= APPROVED_THRESHOLD:
            print(f"    {name}: already approved")
            continue
        print(f"    {name}: approving…")
        try:
            tx = usdc.functions.approve(spender, MAX_UINT).build_transaction({
                "from": addr,
                "nonce": nonce,
                "gas": 60000,
                "gasPrice": w3.to_wei("50", "gwei"),
                "chainId": 137,
            })
            signed = account.sign_transaction(tx)
            txhash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"    tx: {txhash.hex()}")
            receipt = w3.eth.wait_for_transaction_receipt(txhash, timeout=90)
            print(f"    confirmed block={receipt['blockNumber']} status={receipt['status']}")
            if receipt["status"] != 1:
                ok = False
            nonce += 1
        except Exception as e:
            print(f"    ERROR approving {name}: {type(e).__name__}: {e}")
            ok = False
    return ok


def _run_clob(idx: int, entry: dict) -> bool:
    """Update CLOB allowance (collateral + conditional). Returns True on success."""
    # Walk up to repo root so py_clob_client is importable when invoked from
    # either the repo root or scripts/.
    sys.path.insert(0, str(REPO_ROOT))

    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=entry["private_key"],
        signature_type=0,  # EOA — allowance update is signed locally, not via funder
    )
    try:
        creds = client.create_or_derive_api_key()
        client.set_api_creds(creds)
    except Exception as e:
        print(f"  ERROR derive creds: {type(e).__name__}: {e}")
        return False

    ok = True
    for asset_type in (AssetType.COLLATERAL, AssetType.CONDITIONAL):
        try:
            before = client.get_balance_allowance(BalanceAllowanceParams(asset_type=asset_type))
            print(f"    {asset_type}: before balance={before.get('balance')} allowance={before.get('allowance')}")
        except Exception as e:
            print(f"    {asset_type}: read error {type(e).__name__}: {e}")
        try:
            result = client.update_balance_allowance(BalanceAllowanceParams(asset_type=asset_type))
            print(f"    {asset_type}: updated → {result}")
        except Exception as e:
            print(f"    {asset_type}: update error {type(e).__name__}: {e}")
            ok = False
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch USDC + CTF allowance setup for many accounts")
    ap.add_argument("--roster", default=str(DEFAULT_ROSTER), help=f"Roster JSON (default: {DEFAULT_ROSTER})")
    ap.add_argument("--only", type=int, default=None, help="Run only the Nth account (1-indexed)")
    ap.add_argument("--skip-onchain", action="store_true", help="Skip USDC on-chain approvals")
    ap.add_argument("--skip-clob", action="store_true", help="Skip CLOB allowance updates")
    ap.add_argument("--delay", type=float, default=1.5, help="Seconds between accounts (default 1.5)")
    args = ap.parse_args()

    roster = _load_roster(Path(args.roster).resolve())
    print(f"Loaded {len(roster)} account(s) from roster")
    if args.only is not None:
        if not (1 <= args.only <= len(roster)):
            sys.exit(f"--only {args.only} out of range 1..{len(roster)}")
        roster = [roster[args.only - 1]]
        print(f"Running only account #{args.only}")

    w3 = None
    if not args.skip_onchain:
        w3 = _connect_rpc()

    results: list[tuple[str, bool, bool]] = []  # (funder, onchain_ok, clob_ok)
    for i, entry in enumerate(roster, start=1):
        funder = entry["funder"]
        print(f"\n— account #{i} funder={funder} —")
        onchain_ok = True
        clob_ok = True
        if not args.skip_onchain:
            onchain_ok = _run_onchain(w3, i, entry)
        else:
            print("  (skipped on-chain)")
        if not args.skip_clob:
            clob_ok = _run_clob(i, entry)
        else:
            print("  (skipped CLOB)")
        results.append((funder, onchain_ok, clob_ok))
        if i < len(roster):
            time.sleep(args.delay)

    print("\n=== summary ===")
    fail = 0
    for funder, on_ok, clob_ok in results:
        on_s = "skip" if args.skip_onchain else ("ok" if on_ok else "FAIL")
        cl_s = "skip" if args.skip_clob    else ("ok" if clob_ok else "FAIL")
        print(f"  {funder}  onchain={on_s}  clob={cl_s}")
        if on_s == "FAIL" or cl_s == "FAIL":
            fail += 1

    if fail:
        print(f"\n{fail} account(s) had failures — re-run to retry (successful spenders will be skipped).")
        sys.exit(1)
    print("\nAll accounts ready. Restart the engine / multi_runner.")


if __name__ == "__main__":
    main()
