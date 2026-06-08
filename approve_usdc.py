"""
在 Polygon 链上批准 Polymarket 合约使用你的 USDC。
需要钱包里有少量 MATIC 支付 gas。
"""
import os, sys
from web3 import Web3

PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
if not PRIVATE_KEY or "REDACTED" in PRIVATE_KEY:
    print("ERROR: set POLY_PRIVATE_KEY first")
    sys.exit(1)

# Polygon RPC
RPC_URLS = [
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://rpc.ankr.com/polygon",
    "https://1rpc.io/matic",
]
w3 = None
for rpc in RPC_URLS:
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        if _w3.is_connected():
            w3 = _w3
            print(f"Connected via {rpc}")
            break
    except Exception:
        pass
if w3 is None:
    print("ERROR: cannot connect to any Polygon RPC. Check network/proxy.")
    sys.exit(1)

account = w3.eth.account.from_key(PRIVATE_KEY)
print(f"Wallet: {account.address}")
print(f"MATIC balance: {w3.from_wei(w3.eth.get_balance(account.address), 'ether'):.4f}")

# USDC (USDC.e) on Polygon
USDC = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

# Polymarket contracts that need USDC approval
SPENDERS = [
    Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),  # Exchange
    Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a"),  # CTF Exchange
    Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),  # Neg Risk Exchange
]

MAX = 2**256 - 1  # unlimited

ERC20_ABI = [
    {"name": "approve", "type": "function", "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "amount", "type": "uint256"}
    ], "outputs": [{"type": "bool"}], "stateMutability": "nonpayable"},
    {"name": "allowance", "type": "function", "inputs": [
        {"name": "owner", "type": "address"},
        {"name": "spender", "type": "address"}
    ], "outputs": [{"type": "uint256"}], "stateMutability": "view"},
]

usdc = w3.eth.contract(address=USDC, abi=ERC20_ABI)
nonce = w3.eth.get_transaction_count(account.address)

for spender in SPENDERS:
    current = usdc.functions.allowance(account.address, spender).call()
    if current >= MAX // 2:
        print(f"  {spender}: already approved (unlimited)")
        continue

    print(f"  {spender}: approving...")
    tx = usdc.functions.approve(spender, MAX).build_transaction({
        "from": account.address,
        "nonce": nonce,
        "gas": 60000,
        "gasPrice": w3.to_wei("50", "gwei"),
        "chainId": 137,
    })
    signed = account.sign_transaction(tx)
    txhash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  tx sent: {txhash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(txhash, timeout=60)
    print(f"  confirmed: block {receipt['blockNumber']} status={receipt['status']}")
    nonce += 1

print("\nDone. Now restart the engine.")
