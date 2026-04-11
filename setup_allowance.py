import os
import sys
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

key = os.getenv("POLY_PRIVATE_KEY", "").strip()
if not key or "REDACTED" in key:
    print("ERROR: set POLY_PRIVATE_KEY first")
    sys.exit(1)

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=0,
)
creds = client.create_or_derive_api_creds()
client.set_api_creds(creds)

print("Checking current balance/allowance...")
for asset_type in [AssetType.COLLATERAL, AssetType.CONDITIONAL]:
    try:
        result = client.get_balance_allowance(BalanceAllowanceParams(asset_type=asset_type))
        print(f"  {asset_type}: balance={result.get('balance')} allowance={result.get('allowance')}")
    except Exception as e:
        print(f"  {asset_type}: error reading - {e}")

print("\nUpdating allowance...")
for asset_type in [AssetType.COLLATERAL, AssetType.CONDITIONAL]:
    try:
        result = client.update_balance_allowance(BalanceAllowanceParams(asset_type=asset_type))
        print(f"  {asset_type}: done - {result}")
    except Exception as e:
        print(f"  {asset_type}: error - {e}")

print("\nDone. Now restart the engine.")
