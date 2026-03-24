import json
import os
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).parent
ENGINE = BASE / "platforms/polymarket/maker/engine.py"
CONFIG_PATH = BASE / "platforms/polymarket/maker/config.json"
PID_PATH = BASE / "data/.engine.pid"
LOG_PATH = BASE / "data/engine.log"

env = os.environ.copy()

# Route traffic through local Clash proxy when proxy_pool is disabled
env.setdefault("HTTP_PROXY", "http://127.0.0.1:7890")
env.setdefault("HTTPS_PROXY", "http://127.0.0.1:7890")

key = env.get("POLY_PRIVATE_KEY", "").strip()
signer_server_url = ""
try:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    signer_server_url = str((cfg.get("account") or {}).get("signer_server_url", "")).strip()
except Exception:
    signer_server_url = ""

has_local_key = bool(key and "REDACTED" not in key and "REPLACE" not in key)
has_remote_signer = bool(signer_server_url)

if not has_local_key and not has_remote_signer:
    print(
        "ERROR: No valid signer configured. "
        "Set POLY_PRIVATE_KEY or configure account.signer_server_url in config.json."
    )
    sys.exit(1)

if has_local_key:
    print(f"Starting engine with local key: {key[:6]}...{key[-4:]}")
else:
    print(f"Starting engine with remote signer: {signer_server_url}")

with open(LOG_PATH, "a", encoding="utf-8") as lf:
    proc = subprocess.Popen(
        [sys.executable, str(ENGINE)],
        cwd=str(ENGINE.parent),
        stdout=lf,
        stderr=lf,
        env=env,
    )

PID_PATH.write_text(str(proc.pid), encoding="utf-8")
print(f"Engine started, PID: {proc.pid}")
