"""One-shot safe restart: per-account cancel_all → kill multi_runner → spawn new.

Mirrors dashboard/app.py::stop_multi_runner + start_multi_runner. Uses the
same signer/ClobClient path to first cancel every account's open orders,
then hard-kills the engine and launches a fresh multi_runner process.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MAKER_DIR = REPO / "platforms/polymarket/maker"
DATA_DIR = REPO / "data"
PID_FILE = DATA_DIR / ".multi_runner.pid"

# Make the remote-signer + py_clob_client imports resolve the same way the
# dashboard does.
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(MAKER_DIR))

from py_clob_client_v2.client import ClobClient           # noqa: E402
from py_clob_client_v2.clob_types import ApiCreds         # noqa: E402
from platforms.polymarket.maker.remote_signer import (
    AddressStub, BuilderStub, RemoteSignerClient,
)


def build_client(cfg_path: Path):
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    acc = cfg.get("account", {})
    url = str(acc.get("signer_server_url", "")).strip()
    token = str(acc.get("signer_token", "")).strip()
    if not url or not token:
        return None, f"{cfg_path.name}: no signer"
    host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(acc.get("chain_id", 137))
    sig_type = int(acc.get("signature_type", 0))
    funder = acc.get("funder")
    signer = RemoteSignerClient(url, token, funder=funder or None)
    creds = signer.derive_creds()
    client = ClobClient(host=host, chain_id=chain_id)
    client.signer = AddressStub(creds["address"], chain_id)
    client.builder = BuilderStub(sig_type=sig_type, funder=funder)
    client.set_api_creds(ApiCreds(
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        api_passphrase=creds["api_passphrase"],
    ))
    return client, None


def cancel_for_each_account():
    summary = []
    for i in range(1, 31):
        p = MAKER_DIR / f"config_{i}.json"
        if not p.exists():
            continue
        try:
            client, err = build_client(p)
            if err:
                summary.append((i, "skip", err))
                continue
            client.cancel_all()
            summary.append((i, "ok", ""))
        except Exception as e:
            summary.append((i, "fail", f"{type(e).__name__}: {str(e)[:60]}"))
    return summary


def stop():
    print("Cancelling open orders per account …")
    for acc, status, note in cancel_for_each_account():
        print(f"  account {acc}: {status}  {note}")
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip() or "0")
        if pid:
            print(f"Killing multi_runner PID {pid} …")
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True, check=False)
        PID_FILE.unlink(missing_ok=True)
    else:
        print("no PID file — nothing to kill")


def start():
    print("Starting new multi_runner …")
    log_path = DATA_DIR / "multi_runner.log"
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    lf = log_path.open("a", encoding="utf-8")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    proc = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(MAKER_DIR / "multi_runner.py")],
        cwd=str(REPO),
        stdout=lf, stderr=lf,
        creationflags=flags,
        env=env,
    )
    PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    print(f"Spawned PID {proc.pid}")


def main():
    stop()
    time.sleep(2)
    start()
    time.sleep(3)
    if PID_FILE.exists():
        print(f"Done. New PID: {PID_FILE.read_text().strip()}")
    else:
        print("no PID written — check multi_runner.log")


if __name__ == "__main__":
    main()
