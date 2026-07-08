"""Best-effort cancel-all CLI:对每个已配置账号 REST 撤销全部挂单。

非 Streamlit 复用路径:逐行镜像 dashboard/app.py::_build_client_for_config 的
远程 signer 客户端构建(不读本地私钥,凭 config_N.json 里的 signer_server_url
+ signer_token 走 Mac mini signer derive_creds)。供 latitude-console 的
EMERGENCY STOP / Stop / Reload 在 systemctl stop 之前调用(引擎自身收到
SIGTERM 也会撤单,本 CLI 是 belt-and-suspenders,与 dashboard 急停语义一致)。

输出:stdout 一行 JSON {"results": [{"account": N, "status": "ok|skip|fail", "note": str}]}
退出码恒为 0(best-effort;失败体现在 results 里,不阻塞后续 systemctl stop)。
绝不打印 token/creds。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[3]
MAKER_DIR = REPO_DIR / "platforms/polymarket/maker"


def _build_client(config_path: Path):
    """与 dashboard/app.py::_build_client_for_config 相同的构建路径。"""
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        return None, f"Cannot read {config_path.name}: {e}"
    acc = cfg.get("account", {})
    signer_url = str(acc.get("signer_server_url", "")).strip()
    signer_token = str(acc.get("signer_token", "")).strip()
    if not signer_url or not signer_token:
        return None, f"{config_path.name}: no signer configured"

    if str(REPO_DIR) not in sys.path:
        sys.path.insert(0, str(REPO_DIR))
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from platforms.polymarket.maker.remote_signer import AddressStub, BuilderStub, RemoteSignerClient

    host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(acc.get("chain_id", 137))
    sig_type = int(acc.get("signature_type", 0))
    funder = acc.get("funder")

    signer = RemoteSignerClient(signer_url, signer_token, funder=funder or None)
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


def main() -> int:
    results = []
    for i in range(1, 31):
        cfg_path = MAKER_DIR / f"config_{i}.json"
        if not cfg_path.exists():
            continue
        try:
            client, err = _build_client(cfg_path)
            if err:
                results.append({"account": i, "status": "skip", "note": err[:80]})
                continue
            client.cancel_all()
            results.append({"account": i, "status": "ok", "note": ""})
        except Exception as e:
            results.append({"account": i, "status": "fail",
                            "note": f"{type(e).__name__}: {str(e)[:60]}"})
    print(json.dumps({"results": results}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
