"""Feishu 告警推送:轮询本机 /api/state 的 alerts,新增告警推送到 Feishu 群机器人。

- webhook 放 data/feishu_webhook.txt(单行 URL,chmod 600,不入 git);
  文件不存在或为空 → 静默退出(通道搭好等钥匙,不报错)。
- 去重:data/alert_push_state.json 记录已推送告警指纹;
  告警持续存在不重复推,告警消失后指纹清除,同一问题再次出现会再推。
- 由 alert-pusher.timer 每 5 分钟触发一次(oneshot)。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

DATA_DIR = Path(os.getenv("LATITUDE_DATA_DIR", "/home/ubuntu/polymarket-bot/data"))
STATE_URL = os.getenv("LATITUDE_STATE_URL", "http://127.0.0.1:8600/api/state")
WEBHOOK_FILE = DATA_DIR / "feishu_webhook.txt"
PUSH_STATE = DATA_DIR / "alert_push_state.json"


def main() -> int:
    if not WEBHOOK_FILE.exists():
        return 0
    hook = WEBHOOK_FILE.read_text(encoding="utf-8").strip()
    if not hook.startswith("https://"):
        return 0
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(STATE_URL, timeout=10) as resp:
        state = json.load(resp)
    alerts = state.get("alerts") or []

    def plain(t: str) -> str:
        return re.sub(r"<[^>]+>", "", str(t))

    current: dict[str, str] = {}
    for a in alerts:
        text = f"[{a.get('tag', '')}]{'🔴' if a.get('sev') == 'crit' else '🟡'} {plain(a.get('msg', ''))}"
        current[hashlib.sha1(text.encode()).hexdigest()[:16]] = text

    prev: dict = {}
    try:
        prev = json.loads(PUSH_STATE.read_text(encoding="utf-8"))
    except Exception:
        pass

    new = {k: v for k, v in current.items() if k not in prev}
    if new:
        body = "⚠️ Latitude 新告警(" + time.strftime("%m-%d %H:%M") + "):\n" \
               + "\n".join("· " + v for v in new.values()) \
               + f"\n共 {len(current)} 条活跃 → http://100.122.255.98:8502/"
        req = urllib.request.Request(
            hook,
            data=json.dumps({"msg_type": "text", "content": {"text": body}}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with opener.open(req, timeout=10) as resp:
            resp.read()

    # 只保留当前活跃指纹(消失的清除 → 复发会再推)
    PUSH_STATE.write_text(
        json.dumps({k: prev.get(k) or int(time.time()) for k in current}, ensure_ascii=False),
        encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
