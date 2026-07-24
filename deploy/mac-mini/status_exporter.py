"""Latitude 控制台 · Mac mini 只读状态导出器。
只读:launchctl 服务状态 + 进程存活 + 负载;绑定 Tailscale IP,不碰任何密钥/签名逻辑。
"""
import json, subprocess, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BIND = ("100.91.159.54", 8620)
LABELS = ["ai.codex.var-decibel-signer", "ai.codex.predictfun-api-proxy",
          "ai.codex.var-decibel-chrome-health"]
PROC_PATTERNS = {"var_signer": "mac_signer_service", "pf_proxy": "predictfun-api-proxy",
                 "chrome": "Google Chrome"}
_CACHE_LOCK = threading.Lock()
_CACHE_AT = 0.0
_CACHE_BODY = b""

def _services():
    out = {}
    try:
        text = subprocess.run(["launchctl", "list"], capture_output=True, text=True, timeout=5).stdout
        for line in text.splitlines():
            parts = line.split("\t")
            if len(parts) >= 3 and parts[2] in LABELS:
                pid = parts[0].strip()
                out[parts[2]] = {"running": pid.isdigit(), "pid": int(pid) if pid.isdigit() else None,
                                  "last_exit": parts[1].strip()}
    except Exception as e:
        out["error"] = str(e)[:100]
    return out

def _procs():
    out = {}
    for name, pat in PROC_PATTERNS.items():
        try:
            r = subprocess.run(["pgrep", "-f", pat], capture_output=True, text=True, timeout=5)
            pids = [p for p in r.stdout.split() if p.strip()]
            out[name] = len(pids)
        except Exception:
            out[name] = None
    return out


def _status_body():
    """Share one recent system probe across concurrent dashboard requests."""
    global _CACHE_AT, _CACHE_BODY
    now = time.monotonic()
    if _CACHE_BODY and now - _CACHE_AT < 5:
        return _CACHE_BODY
    with _CACHE_LOCK:
        now = time.monotonic()
        if _CACHE_BODY and now - _CACHE_AT < 5:
            return _CACHE_BODY
        _CACHE_BODY = json.dumps({
            "ts": int(time.time()),
            "host": "mac-mini",
            "services": _services(),
            "processes": _procs(),
        }).encode()
        _CACHE_AT = time.monotonic()
        return _CACHE_BODY


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path not in ("/status", "/", "/healthz"):
            self.send_response(404); self.end_headers(); return
        body = _status_body()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

if __name__ == "__main__":
    ThreadingHTTPServer(BIND, H).serve_forever()
