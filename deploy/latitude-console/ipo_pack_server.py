"""极简判研包 HTTP 服务(:8085)——只对 tailnet 吐 ipo_judgment_pack.json,不暴露任何其他文件(尤其 webhook 密钥)。Windows 常驻,VPS1 控制台 HTTP 拉取。"""
import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DATA_DIR = Path(os.getenv("IPO_DATA_DIR", r"C:\ops\ipo-advisor\data"))
PACK = DATA_DIR / "ipo_judgment_pack.json"
PORT = int(os.getenv("IPO_PACK_PORT", "8085"))


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        # 只放行判研包这一个资源,其余一律 404(webhook/views 绝不外泄)
        if self.path.rstrip("/").endswith("ipo_judgment_pack.json") or self.path in ("/", "/pack"):
            try:
                body = PACK.read_bytes()
            except Exception:
                body = b'{"present": false}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
