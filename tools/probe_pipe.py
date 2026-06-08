"""Count HK/JP candidate nodes in GLOBAL group."""
import json, sys, win32file, pywintypes  # type: ignore

PIPE = r"\\.\pipe\verge-mihomo"


def _dechunk(data):
    out, i, n = bytearray(), 0, len(data)
    while i < n:
        crlf = data.find(b"\r\n", i)
        if crlf < 0: break
        try: size = int(data[i:crlf].split(b";", 1)[0].strip(), 16)
        except ValueError: break
        i = crlf + 2
        if size == 0: break
        if i + size > n: break
        out += data[i:i+size]
        i += size + 2
    return bytes(out)


def req(method, path):
    r = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode()
    h = win32file.CreateFile(PIPE, win32file.GENERIC_READ | win32file.GENERIC_WRITE, 0, None, win32file.OPEN_EXISTING, 0, None)
    try:
        win32file.WriteFile(h, r)
        buf = b""
        while True:
            try:
                _, c = win32file.ReadFile(h, 8192)
                if not c: break
                buf += c
                if len(buf) > 4_000_000: break
            except pywintypes.error as e:
                if e.winerror in (109, 232): break
                raise
    finally: win32file.CloseHandle(h)
    head_end = buf.find(b"\r\n\r\n")
    head = buf[:head_end].decode("iso-8859-1")
    body = buf[head_end+4:]
    if "chunked" in head.lower(): body = _dechunk(body)
    return json.loads(body.decode("utf-8"))


sys.stdout.reconfigure(errors="replace")
data = req("GET", "/proxies")
g = data["proxies"]["GLOBAL"]
nodes = g["all"]
now = g.get("now", "")
hk = [n for n in nodes if "hong kong" in n.lower()]
jp = [n for n in nodes if "japan" in n.lower()]
print(f"GLOBAL current node: {now}")
print(f"GLOBAL total: {len(nodes)}")
print(f"HK nodes: {len(hk)}")
for n in hk[:20]:
    print(f"  {n}")
print(f"... ({len(hk)} total)" if len(hk) > 20 else "")
print(f"JP nodes: {len(jp)}")
for n in jp[:20]:
    print(f"  {n}")
print(f"... ({len(jp)} total)" if len(jp) > 20 else "")
