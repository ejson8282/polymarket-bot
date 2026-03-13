import os, re, shutil
from pathlib import Path

src = Path(r"C:\Users\Administrator.DESKTOP-00BT8F3\.openclaw\workspace")
dst = Path(r"C:\Users\Administrator.DESKTOP-00BT8F3\Desktop\project_sanitized")

if dst.exists():
    shutil.rmtree(dst)
shutil.copytree(src, dst)

text_ext = {'.py','.json','.md','.txt','.yml','.yaml','.ini','.bat','.ps1','.env','.toml','.csv'}

kv_patterns = [
    (re.compile(r'("private_key"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'("api_key"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'("api_secret"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'("api_passphrase"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'("discord_webhook"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'("funder"\s*:\s*")([^"]*)(")', re.I), r'\1REDACTED\3'),
    (re.compile(r'(POLY_PRIVATE_KEY\s*=\s*)(.+)', re.I), r'\1REDACTED'),
]

hex_addr = re.compile(r'0x[a-fA-F0-9]{40}')
webhook = re.compile(r'https://discord\.com/api/webhooks/[^\s"\']+', re.I)

removed = 0
edited = 0
for p in dst.rglob('*'):
    if p.is_dir():
        continue
    if p.suffix.lower() in {'.log','.pid','.pyc'} or p.name.startswith('.overnight_reprice'):
        try:
            p.unlink()
            removed += 1
        except Exception:
            pass
        continue

    if p.suffix.lower() not in text_ext:
        continue
    try:
        s = p.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        continue
    orig = s
    for pat, rep in kv_patterns:
        s = pat.sub(rep, s)
    s = webhook.sub('REDACTED_WEBHOOK', s)
    s = hex_addr.sub('0xREDACTEDADDRESS', s)

    if s != orig:
        p.write_text(s, encoding='utf-8')
        edited += 1

if (dst / '.git').exists():
    shutil.rmtree(dst / '.git', ignore_errors=True)

print(f"SANITIZED_COPY_CREATED={dst}")
print(f"FILES_EDITED={edited}")
print(f"FILES_REMOVED={removed}")
