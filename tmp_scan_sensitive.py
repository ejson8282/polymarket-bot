import os,re
root='polymatrix'
pat=re.compile(r'(private_key|api_key|api_secret|api_passphrase|discord_webhook|webhook|funder|0x[a-fA-F0-9]{40})',re.I)
for dp,_,fs in os.walk(root):
    for f in fs:
        if f.endswith(('.py','.json','.md','.txt','.yml','.yaml','.ini','.bat','.ps1','.env')):
            p=os.path.join(dp,f)
            try:
                s=open(p,encoding='utf-8',errors='ignore').read()
            except Exception:
                continue
            if pat.search(s):
                print(p)
