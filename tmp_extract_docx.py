import zipfile, re, pathlib
files=[r"C:\Users\Administrator.DESKTOP-00BT8F3\.openclaw\media\inbound\72d34850-60b5-4508-bcac-238fca8a2d0c.docx", r"C:\Users\Administrator.DESKTOP-00BT8F3\.openclaw\media\inbound\a4a4f01f-dd29-44d5-81e4-16e7b140841c.docx"]
for f in files:
    p=pathlib.Path(f)
    print(f"\n=====FILE===== {p.name}")
    with zipfile.ZipFile(p) as z:
        xml=z.read('word/document.xml').decode('utf-8','ignore')
    txt=re.sub(r'<w:tab[^>]*/>','\t',xml)
    txt=re.sub(r'</w:p>','\n',txt)
    txt=re.sub(r'<[^>]+>','',txt)
    txt=txt.replace('&amp;','&').replace('&lt;','<').replace('&gt;','>')
    lines=[ln.strip() for ln in txt.splitlines() if ln.strip()]
    print('\n'.join(lines[:280]))
