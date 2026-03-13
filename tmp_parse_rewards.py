import requests,re,json
u='https://polymarket.com/zh/rewards?id=rate_per_day&desc=true&q=&page=1'
html=requests.get(u,timeout=20).text
m=re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',html)
print('nextdata',bool(m))
if m:
    data=json.loads(m.group(1))
    props=data.get('props') or {}
    pp=props.get('pageProps') or {}
    print('route',data.get('page'))
    print('query',data.get('query'))
    print('pp_keys',list(pp.keys())[:60])
    print('pp_preview',str(pp)[:1200])
