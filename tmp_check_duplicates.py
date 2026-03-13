import json
from pathlib import Path
from collections import defaultdict
from scan_markets import fetch_markets, normalize_market

cfg=json.loads(Path('polymatrix/config.json').read_text(encoding='utf-8'))
markets=cfg.get('markets',[])
ids=[str(m.get('token_id','')) for m in markets if str(m.get('token_id','')).isdigit()]
print('configured token count',len(ids))
raw=fetch_markets(limit=1000)
by_token={}
for r in raw:
    n=normalize_market(r)
    t=str(n.get('token_id',''))
    if t:
        by_token[t]=r

missing=[]
by_condition=defaultdict(list)
for t in ids:
    r=by_token.get(t)
    if not r:
        missing.append(t)
        continue
    cond=str(r.get('conditionId') or r.get('condition_id') or '')
    slug=str(r.get('slug') or '')
    by_condition[cond].append((t,slug))

multi=[(c,v) for c,v in by_condition.items() if c and len(v)>1]
print('missing',len(missing))
print('duplicate condition groups',len(multi))
for c,v in multi[:50]:
    print('\nCOND',c,'count',len(v))
    for x in v:
        print(' ',x[0],x[1])
