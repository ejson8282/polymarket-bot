from polymatrix.scan_markets import fetch_markets, normalize_market

raw = fetch_markets()
markets = [normalize_market(m) for m in raw]

cands = [
    m
    for m in markets
    if (m.get('reward', 0) >= 200 and m.get('volume24h', 0) >= 50000 and m.get('token_id'))
]
cands.sort(key=lambda x: x.get('score', 0), reverse=True)

print(f"candidates={len(cands)} (reward>=200, vol24h>=50k)")
print("top20:\n")
for i, m in enumerate(cands[:20], 1):
    print(f"{i:02d}. reward={m['reward']:.2f} score={m['score']:.2f} vol24h={m['volume24h']:.0f} spread={m['quotedSpread']:.4f}")
    print(f"    slug={m['slug']}")
    print(f"    token_id={m['token_id']}")
    print(f"    q={m['question'][:120]}")
    print()
