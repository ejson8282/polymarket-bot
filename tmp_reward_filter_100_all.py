from polymatrix.scan_markets import fetch_markets, normalize_market

raw = fetch_markets()
ms = [normalize_market(m) for m in raw]

c = [m for m in ms if (m.get('reward', 0) >= 100 and m.get('volume24h', 0) >= 50000 and m.get('token_id'))]
c.sort(key=lambda x: x.get('score', 0), reverse=True)

print('count', len(c))
for i, m in enumerate(c, 1):
    print(f"{i:02d}. reward={m['reward']:.0f} | vol24h={m['volume24h']:.0f} | score={m['score']:.1f}")
    print(f"    slug={m['slug']}")
    print(f"    token_id={m['token_id']}")
