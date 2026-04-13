"""
POLY//ARB — Mispricing & Arbitrage Scanner
Finds:
  1. Markets where YES + NO prices don't sum to ~$1 (mispriced)
  2. Correlated markets with diverging odds (relative arb)
  3. Wallets with large positions on mispriced markets (exploiters)
"""

import urllib.request, json, time
from datetime import datetime

<<<<<<< HEAD
# -- HELPERS -------------------------------------------------------------------
=======
# ── HELPERS ───────────────────────────────────────────────────────────────────
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

def fetch(url, retries=2):
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except Exception as e:
            if i == retries - 1:
                return None
            time.sleep(1)

def fmt(v): return f"${v:,.2f}"
def pct(v):  return f"{v*100:.1f}¢"

<<<<<<< HEAD
# -- 1. FETCH TOP MARKETS ------------------------------------------------------
=======
# ── 1. FETCH TOP MARKETS ──────────────────────────────────────────────────────
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

print("\n" + "="*70)
print("  POLY//ARB SCANNER  —  " + datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
print("="*70)

print("\n[1/3] Fetching active markets...")
markets = []
for offset in range(0, 300, 100):
    data = fetch(f"https://gamma-api.polymarket.com/markets?active=true&limit=100&offset={offset}&sort=volume&ascending=false")
    if not data:
        break
    batch = data if isinstance(data, list) else data.get('markets', data.get('data', []))
    if not batch:
        break
    markets.extend(batch)
    if len(batch) < 100:
        break

print(f"    Loaded {len(markets)} markets")

<<<<<<< HEAD
# -- 2. MISPRICE SCAN ---------------------------------------------------------

print("\n[2/3] Scanning for mispriced markets (YES + NO != 100¢)...")
=======
# ── 2. MISPRICE SCAN ─────────────────────────────────────────────────────────

print("\n[2/3] Scanning for mispriced markets (YES + NO ≠ 100¢)...")
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

mispriced = []
no_arb    = []  # sum < 1 = can buy both and guarantee profit
over_arb  = []  # sum > 1 = can sell both

for m in markets:
    try:
        prices = m.get('outcomePrices', '[]')
        if isinstance(prices, str):
            prices = json.loads(prices)
        if len(prices) < 2:
            continue

        yes = float(prices[0])
        no  = float(prices[1])
        total = yes + no
        edge  = abs(total - 1.0)

        if edge < 0.005:
            continue  # within 0.5¢ — normal spread

        vol   = float(m.get('volume', 0) or 0)
        vol24 = float(m.get('volume24hr', 0) or 0)
        slug  = m.get('slug', '')
        q     = m.get('question', 'Unknown')[:60]
        liq   = float(m.get('liquidity', 0) or 0)

        entry = {
            "question": q,
            "slug":     slug,
            "yes":      yes,
            "no":       no,
            "total":    total,
            "edge":     edge,
            "vol":      vol,
            "vol24":    vol24,
            "liq":      liq,
        }

        if total < 0.98:    # can buy YES+NO for <$1, guaranteed $1 payout
            no_arb.append(entry)
        elif total > 1.02:  # prices sum >$1, sell both sides
            over_arb.append(entry)
        else:
            mispriced.append(entry)

    except Exception:
        continue

# Sort by edge size descending
no_arb.sort(key=lambda x: -x['edge'])
over_arb.sort(key=lambda x: -x['edge'])
mispriced.sort(key=lambda x: -x['edge'])

<<<<<<< HEAD
print(f"\n  +- RISK-FREE ARB (YES+NO < $1.00) — buy both sides, profit guaranteed -+")
if no_arb:
    for m in no_arb[:8]:
        profit_per_dollar = 1.0 - m['total']
        print(f"  |  YES={pct(m['yes'])} + NO={pct(m['no'])} = {pct(m['total'])}  "
              f"PROFIT={pct(profit_per_dollar)}/share  VOL24={fmt(m['vol24'])}")
        print(f"  |  → {m['question']}")
        print(f"  |    https://polymarket.com/event/{m['slug']}")
        print(f"  |")
else:
    print("  |  None found")

print(f"  +- OVERPRICED MARKETS (YES+NO > $1.00) — sell both sides -------------+")
if over_arb:
    for m in over_arb[:8]:
        profit_per_dollar = m['total'] - 1.0
        print(f"  |  YES={pct(m['yes'])} + NO={pct(m['no'])} = {pct(m['total'])}  "
              f"EDGE={pct(profit_per_dollar)}/share  VOL24={fmt(m['vol24'])}")
        print(f"  |  → {m['question']}")
        print(f"  |    https://polymarket.com/event/{m['slug']}")
        print(f"  |")
else:
    print("  |  None found (market is efficient)")
=======
print(f"\n  ┌─ RISK-FREE ARB (YES+NO < $1.00) — buy both sides, profit guaranteed ─┐")
if no_arb:
    for m in no_arb[:8]:
        profit_per_dollar = 1.0 - m['total']
        print(f"  │  YES={pct(m['yes'])} + NO={pct(m['no'])} = {pct(m['total'])}  "
              f"PROFIT={pct(profit_per_dollar)}/share  VOL24={fmt(m['vol24'])}")
        print(f"  │  → {m['question']}")
        print(f"  │    https://polymarket.com/event/{m['slug']}")
        print(f"  │")
else:
    print("  │  None found")

print(f"  └─ OVERPRICED MARKETS (YES+NO > $1.00) — sell both sides ─────────────┐")
if over_arb:
    for m in over_arb[:8]:
        profit_per_dollar = m['total'] - 1.0
        print(f"  │  YES={pct(m['yes'])} + NO={pct(m['no'])} = {pct(m['total'])}  "
              f"EDGE={pct(profit_per_dollar)}/share  VOL24={fmt(m['vol24'])}")
        print(f"  │  → {m['question']}")
        print(f"  │    https://polymarket.com/event/{m['slug']}")
        print(f"  │")
else:
    print("  │  None found (market is efficient)")
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

print(f"\n  MINOR MISPRICINGS (0.5¢–2¢ edge, {len(mispriced)} found):")
for m in mispriced[:5]:
    direction = "CHEAP" if m['total'] < 1.0 else "EXPENSIVE"
    print(f"    {direction}  YES={pct(m['yes'])} NO={pct(m['no'])}  "
          f"edge={m['edge']*100:.2f}¢  vol={fmt(m['vol'])}")
    print(f"    → {m['question']}")

<<<<<<< HEAD
# -- 3. FIND EXPLOITER WALLETS -------------------------------------------------
=======
# ── 3. FIND EXPLOITER WALLETS ─────────────────────────────────────────────────
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

print("\n[3/3] Finding wallets exploiting mispriced markets...")

arb_targets = (no_arb + over_arb)[:5]
exploiters = {}  # addr -> [{market, size, pnl, side}]

for m in arb_targets:
    # Get position holders for this market via data API
    slug = m['slug']
    if not slug:
        continue

    # Fetch top position holders
    data = fetch(f"https://data-api.polymarket.com/positions?market={slug}&limit=20&sort=size&ascending=false")
    if not data:
        continue
    positions = data if isinstance(data, list) else data.get('data', [])

    for p in positions:
        addr = p.get('proxyWallet', p.get('user', p.get('maker', '')))
        if not addr:
            continue
        size  = float(p.get('size', p.get('shares', 0)) or 0)
        pnl   = float(p.get('unrealizedPnl', p.get('pnl', 0)) or 0)
        side  = p.get('outcome', p.get('side', '?'))
        if size < 10:
            continue  # skip tiny positions

        if addr not in exploiters:
            exploiters[addr] = []
        exploiters[addr].append({
            "market": m['question'][:40],
            "size":   size,
            "pnl":    pnl,
            "side":   side,
            "edge":   m['edge'],
        })

# Also check the big known wallet
known = [
    '0x751a2b86cab503496efd325c8344e10159349ea1',
    '0xde17f7144fbd0eddb2679132c10ff5e74b120988',
    '0x1979ae6b7e6534de9c4539d0c205e582ca637c9d',
    '0x59a0744db1f39ff3afccd175f80e6e8dfc239a09',
]

<<<<<<< HEAD
print(f"\n  +- KNOWN WHALE WALLET POSITIONS ON MISPRICED MARKETS ------------------+")
=======
print(f"\n  ┌─ KNOWN WHALE WALLET POSITIONS ON MISPRICED MARKETS ──────────────────┐")
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d
for addr in known:
    data = fetch(f"https://data-api.polymarket.com/positions?user={addr}&limit=50")
    if not data:
        continue
    positions = data if isinstance(data, list) else []

    # Cross-reference with mispriced markets
    arb_slugs = {m['slug'] for m in arb_targets}
    hits = []
    for p in positions:
        mkt_slug = p.get('market', p.get('conditionId', ''))
        if any(s and s in str(mkt_slug) for s in arb_slugs):
            hits.append(p)

    val_data = fetch(f"https://data-api.polymarket.com/value?user={addr}")
    if isinstance(val_data, list): val_data = val_data[0] if val_data else {}
    port = float((val_data or {}).get('portfolioValue', (val_data or {}).get('value', 0)) or 0)

    if port > 0 or hits:
        short = addr[:8] + '...' + addr[-4:]
<<<<<<< HEAD
        print(f"  |  {short}  portfolio={fmt(port)}  arb_positions={len(hits)}")
        for h in hits[:2]:
            print(f"  |    {h.get('outcome','?')} {h.get('size',0):.0f} shares — {h.get('title','')[:40]}")

print(f"  +-----------------------------------------------------------------------+")

# -- 4. SUMMARY ----------------------------------------------------------------
=======
        print(f"  │  {short}  portfolio={fmt(port)}  arb_positions={len(hits)}")
        for h in hits[:2]:
            print(f"  │    {h.get('outcome','?')} {h.get('size',0):.0f} shares — {h.get('title','')[:40]}")

print(f"  └───────────────────────────────────────────────────────────────────────┘")

# ── 4. SUMMARY ────────────────────────────────────────────────────────────────
>>>>>>> 17eb9c02ef5228ab21726c08e305190dfbac2e5d

print("\n" + "="*70)
print("  SUMMARY")
print("="*70)
print(f"  Markets scanned:     {len(markets)}")
print(f"  Risk-free arb:       {len(no_arb)}  (buy YES+NO < $1)")
print(f"  Overpriced arb:      {len(over_arb)}  (sell YES+NO > $1)")
print(f"  Minor mispricings:   {len(mispriced)}")
print(f"  Exploiter wallets:   {len(exploiters)}")

if no_arb:
    best = no_arb[0]
    print(f"\n  BEST OPPORTUNITY:")
    print(f"  Buy YES @ {pct(best['yes'])} + NO @ {pct(best['no'])} = {pct(best['total'])}")
    print(f"  Guaranteed profit: {pct(1.0 - best['total'])} per share")
    print(f"  Market: {best['question']}")
    print(f"  URL: https://polymarket.com/event/{best['slug']}")

print("\n  Run again to refresh. Markets update every few minutes.")
print("="*70 + "\n")
