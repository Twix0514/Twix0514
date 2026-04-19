"""
Build targets.json from elite_wallets.json + live position enrichment.
Ranks by lifetime PnL. Used by bot for copy trading and whale_in_market checks.
"""
import json, time, urllib.request

ELITE_FILE = "elite_wallets.json"
OUT_FILE   = "targets.json"
TOP_N      = 50

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  ERR: {e}")
        return None

def enrich(wallet: str) -> dict:
    """Pull live stats from data-api."""
    data = fetch(f"https://data-api.polymarket.com/positions?user={wallet}&limit=500")
    if not data:
        return {"active_positions": 0, "total_value": 0.0}
    active = [p for p in data if float(p.get("currentValue", 0) or 0) > 0.10]
    total_val = sum(float(p.get("currentValue", 0) or 0) for p in data)
    return {
        "active_positions": len(active),
        "total_value":      round(total_val, 2),
        "top_markets":      [p.get("title", "")[:50] for p in active[:3]],
    }

def main():
    elite = json.loads(open(ELITE_FILE).read())
    # Sort by pnl_all descending — already curated list of top performers
    elite.sort(key=lambda x: float(x.get("pnl_all", 0) or 0), reverse=True)
    elite = elite[:TOP_N]

    print(f"Building targets.json from {len(elite)} elite wallets...")
    results = []
    for i, w in enumerate(elite):
        wallet = w["wallet"]
        name   = w.get("name", wallet[:14])
        print(f"  [{i+1}/{len(elite)}] {name[:25]:<25}", end=" ", flush=True)
        extra  = enrich(wallet)
        results.append({
            "rank":             i + 1,
            "wallet":           wallet,
            "name":             name,
            "tier":             w.get("tier", "T?"),
            "pnl_all":          round(float(w.get("pnl_all", 0) or 0), 2),
            "pnl_month":        round(float(w.get("pnl_month", 0) or 0), 2),
            **extra,
        })
        print(f"val=${extra['total_value']:,.0f} | {extra['active_positions']} active")
        time.sleep(0.25)

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nTop {len(results)} written to {OUT_FILE}")
    for e in results[:10]:
        print(f"  #{e['rank']} {e['name'][:22]:<22} | ${e['pnl_all']:>12,.0f} all-time | ${e['pnl_month']:>10,.0f}/mo")

if __name__ == "__main__":
    main()
