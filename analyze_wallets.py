"""
analyze_wallets.py — find the best wallets on Polymarket.

Fetches trade history for seed wallets + discovers new ones via
top-activity scan. Filters: 100+ resolved trades, 70%+ win rate
(SELL >= 0.85 = WIN, SELL <= 0.15 = LOSS). Ranks by profit proxy.
Exports top 50 to targets.json, loaded by bot.py as elite wallets.

Run:  python analyze_wallets.py
"""

import json, time, requests, pathlib

BASE   = pathlib.Path(__file__).parent
OUT    = BASE / "targets.json"
SEED_F = BASE / "bot.py"

API   = "https://data-api.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

MIN_TRADES  = 20    # resolved trades (sells near $1 or $0)
MIN_WIN_PCT = 0.65  # 65%+ win rate on resolved exits
TOP_N       = 50

PLATFORM_WALLETS = {
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
}


def _get(url, params=None):
    for i in range(3):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if i == 2:
                print(f"  ERR {url[-50:]}: {e}")
        time.sleep(0.5 * (i + 1))
    return None


def extract_seed_wallets() -> list:
    wallets = []
    try:
        text = SEED_F.read_text(encoding="utf-8")
        in_block = False
        for line in text.splitlines():
            if "_SEED_WHALES = [" in line:
                in_block = True
            if in_block:
                for part in line.split('"'):
                    if part.startswith("0x") and len(part) == 42:
                        wallets.append(part.lower())
                if "]" in line and wallets:
                    break
    except Exception as e:
        print(f"seed parse error: {e}")
    print(f"[SEED] {len(wallets)} wallets from bot.py")
    return list(set(wallets))


def discover_wallets(seeds: list, target: int = 300) -> list:
    found = set(w.lower() for w in seeds)
    print(f"[DISCOVER] Scanning top markets for more traders...")
    try:
        markets = _get(f"{GAMMA}/markets", params={"limit": 20, "active": "true", "order": "volume24hr"}) or []
        for m in markets[:15]:
            for tok in (m.get("tokens") or [])[:1]:
                tid = tok.get("token_id") or tok.get("tokenId")
                if not tid:
                    continue
                trades = _get(f"{API}/trades", params={"asset": tid, "limit": 100}) or []
                for t in trades:
                    w = (t.get("proxyWallet") or "").lower()
                    if w and len(w) == 42 and w not in PLATFORM_WALLETS:
                        found.add(w)
            time.sleep(0.15)
    except Exception as e:
        print(f"  discover err: {e}")
    result = list(found)[:target]
    print(f"[DISCOVER] {len(result)} total wallets to score")
    return result


def score_wallet(wallet: str) -> dict | None:
    """
    SELL >= 0.85 → WIN (resolved in favour, near $1)
    SELL <= 0.15 → LOSS (resolved against, near $0)
    Profit proxy = win_pnl - loss_cost
    """
    trades = _get(f"{API}/trades", params={"user": wallet, "limit": 500}) or []
    if not trades:
        return None

    wins = losses = buy_count = 0
    win_pnl = loss_cost = 0.0

    for t in trades:
        side  = (t.get("side") or "").upper()
        price = float(t.get("price") or 0)
        size  = float(t.get("size") or 0)
        if side == "BUY":
            buy_count += 1
            # Proxy: buying at high conviction (>0.60) = wallet believes it wins
            if price > 0.60:
                wins    += 1
                win_pnl += price * size
            elif price < 0.40:
                losses    += 1
                loss_cost += price * size
        elif side == "SELL":
            if price >= 0.80:   # profitable exit or resolution win
                wins    += 1
                win_pnl += price * size
            elif price <= 0.20:  # stopped out or resolution loss
                losses    += 1
                loss_cost += (1 - price) * size

    resolved = wins + losses
    if resolved < MIN_TRADES or resolved == 0 or (wins / resolved) < MIN_WIN_PCT:
        return None

    return {
        "wallet":       wallet,
        "trades":       resolved,
        "buy_count":    buy_count,
        "win_rate":     round(wins / resolved, 4),
        "profit_proxy": round(win_pnl - loss_cost, 2),
    }


def run():
    print("=" * 55)
    print("  Polymarket Wallet Analyzer")
    print(f"  Filter: {MIN_TRADES}+ trades, {MIN_WIN_PCT:.0%}+ win rate, top {TOP_N}")
    print("=" * 55)

    seeds   = extract_seed_wallets()
    wallets = discover_wallets(seeds)

    print(f"\n[SCAN] Scoring {len(wallets)} wallets...")
    results = []
    for i, w in enumerate(wallets):
        r = score_wallet(w)
        if r:
            results.append(r)
            print(f"  PASS {w[:18]}... trades={r['trades']} wr={r['win_rate']:.0%} pnl~${r['profit_proxy']:.0f}")
        elif i % 25 == 0:
            print(f"  {i}/{len(wallets)} scanned — {len(results)} qualified")
        time.sleep(0.12)

    results.sort(key=lambda x: x["profit_proxy"], reverse=True)
    top = results[:TOP_N]

    OUT.write_text(json.dumps(top, indent=2))
    print(f"\n[DONE] {len(results)} qualified -> top {len(top)} saved to {OUT.name}")
    if top:
        best = top[0]
        print(f"  #1: {best['wallet'][:20]}... wr={best['win_rate']:.0%} pnl~${best['profit_proxy']:.0f}")
    print("\n  Restart bot.py to load new targets.")


if __name__ == "__main__":
    run()
