"""
POLY//WALLET SCANNER
Scans up to 14,000 Polymarket wallets. Keeps only those with >70% win rate.
Outputs elite_wallets.json — loaded by bot.py for the whale-signal agent.

Run: python wallet_scanner.py
Re-runs automatically every 24h when imported by bot.py.
"""

import json, time, threading, logging, urllib.request, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger("WALLET_SCANNER")

ELITE_FILE   = Path(__file__).parent / "elite_wallets.json"
DATA_API     = "https://data-api.polymarket.com"
GAMMA_API    = "https://gamma-api.polymarket.com"
MIN_WIN_RATE = 0.70
MIN_TRADES   = 20      # ignore wallets with tiny sample sizes
MAX_WALLETS  = 14_000
WORKERS      = 12      # concurrent wallet analysis threads

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json",
    "Origin":     "https://polymarket.com",
    "Referer":    "https://polymarket.com/",
}

_cache_lock = threading.Lock()


def _get(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def fetch_wallet_list(max_wallets=MAX_WALLETS):
    """Page through leaderboard + recent traders to collect wallet addresses."""
    addresses = set()

    # 1. Leaderboard (top profit traders)
    log.info("[SCAN] Fetching leaderboard...")
    for offset in range(0, min(5000, max_wallets), 100):
        data = _get(f"{DATA_API}/leaderboard?limit=100&offset={offset}&window=all")
        if not data or not isinstance(data, list):
            break
        for row in data:
            addr = row.get("proxyWallet") or row.get("address") or ""
            if addr:
                addresses.add(addr)
        if len(data) < 100:
            break
        if len(addresses) >= max_wallets // 2:
            break
        time.sleep(0.05)

    log.info(f"[SCAN] Leaderboard: {len(addresses)} wallets")

    # 2. Recent active traders (last 48h trades stream)
    log.info("[SCAN] Fetching recent traders...")
    for offset in range(0, 5000, 100):
        data = _get(f"{DATA_API}/trades?limit=100&offset={offset}&amount_min=10")
        if not data or not isinstance(data, list):
            break
        for t in data:
            addr = t.get("proxyWallet") or ""
            if addr:
                addresses.add(addr)
        if len(data) < 100 or len(addresses) >= max_wallets:
            break
        time.sleep(0.03)

    log.info(f"[SCAN] Total unique wallets collected: {len(addresses)}")
    return list(addresses)[:max_wallets]


def analyze_wallet(address):
    """
    Fetch resolved trade history for one wallet.
    Returns dict with win_rate, total_trades, profit — or None if insufficient data.
    """
    try:
        # Get resolved positions (positions with final P&L)
        data = _get(f"{DATA_API}/positions?user={address}&sizeThreshold=0&limit=500")
        if not data or not isinstance(data, list):
            return None

        resolved = [p for p in data if p.get("redeemable") or p.get("cashBalance") is not None]
        if len(resolved) < MIN_TRADES:
            # Also try activity endpoint
            acts = _get(f"{DATA_API}/activity?user={address}&limit=200")
            if not acts or not isinstance(acts, list):
                return None
            resolved = [a for a in acts if a.get("type") in ("REDEEM", "TRADE") and a.get("cashFlow") is not None]
            if len(resolved) < MIN_TRADES:
                return None

        wins   = 0
        losses = 0
        total_profit = 0.0

        for p in resolved:
            # Positions endpoint
            cash = p.get("cashBalance") or p.get("cashFlow") or 0
            try:
                cash = float(cash)
            except Exception:
                continue
            cost = float(p.get("initialValue") or p.get("cost") or 0)

            if cash > cost:
                wins += 1
            elif cash < cost:
                losses += 1
            total_profit += (cash - cost)

        total = wins + losses
        if total < MIN_TRADES:
            return None

        win_rate = wins / total
        if win_rate < MIN_WIN_RATE:
            return None

        return {
            "address":     address,
            "win_rate":    round(win_rate, 4),
            "wins":        wins,
            "losses":      losses,
            "total_trades": total,
            "profit_usd":  round(total_profit, 2),
        }
    except Exception:
        return None


def run_scan(max_wallets=MAX_WALLETS):
    """Full scan: collect wallets, analyze, save elite_wallets.json."""
    log.info("=" * 60)
    log.info("  POLY//WALLET SCANNER")
    log.info(f"  Target: {max_wallets:,} wallets | Min WR: {MIN_WIN_RATE:.0%}")
    log.info("=" * 60)

    wallets = fetch_wallet_list(max_wallets)
    log.info(f"[SCAN] Analyzing {len(wallets):,} wallets with {WORKERS} threads...")

    elite = []
    done  = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(analyze_wallet, addr): addr for addr in wallets}
        for fut in as_completed(futs):
            done += 1
            result = fut.result()
            if result:
                elite.append(result)
            if done % 500 == 0:
                log.info(f"[SCAN] {done:,}/{len(wallets):,} analyzed | {len(elite)} elite so far")

    # Sort by win rate descending
    elite.sort(key=lambda x: (-x["win_rate"], -x["total_trades"]))

    meta = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "total_scanned": len(wallets),
        "elite_count": len(elite),
        "min_win_rate": MIN_WIN_RATE,
        "wallets": elite,
    }

    with _cache_lock:
        ELITE_FILE.write_text(json.dumps(meta, indent=2))

    log.info(f"[SCAN] Done. {len(elite)} elite wallets (>{MIN_WIN_RATE:.0%} WR) saved to {ELITE_FILE.name}")
    log.info(f"[SCAN] Top 5:")
    for w in elite[:5]:
        log.info(f"  {w['address'][:16]}...  WR={w['win_rate']:.1%}  trades={w['total_trades']}  P&L=${w['profit_usd']:+,.0f}")

    return elite


def load_elite_wallets():
    """Load cached elite wallets. Returns list of dicts."""
    if not ELITE_FILE.exists():
        return []
    try:
        data = json.loads(ELITE_FILE.read_text())
        return data.get("wallets", [])
    except Exception:
        return []


def elite_wallets_fresh(max_age_hours=24):
    """True if elite_wallets.json exists and is less than max_age_hours old."""
    if not ELITE_FILE.exists():
        return False
    try:
        data   = json.loads(ELITE_FILE.read_text())
        scanned = datetime.fromisoformat(data["scanned_at"])
        age_h   = (datetime.now(timezone.utc) - scanned).total_seconds() / 3600
        return age_h < max_age_hours
    except Exception:
        return False


def start_background_scanner():
    """Launch wallet scanner in background thread. Refreshes every 24h."""
    def _loop():
        while True:
            if not elite_wallets_fresh(max_age_hours=24):
                try:
                    run_scan()
                except Exception as e:
                    log.error(f"[SCAN] Error: {e}")
            time.sleep(3600)  # check every hour, scan if stale

    t = threading.Thread(target=_loop, name="wallet-scanner", daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    run_scan()
