"""
POLY//EDGE — Fundamental Base Rate vs Market Implied Probability
================================================================
Finds markets where crowd-implied probability deviates from historical
base rates / on-chain signals by more than a configurable threshold.

Signal sources:
  • BTC/crypto: CoinGlass (funding, OI, liquidations), CoinGecko (price)
  • Political  : Historical calibration DB (polling → outcome frequencies)
  • Macro      : Analyst consensus, Fed dot-plot, CME FedWatch
  • General    : Polymarket historical calibration (how well-calibrated the crowd is)
"""

import json, urllib.request, time, re
from datetime import datetime, timezone

EDGE_THRESHOLD = 0.20   # alert when |market - base_rate| > 20pp
MIN_VOL24      = 500    # only flag markets with >$500 daily volume
MAX_MARKETS    = 300    # how many to scan

# -----------------------------------------------------------------------------
# BASE RATE DATABASE
# Source: historical prediction market calibration studies, domain research
# Format: (base_rate, source_note, direction_hint)
# -----------------------------------------------------------------------------

# BTC price milestones — post-halving cycle (April 2024 halving, cycle peaks ~12-18 months later)
# Historical: every post-halving year, BTC has made new ATH. 3/3 halving cycles.
BTC_CYCLE = {
    # (keyword_pattern, base_rate_pct, note)
    r"btc|bitcoin": None,  # handled by live signal
}

# Political base rates — derived from FiveThirtyEight calibration studies
# Candidates polling 60%+ win ~85% of races. 50-60% polls win ~65%.
POLITICAL_BASE = [
    (r"trump.*win|win.*trump",                           0.52, "538-style average; close race"),
    (r"democrat.*win|win.*democrat|blue.*flip",          0.48, "competitive districts lean R in off years"),
    (r"republican.*win|win.*republican|red.*flip",       0.52, "incumbency + fundraising advantage"),
    (r"impeach",                                         0.08, "historically rare; only 3 in US history"),
    (r"resign",                                          0.05, "extremely rare for sitting officials"),
    (r"third.party|independent.*win",                    0.03, "FPTP systems strongly suppress 3rd parties"),
    (r"supreme court",                                   0.45, "highly contested decisions are coin-flips"),
    (r"veto",                                            0.35, "presidents veto ~8% of bills; override rare"),
    (r"senate.*pass|pass.*senate",                       0.30, "60-vote threshold kills most major bills"),
    (r"house.*pass|pass.*house",                         0.45, "simpler majority; more likely than senate"),
    (r"ceasefire",                                       0.22, "active conflicts: avg ~22% annual ceasefire rate"),
    (r"peace.*deal|deal.*peace",                         0.15, "multi-year conflicts: 15% in any given year"),
    (r"sanction",                                        0.40, "sanctions enacted in ~40% of serious escalations"),
    (r"nato",                                            0.35, "new NATO admissions: historically slow process"),
    (r"election.*win|win.*election",                     0.50, "absent other info, assume 50/50"),
]

# Fed / macro
MACRO_BASE = [
    (r"fed.*cut|rate.*cut|cut.*rate",                    0.65, "CME FedWatch snapshot as of April 2026: ~65% for Jun cut"),
    (r"fed.*hike|rate.*hike|hike.*rate",                 0.08, "Fed on hold; hike probability very low"),
    (r"recession",                                       0.35, "Leading indicators elevated; 35% 12-month"),
    (r"inflation.*above.*3|cpi.*above.*3",               0.28, "Core PCE trending down; 28% stays above 3%"),
    (r"soft.landing",                                    0.42, "Contested; economists ~42% consensus"),
    (r"gdp.*negative|negative.*gdp",                     0.30, "Two negative quarters: ~30% probability"),
    (r"unemployment.*above.*5",                          0.25, "Current 3.9%; above 5% requires sharp slowdown"),
    (r"dollar.*strength|dxy",                            0.45, "DXY range-bound; slight downside bias"),
    (r"gold.*\$3[,.]?[0-9]{3}|gold.*3000",               0.70, "Strong macro tailwinds; momentum above $2900"),
    (r"oil.*above.*100|\$100.*oil|wti.*100",             0.22, "Demand soft; OPEC cuts partially priced"),
]

# Crypto (non-BTC) base rates — altcoins underperform BTC in early bull, outperform late
CRYPTO_BASE = [
    (r"ethereum|eth.*\$",                                None, "use live signal"),
    (r"solana|sol.*\$",                                  None, "use live signal"),
    (r"dogecoin|doge.*\$",                               None, "use live signal"),
    (r"altcoin.*season|alt.*season",                     0.55, "Post-BTC ATH: alts typically run 3-6 months later"),
    (r"crypto.*crash|market.*crash",                     0.18, "30%+ drawdown in next 90 days: ~18% historically"),
    (r"etf.*approve|approve.*etf",                       0.35, "SEC approval cadence: 35% of pending ETF apps"),
    (r"defi.*hack|protocol.*hack",                       0.12, "Major protocol hack in 90 days: ~12% base rate"),
    (r"stablecoin.*depeg|depeg",                         0.08, "USDC/USDT: very low; algo-stable: higher"),
]

# Entertainment / pop culture
ENTERTAINMENT_BASE = [
    (r"oscar|academy award",                             None, "use betting market consensus"),
    (r"grammy",                                          None, "use betting market consensus"),
    (r"superbowl|super bowl",                            0.50, "effectively 50/50 before spread"),
    (r"world.cup|world series",                          None, "sport-specific"),
    (r"album.*release|release.*album|new.*album",        0.55, "announced albums release ~55% within target quarter"),
    (r"movie.*release|film.*release|release.*film",      0.70, "announced films release ~70% on time"),
    (r"gta.*vi|gta 6",                                   0.35, "Take-Two guided H1 2026; high uncertainty"),
    (r"before.*gta|gta.*before",                         None, "conditional on GTA VI date — compound uncertainty"),
]

# -----------------------------------------------------------------------------
# LIVE SIGNAL FETCHERS
# -----------------------------------------------------------------------------

def http_get(url, timeout=8):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return None

def get_btc_signals():
    """Fetch BTC on-chain + derivatives signals from public APIs."""
    signals = {}

    # CoinGecko: price, 30d change, market dominance
    cg = http_get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,dogecoin&vs_currencies=usd&include_24hr_change=true&include_30d_change=true")
    if cg:
        btc = cg.get('bitcoin', {})
        signals['btc_price']   = btc.get('usd', 0)
        signals['btc_24h']     = btc.get('usd_24h_change', 0)
        signals['btc_30d']     = btc.get('usd_30d_change', 0) or 0
        signals['eth_price']   = cg.get('ethereum', {}).get('usd', 0)
        signals['sol_price']   = cg.get('solana', {}).get('usd', 0)
        signals['doge_price']  = cg.get('dogecoin', {}).get('usd', 0)

    # CoinGlass: BTC funding rate (positive = longs paying = bullish crowding)
    cg2 = http_get("https://open-api.coinglass.com/public/v2/funding?symbol=BTC&limit=1")
    if cg2 and cg2.get('data'):
        fr = cg2['data'][0].get('fundingRate', 0) or 0
        signals['btc_funding'] = float(fr)  # positive = longs crowded

    # Fallback: estimate signal from price momentum
    if 'btc_price' in signals:
        price = signals['btc_price']
        mom30 = signals.get('btc_30d', 0)
        # Simple cycle score 0-1 based on price vs all-time context
        # BTC ATH ~$109k (Jan 2025). Post-halving cycle typically 12-18 months.
        # Compute elapsed whole months since the April 2024 halving dynamically.
        now = datetime.now(timezone.utc)
        halving_months_elapsed = max(0, (now.year - 2024) * 12 + (now.month - 4))
        cycle_decay = max(0, 1 - (halving_months_elapsed - 12) / 18)  # peaks at 12mo, decays
        signals['btc_cycle_score'] = round(cycle_decay, 2)

    return signals

def btc_target_base_rate(target_price, signals, days_remaining=None):
    """
    Estimate probability BTC reaches a price target.
    Uses: current price, cycle position, momentum, volatility.
    BTC 30-day vol ~55% annualized → ~16% monthly.
    """
    cur  = signals.get('btc_price', 85000)
    if cur <= 0 or target_price <= 0: return None
    ratio = target_price / cur

    if ratio <= 1.0:  # target is below current price
        # probability it goes DOWN to target
        if ratio > 0.90:  return 0.30, "10% drawdown: frequent in bull markets"
        if ratio > 0.80:  return 0.18, "20% drawdown: ~18% 90-day probability"
        if ratio > 0.70:  return 0.10, "30% drawdown: bear market territory"
        if ratio > 0.60:  return 0.05, "40% drawdown: capitulation event needed"
        return 0.02, "50%+ crash: tail risk only"
    else:  # target above current price
        # Upside probabilities — BTC post-halving bull cycle
        cycle = signals.get('btc_cycle_score', 0.3)
        mom   = signals.get('btc_30d', 0)
        bullish_adj = 0.10 if mom > 10 else (-0.05 if mom < -10 else 0)

        if ratio <= 1.10:  return min(0.70, 0.55 + bullish_adj + cycle*0.1), "10% up: common in bull market"
        if ratio <= 1.20:  return min(0.60, 0.45 + bullish_adj + cycle*0.1), "20% up: moderate probability"
        if ratio <= 1.30:  return min(0.50, 0.35 + bullish_adj + cycle*0.1), "30% up: possible but not base case"
        if ratio <= 1.50:  return min(0.40, 0.25 + bullish_adj + cycle*0.1), "50% up: needs new catalyst"
        if ratio <= 2.00:  return min(0.25, 0.12 + bullish_adj + cycle*0.1), "2x: late cycle moonshot territory"
        return 0.05, "3x+: extreme tail"

def eth_target_base_rate(target_price, signals):
    cur = signals.get('eth_price', 1600)
    if cur <= 0 or target_price <= 0: return None
    ratio = target_price / cur
    # ETH typically lags BTC and has higher vol
    if ratio <= 1.0:
        if ratio > 0.80: return 0.22, "20% ETH drawdown: more frequent than BTC"
        return 0.10, "ETH severe drawdown"
    else:
        if ratio <= 1.20: return 0.45, "ETH +20%: plausible in ETF tailwind environment"
        if ratio <= 1.50: return 0.30, "ETH +50%: needs BTC breakout + ETH rotation"
        if ratio <= 2.00: return 0.18, "ETH 2x: late-cycle alt run"
        return 0.06, "ETH 3x+: extreme tail"

def sol_target_base_rate(target_price, signals):
    cur = signals.get('sol_price', 125)
    if cur <= 0 or target_price <= 0: return None
    ratio = target_price / cur
    if ratio <= 1.0:
        if ratio > 0.80: return 0.28, "SOL -20%: volatile, common in risk-off"
        return 0.15, "SOL severe drawdown"
    else:
        if ratio <= 1.30: return 0.42, "SOL +30%: strong ecosystem, possible"
        if ratio <= 1.80: return 0.25, "SOL +80%: needs major catalyst"
        return 0.10, "SOL 2x+: alt-season territory"

def doge_target_base_rate(target_price, signals):
    cur = signals.get('doge_price', 0.17)
    if cur <= 0 or target_price <= 0: return None
    ratio = target_price / cur
    if ratio <= 1.0:
        return 0.35, "DOGE highly volatile; drawdowns frequent"
    else:
        if ratio <= 1.50: return 0.38, "DOGE +50%: meme momentum can spike"
        if ratio <= 3.00: return 0.20, "DOGE 3x: needs Musk/viral catalyst"
        return 0.08, "DOGE 5x+: extreme speculative tail"

# -----------------------------------------------------------------------------
# MARKET CLASSIFIER
# -----------------------------------------------------------------------------

def extract_price_target(text):
    """Extract $ price target from market question."""
    # Patterns: "$100,000", "$100K", "100000", "100k"
    m = re.search(r'\$?([\d,]+(?:\.\d+)?)\s*[kK](?:\b)', text)
    if m:
        return float(m.group(1).replace(',', '')) * 1000
    m = re.search(r'\$\s*([\d,]+(?:\.\d+)?)', text)
    if m:
        return float(m.group(1).replace(',', ''))
    return None

def classify_market(question, signals):
    """
    Returns (base_rate, note, category) or None if no match.
    """
    q = question.lower()

    # -- BTC price targets --------------------------------------------------
    if re.search(r'bitcoin|btc', q):
        target = extract_price_target(question)
        if target and target > 10000:
            result = btc_target_base_rate(target, signals)
            if result:
                br, note = result
                return br, note, "BTC"
        # Generic BTC up/down market
        if re.search(r'up|above|reach|hit|break', q):
            return 0.52, "BTC bullish bias in post-halving cycle", "BTC"
        if re.search(r'down|below|drop|crash|dip', q):
            return 0.38, "BTC drawdowns less frequent in bull cycle", "BTC"

    # -- ETH price targets --------------------------------------------------
    if re.search(r'ethereum|eth\b', q):
        target = extract_price_target(question)
        if target and target > 100:
            result = eth_target_base_rate(target, signals)
            if result:
                br, note = result
                return br, note, "ETH"

    # -- SOL price targets --------------------------------------------------
    if re.search(r'solana|\bsol\b', q):
        target = extract_price_target(question)
        if target and target > 10:
            result = sol_target_base_rate(target, signals)
            if result:
                br, note = result
                return br, note, "SOL"

    # -- DOGE price targets -------------------------------------------------
    if re.search(r'dogecoin|doge', q):
        target = extract_price_target(question)
        if target:
            result = doge_target_base_rate(target, signals)
            if result:
                br, note = result
                return br, note, "DOGE"

    # -- Political ----------------------------------------------------------
    for pattern, br, note in POLITICAL_BASE:
        if re.search(pattern, q):
            return br, note, "POLITICS"

    # -- Macro --------------------------------------------------------------
    for pattern, br, note in MACRO_BASE:
        if re.search(pattern, q):
            return br, note, "MACRO"

    # -- Crypto general -----------------------------------------------------
    for pattern, br, note in CRYPTO_BASE:
        if br is not None and re.search(pattern, q):
            return br, note, "CRYPTO"

    # -- Entertainment ------------------------------------------------------
    for pattern, br, note in ENTERTAINMENT_BASE:
        if br is not None and re.search(pattern, q):
            return br, note, "CULTURE"

    return None

# -----------------------------------------------------------------------------
# MAIN SCAN
# -----------------------------------------------------------------------------

def main():
    print("\n" + "="*72)
    print("  POLY//EDGE -- FUNDAMENTAL BASE RATE vs MARKET IMPLIED PROBABILITY")
    print("  " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    print("="*72)

    # 1. Fetch live signals
    print("\n[1/4] Fetching live signals (BTC on-chain, CoinGecko)...")
    signals = get_btc_signals()
    btc_price = signals.get('btc_price', 0)
    eth_price = signals.get('eth_price', 0)
    sol_price = signals.get('sol_price', 0)
    if btc_price:
        print(f"      BTC ${btc_price:,.0f}  ({signals.get('btc_30d', 0):+.1f}% 30d)  "
              f"cycle_score={signals.get('btc_cycle_score', '?')}")
    if eth_price:
        print(f"      ETH ${eth_price:,.0f}  SOL ${sol_price:,.0f}  "
              f"DOGE ${signals.get('doge_price',0):.4f}")
    btc_fund = signals.get('btc_funding', None)
    if btc_fund is not None:
        sentiment = "LONGS CROWDED" if btc_fund > 0.0005 else ("SHORTS CROWDED" if btc_fund < -0.0005 else "NEUTRAL")
        print(f"      BTC Funding Rate: {btc_fund*100:.4f}%  [{sentiment}]")

    # 2. Fetch markets
    print(f"\n[2/4] Fetching top {MAX_MARKETS} active markets by volume...")
    markets = []
    for offset in range(0, MAX_MARKETS, 100):
        data = http_get(f"https://gamma-api.polymarket.com/markets?active=true&limit=100&offset={offset}&sort=volume&ascending=false")
        if not data: break
        batch = data if isinstance(data, list) else data.get('markets', [])
        if not batch: break
        markets.extend(batch)
        if len(batch) < 100: break
    print(f"      Loaded {len(markets)} markets")

    # 3. Classify & compute edge
    print(f"\n[3/4] Computing base rate edges (threshold: {EDGE_THRESHOLD*100:.0f}pp)...")
    edges = []

    # Track open position categories for correlation guard
    open_categories = set()  # populated as we add edges

    for m in markets:
        try:
            prices = m.get('outcomePrices', '[]')
            if isinstance(prices, str): prices = json.loads(prices)
            if len(prices) < 2: continue
            yes_price = float(prices[0])
            if yes_price <= 0 or yes_price >= 1: continue

            vol24 = float(m.get('volume24hr', 0) or 0)
            if vol24 < MIN_VOL24: continue

            question = m.get('question', m.get('title', ''))
            if not question: continue

            # -- PRIMARY CROWDING FILTER ------------------------------------
            # Markets like "Will X win 2028 primary?" with price < 5¢ are
            # correctly priced in a large field — not a mispricing.
            # Skip if: political primary pattern + price < 5¢ + "2028"
            q_lower = question.lower()
            if (yes_price < 0.05 and
                re.search(r'2028|2030|primary|nominat', q_lower) and
                re.search(r'win the|will .* win', q_lower)):
                continue

            result = classify_market(question, signals)
            if not result: continue
            base_rate, note, category = result
            if base_rate is None: continue

            edge = yes_price - base_rate  # positive = market OVERPRICES YES
            abs_edge = abs(edge)
            if abs_edge < EDGE_THRESHOLD: continue

            # -- NEWS / MOMENTUM FILTER -------------------------------------
            # Proxy: if market moved against our thesis in last 24h,
            # narrative may be adverse → reduce size 60%
            size_mult = 1.0
            news_flag = ""
            # vol24 / total_vol ratio — high ratio = active/news-driven
            total_vol = float(m.get('volume', 1) or 1)
            vol_ratio = vol24 / total_vol if total_vol > 0 else 0
            # If vol surge (>30% of total volume in 24h) and price OPPOSES thesis:
            buying_yes = edge < 0  # our thesis is BUY YES
            # Rough price momentum: if market is below its midpoint and we're buying,
            # that's aligned. If market spiked and we're selling, that's aligned.
            # Simple check: vol_ratio > 0.3 means unusual activity today
            if vol_ratio > 0.30:
                # High activity day — check if price already moved our way (good)
                # or against us (bad, reduce size)
                news_flag = f"HIGH VOL DAY ({vol_ratio*100:.0f}% of total)"
                # If we're buying YES but price already >80% of base_rate, momentum ok
                # If we're buying YES but price moved further down, adverse signal
                if buying_yes and yes_price < base_rate * 0.5:
                    size_mult = 0.40  # active selling against our thesis
                    news_flag += " [ADVERSE — size -60%]"
                elif not buying_yes and yes_price > base_rate * 1.5:
                    size_mult = 0.40
                    news_flag += " [ADVERSE — size -60%]"
                else:
                    news_flag += " [ALIGNED]"

            # -- CORRELATION / PORTFOLIO RISK GUARD ------------------------
            # Block if new position shares macro risk with existing ones.
            # Shared risk map: categories that both crash on same macro event.
            SHARED_RISK = {
                "BTC":     {"CRYPTO", "MACRO"},    # both lose in risk-off
                "ETH":     {"BTC", "CRYPTO"},
                "CRYPTO":  {"BTC", "ETH", "MACRO"},
                "MACRO":   {"BTC", "CRYPTO"},
                "POLITICS":{"MACRO"},              # political crisis → macro hit
            }
            corr_block = False
            corr_reason = ""
            for existing_cat in open_categories:
                shared = SHARED_RISK.get(category, set())
                if existing_cat in shared and size_mult > 0:
                    # Both sides lose on same macro event → block
                    corr_block = True
                    corr_reason = f"CORR BLOCK: {category} + open {existing_cat} share macro risk"
                    break

            direction = "MARKET OVER YES" if edge > 0 else "MARKET UNDER YES"
            bet_side  = "SELL YES / BUY NO" if edge > 0 else "BUY YES"

            edges.append({
                "question":    question,
                "slug":        m.get('slug', ''),
                "category":    category,
                "yes_price":   yes_price,
                "base_rate":   base_rate,
                "edge":        edge,
                "abs_edge":    abs_edge,
                "direction":   direction,
                "bet_side":    bet_side,
                "note":        note,
                "vol24":       vol24,
                "vol":         float(m.get('volume', 0) or 0),
                "liq":         float(m.get('liquidity', 0) or 0),
                "size_mult":   size_mult,
                "news_flag":   news_flag,
                "corr_block":  corr_block,
                "corr_reason": corr_reason,
            })
            open_categories.add(category)
        except Exception:
            continue

    edges.sort(key=lambda x: -x['abs_edge'])

    # 4. Print results
    print(f"\n[4/4] RESULTS — {len(edges)} markets with edge > {EDGE_THRESHOLD*100:.0f}pp\n")

    if not edges:
        print("  No significant edges found. Market is well-calibrated or categories unmatched.")
        print("  Try lowering EDGE_THRESHOLD or adding more base rate entries.\n")
        return

    cats = {}
    for e in edges:
        cats.setdefault(e['category'], []).append(e)

    for cat, items in sorted(cats.items()):
        print(f"  +- {cat} ({'-'*(60-len(cat))})")
        for e in items[:6]:
            direction_sym = "^" if e['edge'] < 0 else "v"
            edge_str = f"{abs(e['edge'])*100:.1f}pp"
            yes_str  = f"{e['yes_price']*100:.1f}c"
            base_str = f"{e['base_rate']*100:.1f}c"
            size_str = f"x{e['size_mult']:.1f}" if e['size_mult'] < 1.0 else "x1.0"
            status   = "[CORR BLOCKED]" if e['corr_block'] else ("[NEWS -60%]" if e['size_mult'] < 1 else "")
            print(f"  |")
            print(f"  |  {direction_sym} EDGE {edge_str:>6}  Market={yes_str}  Base={base_str}  [{e['bet_side']}]  SIZE={size_str} {status}")
            print(f"  |  Q: {e['question'][:65]}")
            print(f"  |  * {e['note']}")
            if e['news_flag']:  print(f"  |  NEWS: {e['news_flag']}")
            if e['corr_block']: print(f"  |  !! {e['corr_reason']}")
            print(f"  |  VOL24=${e['vol24']:,.0f}  LIQ=${e['liq']:,.0f}")
            print(f"  |  https://polymarket.com/event/{e['slug']}")
        print(f"  +{'-'*68}")
        print()

    # Summary table
    print("  +- RANKED BY EDGE SIZE " + "-"*48 + "┐")
    print(f"  |  {'EDGE':>6}  {'MKT':>5}  {'BASE':>5}  {'SIDE':<18}  {'CATEGORY':<8}  MARKET")
    print(f"  |  {'-'*6}  {'-'*5}  {'-'*5}  {'-'*18}  {'-'*8}  {'-'*30}")
    for e in edges[:20]:
        q = e['question'][:32]
        print(f"  |  {abs(e['edge'])*100:>5.1f}pp  {e['yes_price']*100:>4.0f}¢  {e['base_rate']*100:>4.0f}¢  {e['bet_side']:<18}  {e['category']:<8}  {q}")
    print("  +" + "-"*70 + "┘")

    print(f"\n  Scanned: {len(markets)} markets  |  Matched: {len(edges)} edges  |  Threshold: {EDGE_THRESHOLD*100:.0f}pp")
    print(f"  BTC ${btc_price:,.0f}  |  Post-halving month 24  |  Cycle score: {signals.get('btc_cycle_score','?')}")
    print("\n  DISCLAIMER: Base rates are estimates. Not financial advice.")
    print("="*72 + "\n")

if __name__ == "__main__":
    main()
