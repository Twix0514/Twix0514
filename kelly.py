"""
POLY//KELLY — Adaptive position sizing engine
=============================================
1. Temporal bias   — BTC win-rate by hour/day, only trade >65% windows
2. Kelly criterion — f* = (bp - q) / b, half-Kelly applied
3. Volatility filter — skip noisy conditions (BTC vol > threshold)
4. Bayesian updater — Beta conjugate prior, improves after every resolved trade

Persistence: all state saved to kelly_state.json in same directory.
"""

import json, math, pathlib, time, urllib.request
from datetime import datetime, timezone

STATE_FILE = pathlib.Path(__file__).parent / "kelly_state.json"
HALF_KELLY = 0.5   # always use half-Kelly for safety
MIN_EDGE   = 0.05  # minimum Kelly fraction to bother trading (5% of bankroll)
MAX_FRAC   = 0.25  # hard cap: never risk more than 25% of bankroll on one bet

# ── DEFAULT STATE ─────────────────────────────────────────────────────────────
def _default_state():
    return {
        # Bayesian Beta priors per category: {cat: [alpha, beta]}
        # Alpha = wins + 1, Beta = losses + 1 (Jeffreys prior)
        "priors": {
            "BTC":      [2, 2],   # start neutral
            "ETH":      [2, 2],
            "SOL":      [2, 2],
            "DOGE":     [2, 2],
            "POLITICS": [2, 2],
            "MACRO":    [2, 2],
            "CRYPTO":   [2, 2],
            "CULTURE":  [2, 2],
            "SIGNAL":   [2, 2],
            "COPY":     [2, 2],
        },
        # Temporal bias: win counts by UTC hour [0-23] and day [0=Mon..6=Sun]
        # {hour: [wins, total], day: [wins, total]}
        "hourly": {str(h): [0, 0] for h in range(24)},
        "daily":  {str(d): [0, 0] for d in range(7)},
        # Trade history for audit
        "trades": [],
        # Volatility baseline (BTC 24h change % EMA)
        "vol_ema": 0.0,
        "vol_ema_alpha": 0.2,
    }

# ── PERSISTENCE ───────────────────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        try:
            s = json.loads(STATE_FILE.read_text())
            # Merge missing keys from default
            d = _default_state()
            for k, v in d.items():
                if k not in s:
                    s[k] = v
                elif isinstance(v, dict):
                    for kk, vv in v.items():
                        if kk not in s[k]:
                            s[k][kk] = vv
            return s
        except Exception:
            pass
    return _default_state()

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ── VOLATILITY FILTER ─────────────────────────────────────────────────────────
def fetch_btc_vol():
    """Fetch BTC 24h % change as volatility proxy."""
    try:
        req = urllib.request.Request(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.loads(r.read())
        return abs(float(data["bitcoin"].get("usd_24h_change", 0) or 0))
    except Exception:
        return None

def update_vol_ema(state):
    vol = fetch_btc_vol()
    if vol is not None:
        alpha = state["vol_ema_alpha"]
        state["vol_ema"] = alpha * vol + (1 - alpha) * state["vol_ema"]
        save_state(state)
    return state["vol_ema"]

def vol_ok(state, threshold=6.0):
    """
    Returns True if volatility is within acceptable range.
    Skip if BTC 24h move EMA > threshold% (default 6%).
    """
    vol = state.get("vol_ema", 0)
    if vol == 0:
        return True   # no data yet — allow
    ok = vol < threshold
    return ok, round(vol, 2)

# ── TEMPORAL BIAS ─────────────────────────────────────────────────────────────
def temporal_edge(state, min_confidence=0.65, min_trades=5):
    """
    Returns (edge_ok: bool, win_rate: float, reason: str)
    Based on current UTC hour and day of week win rates.
    """
    now = datetime.now(timezone.utc)
    h   = str(now.hour)
    d   = str(now.weekday())

    hourly = state["hourly"].get(h, [0, 0])
    daily  = state["daily"].get(d, [0, 0])

    h_wins, h_total = hourly
    d_wins, d_total = daily

    # Need minimum trades before trusting the signal
    if h_total < min_trades and d_total < min_trades:
        return True, 0.0, f"insufficient data (hour={h_total} trades, day={d_total} trades) — allowing"

    rates = []
    if h_total >= min_trades:
        rates.append(h_wins / h_total)
    if d_total >= min_trades:
        rates.append(d_wins / d_total)

    win_rate = sum(rates) / len(rates) if rates else 0.5

    if win_rate >= min_confidence:
        return True,  win_rate, f"temporal OK — hour={h_wins}/{h_total} day={d_wins}/{d_total} ({win_rate*100:.0f}%)"
    else:
        return False, win_rate, f"temporal SKIP — win rate {win_rate*100:.0f}% < {min_confidence*100:.0f}% threshold"

# ── BAYESIAN UPDATER ──────────────────────────────────────────────────────────
def bayesian_prob(state, category):
    """
    Return posterior mean probability of winning for this category.
    Uses Beta distribution mean: alpha / (alpha + beta)
    """
    prior = state["priors"].get(category, [2, 2])
    alpha, beta = prior
    return alpha / (alpha + beta)

def update_prior(state, category, won: bool):
    """
    Update Beta prior after trade resolves.
    Won=True: alpha += 1. Won=False: beta += 1.
    """
    if category not in state["priors"]:
        state["priors"][category] = [2, 2]
    if won:
        state["priors"][category][0] += 1
    else:
        state["priors"][category][1] += 1
    save_state(state)

def record_temporal(state, won: bool):
    """Update hourly and daily win tracking."""
    now = datetime.now(timezone.utc)
    h, d = str(now.hour), str(now.weekday())
    for key, bucket in [("hourly", h), ("daily", d)]:
        rec = state[key].get(bucket, [0, 0])
        rec[1] += 1
        if won: rec[0] += 1
        state[key][bucket] = rec
    save_state(state)

# ── KELLY CALCULATOR ──────────────────────────────────────────────────────────
def kelly_size(portfolio_usd, p_win, yes_price, min_trade_usd=2.0):
    """
    Full Kelly: f* = (bp - q) / b
      b = net odds = (1 - yes_price) / yes_price   (payout per $ risked on YES)
      p = probability of YES resolving True
      q = 1 - p

    Returns:
      fraction  — Kelly fraction (0 to MAX_FRAC)
      size_usd  — dollar amount to bet
      size_shares — shares at current price
      skip      — True if edge is negative or too small
      reason    — explanation string
    """
    if yes_price <= 0 or yes_price >= 1:
        return 0, 0, 0, True, "invalid price"

    b = (1 - yes_price) / yes_price   # net payout ratio
    q = 1 - p_win
    f_full = (b * p_win - q) / b      # Kelly formula

    if f_full <= 0:
        return 0, 0, 0, True, f"negative edge: f*={f_full:.3f} (p={p_win:.2f}, b={b:.2f})"

    f_half = f_full * HALF_KELLY                     # half-Kelly
    f_capped = min(f_half, MAX_FRAC)                 # hard cap

    if f_capped < MIN_EDGE:
        return f_capped, 0, 0, True, f"edge too small: f*={f_full:.3f} -> half={f_half:.3f} < {MIN_EDGE} minimum"

    size_usd    = max(min_trade_usd, portfolio_usd * f_capped)
    size_shares = max(5.0, round(size_usd / yes_price, 2))

    reason = (f"Kelly f*={f_full:.3f} -> half={f_half:.3f} -> capped={f_capped:.3f} "
              f"-> ${size_usd:.2f} ({size_shares} shares @ {yes_price})")

    return f_capped, size_usd, size_shares, False, reason

# ── COMBINED GATE ─────────────────────────────────────────────────────────────
def evaluate_trade(portfolio_usd, yes_price, category, source="SIGNAL",
                   min_temporal_wr=0.65, max_vol=6.0):
    """
    Full pre-trade evaluation.

    Returns dict:
      allowed   bool
      size_usd  float
      size_shares float
      p_win     float  (Bayesian posterior)
      kelly_f   float
      reasons   list[str]
    """
    state   = load_state()
    reasons = []
    allowed = True

    # 1. Volatility filter
    v_ok, vol = vol_ok(state, threshold=max_vol)
    if not v_ok:
        reasons.append(f"VOL SKIP — BTC 24h move {vol}% > {max_vol}% limit")
        allowed = False
    else:
        reasons.append(f"vol OK ({vol}%)")

    # 2. Temporal bias
    t_ok, wr, t_reason = temporal_edge(state, min_confidence=min_temporal_wr)
    reasons.append(t_reason)
    if not t_ok:
        allowed = False

    # 3. Bayesian win probability
    p_win = bayesian_prob(state, category)
    reasons.append(f"Bayesian p_win={p_win:.3f} (category={category})")

    # 4. Kelly sizing
    kelly_f, size_usd, size_shares, skip, k_reason = kelly_size(
        portfolio_usd, p_win, yes_price
    )
    reasons.append(k_reason)
    if skip:
        allowed = False

    return {
        "allowed":      allowed,
        "size_usd":     size_usd,
        "size_shares":  size_shares,
        "p_win":        p_win,
        "kelly_f":      kelly_f,
        "vol":          vol,
        "temporal_wr":  wr,
        "reasons":      reasons,
        "state":        state,
    }

def record_trade_result(category, won: bool):
    """Call this when a position resolves. Updates Bayesian prior + temporal tracking."""
    state = load_state()
    update_prior(state, category, won)
    record_temporal(state, won)
    save_state(state)

# ── CLI REPORT ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    state = load_state()
    print("\n POLY//KELLY — System State Report")
    print("=" * 50)

    print("\n Bayesian Priors (posterior win rate per category):")
    for cat, (a, b) in state["priors"].items():
        wr   = a / (a + b)
        conf = a + b - 2  # number of real trades
        bar  = "#" * int(wr * 20)
        print(f"  {cat:<10} {wr*100:5.1f}%  [{bar:<20}]  n={conf}")

    print("\n Temporal Win Rates (UTC hour):")
    hot = []
    for h, (w, t) in state["hourly"].items():
        if t >= 3:
            wr = w / t
            tag = " <<< HOT" if wr >= 0.65 else ""
            print(f"  Hour {int(h):02d}:00  {wr*100:.0f}%  ({w}/{t}){tag}")
            if wr >= 0.65: hot.append(int(h))

    if hot:
        print(f"\n  Best trading hours (>65%): {sorted(hot)}")

    vol = state.get("vol_ema", 0)
    print(f"\n Volatility EMA: {vol:.2f}%  ({'OK' if vol < 6 else 'SKIP'})")

    print("\n Kelly example (p=0.65, price=0.40, portfolio=$100):")
    f, usd, shares, skip, reason = kelly_size(100, 0.65, 0.40)
    print(f"  {reason}")
    print()
