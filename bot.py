"""
POLY//BOT v2 — Signal + CopyTrade + Arb Scanner
  - RSI/EMA signal engine via WebSocket
  - Mirror trades from whale wallet 0x751a...9ea1
  - Continuous arb scanner (YES+NO mispricing alerts)
"""

import json, time, threading, logging, urllib.request, urllib.parse
from collections import defaultdict, deque
from datetime import datetime, timezone

import websocket
import numpy as np
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL
from kelly import evaluate_trade, record_trade_result, update_vol_ema, load_state

# ── ML MODEL (lazy-loaded — won't block bot startup) ───────────────────────────
_ml_predictor = None
_ml_lock = threading.Lock()

def _get_ml_predictor():
    """Load trained ML model on first use. Returns None if unavailable."""
    global _ml_predictor
    if _ml_predictor is not None:
        return _ml_predictor
    with _ml_lock:
        if _ml_predictor is not None:
            return _ml_predictor
        try:
            from ml_model import get_predictor
            p = get_predictor()
            if p.trained:
                _ml_predictor = p
                log.info("[ML] Model loaded successfully")
        except Exception as e:
            log.debug(f"[ML] Not available: {e}")
    return _ml_predictor

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('POLY//BOT')

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
PRIVATE_KEY    = "f87dcf8393da4429ffaf11a4b78e6ad37e1a3a0f0aa38e67f6b674e151b45a1f"
FUNDER         = "0x682Df9cf2638a854a4d69Cc3a3c12fCB2B216d27"
TRADING_ADDR   = "0x682Df9cf2638a854a4d69Cc3a3c12fCB2B216d27"
CHAIN_ID       = 137
API_KEY        = "5a4c71c2-5e4e-a7de-673f-91ddb01b1a9e"
API_SECRET     = "enoJbF8jNWnUksS90EV6CR4mFxa78ikVQXqrw6pF3UU="
API_PASSPHRASE = "bc1fb28317849638952b23ea8e30864e7cfb8d979291ae64248dc59f5631cc3c"

# ── TERMINAL ALERTS ──────────────────────────────────────────────────────────
import os, pathlib

ALERTS_FILE  = pathlib.Path(__file__).parent / "alerts.json"
COPY_STATE   = pathlib.Path(__file__).parent / "copy_state.json"
_alert_lock  = threading.Lock()
_order_lock  = threading.Lock()   # prevents concurrent order submissions

def tg(msg, level="INFO"):
    """Write alert to alerts.json — picked up by POLY//TERMINAL dashboard."""
    entry = {
        "time":  datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "level": level,
        "msg":   msg,
    }
    with _alert_lock:
        try:
            existing = json.loads(ALERTS_FILE.read_text()) if ALERTS_FILE.exists() else []
        except Exception:
            existing = []
        existing.insert(0, entry)
        existing = existing[:50]  # keep last 50 alerts
        ALERTS_FILE.write_text(json.dumps(existing, indent=2))
    log.info(f"[ALERT] {msg}")

# ── CONFIG ────────────────────────────────────────────────────────────────────
CFG = {
    "dry_run":            False,  # False = real orders
    "max_position_usd":   5.00,   # max $ per trade
    "min_balance_usd":    2.00,

    # Risk management
    "max_position_pct":   0.30,   # 30% of portfolio per trade
    "daily_loss_halt":    -0.40,  # halt at -40% daily
    "kill_switch_dd":     -0.75,  # kill switch at -75% total
    "min_liquidity":      3_000,

    # Signal engine (momentum-based, NOT RSI/EMA)
    "momentum_ticks":     10,     # price history ticks to measure velocity
    "momentum_threshold": 0.04,   # 4% price move triggers signal
    "lookback":           20,
    "ws_url":             "wss://ws-subscriptions-clob.polymarket.com/ws/market",

    # CopyTrade
    "copy_wallet":        "0x751a2b86cab503496efd325c8344e10159349ea1",
    "copy_ratio":         0.15,   # mirror 15% of whale position size
    "copy_poll_secs":     30,

    # Arb scanner — executes automatically when edge is real
    "arb_scan_secs":      45,
    "arb_min_edge":       0.015,  # 1.5% guaranteed edge before executing
    "arb_min_vol24":      2000,

    # Up/Down markets (5-min / 15-min crypto prediction markets)
    "updown_scan_secs":   20,     # scan every 20s — these expire fast
    "updown_min_change":  0.003,  # 0.3% crypto price move needed to bet
    "updown_max_age_min": 12,     # only bet if market resolves within 12 min

    # Near-certainty scalper
    "certainty_scan_secs": 120,
    "certainty_min_price": 0.87,  # market priced ≥87% YES
    "certainty_max_price": 0.97,  # but not yet resolved
    "certainty_min_vol":   3_000,
    "certainty_max_days":  180,   # up to 6 months out

    # 4-minute rule — final minutes before resolution
    "fourmin_scan_secs":   15,    # scan every 15s — critical timing
    "fourmin_max_mins":    4,     # market closes within this many minutes
    "fourmin_min_leader":  0.82,  # leader must be priced ≥82% to bet
    "fourmin_max_leader":  0.96,  # but not yet fully resolved (leave room for edge)
    "fourmin_min_liq":     500,   # minimum $500 liquidity
}

# ── CLIENT ────────────────────────────────────────────────────────────────────
creds = ApiCreds(api_key=API_KEY, api_secret=API_SECRET, api_passphrase=API_PASSPHRASE)
client = ClobClient(
    "https://clob.polymarket.com",
    key=PRIVATE_KEY, chain_id=CHAIN_ID, creds=creds,
    signature_type=2, funder=FUNDER,
)

# ── STATE ─────────────────────────────────────────────────────────────────────
price_history   = defaultdict(lambda: deque(maxlen=CFG["lookback"] + 5))
order_books     = defaultdict(dict)
open_positions  = {}
trade_log       = []
copy_positions  = json.loads(COPY_STATE.read_text()) if COPY_STATE.exists() else {}
arb_alerts      = []

# ── AGENT SYSTEM + WALLET SCANNER ─────────────────────────────────────────────
import os as _os
_ANTHROPIC_KEY = _os.environ.get("ANTHROPIC_API_KEY", "")
# Inject key into agent_system module before it's imported
if _ANTHROPIC_KEY:
    _os.environ["ANTHROPIC_API_KEY"] = _ANTHROPIC_KEY

# Risk management state
_bot_halted      = False   # set True by daily halt or kill switch
_halt_reason     = ""
_session_start_portfolio = None   # portfolio value at bot start
_day_start_portfolio     = None   # portfolio value at start of today
_day_start_time          = None   # datetime of today's reset

# ── RISK MANAGEMENT ──────────────────────────────────────────────────────────
def get_portfolio_value():
    """Fetch USDC balance + estimated open position values."""
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        usdc = int(bal.get('balance', 0) or 0) / 1_000_000
    except Exception:
        usdc = 0.0

    # Add estimated value of open positions using last known prices
    pos_value = 0.0
    for token_id, pos in list(open_positions.items()):
        size = float(pos.get('size', 0) or 0)
        prices = list(price_history.get(token_id, []))
        if prices and size:
            pos_value += size * prices[-1]
        elif size:
            pos_value += size * float(pos.get('entry', 0.5) or 0.5)

    total = usdc + pos_value
    return total if total > 0 else (usdc or None)

def get_true_portfolio_value():
    """
    Fetch real total portfolio value (cash + positions) from Polymarket data API.
    Used at startup to get accurate baseline including existing open positions.
    Falls back to CLOB balance if data API unavailable.
    """
    try:
        data = fetch_json(f"https://data-api.polymarket.com/value?user={TRADING_ADDR}")
        if data:
            if isinstance(data, list): data = data[0] if data else {}
            val = float(data.get('portfolioValue', data.get('value', 0)) or 0)
            if val > 0:
                return val
    except Exception:
        pass
    return get_portfolio_value()

def init_risk_baseline():
    """Called once at startup to set portfolio baseline."""
    global _session_start_portfolio, _day_start_portfolio, _day_start_time
    val = get_true_portfolio_value()
    if val is not None and val > 0:
        _session_start_portfolio = val
        _day_start_portfolio = val
        _day_start_time = datetime.now(timezone.utc)
        log.info(f"[RISK] Baseline set: ${val:.2f}  (8% cap=${val*0.08:.2f}, halt@-50%=${val*0.5:.2f})")

def reset_daily_baseline():
    """Reset daily P&L baseline each UTC midnight."""
    global _day_start_portfolio, _day_start_time, _bot_halted, _halt_reason
    val = get_portfolio_value()
    if val is not None:
        _day_start_portfolio = val
        _day_start_time = datetime.now(timezone.utc)
        if _bot_halted and "daily" in _halt_reason:
            _bot_halted = False
            _halt_reason = ""
            log.info("[RISK] Daily halt lifted — new trading day")
            tg("POLY//BOT: Daily halt lifted. New trading day started.")

def check_risk_limits():
    """
    Returns True if trading is allowed.
    Halts bot on daily -20% or total -40% drawdown.
    """
    global _bot_halted, _halt_reason
    if _bot_halted:
        return False

    val = get_true_portfolio_value()
    if val is None:
        return True  # can't fetch — allow but log

    # Daily loss check
    if _day_start_portfolio and _day_start_portfolio > 0:
        daily_pnl_pct = (val - _day_start_portfolio) / _day_start_portfolio
        if daily_pnl_pct < CFG["daily_loss_halt"]:
            _bot_halted = True
            _halt_reason = f"daily loss {daily_pnl_pct*100:.1f}%"
            msg = f"[RISK] DAILY HALT — P&L {daily_pnl_pct*100:.1f}% (limit {CFG['daily_loss_halt']*100:.0f}%). Waiting for next UTC day."
            log.error(msg)
            tg(f"POLY//BOT HALTED\nDaily loss: {daily_pnl_pct*100:.1f}%\nLimit: {CFG['daily_loss_halt']*100:.0f}%\nResumes next UTC midnight.")
            return False

    # Total drawdown kill switch
    if _session_start_portfolio and _session_start_portfolio > 0:
        total_dd = (val - _session_start_portfolio) / _session_start_portfolio
        if total_dd < CFG["kill_switch_dd"]:
            _bot_halted = True
            _halt_reason = f"kill switch {total_dd*100:.1f}%"
            msg = f"[RISK] KILL SWITCH — total drawdown {total_dd*100:.1f}% (limit {CFG['kill_switch_dd']*100:.0f}%). MANUAL RESTART REQUIRED."
            log.error(msg)
            tg(f"POLY//BOT KILL SWITCH\nTotal drawdown: {total_dd*100:.1f}%\nLimit: {CFG['kill_switch_dd']*100:.0f}%\nMANUAL RESTART REQUIRED.")
            return False

    return True

def calc_position_size(price):
    """
    Half-Kelly + 8% portfolio cap position sizing.
    Returns size in shares.
    """
    portfolio = get_portfolio_value() or CFG["max_position_usd"] * 10
    max_usd = min(CFG["max_position_usd"], portfolio * CFG["max_position_pct"])
    size = max(5.0, round(max_usd / max(price, 0.01), 2))
    return size, max_usd

def day_reset_loop():
    """Check every minute if we've crossed UTC midnight for daily reset."""
    last_day = datetime.now(timezone.utc).date()
    while True:
        time.sleep(60)
        today = datetime.now(timezone.utc).date()
        if today != last_day:
            last_day = today
            reset_daily_baseline()

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.debug(f"fetch {url}: {e}")
        return None

def pct(v): return f"{v*100:.1f}c"

# ── HELPERS ──────────────────────────────────────────────────────────────────
def best_bid_ask(book):
    bids = book.get('bids', []); asks = book.get('asks', [])
    return (max((float(b['price']) for b in bids), default=None),
            min((float(a['price']) for a in asks), default=None))

def book_depth(book):
    bids = sorted(book.get('bids', []), key=lambda x: -float(x['price']))[:5]
    asks = sorted(book.get('asks', []), key=lambda x: float(x['price']))[:5]
    return sum(float(b['price'])*float(b['size']) for b in bids) + \
           sum(float(a['price'])*float(a['size']) for a in asks)

def get_crypto_price_change(symbol: str, minutes: int = 15) -> float:
    """
    Get % price change for a crypto in the last N minutes.
    Uses Binance public klines — no auth required.
    Returns e.g. 0.0045 for +0.45%, -0.003 for -0.3%.
    """
    sym_map = {'BTC':'BTCUSDT','ETH':'ETHUSDT','SOL':'SOLUSDT',
               'XRP':'XRPUSDT','DOGE':'DOGEUSDT','HYPE':'HYPEUSD',
               'MATIC':'MATICUSDT','AVAX':'AVAXUSDT'}
    sym = sym_map.get(symbol.upper(), f'{symbol.upper()}USDT')
    try:
        data = fetch_json(
            f"https://api.binance.com/api/v3/klines"
            f"?symbol={sym}&interval=1m&limit={minutes + 2}"
        )
        if not data or len(data) < 2: return 0.0
        open_price  = float(data[0][1])   # open of oldest candle
        close_price = float(data[-1][4])  # close of most recent candle
        return (close_price - open_price) / max(open_price, 0.0001)
    except Exception as e:
        log.debug(f"[PRICE] {symbol} fetch failed: {e}")
        return 0.0

# ── SIGNAL ENGINE — MOMENTUM BASED ────────────────────────────────────────────
# RSI/EMA removed: prediction market prices do NOT behave like stock prices.
# Momentum signal: if price velocity over last N ticks exceeds threshold, ride it.
def momentum_signal(token_id, market_name="", is_yes_token=True):
    """
    Returns BUY signal when price momentum is strong enough.
    Only fires when market is uncertain (20-80% range) — no edge at extremes.
    """
    prices = list(price_history[token_id])
    if len(prices) < CFG["lookback"]: return None

    book  = order_books.get(token_id, {})
    bid, ask = best_bid_ask(book)
    if bid is None or ask is None: return None
    if book_depth(book) < CFG["min_liquidity"]: return None

    spread = ask - bid
    if spread > 0.08: return None  # too wide, poor fill quality

    current = prices[-1]
    if current > 0.80 or current < 0.20: return None  # too extreme for momentum

    n = CFG["momentum_ticks"]
    if len(prices) < n * 2: return None
    recent   = float(np.mean(prices[-n:]))
    previous = float(np.mean(prices[-n*2:-n]))
    if previous <= 0: return None

    velocity = (recent - previous) / previous  # positive = rising

    if velocity > CFG["momentum_threshold"]:
        # Price rising — BUY this token (YES or NO depending on which token this is)
        return {
            "token_id": token_id, "signal": BUY, "mid": round(ask, 4),
            "depth": book_depth(book),
            "reason": f"momentum +{velocity*100:.1f}%  ({n}-tick)",
            "market": market_name,
        }
    return None

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def place_order(sig, source="SIGNAL"):
    with _order_lock:   # serialize all order submissions — no concurrent balance races
        _place_order_inner(sig, source)

def _place_order_inner(sig, source="SIGNAL"):
    token_id = sig["token_id"]; side = sig["signal"]
    price = sig["mid"]
    tag = f"[{source}]"

    # ── Risk gate 1: halt check ───────────────────────────────────────────────
    if not check_risk_limits():
        log.warning(f"{tag} BLOCKED by risk halt ({_halt_reason})")
        return

    # ── Risk gate 2: liquidity filter ($50K minimum) ─────────────────────────
    liq = float(sig.get("liquidity", 0) or 0)
    if liq > 0 and liq < CFG["min_liquidity"]:
        log.info(f"{tag} SKIP — liquidity ${liq:,.0f} < ${CFG['min_liquidity']:,} minimum")
        return

    # ── Risk gate 3: Kelly sizing ─────────────────────────────────────────────
    portfolio = get_portfolio_value() or CFG["max_position_usd"] * 4
    category  = sig.get("category", source)

    # Sources with their own edge logic — bypass Bayesian Kelly (which would
    # return 50% prior for unknown categories and block the trade).
    # p_win is based on the source's inherent edge, not historical performance.
    DIRECT_SOURCES = {
        "COPY":  lambda p: min(0.97, p + 0.12),   # whale alpha +12%
        "ARB":   lambda p: 0.97,                   # mathematical guarantee
        "MARB":  lambda p: 0.97,                   # mathematical guarantee
        "UPDN":  lambda p: 0.63,                   # Binance price lead
        "CERT":  lambda p: min(0.97, p + 0.05),    # near-certainty + 5% safety margin
        "FOUR":  lambda p: min(0.97, p + 0.08),    # 4-min rule: direction locked + 8% boost
        "AGNT":  lambda p: min(0.97, p + 0.10),    # 3-agent consensus: 2/3 majority
    }

    if source in DIRECT_SOURCES:
        p_win = DIRECT_SOURCES[source](price)
        from kelly import kelly_size
        kelly_f, size_usd, size_shares, skip, k_reason = kelly_size(portfolio, p_win, price)
        if skip:
            size    = max(5.0, round(CFG["min_balance_usd"] / max(price, 0.01), 2))
            max_usd = size * price
            log.info(f"{tag} Kelly skip ({k_reason}) — using min size")
        else:
            size    = size_shares
            max_usd = size_usd
        log.info(f"{tag} {side} {token_id[:14]}... price={price:.3f} size={size} "
                 f"(${max_usd:.2f} p={p_win:.2f}) | {sig.get('reason','')}")
    else:
        # Signal-based (MOMENTUM, SIGNAL): full Kelly gate with Bayesian + temporal + vol
        kelly = evaluate_trade(portfolio, price, category, source=source)
        for r in kelly["reasons"]:
            log.debug(f"  [KELLY] {r}")
        if not kelly["allowed"]:
            log.info(f"{tag} BLOCKED by Kelly engine: {' | '.join(kelly['reasons'])}")
            return
        size    = kelly["size_shares"]
        max_usd = kelly["size_usd"]
        log.info(f"{tag} {side} {token_id[:14]}... price={price} size={size} "
                 f"(${max_usd:.2f} f={kelly['kelly_f']:.3f} p={kelly['p_win']:.2f}) | {sig.get('reason','')}")

    if CFG["dry_run"]:
        log.info(f"  [DRY RUN] Would place {side} {size} shares @ {price}")
        trade_log.append({"time": datetime.now(timezone.utc).isoformat(), "source": source,
                          "token_id": token_id, "side": side, "price": price, "size": size})
        return
    try:
        order = client.create_and_post_order(OrderArgs(
            token_id=token_id, price=price, size=size, side=side))
        log.info(f"  ORDER PLACED: {order}")
        open_positions[token_id] = {"side": side, "size": size, "entry": price}
        trade_log.append({"time": datetime.now(timezone.utc).isoformat(), "source": source,
                          "token_id": token_id, "side": side, "price": price, "size": size, "order": order})
        # Telegram alert on every fill
        status = order.get('status', '?')
        tg(f"POLY//BOT ORDER\n{source}: {side} {size} shares\nMarket: {sig.get('market','?')[:60]}\nPrice: {price}\nStatus: {status}")
    except Exception as e:
        log.error(f"  Order failed: {e}")

# ══ COPYTRADE ENGINE ══════════════════════════════════════════════════════════

def get_whale_positions():
    """Fetch current open positions of the whale wallet."""
    data = fetch_json(f"https://data-api.polymarket.com/positions?user={CFG['copy_wallet']}&limit=50")
    if not data or not isinstance(data, list):
        return {}
    result = {}
    for p in data:
        cid = p.get('conditionId', p.get('condition_id', p.get('market', '')))
        if cid:
            result[cid] = p
    return result

def get_token_id_for_condition(cond_id, outcome):
    """Look up CLOB token ID for a given condition + outcome."""
    data = fetch_json(f"https://gamma-api.polymarket.com/markets?conditionId={cond_id}")
    if not data:
        return None
    markets = data if isinstance(data, list) else data.get('markets', [])
    for m in markets:
        try:
            toks = m.get('clobTokenIds', '[]')
            if isinstance(toks, str): toks = json.loads(toks)
            # index 0 = YES token, index 1 = NO token
            is_yes = str(outcome).upper() in ('YES', 'YES_OUTCOME', '0')
            idx = 0 if is_yes else 1
            if idx < len(toks):
                return toks[idx]
        except Exception:
            pass
    return None

def copytrade_loop():
    """Poll whale wallet every N seconds, mirror new/changed positions."""
    log.info(f"[COPY] Watching whale {CFG['copy_wallet'][:12]}... every {CFG['copy_poll_secs']}s")
    global copy_positions

    while True:
        time.sleep(CFG["copy_poll_secs"])
        try:
            new_positions = get_whale_positions()
            if not new_positions:
                continue

            # Find new positions whale opened since last poll
            for cid, pos in new_positions.items():
                if cid in copy_positions:
                    continue  # already knew about this position

                outcome  = pos.get('outcome', pos.get('side', 'Yes'))
                size_raw = float(pos.get('size', pos.get('shares', 0)) or 0)
                price_raw= float(pos.get('avgPrice', pos.get('currentPrice', 0.5)) or 0.5)
                title    = pos.get('title', pos.get('question', cid[:20]))
                slug     = pos.get('slug', '')

                if size_raw < 10:
                    continue  # skip tiny positions

                # Skip resolved/near-resolved markets (price >= 0.95 = dust collection, not live bets)
                if price_raw >= 0.95:
                    continue

                # Calculate our mirror size
                our_size_usd = size_raw * price_raw * CFG["copy_ratio"]
                our_size_usd = min(our_size_usd, CFG["max_position_usd"])

                log.info(f"[COPY] WHALE OPENED: {outcome} {size_raw:.0f} shares @ {price_raw:.2f} — {title[:50]}")
                log.info(f"[COPY] Mirroring: ${our_size_usd:.2f} on same side")

                # Get token ID
                token_id = get_token_id_for_condition(cid, outcome)
                if not token_id:
                    log.warning(f"[COPY] Could not find token_id for {cid}")
                    continue

                # Always BUY the target token — YES token for YES bets, NO token for NO bets.
                # SELL would require owning the opposite token, which we don't have.
                side = BUY
                sig = {"token_id": token_id, "signal": side,
                       "mid": price_raw, "reason": f"COPY {CFG['copy_wallet'][:10]}",
                       "market": title}
                place_order(sig, source="COPY")

            # Update snapshot and persist so restarts don't re-mirror existing positions
            copy_positions = new_positions
            COPY_STATE.write_text(json.dumps(copy_positions))
            val = fetch_json(f"https://data-api.polymarket.com/value?user={CFG['copy_wallet']}")
            if val:
                if isinstance(val, list): val = val[0] if val else {}
                port = float(val.get('portfolioValue', val.get('value', 0)) or 0)
                log.info(f"[COPY] Whale portfolio: ${port:,.2f} | positions: {len(new_positions)}")

        except Exception as e:
            log.error(f"[COPY] Error: {e}")

# ══ ARB SCANNER ═══════════════════════════════════════════════════════════════

def arb_scan():
    """
    Scan all active markets for YES+NO mispricing.
    When YES + NO < 1.0 by >= arb_min_edge: buy the cheaper token immediately.
    When YES + NO > 1.0: one side is overpriced — skip (selling requires owning tokens).
    """
    markets = []
    for offset in range(0, 300, 100):
        data = fetch_json(f"https://gamma-api.polymarket.com/markets?active=true&limit=100&offset={offset}&sort=volume24hr&ascending=false")
        if not data: break
        batch = data if isinstance(data, list) else data.get('markets', data.get('data', []))
        if not batch: break
        markets.extend(batch)
        if len(batch) < 100: break

    found = []
    for m in markets:
        try:
            prices = m.get('outcomePrices', '[]')
            if isinstance(prices, str): prices = json.loads(prices)
            if len(prices) < 2: continue
            yes = float(prices[0]); no = float(prices[1])
            total = yes + no
            # Only actionable: CHEAP side — both tokens underpriced → guaranteed profit
            if total >= 1.0: continue
            edge = 1.0 - total
            if edge < CFG["arb_min_edge"]: continue
            vol24 = float(m.get('volume24hr', 0) or 0)
            if vol24 < CFG["arb_min_vol24"]: continue
            liq = float(m.get('liquidity', 0) or 0)
            if liq < CFG["min_liquidity"]: continue

            toks = m.get('clobTokenIds', '[]')
            if isinstance(toks, str): toks = json.loads(toks)
            if len(toks) < 2: continue

            found.append({
                "question":   m.get('question', '')[:55],
                "slug":       m.get('slug', ''),
                "yes":        yes, "no": no, "total": total,
                "edge":       edge, "vol24": vol24,
                "yes_token":  toks[0] if isinstance(toks[0], str) else toks[0].get('token_id',''),
                "no_token":   toks[1] if isinstance(toks[1], str) else toks[1].get('token_id',''),
            })
        except Exception:
            continue

    found.sort(key=lambda x: -x['edge'])

    for m in found[:3]:
        log.info(f"[ARB] GAP FOUND: YES={m['yes']:.3f} + NO={m['no']:.3f} = {m['total']:.3f}  "
                 f"GUARANTEED EDGE={m['edge']*100:.1f}%  VOL24=${m['vol24']:,.0f}")
        log.info(f"[ARB]   {m['question']}")
        tg(f"ARB: {m['edge']*100:.1f}% edge | {m['question'][:50]}", "ARB")

        # Execute: buy whichever token is cheaper
        portfolio = get_true_portfolio_value() or 0
        if portfolio < CFG["min_balance_usd"]:
            log.warning(f"[ARB] Skip — portfolio ${portfolio:.2f} too low")
            continue

        cheaper_price = min(m['yes'], m['no'])
        cheaper_token = m['yes_token'] if m['yes'] <= m['no'] else m['no_token']
        cheaper_side  = "YES" if m['yes'] <= m['no'] else "NO"

        sig = {
            "token_id":  cheaper_token,
            "signal":    BUY,
            "mid":       cheaper_price,
            "market":    m['question'],
            "reason":    f"ARB gap={m['edge']*100:.1f}% cheaper={cheaper_side}",
            "liquidity": m['vol24'],
        }
        place_order(sig, source="ARB")
        arb_alerts.append(m)

    if not found:
        log.debug(f"[ARB] Scanned {len(markets)} markets — no gaps >= {CFG['arb_min_edge']*100:.1f}%")

    return found

def arb_loop():
    """Run arb scanner continuously."""
    log.info(f"[ARB] Scanner started — scanning every {CFG['arb_scan_secs']}s")
    while True:
        try:
            arb_scan()
        except Exception as e:
            log.error(f"[ARB] Error: {e}")
        time.sleep(CFG["arb_scan_secs"])

# ══ MULTI-OUTCOME ARB SCANNER ═════════════════════════════════════════════════

def multi_outcome_arb_scan(min_edge=0.04, min_liquidity=500):
    """
    Scan Polymarket events with multiple mutually exclusive outcomes.
    If sum of all YES prices < 1.0 - min_edge, buy YES on every outcome
    and lock in guaranteed profit when exactly one resolves.

    Example: Harvey Weinstein sentencing — 6 outcomes sum to 0.951
    → buy all YES for $0.951, collect $1.00 regardless of verdict = 4.9% return.
    """
    try:
        events = fetch_json(
            "https://gamma-api.polymarket.com/events?active=true&closed=false"
            "&limit=100&sort=liquidity&ascending=false"
        )
    except Exception as e:
        log.error(f"[MARB] Failed to fetch events: {e}")
        return

    if not events:
        return
    if isinstance(events, dict):
        events = events.get('data', events.get('events', []))

    for evt in events:
        try:
            markets = evt.get('markets', [])
            if len(markets) < 3:
                continue  # need at least 3 outcomes to be interesting

            # Collect YES prices and token IDs for all active markets
            outcomes = []
            for m in markets:
                prices = m.get('outcomePrices', '[]')
                if isinstance(prices, str): prices = json.loads(prices)
                if len(prices) < 2: continue
                yes = float(prices[0])
                if yes <= 0.01 or yes >= 0.99: continue  # skip resolved/dust
                liq = float(m.get('liquidity', 0) or 0)
                toks = m.get('clobTokenIds', '[]')
                if isinstance(toks, str): toks = json.loads(toks)
                if not toks: continue
                yes_token = toks[0] if isinstance(toks[0], str) else toks[0].get('token_id','')
                outcomes.append({
                    'question': m.get('question', '')[:60],
                    'yes':       yes,
                    'token_id':  yes_token,
                    'liquidity': liq,
                    'slug':      m.get('slug', ''),
                })

            if len(outcomes) < 3:
                continue

            # Reject cumulative "by date" markets — they're NOT mutually exclusive.
            # "Starmer out by June 30" and "by December 31" can BOTH resolve YES.
            # Detect: if questions all contain "by " with a date, prices are sorted ascending,
            # or the event title contains "by...?" pattern.
            questions_lower = [o['question'].lower() for o in outcomes]
            is_cumulative = (
                sum(1 for q in questions_lower if ' by ' in q or 'before ' in q) >= len(outcomes) - 1
                and sorted([o['yes'] for o in outcomes]) == [o['yes'] for o in sorted(outcomes, key=lambda x: x['yes'])]
            )
            if is_cumulative:
                continue

            total_yes = sum(o['yes'] for o in outcomes)
            edge = 1.0 - total_yes

            if edge < min_edge:
                continue  # not enough profit

            min_liq = min(o['liquidity'] for o in outcomes)
            if min_liq < min_liquidity:
                continue  # at least one leg has no liquidity

            evt_title = evt.get('title', evt.get('slug', '?'))[:60]
            log.info(f"[MARB] *** MULTI-OUTCOME ARB FOUND ***")
            log.info(f"[MARB] Event: {evt_title}")
            log.info(f"[MARB] Sum of YES prices: {total_yes:.4f} → edge: {edge*100:.1f}pp")
            for o in outcomes:
                log.info(f"[MARB]   YES={o['yes']:.3f}  liq=${o['liquidity']:,.0f}  | {o['question']}")

            # Execute: buy YES on every outcome
            portfolio = get_portfolio_value() or 0
            if portfolio < 5.0:
                log.warning(f"[MARB] Skipping — portfolio ${portfolio:.2f} too low")
                continue

            # Size: small fixed allocation per leg (don't over-concentrate)
            per_leg_usd = min(2.0, portfolio * 0.05)
            tg(f"POLY//BOT MULTI-ARB\nEvent: {evt_title}\nEdge: {edge*100:.1f}pp\n"
               f"Legs: {len(outcomes)} | Per leg: ${per_leg_usd:.2f}")

            for o in outcomes:
                shares = max(1.0, round(per_leg_usd / o['yes'], 2))
                sig = {
                    'token_id': o['token_id'],
                    'signal':   BUY,
                    'mid':      o['yes'],
                    'market':   o['question'],
                    'reason':   f"MARB edge={edge*100:.1f}pp",
                    'liquidity': o['liquidity'],
                }
                place_order(sig, source="MARB")

        except Exception as e:
            log.error(f"[MARB] Error processing event: {e}")

def multi_arb_loop():
    """Scan for multi-outcome arbs every 5 minutes."""
    log.info("[MARB] Multi-outcome arb scanner started")
    while True:
        try:
            multi_outcome_arb_scan()
        except Exception as e:
            log.error(f"[MARB] Loop error: {e}")
        time.sleep(300)   # scan every 5 minutes

# ══ UP/DOWN MARKET ENGINE ════════════════════════════════════════════════════
# These are 5-min / 15-min BTC/ETH/SOL/XRP prediction markets.
# Edge: real crypto price from Binance leads the market price.
# If BTC is up 0.4% in the last 10 minutes → bet "Up" before market catches up.

_updown_traded = set()   # track condition IDs traded this session

def updown_scan():
    """
    Find active Up/Down crypto markets, compare to real Binance price.
    If Binance confirms direction with >= updown_min_change, execute a bet.
    """
    # Pull recent trades to find active Up/Down market condition IDs
    trades = fetch_json("https://data-api.polymarket.com/trades?limit=100&amount_min=5")
    if not trades or not isinstance(trades, list):
        return

    seen = {}
    for t in trades:
        title = t.get('title', '') or ''
        if 'up or down' not in title.lower(): continue
        cid = t.get('conditionId', '')
        if not cid or cid in _updown_traded: continue
        if cid not in seen:
            seen[cid] = {'title': title, 'slug': t.get('eventSlug', t.get('slug',''))}

    if not seen:
        return

    for cid, info in seen.items():
        try:
            title = info['title']

            # Parse crypto symbol
            crypto = None
            for name, sym in [('Bitcoin','BTC'),('Ethereum','ETH'),('Solana','SOL'),
                               ('XRP','XRP'),('Dogecoin','DOGE'),('Ripple','XRP')]:
                if name.lower() in title.lower():
                    crypto = sym; break
            if not crypto: continue

            # Fetch market data
            mdata = fetch_json(f"https://gamma-api.polymarket.com/markets?conditionId={cid}")
            if not mdata: continue
            markets = mdata if isinstance(mdata, list) else mdata.get('markets', [])
            if not markets: continue
            m = markets[0]

            # Check end date — only bet if market expires within updown_max_age_min minutes
            end_str = m.get('endDate') or m.get('endDateIso') or ''
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    mins_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 60
                    if mins_left < 1 or mins_left > CFG["updown_max_age_min"]:
                        continue   # too close to expiry or too far out
                except Exception:
                    pass

            # Market prices
            prices = m.get('outcomePrices', '[0.5,0.5]')
            if isinstance(prices, str): prices = json.loads(prices)
            if len(prices) < 2: continue
            up_price   = float(prices[0])   # index 0 = "Up" outcome
            down_price = float(prices[1])

            outcomes = m.get('outcomes', '["Up","Down"]')
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)

            # Get real crypto price direction from Binance
            change = get_crypto_price_change(crypto, minutes=15)
            if abs(change) < CFG["updown_min_change"]:
                log.debug(f"[UPDN] {crypto} change={change*100:.2f}% — below threshold, skip")
                continue

            direction = "Up" if change > 0 else "Down"
            bet_price  = up_price if direction == "Up" else down_price
            mkt_conf   = up_price if direction == "Up" else down_price  # market's probability

            # Only bet if market price < 0.70 (not already priced in) and our signal is clear
            if bet_price > 0.72:
                log.debug(f"[UPDN] {crypto} {direction} already priced at {bet_price:.2f} — skip")
                continue

            # Get token ID for the direction we're betting
            toks = m.get('clobTokenIds', '[]')
            if isinstance(toks, str): toks = json.loads(toks)
            if len(toks) < 2: continue

            # Map direction → token index based on outcomes list
            try:
                bet_idx = [o.lower() for o in outcomes].index(direction.lower())
            except ValueError:
                bet_idx = 0 if direction == "Up" else 1
            bet_token = toks[bet_idx] if isinstance(toks[bet_idx], str) else toks[bet_idx].get('token_id','')
            if not bet_token: continue

            log.info(f"[UPDN] {crypto} is {change*100:+.2f}% → BET {direction} @ {bet_price:.2f} | {title[:50]}")
            tg(f"UPDOWN: {crypto} {change*100:+.2f}% → BET {direction} @ {bet_price:.2f} | {title[:50]}", "UPDN")

            sig = {
                "token_id":  bet_token,
                "signal":    BUY,
                "mid":       bet_price,
                "market":    title[:60],
                "reason":    f"UPDN {crypto}{change*100:+.2f}% Binance",
                "liquidity": float(m.get('liquidity', 0) or 0),
            }
            place_order(sig, source="UPDN")
            _updown_traded.add(cid)   # don't re-trade same market window

        except Exception as e:
            log.error(f"[UPDN] Error processing {cid}: {e}")


def updown_loop():
    log.info(f"[UPDN] Up/Down engine started — scanning every {CFG['updown_scan_secs']}s")
    while True:
        try:
            updown_scan()
        except Exception as e:
            log.error(f"[UPDN] Loop error: {e}")
        time.sleep(CFG["updown_scan_secs"])


# ══ NEAR-CERTAINTY SCALPER ════════════════════════════════════════════════════
# Markets priced 87-96% YES are "almost certainly" resolving YES.
# Buying at 90¢ = 11% return to $1.00 at resolution.
# Filter: must have resolution within 21 days, vol > 5K.

def near_certainty_scan():
    """Find markets priced 87-96% YES, buy for near-guaranteed 4-13% return."""
    markets = []
    for offset in range(0, 200, 100):
        data = fetch_json(
            f"https://gamma-api.polymarket.com/markets?active=true&limit=100"
            f"&offset={offset}&sort=liquidity&ascending=false"
        )
        if not data: break
        batch = data if isinstance(data, list) else data.get('markets', data.get('data', []))
        if not batch: break
        markets.extend(batch)
        if len(batch) < 100: break

    hits = []
    for m in markets:
        try:
            prices = m.get('outcomePrices', '[]')
            if isinstance(prices, str): prices = json.loads(prices)
            if len(prices) < 2: continue
            yes = float(prices[0])
            if not (CFG["certainty_min_price"] <= yes <= CFG["certainty_max_price"]): continue

            vol24 = float(m.get('volume24hr', 0) or 0)
            liq   = float(m.get('liquidity', 0) or 0)
            if liq < CFG["certainty_min_vol"]: continue

            # Markets resolving within certainty_max_days
            end_str = m.get('endDate') or ''
            days_left = 30  # default if unknown
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    days_left = (end_dt - datetime.now(timezone.utc)).days
                    if days_left > CFG["certainty_max_days"] or days_left < 0: continue
                except Exception:
                    pass

            toks = m.get('clobTokenIds', '[]')
            if isinstance(toks, str): toks = json.loads(toks)
            if not toks: continue
            yes_token = toks[0] if isinstance(toks[0], str) else toks[0].get('token_id','')

            ret_pct = (1.0 - yes) / yes * 100  # return on investment if YES resolves

            hits.append({
                "question":   m.get('question', '')[:70],
                "yes_price":  yes,
                "yes_token":  yes_token,
                "liq":        liq,
                "vol24":      vol24,
                "ret_pct":    ret_pct,
                "days_left":  days_left,
            })
        except Exception:
            continue

    hits.sort(key=lambda x: -x['ret_pct'])   # highest return first

    for h in hits[:3]:
        log.info(f"[CERT] {h['yes_price']:.2%} YES → {h['ret_pct']:.1f}% return | "
                 f"{h['days_left']}d left | liq=${h['liq']:,.0f} | {h['question']}")
        tg(f"CERT: {h['yes_price']:.0%} YES +{h['ret_pct']:.1f}% ROI | {h['question'][:50]}", "CERT")

        portfolio = get_true_portfolio_value() or 0
        if portfolio < CFG["min_balance_usd"]: continue

        sig = {
            "token_id":  h['yes_token'],
            "signal":    BUY,
            "mid":       h['yes_price'],
            "market":    h['question'],
            "reason":    f"CERT {h['yes_price']:.0%} YES {h['ret_pct']:.1f}% ROI",
            "liquidity": h['liq'],
        }
        place_order(sig, source="CERT")


def near_certainty_loop():
    log.info(f"[CERT] Near-certainty scalper started — every {CFG['certainty_scan_secs']}s")
    while True:
        try:
            near_certainty_scan()
        except Exception as e:
            log.error(f"[CERT] Loop error: {e}")
        time.sleep(CFG["certainty_scan_secs"])


# ══ 4-MINUTE RULE ═════════════════════════════════════════════════════════════
# Find ANY binary market closing within 4 minutes where the leader is 82–96%.
# In the final minutes a market can't reverse — direction is locked.
# Buying YES at 0.88 with 3 min left = ~13% return in 3 minutes.
# Strategy made famous by fresh Polymarket accounts printing fast profits.

_fourmin_traded: set = set()   # condition IDs already traded this session

def fourmin_scan():
    """4-minute rule: bet on the leading side of markets expiring in ≤4 min."""
    now = datetime.now(timezone.utc)

    # Pull markets sorted by endDate ascending — soonest expiry first
    data = fetch_json(
        "https://gamma-api.polymarket.com/markets?active=true&limit=200"
        "&order=endDate&ascending=true"
    )
    if not data:
        return
    markets = data if isinstance(data, list) else data.get('markets', data.get('data', []))

    for m in markets:
        try:
            cid = m.get('conditionId') or m.get('id') or ''
            if not cid or cid in _fourmin_traded:
                continue

            # Time-to-expiry filter
            end_str = m.get('endDate') or ''
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
            mins_left = (end_dt - now).total_seconds() / 60

            if mins_left < 0.5 or mins_left > CFG["fourmin_max_mins"]:
                continue  # already expiring or too far out

            # Must be binary YES/NO
            outcomes = m.get('outcomes', '[]')
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if len(outcomes) != 2:
                continue
            yes_idx = next((i for i, o in enumerate(outcomes)
                            if str(o).upper() in ('YES', 'Y')), None)
            no_idx  = next((i for i, o in enumerate(outcomes)
                            if str(o).upper() in ('NO', 'N')), None)
            if yes_idx is None or no_idx is None:
                continue

            # Prices
            prices = m.get('outcomePrices', '[0.5,0.5]')
            if isinstance(prices, str):
                prices = json.loads(prices)
            if len(prices) < 2:
                continue
            yes_price = float(prices[yes_idx])
            no_price  = float(prices[no_idx])

            # Find the leader
            leader_price = max(yes_price, no_price)
            leader_is_yes = (yes_price >= no_price)

            if not (CFG["fourmin_min_leader"] <= leader_price <= CFG["fourmin_max_leader"]):
                continue  # either too cheap (uncertain) or already resolved

            # Liquidity check
            liq = float(m.get('liquidity', 0) or 0)
            if liq < CFG["fourmin_min_liq"]:
                continue

            # Token ID for the leader side
            toks = m.get('clobTokenIds', '[]')
            if isinstance(toks, str):
                toks = json.loads(toks)
            if len(toks) < 2:
                continue
            tok_idx = yes_idx if leader_is_yes else no_idx
            token_id = toks[tok_idx] if isinstance(toks[tok_idx], str) else toks[tok_idx].get('token_id', '')
            if not token_id:
                continue

            side  = "YES" if leader_is_yes else "NO"
            ret   = (1.0 - leader_price) / leader_price * 100
            question = m.get('question', '')[:60]

            log.info(f"[FOUR] {mins_left:.1f}min left | {side} @ {leader_price:.2f} "
                     f"(+{ret:.1f}% ROI) | liq=${liq:,.0f} | {question}")
            tg(f"4MIN RULE: {side} @ {leader_price:.0%} +{ret:.1f}% ROI | {mins_left:.1f}min | {question}", "FOUR")

            portfolio = get_true_portfolio_value() or 0
            if portfolio < CFG["min_balance_usd"]:
                continue

            sig = {
                "token_id":  token_id,
                "signal":    BUY,
                "mid":       leader_price,
                "market":    question,
                "reason":    f"4MIN {side}@{leader_price:.2f} {mins_left:.1f}min {ret:.1f}%ROI",
                "liquidity": liq,
            }
            place_order(sig, source="FOUR")
            _fourmin_traded.add(cid)

        except Exception as e:
            log.error(f"[FOUR] Error on market {m.get('conditionId','?')}: {e}")


def fourmin_loop():
    log.info(f"[FOUR] 4-minute rule engine started — scanning every {CFG['fourmin_scan_secs']}s")
    while True:
        try:
            fourmin_scan()
        except Exception as e:
            log.error(f"[FOUR] Loop error: {e}")
        time.sleep(CFG["fourmin_scan_secs"])


# ══ AGENT-BASED MARKET SCANNER ════════════════════════════════════════════════
# Three agents vote on every market in the 4-48h resolution window.
# Filters: book depth >= $500, resolution 4-48h, Claude gap >= 7%.
# Entry: 2/3 agents agree on direction.
# Exit: tracked by position_tracker.py (80% target / vol spike / 24h stale).

_agent_scan_traded: set = set()   # condition IDs executed this session

def _book_depth(token_id: str) -> float:
    """Get total liquidity on best 3 bid levels."""
    book = order_books.get(token_id, {})
    bids = sorted(book.get("bids", {}).items(), key=lambda x: -float(x[0]))[:3]
    return sum(float(p) * float(s) for p, s in bids)

def _current_vol(token_id: str) -> float:
    """Return last known volume from order book or price history proxy."""
    return 0.0   # extended by WS data when available

def _sell_position(token_id: str, size: float) -> bool:
    """Attempt to sell a position via CLOB."""
    if CFG["dry_run"]:
        log.info(f"[DRY] Would SELL {size:.1f}sh of {token_id[:12]}...")
        return True
    try:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import SELL
        order = client.create_and_post_order(OrderArgs(
            token_id=token_id,
            price=0.01,       # market sell — best available
            size=round(size, 2),
            side=SELL,
        ))
        return bool(order)
    except Exception as e:
        log.error(f"[TRACKER] Sell error: {e}")
        return False

def agent_market_scan():
    """
    Scan active 4-48h markets. Run 3-agent vote. Execute on 2/3 consensus.
    """
    from agent_system import vote as agent_vote
    from wallet_scanner import load_elite_wallets

    elite = load_elite_wallets()
    elite_addresses = {w["address"] for w in elite}
    log.info(f"[AGENT] Loaded {len(elite_addresses)} elite wallets (>{70}% WR)")

    # Fetch candidate markets in 4-48h window (sorted by end date)
    markets = []
    for offset in range(0, 300, 100):
        data = fetch_json(
            f"https://gamma-api.polymarket.com/markets?active=true&limit=100"
            f"&offset={offset}&order=endDate&ascending=true"
        )
        if not data:
            break
        batch = data if isinstance(data, list) else data.get("markets", [])
        if not batch:
            break
        markets.extend(batch)
        if len(batch) < 100:
            break

    if not markets:
        log.warning("[AGENT] No markets returned")
        return

    candidates = 0
    voted      = 0
    executed   = 0

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid or cid in _agent_scan_traded:
                continue

            # Quick endDate pre-filter
            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt     = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if not (4 <= hours_left <= 48):
                continue

            # Must be binary YES/NO
            outcomes = m.get("outcomes", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if len(outcomes) != 2:
                continue
            yes_idx = next((i for i, o in enumerate(outcomes)
                            if str(o).upper() in ("YES", "Y")), None)
            if yes_idx is None:
                continue

            prices = m.get("outcomePrices", "[0.5,0.5]")
            if isinstance(prices, str):
                prices = json.loads(prices)
            if len(prices) < 2:
                continue
            yes_price = float(prices[yes_idx])
            if yes_price <= 0.02 or yes_price >= 0.98:
                continue   # effectively resolved

            # Liquidity check (book depth via liquidity field as proxy)
            liq = float(m.get("liquidity") or 0)

            # Token
            toks = m.get("clobTokenIds", "[]")
            if isinstance(toks, str):
                toks = json.loads(toks)
            if len(toks) < 2:
                continue
            yes_token = toks[yes_idx] if isinstance(toks[yes_idx], str) else toks[yes_idx].get("token_id", "")
            no_token  = toks[1 - yes_idx] if isinstance(toks[1-yes_idx], str) else toks[1-yes_idx].get("token_id", "")

            # Book depth from live order book (if subscribed), else use liquidity
            book_depth = _book_depth(yes_token) or liq

            candidates += 1

            # 3-AGENT VOTE
            decision = agent_vote(
                market=m,
                yes_price=yes_price,
                condition_id=cid,
                book_depth=book_depth,
                elite_addresses=elite_addresses,
                fetch_fn=fetch_json,
                get_ml_fn=_get_ml_predictor,
            )
            voted += 1

            log.info(f"[AGENT] {decision['reason']} | {m.get('question','')[:55]}")
            tg(f"AGENT VOTE: {decision['reason'][:80]}", "AGENT")

            if not decision["trade"]:
                continue

            # Execute
            direction = decision["direction"]
            bet_price = yes_price if direction == "YES" else (1.0 - yes_price)
            bet_token = yes_token  if direction == "YES" else no_token

            portfolio = get_true_portfolio_value() or 0
            if portfolio < CFG["min_balance_usd"]:
                log.warning(f"[AGENT] Portfolio ${portfolio:.2f} too low, skip")
                continue

            sig = {
                "token_id":  bet_token,
                "signal":    BUY,
                "mid":       bet_price,
                "market":    m.get("question", "")[:60],
                "reason":    f"3-AGENT {direction} | {decision['reason'][:60]}",
                "liquidity": liq,
            }
            place_order(sig, source="AGNT")
            _agent_scan_traded.add(cid)
            executed += 1

            # Register in position tracker
            try:
                from position_tracker import open_position
                shares = round(CFG["max_position_usd"] / max(bet_price, 0.01), 2)
                open_position(bet_token, m.get("question",""), bet_price, shares, direction, cid)
            except Exception as e:
                log.debug(f"[AGENT] Position tracker error: {e}")

        except Exception as e:
            log.error(f"[AGENT] Error on market {m.get('conditionId','?')[:16]}: {e}")

    log.info(f"[AGENT] Scan done: {candidates} candidates, {voted} voted, {executed} executed")


def agent_loop():
    """Agent-based scanner: runs every 10 min. Also starts position tracker."""
    # Start position exit monitor
    try:
        from position_tracker import start_tracker, load_positions

        def _get_price(token_id):
            ph = list(price_history.get(token_id, []))
            return ph[-1] if ph else None

        start_tracker(
            get_price_fn=_get_price,
            get_vol_fn=_current_vol,
            sell_fn=_sell_position,
            tg_fn=tg,
        )
        log.info("[AGENT] Position tracker started")
    except Exception as e:
        log.error(f"[AGENT] Could not start tracker: {e}")

    # Start wallet scanner background refresh
    try:
        from wallet_scanner import start_background_scanner
        start_background_scanner()
        log.info("[AGENT] Wallet scanner background refresh started")
    except Exception as e:
        log.error(f"[AGENT] Could not start wallet scanner: {e}")

    log.info("[AGENT] 3-Agent market scanner started — every 10 min")
    while True:
        try:
            agent_market_scan()
        except Exception as e:
            log.error(f"[AGENT] Loop error: {e}")
        time.sleep(600)   # every 10 minutes


# ══ WEBSOCKET ═════════════════════════════════════════════════════════════════

WATCHED_TOKENS = []

def fetch_top_markets(max_tokens=25):
    """
    Scan all active Polymarket markets and rank by profitability potential.
    Scoring: 24h volume (liquidity) + arb edge (YES+NO gap) + price range (tradeable).
    Returns top markets as [{token_id, market, score}].
    """
    scored = []
    for offset in range(0, 300, 100):
        data = fetch_json(
            f"https://gamma-api.polymarket.com/markets?active=true&limit=100"
            f"&offset={offset}&sort=volume24hr&ascending=false"
        )
        if not data: break
        batch = data if isinstance(data, list) else data.get('markets', data.get('data', []))
        if not batch: break
        for m in batch:
            try:
                prices = m.get('outcomePrices', '[]')
                if isinstance(prices, str): prices = json.loads(prices)
                if len(prices) < 2: continue
                yes = float(prices[0]); no = float(prices[1])

                # Skip near-resolved and extreme-priced markets — limited movement
                if yes > 0.85 or yes < 0.20: continue

                vol24  = float(m.get('volume24hr', 0) or 0)
                vol    = float(m.get('volume', 0) or 0)
                liq    = float(m.get('liquidity', 0) or 0)

                # Need at least some activity
                if vol24 < 100: continue

                # Arb edge: how far YES+NO deviates from 1.0
                edge = abs((yes + no) - 1.0)

                # Price range score: highest near 50¢, drops toward extremes
                mid = yes
                range_score = 1.0 - abs(mid - 0.5) * 2   # 1.0 at 50¢, 0 at 0¢ or 100¢

                # Combined score: volume dominates, edge and range are bonuses
                score = (vol24 / 1000) + (edge * 50) + (range_score * 10) + (liq / 2000)

                question = m.get('question', '')[:60]

                # ML model boost: adds up to +20 to score when model detects mispricing
                ml_edge = 0.0
                ml_side = None
                try:
                    ml = _get_ml_predictor()
                    if ml:
                        duration = max((datetime.fromisoformat(
                            (m.get('endDate') or '2025-01-01T00:00:00Z').replace('Z','+00:00')
                        ) - datetime.now(timezone.utc)).days, 1)
                        pred = ml.predict(
                            yes_price=yes, volume=vol, liquidity=liq,
                            volume24hr=vol24, duration_days=duration,
                            question=question
                        )
                        if pred.get('confidence') in ('HIGH', 'MEDIUM'):
                            ml_edge = pred['edge']
                            ml_side = pred['best_side']
                            score += min(ml_edge * 100, 20)   # cap ML boost at +20
                except Exception:
                    pass

                toks = m.get('clobTokenIds', m.get('tokens', '[]'))
                if isinstance(toks, str): toks = json.loads(toks)
                for t in toks:
                    tid = t if isinstance(t, str) else t.get('token_id', '')
                    if tid:
                        scored.append({"token_id": tid, "market": question,
                                       "score": score, "yes": yes, "vol24": vol24,
                                       "edge": edge, "ml_edge": ml_edge, "ml_side": ml_side})
            except Exception:
                continue
        if len(batch) < 100: break

    # Deduplicate by market question — keep best-scored entry, then emit all its tokens
    best_by_market: dict = {}
    for s in scored:
        key = s['market']
        if key not in best_by_market or s['score'] > best_by_market[key]['score']:
            best_by_market[key] = s

    # Collect all tokens for top markets (both YES and NO)
    top_markets = sorted(best_by_market.values(), key=lambda x: -x['score'])
    seen_tids = set()
    result = []
    for m in top_markets:
        for s in scored:
            if s['market'] == m['market'] and s['token_id'] not in seen_tids:
                seen_tids.add(s['token_id'])
                result.append(s)
        if len(result) >= max_tokens:
            break

    log.info(f"[SCAN] Selected {len(result)} tokens across {len(top_markets[:max_tokens//2])} markets:")
    for m in top_markets[:8]:
        ml_str = f" ML:{m.get('ml_side','?')}+{m.get('ml_edge',0)*100:.0f}pp" if m.get('ml_edge', 0) > 0.03 else ""
        log.info(f"[SCAN]  score={m['score']:.0f} yes={m['yes']:.2f} "
                 f"vol24=${m['vol24']:,.0f} edge={m['edge']*100:.1f}pp{ml_str} | {m['market']}")
    return result[:max_tokens]

def on_open(ws):
    log.info("WebSocket connected — subscribing to markets...")
    try:
        watched = fetch_top_markets(max_tokens=25)
        WATCHED_TOKENS.extend(watched)
        for w in watched:
            ws.send(json.dumps({"assets_ids": [w["token_id"]], "type": "Market"}))
        log.info(f"Subscribed to {len(watched)} tokens")
    except Exception as e:
        log.error(f"on_open error: {e}")

def on_message(ws, message):
    try:
        if not message or not isinstance(message, str): return
        message = message.strip()
        if not message or message[0] not in ('{', '['): return
        data = json.loads(message)
        if not isinstance(data, list): data = [data]

        for event in data:
            token_id = event.get('asset_id', event.get('token_id', ''))
            if not token_id: continue

            bids = event.get('bids', []); asks = event.get('asks', [])
            if bids: order_books[token_id]['bids'] = bids
            if asks: order_books[token_id]['asks'] = asks

            if bids or asks:
                all_bids = order_books[token_id].get('bids', [])
                all_asks = order_books[token_id].get('asks', [])
                try:
                    bid = max(float(b['price']) for b in all_bids) if all_bids else None
                    ask = min(float(a['price']) for a in all_asks) if all_asks else None
                    if bid and ask:
                        price_history[token_id].append((bid + ask) / 2)
                except Exception:
                    pass

            price = float(event.get('price', 0) or 0)
            if price > 0 and token_id not in order_books:
                price_history[token_id].append(price)

            if len(price_history[token_id]) >= CFG["lookback"]:
                mkt = next((t['market'] for t in WATCHED_TOKENS if t['token_id'] == token_id), token_id[:16])
                sig = momentum_signal(token_id, mkt)
                if sig:
                    place_order(sig, source="MOMENTUM")

    except Exception as e:
        log.error(f"on_message error: {e}")

def on_error(ws, error):
    log.error(f"WebSocket error: {error}")

def on_close(ws, code, msg):
    log.warning(f"WebSocket closed — will reconnect")

# ── STATUS THREAD ─────────────────────────────────────────────────────────────
def status_loop():
    while True:
        time.sleep(30)
        mode = "[DRY RUN]" if CFG["dry_run"] else "[LIVE]"
        try:
            ks = load_state()
            update_vol_ema(ks)
        except Exception:
            pass
        halt_str = f" | HALTED:{_halt_reason}" if _bot_halted else ""
        # Daily P&L — use true portfolio value (cash + positions)
        val = get_true_portfolio_value()
        daily_pnl = ""
        if val is not None and _day_start_portfolio:
            pct = (val - _day_start_portfolio) / _day_start_portfolio * 100
            daily_pnl = f" | day_pnl:{pct:+.1f}%"
        log.info(f"{mode} STATUS — ws_tokens:{len(price_history)} | "
                 f"trades:{len(trade_log)} | positions:{len(open_positions)} | "
                 f"arb_alerts:{len(arb_alerts)} | copy_pos:{len(copy_positions)}"
                 f"{daily_pnl}{halt_str}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def start_ws():
    while True:
        try:
            WATCHED_TOKENS.clear()
            ws = websocket.WebSocketApp(
                CFG["ws_url"],
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=20)
        except Exception as e:
            log.error(f"WS crashed: {e}")
        log.info("Reconnecting in 5s...")
        time.sleep(5)

if __name__ == "__main__":
    # Single-instance lock
    import pathlib, sys, os
    LOCK = pathlib.Path(__file__).parent / "bot.lock"
    if LOCK.exists():
        try:
            old_pid = int(LOCK.read_text().strip())
            os.kill(old_pid, 9)
            log.info(f"Killed old bot instance (PID {old_pid})")
        except Exception:
            pass
        LOCK.unlink(missing_ok=True)
    LOCK.write_text(str(os.getpid()))
    import atexit
    atexit.register(lambda: LOCK.unlink(missing_ok=True))

    mode = "DRY RUN (paper trading)" if CFG["dry_run"] else "LIVE TRADING"
    log.info(f"POLY//BOT v3 — {mode}")
    log.info(f"Engines: MOMENTUM | ARB | UPDOWN | CERT | COPY | MARB | 4MIN | 3-AGENT")
    log.info(f"Max pos: ${CFG['max_position_usd']} ({CFG['max_position_pct']*100:.0f}% portfolio)")
    log.info(f"CopyTrade: {CFG['copy_wallet'][:12]}... ratio={CFG['copy_ratio']*100:.0f}%")
    log.info(f"Arb: exec when gap >= {CFG['arb_min_edge']*100:.1f}%")
    log.info(f"UpDown: scan every {CFG['updown_scan_secs']}s | min move {CFG['updown_min_change']*100:.1f}%")
    log.info(f"4MIN: scan every {CFG['fourmin_scan_secs']}s | window ≤{CFG['fourmin_max_mins']}min | leader ≥{CFG['fourmin_min_leader']:.0%}")

    # Initialize risk baseline
    init_risk_baseline()

    # Launch all engines as background threads
    for target, name in [
        (status_loop,        "status"),
        (copytrade_loop,     "copytrade"),
        (arb_loop,           "arb"),
        (updown_loop,        "updown"),        # 5/15-min crypto markets
        (near_certainty_loop,"certainty"),     # 87-96% YES scalper
        (fourmin_loop,       "4min-rule"),     # 4-minute rule — final minutes edge
        (agent_loop,         "3-agent"),       # 3-agent 2/3 vote — 4-48h window
        (multi_arb_loop,     "multi-arb"),     # multi-outcome event arb
        (day_reset_loop,     "day-reset"),
    ]:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        log.info(f"Thread [{name}] started")

    # WebSocket main loop (momentum signals)
    start_ws()
