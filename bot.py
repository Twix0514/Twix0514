"""
POLY//BOT v5 — Four proven edges:
  1. UPDN  — 5-min crypto markets (BTC/ETH/SOL/XRP/DOGE/BNB), enter last 30-60s (ET hours)
  2. LIVE  — Any binary market closing in ≤4 min where leader is 82-96%
  3. DRIFT — Momentum on top liquid markets: buy when price drifts 4%+ consistently
  4. WX    — Weather forecast vs Polymarket price, buy <20¢ sell at 45¢
"""

import json, time, re, threading, logging, pathlib, os
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

try:
    from web3 import Web3
    from eth_account.messages import encode_defunct
    _WEB3_OK = True
except ImportError:
    _WEB3_OK = False

import websocket
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY, SELL
try:
    from strategy_consensus import ArbitrageAgent, ConvergenceAgent, WhaleCopyAgent
    _CONSENSUS_OK = True
except Exception:
    _CONSENSUS_OK = False

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('BOT')

# ── CREDENTIALS (secrets_local.py or env vars override) ──────────────────────
PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "72f882593b660160169ee4d14165dbd3ad15626b6f45632373dd2774e7294300")
FUNDER      = os.environ.get("POLY_FUNDER",      "0x361A9c14e3aD1B8Ed9ef35014fD1B5dCcB72eC07")
CHAIN_ID    = 137
PROXY       = os.environ.get("POLY_PROXY", "")
try:
    from secrets_local import PRIVATE_KEY, FUNDER, PROXY  # type: ignore
except Exception:
    pass

# Fast mode only increases scan cadence; risk caps and filters remain unchanged.
_FAST_MODE = os.environ.get("POLY_FAST_MODE", "1").strip().lower() not in ("0", "false", "no", "off")

def _pace(normal, fast):
    return fast if _FAST_MODE else normal

# ── CONFIG ────────────────────────────────────────────────────────────────────
CFG = {
    "dry_run":         False,
    "max_usd":         0.75,   # 5-8% of a $10 bankroll — never bet the house
    "min_free_usdc":   0.50,   # lower floor to avoid constant SKIPs with 6 engines active
    "daily_halt_pct":  -0.15,  # halt after -15% day — capital preservation first
    "max_positions":   6,      # 6 slots across all engines — per-source caps prevent hoarding

    # UPDN — only trade when market AGREES with our signal (proven from trade history)
    # Losses at 0.30-0.44, wins at 0.55+. Market calibration > Binance signal alone.
    "updn_max_price":  0.88,   # soft cap guidance; timeframe caps below are authoritative
    "updn_min_price":  0.52,   # slightly wider acceptance while keeping market-agreement guard

    # WEATHER
    "wx_buy_under":    0.20,   # buy YES when price < 20¢ and forecast says it'll happen
    "wx_sell_at":      0.45,   # target exit price
    "wx_min_edge":     0.08,   # 8% gap between forecast and market price
    "wx_max_days":     2,      # only markets resolving within 2 days
    "wx_min_liq":      100,

    # LIVE (6-min rule)
    "live_max_mins":   6.0,
    "live_min_leader": 0.75,   # slightly wider for more opportunities
    "live_max_leader": 0.985,  # still avoids near-resolved tails
    "live_min_liq":    500,    # min $500 liquidity — low liq = bad fills + high price impact

    # SPORT
    "sports_min_leader": 0.88,
    "sports_max_leader": 0.985,

    # NEAR
    "near_min_vol24":  1000,
    "near_prefilter_vol24": 500,
    "near_min_liq":    500,
    "near_min_leader": 0.70,
    "near_max_leader": 0.92,
    "near_min_days":   0.25,
    "near_max_days":   3.0,
    "near_prefilter_limit": 150,
    "near_blacklist_sec": 900,
    "near_vol_skip_blacklist_hits": 8,

    # WEATHER / POLITICS category expansion
    "wx_scan_sleep_sec": _pace(120, 45),
    "politics_min_liq":  2000,
    "politics_min_vol24": 8000,
    "politics_min_leader": 0.68,
    "politics_max_leader": 0.93,
    "politics_min_hours": 1,
    "politics_max_hours": 14 * 24,
    "politics_scan_sleep_sec": _pace(20, 8),
    "politics_cooldown_sec": 2 * 3600,

    # DRIFT — momentum on top liquid markets
    "drift_min_move":  0.025,  # 2.5% price drift required
    "drift_readings":  2,      # readings before firing (2min each = 4min warmup)
    "drift_max_price": 0.88,
    "drift_min_price": 0.08,
    "drift_exit_pct":  0.20,   # exit at +20% profit

    # WebSocket
    "ws_url":          "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "lookback":        20,
    "momentum_thresh": 0.04,

    # Loop pacing (faster polling does NOT change risk sizing/halts)
    "updn_cache_refresh_sec": _pace(90, 45),
    "updn_slug_fetch_gap_sec": _pace(0.05, 0.03),
    "updn_scan_sleep_sec": _pace(5, 2),
    "live_scan_sleep_sec": _pace(5, 2),
    "sports_scan_sleep_sec": _pace(10, 4),
    "near_scan_sleep_sec": _pace(180, 45),
    "copy_scan_sleep_sec": _pace(15, 5),
    "copy_wallets_per_scan": max(1, int(_pace(1, 3))),
    "status_sleep_sec": _pace(10, 5),
    "server_watchdog_sleep_sec": _pace(30, 15),
    "ws_reconnect_sleep_sec": _pace(5, 2),
}

# ── PROXY SETUP ───────────────────────────────────────────────────────────────
_direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))

if PROXY:
    os.environ["HTTP_PROXY"]  = PROXY
    os.environ["HTTPS_PROXY"] = PROXY
    os.environ["ALL_PROXY"]   = PROXY
    log.info(f"[PROXY] {PROXY}")

# ── CLIENT ────────────────────────────────────────────────────────────────────
client = ClobClient("https://clob.polymarket.com", key=PRIVATE_KEY,
                    chain_id=CHAIN_ID, signature_type=2, funder=FUNDER)
try:
    client.set_api_creds(client.create_or_derive_api_creds())
    log.info("[AUTH] API creds derived")
except Exception as e:
    log.warning(f"[AUTH] {e}")

# ── FILES ─────────────────────────────────────────────────────────────────────
BASE        = pathlib.Path(__file__).parent
ALERTS_FILE = BASE / "alerts.json"
STATUS_FILE = BASE / "status.json"
LOCK_FILE   = BASE / "bot.lock"

# ── STATE ─────────────────────────────────────────────────────────────────────
_order_lock      = threading.Lock()
_bot_halted      = False
_halt_reason     = ""
_day_start_val   = None
_session_start   = None
_wx_positions    = {}      # token_id -> entry_price (for weather exits)
trade_log        = []
open_positions   = {}
price_history    = defaultdict(lambda: deque(maxlen=CFG["lookback"] + 5))
order_books      = defaultdict(dict)
WATCHED_TOKENS   = []
# WS order queue — on_message enqueues signals; ws_order_worker processes them off the WS thread
import queue as _queue
_ws_order_queue: _queue.Queue = _queue.Queue(maxsize=20)

# Cached portfolio value — updated every 30s in status_loop, read in check_halt()
# Avoids making HTTP calls inside _order_lock (which stalls all threads for 10s)
_cached_portfolio_val: float = 0.0
_cached_portfolio_ts:  float = 0.0

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch(url: str):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with _direct.open(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.debug(f"fetch {url[:70]}: {e}")
        return None

def tg(msg: str, level: str = "INFO"):
    entry = {"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "level": level, "msg": msg}
    try:
        existing = json.loads(ALERTS_FILE.read_text()) if ALERTS_FILE.exists() else []
        existing.insert(0, entry)
        ALERTS_FILE.write_text(json.dumps(existing[:200], indent=2))
    except Exception:
        pass
    log.info(f"[ALERT] {msg}")

def free_usdc() -> float:
    try:
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        raw = float(bal.get("balance", 0) or 0)
        return raw / 1_000_000 if raw > 1000 else raw
    except Exception:
        return 0.0

def portfolio_val() -> float:
    try:
        data = fetch(f"https://data-api.polymarket.com/value?user={FUNDER}")
        if data:
            item = data[0] if isinstance(data, list) else data
            v = float(item.get("portfolioValue") or item.get("value") or 0)
            if v > 0:
                return v
    except Exception:
        pass
    # Fallback: USDC + mark-to-market positions using live CLOB price (not stale entry)
    usdc = free_usdc()
    pos_val = 0.0
    for tid, p in list(open_positions.items()):
        mid = clob_mid(tid)
        sz  = float(p.get("size", 0))
        pos_val += sz * (mid if mid > 0 else float(p.get("entry", 0.5)))
    return usdc + pos_val

def parse_prices(m):
    p = m.get("outcomePrices", "[0.5,0.5]")
    return [float(x) for x in (json.loads(p) if isinstance(p, str) else p)]

def parse_tokens(m):
    t = m.get("clobTokenIds", "[]")
    raw = json.loads(t) if isinstance(t, str) else t
    return [x if isinstance(x, str) else x.get("token_id", "") for x in raw]

def parse_outcomes(m):
    o = m.get("outcomes", '["Yes","No"]')
    return json.loads(o) if isinstance(o, str) else o

# ── RISK MANAGEMENT ───────────────────────────────────────────────────────────
def init_baseline():
    global _day_start_val, _session_start
    val = portfolio_val()
    if val > 0:
        _day_start_val = val
        _session_start = val
        log.info(f"[RISK] Baseline ${val:.2f} | halt@-20%=${val*0.8:.2f}")

def check_halt() -> bool:
    global _bot_halted, _halt_reason
    if _bot_halted:
        return False
    if _day_start_val:
        # Use cached value — never make HTTP calls inside _order_lock
        val = _cached_portfolio_val if _cached_portfolio_val > 0 else _day_start_val
        pct = (val - _day_start_val) / _day_start_val
        if pct < CFG["daily_halt_pct"]:
            _bot_halted = True
            _halt_reason = f"daily loss {pct*100:.1f}%"
            try:
                tg(f"BOT HALTED — daily loss {pct*100:.1f}%. Resumes at midnight UTC.", "WARN")
            except Exception:
                pass  # never let alert failure prevent halt from taking effect
            return False
    return True

def reset_daily():
    global _day_start_val, _bot_halted, _halt_reason
    val = portfolio_val()
    if val > 0:
        _day_start_val = val
    if _bot_halted and "daily" in _halt_reason:
        _bot_halted = False
        _halt_reason = ""
        tg("Daily halt lifted — new trading day.", "INFO")

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def place_order(token_id: str, price: float, size_usd: float, market: str, source: str,
                p_win: float = 0.0, expected_gap: float = 0.15):
    with _order_lock:
        if not check_halt():
            return
        # Cross-engine dedup: never double-enter the same token
        if token_id in open_positions:
            log.debug(f"[{source}] SKIP — already holding {token_id[:16]}")
            return
        # Cap concurrent positions globally
        if len(open_positions) >= CFG["max_positions"]:
            log.warning(f"[{source}] SKIP — max {CFG['max_positions']} positions reached")
            return
        # Per-source caps: prevent any single engine monopolizing all slots
        _SRC_CAPS = {"COPY": 2, "UPDN": 3}
        src_group = source.split("/")[0]
        if src_group in _SRC_CAPS:
            src_count = sum(1 for p in open_positions.values()
                            if p.get("source", "").startswith(src_group))
            if src_count >= _SRC_CAPS[src_group]:
                log.debug(f"[{source}] SKIP — {src_group} cap {_SRC_CAPS[src_group]} reached")
                return
        f = free_usdc()
        if f < CFG["min_free_usdc"]:
            log.warning(f"[{source}] SKIP — free USDC ${f:.2f}")
            return
        # Kelly sizing: if p_win provided, use quarter-Kelly; else fall back to max_usd
        bankroll = _cached_portfolio_val or f
        port_cap = max(0.50, bankroll * 0.10)
        if p_win > 0.5:
            kelly = kelly_size(p_win, price, bankroll)
            size_usd = min(kelly if kelly > 0 else size_usd, f * 0.9, CFG["max_usd"], port_cap)
        else:
            size_usd = min(size_usd, f * 0.9, CFG["max_usd"], port_cap)
        shares = max(1, round(size_usd / max(price, 0.01)))  # Polymarket min = 1 share
        size_usd = round(shares * price, 2)
        if size_usd > f * 0.9:
            log.warning(f"[{source}] SKIP — {shares}sh @ {price:.3f} costs ${size_usd:.2f}, only ${f:.2f} free")
            return
        cost = round(shares * price, 2)
        if CFG["dry_run"]:
            log.info(f"[DRY] {source} BUY {shares}sh @ {price:.3f} (${cost}) | {market[:50]}")
            open_positions[token_id] = {"size": shares, "entry": price, "source": source}
            trade_log.append({"source": source, "token_id": token_id, "price": price, "size": shares})
            return
        try:
            order  = client.create_and_post_order(OrderArgs(token_id=token_id, price=price, size=shares, side=BUY))
            status = order.get("status", "?")
            tg(f"ORDER [{source}] BUY {shares}sh @ {price:.3f} (${cost}) | {market[:50]} | {status}")
            log.info(f"[{source}] ORDER: {shares}sh @ {price:.3f} = ${cost} | {status}")
            # Only track as open if order was accepted (not unmatched/cancelled)
            if status not in ("unmatched", "cancelled", "error"):
                open_positions[token_id] = {
                    "size": shares, "entry": price, "source": source, "market": market,
                    "entry_time": time.time(), "expected_gap": expected_gap,
                }
                trade_log.append({"source": source, "token_id": token_id, "price": price, "size": shares})
                if len(trade_log) > 500:
                    del trade_log[:250]  # drop oldest half when full
        except Exception as e:
            log.error(f"[{source}] Order failed: {e}")

def sell_position(token_id: str, price: float, market: str, source: str):
    # BUG3 FIX: read pos and pop INSIDE lock atomically; only pop on success
    with _order_lock:
        pos = open_positions.get(token_id)
        if not pos:
            return
        shares = float(pos.get("size", 0))
        if shares <= 0:
            return
        try:
            order = client.create_and_post_order(OrderArgs(token_id=token_id, price=price, size=shares, side=SELL))
            status = order.get("status", "?")
            # Only remove position if order was accepted/filled — keep tracking on unmatched/cancelled
            if status in ("matched", "delayed", "live"):
                open_positions.pop(token_id, None)
            elif status in ("unmatched", "cancelled"):
                log.warning(f"[{source}] SELL unmatched — still holding {shares}sh @ {price:.3f}")
            tg(f"SELL [{source}] {shares}sh @ {price:.3f} | {market[:50]} | {status}")
            log.info(f"[{source}] SELL {shares}sh @ {price:.3f} | {status}")
        except Exception as e:
            log.error(f"[{source}] Sell failed: {e}")

# ── ENGINE 1: MULTI-TIMEFRAME CRYPTO UP/DOWN ──────────────────────────────────
# Three timeframes — each with different confirmation depth and win rate:
#   5-min : enter last 0.4-2.5min → 2.5-4.6min confirmed → ~60% win rate
#   15-min: enter last 2-5min    → 10-13min confirmed   → ~68% win rate
#   4-hour: enter last 30-60min  → 180-210min confirmed → ~75% win rate

CRYPTO_MAP = {
    "bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL", "xrp": "XRP", "ripple": "XRP",
    "dogecoin": "DOGE", "doge": "DOGE", "bnb": "BNB", "binance": "BNB",
}
SYM = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT",
       "DOGE": "DOGEUSDT", "BNB": "BNBUSDT"}

# Per-timeframe config — slug_interval is Binance kline interval for lookback
UPDN_TF = {
    # 5m DISABLED — 60% win rate, too low. All losses came from 5m at cheap prices.
    # "5m": { ... }

    # 15m: loosened for a low-vol regime while preserving market-agreement price checks.
    "15m": {
        "interval_min":  15,
        "window_min":    1.0,
        "window_max":    4.5,
        "bin_interval":  "1m",
        "min_edge":      0.0015,  # 0.15% — targeted loosen after repeated edge-low starvation
        "max_price":     0.88,
        "slug_rnd":      15,
    },
    # 1h: new timeframe — 50+ min confirmed move, enter last 5-10 min
    "1h": {
        "interval_min":  60,
        "window_min":    5,
        "window_max":    10,
        "bin_interval":  "5m",
        "min_edge":      0.0040,  # 0.4% over 1h keeps directionality while increasing candidates
        "max_price":     0.87,
        "slug_rnd":      60,
    },
    # 4h: 75%+ base win rate — require strong sustained trend
    "4h": {
        "interval_min":  240,
        "window_min":    20,
        "window_max":    75,
        "bin_interval":  "15m",
        "min_edge":      0.0040,  # 0.4% over 4h — still requires sustained trend
        "max_price":     0.86,
        "slug_rnd":      240,
    },
}

def binance_change(symbol: str, elapsed_min: int, bin_interval: str = "1m") -> float:
    """Price change over `elapsed_min` minutes using completed candles only."""
    sym = SYM.get(symbol, f"{symbol}USDT")
    interval_mins = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}
    candle_min    = interval_mins.get(bin_interval, 1)
    n_candles     = max(3, elapsed_min // candle_min + 3)
    data = fetch(f"https://api.binance.com/api/v3/klines?symbol={sym}&interval={bin_interval}&limit={n_candles}")
    if not data or len(data) < 3:
        return 0.0
    # data[-2] = last COMPLETED candle; data[-1] = in-progress (noisy)
    open_price  = float(data[0][1])
    close_price = float(data[-2][4])
    return (close_price - open_price) / max(open_price, 1e-9)

_UPDN_TRADED_FILE = BASE / "updn_traded.json"

_UPDN_TRADE_TTL = 5 * 3600   # 5h — covers the longest window (4h) + buffer

def _load_updn_traded() -> set:
    """Load persisted traded set — expire entries older than 5h."""
    try:
        now_ts = time.time()
        data = json.loads(_UPDN_TRADED_FILE.read_text())
        return {cid for cid, ts in data.items() if now_ts - ts < _UPDN_TRADE_TTL}
    except Exception:
        return set()

def _save_updn_traded(traded: set):
    now_ts = time.time()
    try:
        existing = {}
        try:
            existing = json.loads(_UPDN_TRADED_FILE.read_text())
        except Exception:
            pass
        # Only add new entries — don't overwrite existing timestamps
        for cid in traded:
            if cid not in existing:
                existing[cid] = now_ts
        existing = {k: v for k, v in existing.items() if now_ts - v < _UPDN_TRADE_TTL}
        _UPDN_TRADED_FILE.write_text(json.dumps(existing))
    except Exception:
        pass

_updn_traded: set = _load_updn_traded()

UPDN_CRYPTOS = {
    "btc": "BTC", "eth": "ETH", "sol": "SOL",
    "xrp": "XRP", "doge": "DOGE", "bnb": "BNB",
}

# Pre-fetched cache: {cid -> market_dict with "tf" key}, refreshed every 90s
_updn_market_cache: dict = {}
_updn_cache_ts: float = 0.0

def _windows_for_tf(tf_key: str, now: datetime) -> list:
    """Generate candidate window start timestamps for a timeframe."""
    cfg   = UPDN_TF[tf_key]
    rnd   = cfg["slug_rnd"]
    itvl  = cfg["interval_min"]
    if rnd < 60:
        base_min = (now.minute // rnd) * rnd
        base     = now.replace(minute=base_min, second=0, microsecond=0)
    else:
        # 4h — round to 4h boundary
        base_h = (now.hour // (rnd // 60)) * (rnd // 60)
        base   = now.replace(hour=base_h, minute=0, second=0, microsecond=0)
    return [base + timedelta(minutes=i * itvl) for i in range(-1, 4)]

def _refresh_updn_cache():
    """Fetch market data for all timeframes (5m, 15m, 4h) across all cryptos."""
    global _updn_market_cache, _updn_cache_ts
    now   = datetime.now(timezone.utc)
    cache = {}
    counts = {}

    for tf_key in UPDN_TF:
        windows = _windows_for_tf(tf_key, now)
        counts[tf_key] = 0
        for window in windows:
            ts = int(window.timestamp())
            for slug_key, sym in UPDN_CRYPTOS.items():
                slug = f"{slug_key}-updown-{tf_key}-{ts}"
                d    = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}")
                time.sleep(CFG["updn_slug_fetch_gap_sec"])
                if not d:
                    continue
                for m in (d if isinstance(d, list) else d.get("markets", [])):
                    cid = m.get("conditionId") or m.get("id") or ""
                    if not cid:
                        continue
                    end_str = m.get("endDate", "")
                    end_dt  = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None
                    cache[cid] = {
                        "market": m, "sym": sym, "tf": tf_key, "end_dt": end_dt,
                        "prices": parse_prices(m), "outcomes": parse_outcomes(m),
                        "toks": parse_tokens(m),
                    }
                    counts[tf_key] += 1

    if cache:
        summary = " | ".join(f"{tf}:{n}" for tf, n in counts.items())
        log.info(f"[UPDN] Cache: {len(cache)} markets — {summary}")
        _updn_market_cache = cache  # only replace cache if we got real data
    else:
        log.warning("[UPDN] Cache refresh got 0 markets — keeping previous cache")
    _updn_cache_ts = time.time()  # always reset timer to avoid hammering on failure

def updn_scan():
    now = datetime.now(timezone.utc)
    traded_this_scan = False   # one trade per scan — prevent multi-crypto simultaneous drain
    scan_total = 0
    scan_in_window = 0
    scan_token_ready = 0
    scan_nonflat = 0

    for cid, entry in list(_updn_market_cache.items()):
        try:
            if cid in _updn_traded or traded_this_scan:
                continue
            scan_total += 1

            end_dt = entry.get("end_dt")
            if not end_dt:
                continue

            tf_key  = entry.get("tf", "15m")
            tf_cfg  = UPDN_TF.get(tf_key)
            if tf_cfg is None:
                continue   # timeframe disabled (e.g. 5m) — skip
            mins_left = (end_dt - now).total_seconds() / 60

            if not (tf_cfg["window_min"] <= mins_left <= tf_cfg["window_max"]):
                _skip_record(f"UPDN/{tf_key}", "out_of_window")
                continue
            scan_in_window += 1

            sym      = entry["sym"]
            outcomes = entry["outcomes"]
            toks     = entry["toks"]
            if len(toks) < 2:
                _skip_record(f"UPDN/{tf_key}", "missing_tokens")
                continue
            scan_token_ready += 1

            try:
                up_i   = [str(o).lower() for o in outcomes].index("up")
                down_i = 1 - up_i
            except ValueError:
                up_i, down_i = 0, 1

            elapsed_min = max(1, round(tf_cfg["interval_min"] - mins_left))
            change      = binance_change(sym, elapsed_min, tf_cfg["bin_interval"])
            if abs(change) < 0.00005:
                _skip_record(f"UPDN/{tf_key}", "flat")
                continue   # truly flat
            scan_nonflat += 1

            direction = "Up" if change > 0 else "Down"
            bet_i     = up_i if direction == "Up" else down_i
            token_id  = toks[bet_i]

            # Edge check — must clear the per-timeframe minimum move
            if abs(change) < tf_cfg["min_edge"]:
                _skip_record(f"UPDN/{tf_key}", "edge_low")
                log.info(f"[UPDN/{tf_key}] {sym} {direction} {change*100:+.3f}% < {tf_cfg['min_edge']*100:.2f}% needed — skip")
                continue

            # Live CLOB price
            live_price = clob_mid(token_id)
            if live_price <= 0:
                cached_prices = entry["prices"]
                live_price = float(cached_prices[bet_i]) if len(cached_prices) > bet_i else 0.5

            if live_price > tf_cfg["max_price"]:
                _skip_record(f"UPDN/{tf_key}", "priced_in")
                log.info(f"[UPDN/{tf_key}] {sym} {direction}@{live_price:.2f} priced in — skip")
                continue
            if live_price < CFG["updn_min_price"]:
                _skip_record(f"UPDN/{tf_key}", "market_disagrees")
                log.info(f"[UPDN/{tf_key}] {sym} {direction}@{live_price:.2f} market disagrees — skip")
                continue

            q   = (entry["market"].get("question") or "")[:60]
            roi = (1 - live_price) / live_price * 100
            magnitude  = min(abs(change), 0.02)
            p_win_updn = 0.55 + (magnitude / 0.02) * 0.23   # 0.55→0.78 based on move size
            exp_gap    = abs(change) * 5
            log.info(f"[UPDN/{tf_key}] {sym} {change*100:+.3f}% ({elapsed_min}min elapsed) "
                     f"→ {direction} @ {live_price:.2f} +{roi:.1f}%ROI | {mins_left:.1f}min left | p={p_win_updn:.0%}")
            tg(f"UPDN/{tf_key} {sym}: {change*100:+.3f}% → {direction} @ {live_price:.0%} +{roi:.1f}% | {mins_left:.1f}min", "UPDN")
            place_order(token_id, live_price, CFG["max_usd"], q, f"UPDN/{tf_key}",
                        p_win=p_win_updn, expected_gap=exp_gap)
            _updn_traded.add(cid)
            _save_updn_traded(_updn_traded)
            traded_this_scan = True

        except Exception as e:
            log.error(f"[UPDN] {e}")

    _scan_record("UPDN", {
        "cache_entries": scan_total,
        "in_window": scan_in_window,
        "token_ready": scan_token_ready,
        "nonflat": scan_nonflat,
    })

_updn_refresh_lock = threading.Lock()

def _refresh_updn_cache_async():
    """Run cache refresh in a background thread so updn_scan() never blocks."""
    if _updn_refresh_lock.locked():
        return  # refresh already in progress
    def _work():
        with _updn_refresh_lock:
            _refresh_updn_cache()
    threading.Thread(target=_work, daemon=True, name="updn-refresh").start()

def updn_loop():
    log.info("[UPDN] Started — BTC ETH SOL XRP DOGE BNB | 24/7 | cache-based slug lookup")
    _refresh_updn_cache()   # blocking load on startup (need cache before first scan)
    while True:
        try:
            # Kick off async refresh periodically — scan continues uninterrupted using old cache
            if time.time() - _updn_cache_ts > CFG["updn_cache_refresh_sec"]:
                _refresh_updn_cache_async()
            updn_scan()
        except Exception as e:
            log.error(f"[UPDN] loop: {e}")
        time.sleep(CFG["updn_scan_sleep_sec"])

# ── ENGINE 2: WEATHER MARKETS ─────────────────────────────────────────────────
# Strategy documented: $1K → $24K buying when price <15¢, selling at 45¢.
# Uses NOAA + OpenMeteo forecast vs. Polymarket price.
# Only trade markets resolving within 2 days.

CITIES = [
    {"city": "New York",    "lat": 40.71, "lon": -74.01, "noaa": "GHCND:USW00094728"},
    {"city": "Los Angeles", "lat": 34.05, "lon": -118.24,"noaa": "GHCND:USW00093134"},
    {"city": "Chicago",     "lat": 41.88, "lon": -87.63, "noaa": "GHCND:USW00094846"},
    {"city": "Miami",       "lat": 25.77, "lon": -80.19, "noaa": "GHCND:USW00012839"},
    {"city": "London",      "lat": 51.51, "lon": -0.13,  "noaa": None},
    {"city": "Paris",       "lat": 48.86, "lon": 2.35,   "noaa": None},
    {"city": "Tokyo",       "lat": 35.68, "lon": 139.69, "noaa": None},
    {"city": "Seoul",       "lat": 37.57, "lon": 126.98, "noaa": None},
    {"city": "Sydney",      "lat": -33.87,"lon": 151.21, "noaa": None},
    {"city": "Dubai",       "lat": 25.20, "lon": 55.27,  "noaa": None},
]

def clob_mid(token_id: str) -> float:
    data = fetch(f"https://clob.polymarket.com/midpoint?token_id={token_id}")
    if data:
        return float(data.get("mid", 0) or 0)
    return 0.0

def clob_book(token_id: str) -> dict:
    """Return {bids_depth, asks_depth} in USD. Returns zeros on failure."""
    data = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
    if not data:
        return {"bids_depth": 0.0, "asks_depth": 0.0}
    bids = sum(float(b.get("size", 0)) * float(b.get("price", 0)) for b in data.get("bids", []))
    asks = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in data.get("asks", []))
    return {"bids_depth": round(bids, 2), "asks_depth": round(asks, 2)}

def kelly_size(p_win: float, market_price: float, bankroll: float, max_fraction: float = 0.25) -> float:
    """Quarter-Kelly position size. Returns 0 if negative EV."""
    if market_price <= 0 or market_price >= 1:
        return 0.0
    b = (1 / market_price) - 1   # payout ratio
    q = 1 - p_win
    f_star = (p_win * b - q) / b
    if f_star <= 0:
        return 0.0
    f_capped = min(f_star, max_fraction)
    return round(bankroll * f_capped, 2)

def score_market(token_id: str, p_win: float, hours_left: float) -> dict | None:
    """Return score dict if market passes all filters, else None."""
    mid = clob_mid(token_id)
    if mid <= 0:
        return None
    gap = abs(p_win - mid)
    if gap < 0.07:                    # edge too thin
        return None
    if hours_left < 4 or hours_left > 168:  # timing out of sweet spot
        return None
    book = clob_book(token_id)
    depth = min(book["bids_depth"], book["asks_depth"])
    if depth < 500:                   # can't fill without price impact
        return None
    ev = round(gap * depth * 0.001, 2)
    return {"gap": round(gap, 3), "depth": depth, "hours": hours_left, "ev": ev, "mid": mid}

# ── TARGET WALLETS (from poly_data analysis) ──────────────────────────────────
_TARGETS_FILE = BASE / "targets.json"
_target_wallets: set = set()

def _load_targets():
    global _target_wallets
    if _TARGETS_FILE.exists():
        try:
            data = json.loads(_TARGETS_FILE.read_text())
            _target_wallets = {e["wallet"].lower() for e in data if e.get("wallet")}
            log.info(f"[TARGETS] Loaded {len(_target_wallets)} elite wallets")
        except Exception as e:
            log.warning(f"[TARGETS] load failed: {e}")

def whale_in_market(token_id: str) -> bool:
    """Return True if any elite target wallet holds this token_id."""
    if not _target_wallets:
        return False
    data = fetch(f"https://data-api.polymarket.com/positions?asset_id={token_id}&sizeThreshold=5")
    if not data:
        return False
    holders = {str(p.get("proxyWallet", "") or p.get("user", "")).lower() for p in data}
    return bool(holders & _target_wallets)

_whale_token_cache = {}  # token_id -> (ts, bool)

def whale_in_market_cached(token_id: str, ttl_sec: float = 90.0) -> bool:
    now = time.time()
    cached = _whale_token_cache.get(token_id)
    if cached and now - cached[0] < ttl_sec:
        return cached[1]
    val = whale_in_market(token_id)
    _whale_token_cache[token_id] = (now, val)
    return val

_consensus_agents = [ArbitrageAgent(), ConvergenceAgent(), WhaleCopyAgent(delay_seconds=60)] if _CONSENSUS_OK else []
_consensus_stats_lock = threading.Lock()
_consensus_stats = {
    "POL":  {"evaluated": 0, "full": 0, "half": 0, "skip": 0, "passthrough": 0},
    "LIVE": {"evaluated": 0, "full": 0, "half": 0, "skip": 0, "passthrough": 0},
    "NEAR": {"evaluated": 0, "full": 0, "half": 0, "skip": 0, "passthrough": 0},
}
_skip_stats_lock = threading.Lock()
_skip_stats = defaultdict(lambda: defaultdict(int))
_scan_stats_lock = threading.Lock()
_scan_stats = {}

def _skip_record(engine: str, reason: str, count: int = 1):
    with _skip_stats_lock:
        _skip_stats[engine][reason] += count

def _scan_record(engine: str, stats: dict):
    with _scan_stats_lock:
        _scan_stats[engine] = stats

def _consensus_record(engine: str, size_factor: float, reason: str):
    with _consensus_stats_lock:
        s = _consensus_stats.setdefault(engine, {"evaluated": 0, "full": 0, "half": 0, "skip": 0, "passthrough": 0})
        s["evaluated"] += 1
        r = (reason or "").lower()
        if "pass-through" in r:
            s["passthrough"] += 1
        elif size_factor >= 1:
            s["full"] += 1
        elif size_factor > 0:
            s["half"] += 1
        else:
            s["skip"] += 1

def consensus_decision(market_payload: dict):
    """Return (size_factor, p_win, reason). size_factor is 0, 0.5, or 1.0."""
    if not _consensus_agents:
        return 1.0, 0.0, "consensus module unavailable — pass-through"

    votes = [agent.evaluate(market_payload) for agent in _consensus_agents]
    buy_votes = [v for v in votes if v.get("action") == "BUY"]
    buy_count = len(buy_votes)
    p_win = sum(float(v.get("confidence", 0.5)) for v in buy_votes) / buy_count if buy_count else 0.0

    if buy_count >= 2:
        return 1.0, p_win, "2+ BUY votes"
    if buy_count == 1:
        return 0.5, p_win, "1 BUY vote"
    return 0.0, 0.0, "agents disagree"

def openmeteo(lat: float, lon: float) -> dict:
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           f"&daily=precipitation_probability_max,temperature_2m_max,rain_sum"
           f"&forecast_days=3&timezone=auto")
    data = fetch(url)
    if not data or "daily" not in data:
        return {}
    d = data["daily"]
    return {
        "rain_prob": float((d.get("precipitation_probability_max") or [50])[0]) / 100,
        "temp_max":  float((d.get("temperature_2m_max") or [20])[0]),
        "rain_mm":   float((d.get("rain_sum") or [0])[0]),
    }

_wx_traded: set = set()

def wx_scan():
    now     = datetime.now(timezone.utc)
    markets = _fetch_live_markets(max_age=120)   # weather is slow-moving, 2min cache is fine
    if not markets:
        return

    # Check weather exits — poll CLOB midpoint directly (WS doesn't cover these tokens)
    for token_id, entry in list(_wx_positions.items()):
        pos = open_positions.get(token_id)
        if not pos:
            _wx_positions.pop(token_id, None)
            continue
        mid = clob_mid(token_id)
        if mid >= CFG["wx_sell_at"]:
            sell_position(token_id, mid, pos.get("market", "?"), "WX-EXIT")
            _wx_positions.pop(token_id, None)

    for m in markets:
        try:
            q   = (m.get("question") or "").strip().lower()
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid or cid in _wx_traded:
                continue

            # Filter: weather keywords
            if not any(kw in q for kw in ("rain", "temperature", "weather", "precipitation",
                                           "snow", "sunny", "cloudy", "humid", "wind")):
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            days_out = (end_dt - now).total_seconds() / 86400
            if days_out < 0 or days_out > CFG["wx_max_days"]:
                continue

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            yes_price = float(prices[0])
            no_price  = float(prices[1])
            liq       = float(m.get("liquidity", 0) or 0)
            if liq < CFG["wx_min_liq"]:
                continue

            # Match city
            city_data = next((c for c in CITIES if c["city"].lower() in q), None)
            if not city_data:
                continue

            forecast = openmeteo(city_data["lat"], city_data["lon"])
            if not forecast:
                continue

            rain_prob = forecast["rain_prob"]
            temp_max  = forecast["temp_max"]

            # Determine forecast direction for this question
            if any(kw in q for kw in ("rain", "precipitation", "wet")):
                forecast_yes = rain_prob
            elif "temperature" in q or "°" in q or "degrees" in q:
                forecast_yes = 0.7 if "above" in q and temp_max > 25 else 0.3
            else:
                forecast_yes = rain_prob  # default

            edge_yes = forecast_yes - yes_price
            edge_no  = (1 - forecast_yes) - no_price

            best_edge  = max(edge_yes, edge_no)
            best_price = yes_price if edge_yes >= edge_no else no_price
            best_token = toks[0]   if edge_yes >= edge_no else toks[1]
            side_name  = "YES"     if edge_yes >= edge_no else "NO"

            if best_edge < CFG["wx_min_edge"]:
                continue
            if best_price > CFG["wx_buy_under"]:
                continue  # only buy cheap — the $1K→$24K strategy buys low and exits at 45¢

            log.info(f"[WX] {city_data['city']} | {side_name} @ {best_price:.2f} "
                     f"| forecast={forecast_yes:.0%} | edge={best_edge*100:.1f}pp | {q[:50]}")
            tg(f"WEATHER {city_data['city']} {side_name} @ {best_price:.0%} "
               f"forecast={forecast_yes:.0%} edge={best_edge*100:.1f}pp | {q[:50]}", "WX")

            place_order(best_token, best_price, CFG["max_usd"], q, "WX")
            _wx_traded.add(cid)
            _wx_positions[best_token] = best_price

        except Exception as e:
            log.error(f"[WX] {e}")

def wx_loop():
    log.info("[WX] Started — NOAA/OpenMeteo vs Polymarket prices | buy <20¢ sell at 45¢")
    last_clear = time.time()
    while True:
        try:
            if time.time() - last_clear > 3600:
                _wx_traded.clear()
                last_clear = time.time()
            wx_scan()
        except Exception as e:
            log.error(f"[WX] loop: {e}")
        time.sleep(CFG["wx_scan_sleep_sec"])

# ── ENGINE: POLITICS — HIGH-LIQUIDITY DIRECTIONAL FOLLOW ─────────────────────
_politics_traded = {}   # token_id -> last trade ts
_POLITICS_KEYWORDS = (
    "election", "president", "senate", "congress", "governor", "mayor",
    "primary", "nominee", "parliament", "poll", "white house", "trump", "biden",
)

def politics_scan():
    now = datetime.now(timezone.utc)
    markets = _fetch_live_markets(max_age=12)
    if not markets:
        return
    scan_total = 0
    scan_keyword = 0
    scan_horizon = 0

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue
            scan_total += 1

            q = (m.get("question") or "")
            q_low = q.lower()
            if not any(k in q_low for k in _POLITICS_KEYWORDS):
                continue
            scan_keyword += 1

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hours_left = (end_dt - now).total_seconds() / 3600
            if not (CFG["politics_min_hours"] <= hours_left <= CFG["politics_max_hours"]):
                continue
            scan_horizon += 1

            liq = float(m.get("liquidity", 0) or 0)
            vol24 = float(m.get("volume24hr", 0) or 0)
            if liq < CFG["politics_min_liq"] or vol24 < CFG["politics_min_vol24"]:
                _skip_record("POL", "liq_or_vol_low")
                continue

            prices = parse_prices(m)
            toks = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            leader_price = max(float(prices[0]), float(prices[1]))
            if not (CFG["politics_min_leader"] <= leader_price <= CFG["politics_max_leader"]):
                _skip_record("POL", "leader_out_of_band")
                continue

            leader_i = 0 if float(prices[0]) >= float(prices[1]) else 1
            token_id = toks[leader_i]
            if token_id in open_positions:
                _skip_record("POL", "already_open")
                continue
            if time.time() - _politics_traded.get(token_id, 0) < CFG["politics_cooldown_sec"]:
                _skip_record("POL", "cooldown")
                continue

            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = leader_price
            if not (CFG["politics_min_leader"] <= live_price <= CFG["politics_max_leader"]):
                _skip_record("POL", "clob_out_of_band")
                continue

            # Consensus gate across arbitrage/convergence/whale-copy agents.
            # Fail-open to current behavior if consensus module/import is unavailable.
            arb_gap = abs((float(prices[0]) + float(prices[1])) - 1.0)
            whale_signal = whale_in_market_cached(token_id)
            market_payload = {
                "question": q,
                "midpoint": live_price,
                "estimate": leader_price,
                "arb_gap": arb_gap,
                "whale_signal": whale_signal,
                "whale_confidence": 0.79 if whale_signal else 0.5,
            }
            size_factor, p_win, c_reason = consensus_decision(market_payload)
            _consensus_record("POL", size_factor, c_reason)
            if size_factor <= 0:
                _skip_record("POL", "consensus_skip")
                log.info(f"[POL] consensus skip — {c_reason} | {(q or '')[:60]}")
                continue

            outcomes = parse_outcomes(m)
            side = outcomes[leader_i] if leader_i < len(outcomes) else ("YES" if leader_i == 0 else "NO")
            roi = (1 - live_price) / max(live_price, 0.01) * 100

            mode = "FULL" if size_factor >= 1 else "HALF"
            log.info(f"[POL] {mode} {side} @ {live_price:.2f} +{roi:.1f}%ROI liq=${liq:,.0f} vol=${vol24:,.0f} | {(q or '')[:60]}")
            tg(f"POL {mode} {side} @ {live_price:.0%} +{roi:.1f}%ROI | {(q or '')[:60]}", "POL")

            size_usd = CFG["max_usd"] * size_factor
            place_order(token_id, live_price, size_usd, (q or "")[:60], "POL", p_win=p_win)
            _politics_traded[token_id] = time.time()
            return  # one trade per scan

        except Exception as e:
            log.error(f"[POL] {e}")

    _scan_record("POL", {
        "markets_seen": scan_total,
        "keyword_matches": scan_keyword,
        "horizon_matches": scan_horizon,
    })

def politics_loop():
    log.info("[POL] Started — politics directional follow | high liq/vol only")
    while True:
        try:
            politics_scan()
        except Exception as e:
            log.error(f"[POL] loop: {e}")
        time.sleep(CFG["politics_scan_sleep_sec"])

# ── ENGINE 3: LIVE / 4-MIN RULE ───────────────────────────────────────────────
# Bet on the leading side (82–96%) of ANY binary market closing in ≤4 min.
# In the final minutes a market can't reverse — direction is locked.
# Works on: live sports, live events, elections night results, crypto milestones.

_live_traded: set = set()

_markets_cache:    list  = []
_markets_cache_ts: float = 0.0

def _fetch_live_markets(max_age: float = 8.0) -> list:
    """Shared market cache — all engines (LIVE, SPORT, NEAR) share one fetch per 8s."""
    global _markets_cache, _markets_cache_ts
    if time.time() - _markets_cache_ts < max_age and _markets_cache:
        return _markets_cache
    d = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=500&sort=volume24hr&ascending=false")
    if d:
        _markets_cache    = d if isinstance(d, list) else d.get("markets", [])
        _markets_cache_ts = time.time()
    return _markets_cache

def live_scan():
    now  = datetime.now(timezone.utc)
    markets = _fetch_live_markets()
    if not markets:
        return
    scan_total = 0
    scan_time_ok = 0
    scan_binary_ok = 0

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid or cid in _live_traded:
                continue
            scan_total += 1

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt    = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            mins_left = (end_dt - now).total_seconds() / 60

            if mins_left > CFG["live_max_mins"]:
                continue
            if mins_left < 0.4:
                continue
            scan_time_ok += 1

            outcomes = parse_outcomes(m)
            if len(outcomes) != 2:
                continue
            scan_binary_ok += 1

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            leader_price = max(prices[0], prices[1])
            leader_i     = prices.index(leader_price)

            if not (CFG["live_min_leader"] <= leader_price <= CFG["live_max_leader"]):
                _skip_record("LIVE", "leader_out_of_band")
                continue

            liq = float(m.get("liquidity", 0) or 0)
            if liq < CFG["live_min_liq"]:
                _skip_record("LIVE", "liq_low")
                continue

            token_id = toks[leader_i]
            if token_id in open_positions:
                _skip_record("LIVE", "already_open")
                continue

            q = (m.get("question") or "")[:60]

            # Verify live CLOB price — gamma prices can be stale by minutes
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = leader_price   # fallback to gamma
            # Re-check price bounds with live price
            if not (CFG["live_min_leader"] <= live_price <= CFG["live_max_leader"]):
                _skip_record("LIVE", "clob_out_of_band")
                log.info(f"[LIVE] {q[:40]} — CLOB {live_price:.2f} outside bounds, skip")
                continue

            arb_gap = abs((float(prices[0]) + float(prices[1])) - 1.0)
            whale_signal = whale_in_market_cached(token_id)
            est = min(0.99, live_price + max(0.01, (CFG["live_max_leader"] - live_price) * 0.35))
            market_payload = {
                "question": q,
                "midpoint": live_price,
                "estimate": est,
                "arb_gap": arb_gap,
                "whale_signal": whale_signal,
                "whale_confidence": 0.79 if whale_signal else 0.5,
            }
            size_factor, p_win, c_reason = consensus_decision(market_payload)
            _consensus_record("LIVE", size_factor, c_reason)
            if size_factor <= 0:
                _skip_record("LIVE", "consensus_skip")
                log.info(f"[LIVE] consensus skip — {c_reason} | {q[:50]}")
                continue

            roi = (1 - live_price) / live_price * 100
            mode = "FULL" if size_factor >= 1 else "HALF"

            log.info(f"[LIVE] {mode} {mins_left:.1f}min | CLOB @ {live_price:.2f} +{roi:.1f}%ROI "
                     f"| liq=${liq:,.0f} | {q}")
            tg(f"LIVE {mode} {outcomes[leader_i]} @ {live_price:.0%} +{roi:.1f}%ROI | {mins_left:.1f}min | {q}", "FOUR")

            size_usd = CFG["max_usd"] * size_factor
            place_order(token_id, live_price, size_usd, q, "FOUR", p_win=p_win)
            _live_traded.add(cid)
            return   # one trade per scan — prevents draining balance in one pass

        except Exception as e:
            log.error(f"[LIVE] {e}")

    _scan_record("LIVE", {
        "markets_seen": scan_total,
        "time_ok": scan_time_ok,
        "binary_ok": scan_binary_ok,
    })

def live_loop():
    log.info("[LIVE] Started — any binary market ≤6min where leader 78–97%")
    last_clear = time.time()
    while True:
        try:
            if time.time() - last_clear > 3600:
                _live_traded.clear()
                last_clear = time.time()
            live_scan()
        except Exception as e:
            log.error(f"[LIVE] loop: {e}")
        time.sleep(CFG["live_scan_sleep_sec"])

# ── ENGINE 4: DRIFT — MOMENTUM ON LIQUID MARKETS ──────────────────────────────
# Works 24/7 on whatever is liquid. Polls top 20 markets every 5 min.
# Buys when price has consistently drifted 4%+ in one direction over 15 min.
# Exits at +25% profit. Avoids already-extreme prices.

_drift_snapshots  = defaultdict(lambda: deque(maxlen=8))  # token_id -> [(ts, mid)]
_drift_positions  = {}   # token_id -> entry_price
_drift_traded     = {}   # token_id -> last_trade_ts (10-min cooldown)
_drift_skip_count = defaultdict(int)   # token_id -> consecutive skip count (evict at 5)

def check_profit_exits():
    # BUG1 FIX: do NOT hold _order_lock here — sell_position acquires it internally.
    # Holding the lock and calling sell_position() caused a deadlock.
    now = time.time()
    for token_id, pos in list(open_positions.items()):
            entry      = float(pos.get("entry", 0))
            if entry <= 0:
                continue
            mid = clob_mid(token_id)
            if mid <= 0:
                continue
            profit_pct = (mid - entry) / entry
            entry_time = float(pos.get("entry_time", now))
            hours_held = (now - entry_time) / 3600
            source     = pos.get("source", "")
            mkt        = pos.get("market", source)
            is_updn    = source.startswith("UPDN")

            # UPDN markets: hold to resolution — only exit on full resolve or stop-loss
            if is_updn:
                if mid >= 0.95:
                    # Sell 2 ticks below mid to become a taker and guarantee fill,
                    # rather than posting a maker offer that may not get hit
                    exit_price = round(max(0.90, mid - 0.02), 3)
                    log.info(f"[EXIT] RESOLVED WIN | {token_id[:16]} profit={profit_pct*100:+.1f}% sell@{exit_price}")
                    tg(f"EXIT RESOLVED WIN +{profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "RESOLVED")
                elif mid <= 0.05:
                    exit_price = round(min(0.10, mid + 0.02), 3)
                    log.info(f"[EXIT] RESOLVED LOSS | {token_id[:16]} profit={profit_pct*100:+.1f}% sell@{exit_price}")
                    tg(f"EXIT RESOLVED LOSS {profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "RESOLVED")
                elif profit_pct <= -0.40:
                    exit_price = round(max(0.01, mid - 0.02), 3)
                    log.info(f"[EXIT] STOP-LOSS | {token_id[:16]} profit={profit_pct*100:+.1f}% sell@{exit_price}")
                    tg(f"EXIT STOP-LOSS {profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "STOP")
                continue

            # Non-UPDN: Rule 1 — target hit (85% of expected move, min 15%)
            expected_gap = max(float(pos.get("expected_gap", 0.15)), 0.15)
            target_pct   = expected_gap * 0.85
            if profit_pct >= target_pct or mid >= 0.95:
                reason = f"+{profit_pct*100:.1f}% TARGET" if profit_pct >= target_pct else "RESOLVED"
                log.info(f"[EXIT] {reason} | {token_id[:16]} entry={entry:.3f} now={mid:.3f}")
                tg(f"EXIT {reason} | {token_id[:16]}", "EXIT")
                sell_position(token_id, mid, mkt, "PROFIT")
                continue

            # Rule 2: volume spike — 3× normal signals smart money leaving
            hist = list(price_history.get(token_id, []))
            if len(hist) >= 10:
                recent_vol   = sum(abs(hist[i] - hist[i-1]) for i in range(-5, 0))
                baseline_vol = sum(abs(hist[i] - hist[i-1]) for i in range(-10, -5))
                if baseline_vol > 0 and recent_vol > baseline_vol * 3:
                    log.info(f"[EXIT] VOLUME SPIKE | {token_id[:16]} profit={profit_pct*100:+.1f}%")
                    tg(f"EXIT VOLUME SPIKE | {token_id[:16]}", "EXIT")
                    sell_position(token_id, mid, mkt, "VOLUME")
                    continue

            # Rule 3: stale thesis — 24h, barely moved
            if hours_held > 24 and abs(profit_pct) < 0.02:
                log.info(f"[EXIT] STALE {hours_held:.0f}h | {token_id[:16]} profit={profit_pct*100:+.1f}%")
                tg(f"EXIT STALE {hours_held:.0f}h | {token_id[:16]}", "EXIT")
                sell_position(token_id, mid, mkt, "STALE")

def drift_scan():
    now = time.time()
    data = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=100&sort=volume24hr&ascending=false")
    if not data:
        return
    markets = data if isinstance(data, list) else data.get("markets", [])

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue

            if any(bk in (m.get("question") or "").lower() for bk in BANNED_KEYWORDS):
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            days_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 86400
            if days_left < 0.5 or days_left > 200:
                continue

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            liq = float(m.get("liquidity", 0) or 0)
            if liq < 200:
                continue

            yes_p = float(prices[0])
            no_p  = float(prices[1])
            yes_tid = toks[0]
            no_tid  = toks[1]

            # Track YES mid price over time
            mid_yes = clob_mid(yes_tid)
            if mid_yes <= 0:
                mid_yes = yes_p
            _drift_snapshots[yes_tid].append((now, mid_yes))

            snaps = list(_drift_snapshots[yes_tid])
            if len(snaps) < CFG["drift_readings"]:
                continue

            old_price = snaps[0][1]
            new_price = snaps[-1][1]
            drift     = (new_price - old_price) / max(old_price, 0.001)

            # Must be consistently moving (no reversals)
            price_seq = [s[1] for s in snaps]
            is_up   = all(price_seq[i] <= price_seq[i+1] + 0.005 for i in range(len(price_seq)-1))
            is_down = all(price_seq[i] >= price_seq[i+1] - 0.005 for i in range(len(price_seq)-1))

            def _evict(tid, reason):
                _drift_snapshots.pop(tid, None)
                _drift_skip_count.pop(tid, None)
                log.info(f"[DRIFT] Evicted {tid[:16]}... — {reason} (will find better market)")

            if not (is_up or is_down):
                _drift_skip_count[yes_tid] += 1
                if _drift_skip_count[yes_tid] >= 5:
                    _evict(yes_tid, "no consistent trend")
                continue
            if abs(drift) < CFG["drift_min_move"]:
                _drift_skip_count[yes_tid] += 1
                if _drift_skip_count[yes_tid] >= 5:
                    _evict(yes_tid, f"drift {drift*100:+.1f}% too thin")
                continue

            # Pick which side to bet — use actual NO mid price, not assumed 1-yes
            if is_up and drift > 0:
                bet_tid   = yes_tid
                bet_price = new_price
                side      = "YES"
            else:
                bet_tid   = no_tid
                no_mid    = clob_mid(no_tid)
                bet_price = no_mid if no_mid > 0 else float(no_p)
                side      = "NO"

            if not (CFG["drift_min_price"] <= bet_price <= CFG["drift_max_price"]):
                _drift_skip_count[yes_tid] += 1
                if _drift_skip_count[yes_tid] >= 5:
                    _evict(yes_tid, f"price {bet_price:.2f} outside range")
                continue

            # Cooldown check
            if now - _drift_traded.get(bet_tid, 0) < 600:
                continue
            if bet_tid in open_positions:
                continue

            q = (m.get("question") or "")[:60]
            roi = (1 - bet_price) / bet_price * 100
            log.info(f"[DRIFT] {side} drift={drift*100:+.1f}% @ {bet_price:.3f} +{roi:.0f}%ROI | {q}")
            tg(f"DRIFT {side} {drift*100:+.1f}% move @ {bet_price:.2f} +{roi:.0f}%ROI | {q}", "DRIFT")

            place_order(bet_tid, bet_price, CFG["max_usd"], q, "DRIFT")
            _drift_traded[bet_tid] = now
            _drift_skip_count[yes_tid] = 0   # reset on successful trade
            return  # one trade per scan cycle

        except Exception as e:
            log.error(f"[DRIFT] {e}")

def drift_loop():
    log.info("[DRIFT] Started — momentum on top liquid markets | 2.5% drift | exit +20%")
    while True:
        try:
            check_profit_exits()
            drift_scan()
        except Exception as e:
            log.error(f"[DRIFT] loop: {e}")
        time.sleep(120)

# ── ENGINE 4b: LIVE SPORTS — SCAN MARKETS 6-30 MIN OUT AT HIGH CONVICTION ─────
# Catches live sports/events markets while still tradeable (not just last 6 min).
# Targets: leader 90-98%, min $500 liquidity, resolving within 30 min.
# These are near-certain wins — the match is effectively over, market just hasn't closed.

_sports_traded = {}   # token_id -> trade_ts

SPORTS_KEYWORDS = (
    "tennis", "basketball", "football", "soccer", "cricket", "esports",
    "golf", "formula 1", " f1 ", "race", "ufc", "boxing", "nfl", "nba",
    "nhl", "mlb", "mls", "serie a", "premier league", "bundesliga",
    "ligue 1", "la liga", "wimbledon", "grand slam", "atp", "wta",
    "beats", "wins set", "wins game", "wins match", "final score",
)

def sports_scan():
    now = datetime.now(timezone.utc)
    now_ts = time.time()
    markets = _fetch_live_markets()
    if not markets:
        return

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt    = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            mins_left = (end_dt - now).total_seconds() / 60

            # 6–45 minute window (LIVE engine handles <6min)
            if mins_left > 45 or mins_left < 6:
                continue

            q_low = (m.get("question") or "").lower()
            # Only live sports/events or crypto UPDN
            is_sports = any(kw in q_low for kw in SPORTS_KEYWORDS)
            is_updn   = "up or down" in q_low

            if not (is_sports or is_updn):
                continue

            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            liq = float(m.get("liquidity", 0) or 0)
            if liq < 200:
                continue

            leader_price = max(prices[0], prices[1])
            leader_i     = prices.index(leader_price)

            # For this engine: high conviction, slightly widened for throughput
            if not (CFG["sports_min_leader"] <= leader_price <= CFG["sports_max_leader"]):
                continue

            token_id = toks[leader_i]

            # Cooldown: only once per token per 2 hours
            if now_ts - _sports_traded.get(token_id, 0) < 7200:
                continue
            if token_id in open_positions:
                continue

            outcomes = parse_outcomes(m)
            roi      = (1 - leader_price) / leader_price * 100
            q        = (m.get("question") or "")[:60]
            kind     = "UPDN" if is_updn else "SPORT"

            log.info(f"[{kind}] {mins_left:.0f}min | leader {leader_price:.2f} +{roi:.1f}%ROI liq=${liq:,.0f} | {q}")
            tg(f"{kind} {mins_left:.0f}min {leader_price:.0%} +{roi:.1f}%ROI | {q}", "LIVE")

            place_order(token_id, leader_price, CFG["max_usd"], q, kind)
            _sports_traded[token_id] = now_ts
            return   # one trade per scan

        except Exception as e:
            log.error(f"[SPORT] {e}")

def sports_loop():
    log.info("[SPORT] Started — live sports/events + UPDN in 6-45min window at 90-98% conviction")
    while True:
        try:
            sports_scan()
        except Exception as e:
            log.error(f"[SPORT] loop: {e}")
        time.sleep(CFG["sports_scan_sleep_sec"])

# ── ENGINE 5: NEAR — HIGH-CONVICTION DIRECTIONAL BIAS ─────────────────────────
# Scans top liquid markets RIGHT NOW. If a market has a strong directional bias
# (price 72-88¢) with high 24h volume AND tight spread AND short horizon, ride it.
# Tightened from original 60-88% / 30d to 72-88% / 7d to reduce reversal risk.

_near_traded     = {}   # token_id -> last trade time (4h cooldown)
_near_blacklist  = {}   # cid -> expiry_ts (skip for 30min when repeatedly filtered)
_near_skip_count = defaultdict(int)   # cid -> consecutive skip count

BANNED_KEYWORDS = (
    "world cup", "fifa", "election", "president", "senate", "congress",
    "2028", "2027", "nominee", "nomina", "political", "inaugur",
    "gta vi", "gta6", "jesus christ", "rihanna", "carti", "bond actor",
    "ceasefire", "putin", "xi jinping",
    "nba finals", "stanley cup", "champions league winner", "super bowl",
    "world series", "win the 2026", "win the 2025",
)

def near_scan():
    now = time.time()
    markets = _fetch_live_markets()
    if not markets:
        return

    now_dt = datetime.now(timezone.utc)
    near_candidates = []
    for market in markets:
        try:
            q_low = (market.get("question") or "").lower()
            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue
            end_str = market.get("endDate") or ""
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            days_left = (end_dt - now_dt).total_seconds() / 86400
            if not (CFG["near_min_days"] <= days_left <= CFG["near_max_days"]):
                continue
            liq = float(market.get("liquidity", 0) or 0)
            if liq < CFG["near_min_liq"]:
                continue
            vol24 = float(market.get("volume24hr", 0) or 0)
            if vol24 < CFG["near_prefilter_vol24"]:
                continue
            score = vol24 + liq * 0.25
            near_candidates.append((score, market))
        except Exception:
            continue
    markets = [market for _, market in sorted(near_candidates, key=lambda item: item[0], reverse=True)[:CFG["near_prefilter_limit"]]]
    if not markets:
        return

    # Purge expired blacklist entries
    for k in [k for k, exp in list(_near_blacklist.items()) if now > exp]:
        del _near_blacklist[k]
        _near_skip_count.pop(k, None)

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue

            # Skip blacklisted markets (repeatedly filtered — find better ones)
            if cid in _near_blacklist:
                _skip_record("NEAR", "blacklisted")
                continue

            q_low = (m.get("question") or "").lower()
            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue

            vol24 = float(m.get("volume24hr", 0) or 0)
            if vol24 < CFG["near_min_vol24"]:
                _skip_record("NEAR", "vol_low")
                _near_skip_count[cid] += 1
                if _near_skip_count[cid] >= CFG["near_vol_skip_blacklist_hits"]:
                    _near_blacklist[cid] = now + CFG["near_blacklist_sec"]
                    mins = max(1, int(CFG["near_blacklist_sec"] // 60))
                    log.info(f"[NEAR] Blacklisted vol-low market for {mins}min | {q_low[:50]}")
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            days_left = (end_dt - now_dt).total_seconds() / 86400
            if days_left < CFG["near_min_days"] or days_left > CFG["near_max_days"]:
                _skip_record("NEAR", "horizon_out_of_band")
                continue

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            yes_p   = float(prices[0])
            no_p    = float(prices[1])
            liq     = float(m.get("liquidity", 0) or 0)
            if liq < CFG["near_min_liq"]:
                _skip_record("NEAR", "liq_low")
                _near_skip_count[cid] += 1
                if _near_skip_count[cid] >= 4:
                    _near_blacklist[cid] = now + 1800
                    log.info(f"[NEAR] Blacklisted liq-low market for 30min | {q_low[:50]}")
                continue

            # High-conviction directional band, slightly widened for throughput
            if CFG["near_min_leader"] <= yes_p <= CFG["near_max_leader"]:
                bet_tid, bet_price, side = toks[0], yes_p, "YES"
            elif CFG["near_min_leader"] <= no_p <= CFG["near_max_leader"]:
                bet_tid, bet_price, side = toks[1], no_p, "NO"
            else:
                _skip_record("NEAR", "leader_out_of_band")
                _near_skip_count[cid] += 1
                if _near_skip_count[cid] >= 6:
                    _near_blacklist[cid] = now + 1800
                    log.info(f"[NEAR] Blacklisted no-edge market for 30min | {q_low[:50]}")
                continue

            # Verify tight spread on CLOB (liquid enough to enter/exit)
            mid = clob_mid(bet_tid)
            if mid <= 0:
                _skip_record("NEAR", "mid_unavailable")
                continue
            spread_ratio = abs(mid - bet_price) / max(bet_price, 0.01)
            if spread_ratio > 0.05:   # mid must be within 5% of gamma price
                _skip_record("NEAR", "wide_gap")
                _near_skip_count[cid] += 1
                if _near_skip_count[cid] >= 4:
                    _near_blacklist[cid] = now + 1800
                    log.info(f"[NEAR] Blacklisted wide-gap market for 30min | {q_low[:50]}")
                continue

            # Cooldown: 4 hours per token
            if now - _near_traded.get(bet_tid, 0) < 14400:
                _skip_record("NEAR", "cooldown")
                continue
            if bet_tid in open_positions:
                _skip_record("NEAR", "already_open")
                continue

            arb_gap = abs((yes_p + no_p) - 1.0)
            whale_signal = whale_in_market_cached(bet_tid)
            est = min(0.99, bet_price + 0.06)
            market_payload = {
                "question": (m.get("question") or "")[:200],
                "midpoint": bet_price,
                "estimate": est,
                "arb_gap": arb_gap,
                "whale_signal": whale_signal,
                "whale_confidence": 0.79 if whale_signal else 0.5,
            }
            size_factor, p_win, c_reason = consensus_decision(market_payload)
            _consensus_record("NEAR", size_factor, c_reason)
            if size_factor <= 0:
                _skip_record("NEAR", "consensus_skip")
                log.info(f"[NEAR] consensus skip — {c_reason} | {(m.get('question') or '')[:50]}")
                continue

            roi = (1 - bet_price) / bet_price * 100
            q   = (m.get("question") or "")[:60]
            mode = "FULL" if size_factor >= 1 else "HALF"
            log.info(f"[NEAR] {mode} {side} @ {bet_price:.2f} +{roi:.0f}%ROI liq=${liq:,.0f} vol=${vol24:,.0f} | {q}")
            tg(f"NEAR {mode} {side} @ {bet_price:.2f} +{roi:.0f}%ROI | vol=${vol24:,.0f} | {q}", "NEAR")

            size_usd = CFG["max_usd"] * size_factor
            place_order(bet_tid, bet_price, size_usd, q, "NEAR", p_win=p_win)
            _near_traded[bet_tid] = now
            _near_skip_count[cid] = 0   # reset on trade
            return  # one trade per scan cycle

        except Exception as e:
            log.error(f"[NEAR] {e}")

def near_loop():
    log.info(f"[NEAR] Started — high-conviction bias on top-volume markets | {CFG['near_min_leader']:.0%}-{CFG['near_max_leader']:.0%} leader")
    while True:
        try:
            near_scan()
        except Exception as e:
            log.error(f"[NEAR] loop: {e}")
        time.sleep(CFG["near_scan_sleep_sec"])

# ── ENGINE 8: AUTO-REDEMPTION — CLAIM WON POSITIONS FOR USDC ─────────────────
# Polymarket settles most positions automatically, but some require an on-chain
# redeemPositions call to the ConditionalTokens contract.
# Uses: web3.py → Gnosis Safe execTransaction from EOA → ConditionalTokens.redeemPositions

_POLYGON_RPC  = "https://polygon-bor-rpc.publicnode.com"
_CT_ADDR      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_USDC_POLY    = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_ZERO_ADDR    = "0x0000000000000000000000000000000000000000"
_CT_ABI = [{"name":"redeemPositions","type":"function","inputs":[
    {"name":"collateralToken","type":"address"},
    {"name":"parentCollectionId","type":"bytes32"},
    {"name":"conditionId","type":"bytes32"},
    {"name":"indexSets","type":"uint256[]"},
],"outputs":[]}]
_SAFE_ABI = [
    {"name":"nonce","type":"function","inputs":[],"outputs":[{"type":"uint256"}]},
    {"name":"getTransactionHash","type":"function","outputs":[{"type":"bytes32"}],"inputs":[
        {"name":"to","type":"address"},{"name":"value","type":"uint256"},
        {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
        {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
        {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
        {"name":"refundReceiver","type":"address"},{"name":"_nonce","type":"uint256"},
    ]},
    {"name":"execTransaction","type":"function","outputs":[{"type":"bool"}],"inputs":[
        {"name":"to","type":"address"},{"name":"value","type":"uint256"},
        {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
        {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
        {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
        {"name":"refundReceiver","type":"address"},{"name":"signatures","type":"bytes"},
    ]},
]

_redeemed_cids: set = set()

def _redeem_position(w3, acct, cid_hex: str, outcome: str) -> bool:
    """Call ConditionalTokens.redeemPositions via the Gnosis Safe proxy wallet."""
    try:
        funder_cs = Web3.to_checksum_address(FUNDER)
        ct_cs     = Web3.to_checksum_address(_CT_ADDR)
        usdc_cs   = Web3.to_checksum_address(_USDC_POLY)

        # YES=indexSet[1], NO=indexSet[2]
        index_set = [1] if outcome.lower() in ("yes", "up") else [2]

        ct   = w3.eth.contract(address=ct_cs,     abi=_CT_ABI)
        safe = w3.eth.contract(address=funder_cs, abi=_SAFE_ABI)

        cid_bytes = bytes.fromhex(cid_hex.replace("0x", ""))
        call_data = ct.encode_abi("redeemPositions", [usdc_cs, b"\x00"*32, cid_bytes, index_set])

        nonce_safe = safe.functions.nonce().call()
        tx_hash    = safe.functions.getTransactionHash(
            ct_cs, 0, call_data, 0, 0, 0, 0, _ZERO_ADDR, _ZERO_ADDR, nonce_safe
        ).call()

        signed  = acct.sign_message(encode_defunct(tx_hash))
        sig     = signed.r.to_bytes(32,"big") + signed.s.to_bytes(32,"big") + bytes([signed.v + 4])

        # Check EOA has MATIC for gas
        matic_bal = w3.eth.get_balance(acct.address)
        gas_price = w3.eth.gas_price
        if matic_bal < gas_price * 200_000:
            matic_needed = w3.from_wei(gas_price * 200_000 - matic_bal, "ether")
            tg(f"REDEEM blocked — need {matic_needed:.4f} MATIC for gas at {acct.address[:16]}...", "WARN")
            return False

        nonce_eoa = w3.eth.get_transaction_count(acct.address)
        tx = safe.functions.execTransaction(
            ct_cs, 0, call_data, 0, 0, 0, 0, _ZERO_ADDR, _ZERO_ADDR, sig
        ).build_transaction({"from": acct.address, "nonce": nonce_eoa,
                             "gas": 250_000, "gasPrice": gas_price, "chainId": 137})

        signed_tx = acct.sign_transaction(tx)
        tx_sent   = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt   = w3.eth.wait_for_transaction_receipt(tx_sent, timeout=60)
        if receipt.status == 1:
            tg(f"REDEEMED {cid_hex[:16]}... | tx={tx_sent.hex()[:16]}... | gas={receipt.gasUsed}", "INFO")
            return True
        else:
            log.error(f"[REDEEM] tx reverted for {cid_hex[:16]}...")
            return False
    except Exception as e:
        log.error(f"[REDEEM] {e}")
        return False

def redeem_loop():
    if not _WEB3_OK:
        log.warning("[REDEEM] web3.py not available — skipping redemption loop")
        return
    try:
        w3   = Web3(Web3.HTTPProvider(_POLYGON_RPC, request_kwargs={"timeout": 15}))
        acct = w3.eth.account.from_key("0x" + PRIVATE_KEY)
    except Exception as e:
        log.error(f"[REDEEM] Init failed: {e}")
        return

    log.info(f"[REDEEM] Started — EOA={acct.address[:16]}... | checks every 5min")
    while True:
        try:
            positions = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.01")
            if isinstance(positions, list):
                for pos in positions:
                    cid       = pos.get("conditionId", "")
                    redeemable = bool(pos.get("redeemable"))
                    size      = float(pos.get("size", 0) or 0)
                    outcome   = pos.get("outcome", "Yes")
                    title     = pos.get("title", "?")[:50]
                    if not redeemable or not cid or size < 0.1 or cid in _redeemed_cids:
                        continue
                    log.info(f"[REDEEM] Found redeemable: {title} | size={size:.2f} | outcome={outcome}")
                    if _redeem_position(w3, acct, cid, outcome):
                        _redeemed_cids.add(cid)
                        tg(f"CLAIMED {size:.2f} shares | {title}", "INFO")
        except Exception as e:
            log.error(f"[REDEEM] loop: {e}")
        time.sleep(300)   # check every 5 minutes

# ── WEBSOCKET — MOMENTUM ON LIVE MARKETS ──────────────────────────────────────
_ws_proxy_keys  = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
_mtum_cooldown  = {}   # token_id -> last order time, 5-min cooldown per token

# ── ENGINE 6: COPY — WHALE TRACKING ───────────────────────────────────────────
# Tracks known high-profit wallets. When a whale makes a fresh BUY (<30 min ago)
# in a non-banned, non-extreme-priced market, we copy at 10% of their size (min $1).

WHALE_FILE   = BASE / "elite_wallets.json"
# Top Polymarket traders by 30-day + all-time PnL (updated from leaderboard API)
_SEED_WHALES = [
    # T1 — consistent in BOTH monthly and all-time top 50
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7",  # HorizonSplendidView  30d=$4.0M all=$2.0M
    "0xefbc5fec8d7b0acdc8911bdd9a98d6964308f9a2",  # reachingthesky       30d=$3.7M all=$3.7M
    "0x2a2c53bd278c04da9962fcf96490e17f3dfb9bc1",  # anon                 30d=$2.7M all=$2.7M
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51",  # beachboy4            30d=$2.7M all=$3.0M
    "0x019782cab5d844f02bafb71f512758be78579f3c",  # majorexploiter       30d=$2.4M all=$3.7M
    "0x2005d16a84ceefa912d4e380cd32e7ff827875ea",  # RN1                  30d=$2.1M all=$7.3M
    "0xee613b3fc183ee44f9da9c05f53e2da107e3debf",  # sovereign2013        30d=$1.8M all=$3.5M
    "0xdc876e6873772d38716fda7f2452a78d426d7ab6",  # 432614799197         30d=$1.5M all=$4.5M
    "0x204f72f35326db932158cba6adff0b9a1da95e14",  # swisstony            30d=$1.5M all=$6.1M
    "0x93abbc022ce98d6f45d4444b594791cc4b7a9723",  # gatorr               30d=$1.4M all=$2.2M
    "0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee",  # kch123               30d=$1.0M all=$11.9M
    "0x507e52ef684ca2dd91f90a9d26d149dd3288beae",  # GamblingIsAllYouNeed 30d=$866K all=$4.7M
    "0x8c80d213c0cbad777d06ee3f58f6ca4bc03102c3",  # SecondWindCapital    30d=$786K all=$1.9M
    # T2 — hot recent traders (new or surge)
    "0x492442eab586f242b53bda933fd5de859c8a3782",  # anon                 30d=$7.1M
    "0xf195721ad850377c96cd634457c70cd9e8308057",  # lo34567Taipe         30d=$1.5M
    "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a",  # bossoskil1           30d=$1.4M
    "0xc8075693f48668a264b9fa313b47f52712fcc12b",  # texaskid             30d=$1.3M
    "0xead152b855effa6b5b5837f53b24c0756830c76a",  # elkmonkey            30d=$1.2M
    "0x777d9f00c2b4f7b829c9de0049ca3e707db05143",  # CarlosMC             30d=$1.1M
    "0xbaa2bcb5439e985ce4ccf815b4700027d1b92c73",  # denizz               30d=$1.0M
    # T3 — all-time legends (may be less active recently)
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",  # Theo4                all=$22.1M
    "0x1f2dd6d473f3e824cd2f8a89d9c69fb96f6ad0cf",  # Fredi9999            all=$16.6M
    "0x78b9ac44a6d7d7a076c14e0ad518b301b63c6b76",  # Len9311238           all=$8.7M
    "0xd235973291b2b75ff4070e9c0b01728c520b0f29",  # zxgngl               all=$7.8M
    "0x863134d00841b2e200492805a01e1e2f5defaa53",  # RepTrump             all=$7.5M
    # T4 — CRYPTO/UPDN specialists (top 15 by 30d crypto PnL from leaderboard)
    "0xde17f7144fbd0eddb2679132c10ff5e74b120988",  # crypto#1             30d=$727K
    "0xd84c2b6d65dc596f49c7b6aadd6d74ca91e407b9",  # BoneReader           30d=$614K
    "0xd0d6053c3c37e727402d84c14069780d360993aa",  # k9Q2mX4L8A7ZP3R      30d=$536K
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a",  # 0x8dxd               30d=$535K
    "0xe1d6b51521bd4365769199f392f9818661bd907c",  # crypto#5             30d=$521K
    "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30",  # Bonereaper           30d=$492K
    "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",  # crypto#7             30d=$490K
    "0x6e1d5040d0ac73709b0621f620d2a60b80d2d0fa",  # crypto#8             30d=$438K
    "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a",  # vidarx               30d=$417K
    "0x1f0ebc543b2d411f66947041625c0aa1ce61cf86",  # crypto#10            30d=$386K
    "0x0006af12cd4dacc450836a0e1ec6ce47365d8c63",  # stingo43             30d=$365K
    "0x04283f2fef49d70d8c55ab240450d17a65bf85b1",  # crypto#12            30d=$306K
    "0x89b5cdaaa4866c1e738406712012a630b4078beb",  # ohanism              30d=$293K
    "0x2eb5714ff6f20f5f9f7662c556dbef5e1c9bf4d4",  # crypto#14            30d=$274K
    "0x3a847382ad6fff9be1db4e073fd9b869f6884d44",  # crypto#15            30d=$254K
]
COPY_WALLETS = []   # populated from file + auto-discovery

def _load_whales():
    """Load tracked wallets from file, supplement with auto-discovered."""
    global COPY_WALLETS
    COPY_WALLETS = list(_SEED_WHALES)
    try:
        data = json.loads(WHALE_FILE.read_text())
        # elite_wallets.json is a plain list; copy_wallets.json may use {"wallets": [...]}
        stored = data if isinstance(data, list) else data.get("wallets", [])
        for w in stored:
            if isinstance(w, str):
                if w and w not in COPY_WALLETS:
                    COPY_WALLETS.append(w)
            elif isinstance(w, dict):
                addr = w.get("wallet") or w.get("address") or w.get("proxyWallet") or ""
                if addr and addr not in COPY_WALLETS:
                    COPY_WALLETS.append(addr)
    except Exception as e:
        log.warning(f"[COPY] elite_wallets load failed: {e}")
    # Also seed from copy_state.json
    try:
        cs = json.loads((BASE / "copy_state.json").read_text())
        if isinstance(cs, dict):
            for v in cs.values():
                w = v.get("proxyWallet", "")
                if w and w not in COPY_WALLETS:
                    COPY_WALLETS.append(w)
    except Exception:
        pass
    # Also load top-ranked wallets from targets.json (poly_data analysis)
    try:
        targets = json.loads(_TARGETS_FILE.read_text())
        for t in targets:
            addr = t.get("wallet", "")
            if addr and addr not in COPY_WALLETS:
                COPY_WALLETS.append(addr)
    except Exception:
        pass
    if COPY_WALLETS:
        log.info(f"[COPY] Loaded {len(COPY_WALLETS)} whale wallets")

_ACTIVE_CUTOFF_DAYS = 14   # drop wallets with no trades in 14 days

def _is_wallet_active(wallet: str) -> bool:
    """Return True if wallet traded within ACTIVE_CUTOFF_DAYS. On network error → True (keep wallet)."""
    data = fetch(f"https://data-api.polymarket.com/activity?user={wallet}&limit=1")
    if data is None:
        return True   # network error: assume still active rather than purging
    if not isinstance(data, list) or not data:
        return False  # empty list = no trades ever
    ts = data[0].get("timestamp", 0)
    try:
        raw_ts = float(ts)
        if raw_ts > 1e12:
            raw_ts /= 1000   # convert ms → s
        age_days = (time.time() - raw_ts) / 86400
        return age_days <= _ACTIVE_CUTOFF_DAYS
    except Exception:
        return True  # parse error: keep wallet

def _discover_whales():
    """Find large active wallets from top-volume markets (fresh every 4h)."""
    found = []
    data = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=20&sort=volume24hr&ascending=false")
    if not data:
        return []
    markets = data if isinstance(data, list) else data.get("markets", [])
    for m in markets[:10]:
        cid = m.get("conditionId") or m.get("id") or ""
        if not cid:
            continue
        # Each active market's recent traders are potential copy targets
        activity = fetch(f"https://data-api.polymarket.com/activity?market={cid}&limit=50")
        if not isinstance(activity, list):
            continue
        for trade in activity:
            wallet   = trade.get("proxyWallet", "")
            usd_size = float(trade.get("usdcSize", 0) or 0)
            if wallet and usd_size >= 1000 and wallet not in found and wallet != FUNDER:
                found.append(wallet)
    return found[:30]

def _refresh_whale_list():
    """Drop inactive wallets, discover fresh ones. Run every 4h."""
    global COPY_WALLETS
    # Filter: keep only wallets active in last 14 days
    active = []
    for w in list(COPY_WALLETS):
        if _is_wallet_active(w):
            active.append(w)
        else:
            log.debug(f"[COPY] Dropping inactive wallet {w[:18]}")
    # Discover new active whales from top markets
    new_whales = _discover_whales()
    added = 0
    for w in new_whales:
        if w not in active:
            active.append(w)
            added += 1
    if not active:
        log.warning("[COPY] Refresh produced empty list (API failure?) — keeping old list")
    else:
        COPY_WALLETS = active
    log.info(f"[COPY] Whale list refreshed: {len(COPY_WALLETS)} active ({added} new discovered)")

_copy_seen:    dict = {}  # cid -> timestamp — prune entries older than 2h to prevent unbounded growth
_copy_wallets_checked = 0

def copy_scan():
    global _copy_wallets_checked
    if not COPY_WALLETS:
        return

    now_ts = time.time()
    wallet = COPY_WALLETS[_copy_wallets_checked % len(COPY_WALLETS)]
    _copy_wallets_checked += 1

    try:
        activity = fetch(f"https://data-api.polymarket.com/activity?user={wallet}&limit=20")
        if not isinstance(activity, list):
            return

        now_dt = datetime.now(timezone.utc)
        for trade in activity:
            raw_ts  = int(trade.get("timestamp", 0) or 0)
            # Polymarket API returns seconds; guard against accidental ms timestamps
            if raw_ts > 1e12:
                raw_ts //= 1000
            age_min = (now_ts - raw_ts) / 60
            if age_min > 45:   # only trades in last 45 min (scan cycle ~15min, 45min = 3x buffer)
                continue
            if age_min < 0:
                continue       # clock skew guard

            if trade.get("type") != "TRADE":
                continue
            # BUG5 FIX: only copy whale BUYs — copying their SELLs as our BUYs is wrong
            side = str(trade.get("side") or "").upper()
            if side in ("SELL", "2", "SHORT"):
                continue
            cid      = trade.get("conditionId", "")
            usd_size = float(trade.get("usdcSize", 0) or 0)
            price    = float(trade.get("price", 0) or 0)
            size     = float(trade.get("size", 0) or 0)

            if not cid or price <= 0 or usd_size < 50:  # lowered from $100 — more signals
                continue
            # Prune _copy_seen entries older than 2h to prevent unbounded growth
            cutoff = now_ts - 7200
            for k in [k for k, v in _copy_seen.items() if v < cutoff]:
                del _copy_seen[k]
            if cid in _copy_seen:
                continue

            if size <= 0 or not (0.10 <= price <= 0.90):
                continue

            # Resolve to token + market question
            mkt_data = fetch(f"https://gamma-api.polymarket.com/markets?conditionId={cid}")
            if not mkt_data:
                continue
            markets_list = mkt_data if isinstance(mkt_data, list) else mkt_data.get("markets", [])
            if not markets_list:
                continue
            m = markets_list[0]

            q_low = (m.get("question") or "").lower()
            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue

            # Skip if market has already closed or has < 5 min left (no time to profit)
            end_str = m.get("endDate") or ""
            if end_str:
                try:
                    end_dt    = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    mins_left = (end_dt - now_dt).total_seconds() / 60
                    if mins_left < 5:
                        continue
                except Exception:
                    pass

            toks = parse_tokens(m)
            if not toks:
                continue

            # Match token by price proximity to whale's trade price
            prices   = parse_prices(m)
            token_id = toks[0]
            for i, p in enumerate(prices):
                if abs(float(p) - price) < 0.06 and i < len(toks):
                    token_id = toks[i]
                    break

            # Use CURRENT live CLOB price — not whale's stale price
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = price   # fallback to whale's price if CLOB unavailable
            # Don't chase if price moved >15% against us since whale's trade
            if live_price > price * 1.15:
                log.info(f"[COPY] {wallet[:14]} price chased too far {price:.2f}→{live_price:.2f}, skip")
                continue
            if not (0.10 <= live_price <= 0.90):
                continue

            # Scale to 10% of whale's trade — place_order applies its own caps (max_usd, Kelly)
            our_usd = usd_size * 0.10
            q = (m.get("question") or "")[:60]

            log.info(f"[COPY] {wallet[:14]}... ${usd_size:.0f} → copy ${our_usd:.1f} @ {live_price:.3f} | {q}")
            tg(f"COPY whale ${usd_size:.0f} → ${our_usd:.1f} @ {live_price:.2f} | {q}", "COPY")

            place_order(token_id, live_price, our_usd, q, "COPY")
            _copy_seen[cid] = now_ts

    except Exception as e:
        log.debug(f"[COPY] {wallet[:14]}: {e}")

def copy_loop():
    _load_whales()
    _refresh_whale_list()   # filter inactive + discover fresh on startup
    log.info(f"[COPY] Started — tracking {len(COPY_WALLETS)} wallets | copies at 10% size")
    last_refresh = time.time()
    while True:
        try:
            # Refresh whale list every 4 hours
            if time.time() - last_refresh > 4 * 3600:
                _refresh_whale_list()
                last_refresh = time.time()
            for _ in range(CFG["copy_wallets_per_scan"]):
                copy_scan()
        except Exception as e:
            log.error(f"[COPY] loop: {e}")
        time.sleep(CFG["copy_scan_sleep_sec"])

def top_tokens(n=50):
    tokens = []
    for limit in [100, 200, 500]:
        data = fetch(f"https://gamma-api.polymarket.com/markets?active=true&limit={limit}&sort=volume24hr&ascending=false")
        if not data:
            break
        markets = data if isinstance(data, list) else data.get("markets", [])
        tokens = []
        for m in markets:
            prices = parse_prices(m)
            if len(prices) < 2:
                continue
            yes = float(prices[0])
            if yes < 0.02 or yes > 0.98:   # only skip fully resolved
                continue
            vol24 = float(m.get("volume24hr", 0) or 0)
            if vol24 < 100:
                continue
            toks = parse_tokens(m)
            if toks:
                tokens.append({"token_id": toks[0], "market": (m.get("question") or "")[:60], "yes": yes})
            if len(tokens) >= n:
                break
        if len(tokens) >= n:
            break
    return tokens[:n]

def momentum_signal(token_id: str) -> dict | None:
    prices = list(price_history[token_id])
    if len(prices) < CFG["lookback"]:
        return None
    book = order_books.get(token_id, {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return None
    bid = max(float(b["price"]) for b in bids)
    ask = min(float(a["price"]) for a in asks)
    if ask - bid > 0.08:
        return None
    mid = (bid + ask) / 2
    if mid > 0.80 or mid < 0.20:
        return None
    old  = sum(prices[:5]) / 5
    new  = sum(prices[-5:]) / 5
    move = (new - old) / max(old, 0.001)
    if move > CFG["momentum_thresh"] and new > old:
        return {"token_id": token_id, "price": ask, "side": "UP", "move": move}
    return None

def on_open(ws):
    log.info(f"[WS] Connected — subscribing to {len(WATCHED_TOKENS)} tokens")
    for t in WATCHED_TOKENS:
        ws.send(json.dumps({"assets_ids": [t["token_id"]], "type": "Market"}))

def on_message(ws, message):
    try:
        if not message or message[0] not in ("{", "["):
            return
        for event in json.loads(message) if message[0] == "[" else [json.loads(message)]:
            tid = event.get("asset_id") or event.get("token_id", "")
            if not tid:
                continue
            bids = event.get("bids", [])
            asks = event.get("asks", [])
            if bids: order_books[tid]["bids"] = bids
            if asks: order_books[tid]["asks"] = asks

            # Single price write per event — prefer order book mid, fallback to trade price
            # (was double-writing both which corrupted momentum history)
            price_written = False
            if bids or asks:
                all_bids = order_books[tid].get("bids", [])
                all_asks = order_books[tid].get("asks", [])
                try:
                    b = max(float(x["price"]) for x in all_bids) if all_bids else None
                    a = min(float(x["price"]) for x in all_asks) if all_asks else None
                    if b and a:
                        price_history[tid].append((b + a) / 2)
                        price_written = True
                except Exception:
                    pass
            if not price_written:
                p = float(event.get("price", 0) or 0)
                if p > 0:
                    price_history[tid].append(p)

            if len(price_history[tid]) >= CFG["lookback"]:
                if time.time() - _mtum_cooldown.get(tid, 0) > 300:
                    sig = momentum_signal(tid)
                    if sig:
                        mkt = next((t["market"] for t in WATCHED_TOKENS if t["token_id"] == tid), tid[:16])
                        tg(f"MOMENTUM {sig['move']*100:+.2f}% @ {sig['price']:.3f} | {mkt}", "INFO")
                        # Enqueue signal — never call place_order() on the WS thread
                        # (place_order blocks 5-10s on HTTP → heartbeat fails → WS drops)
                        try:
                            _ws_order_queue.put_nowait((tid, sig["price"], mkt))
                            _mtum_cooldown[tid] = time.time()
                        except _queue.Full:
                            pass
    except Exception as e:
        log.error(f"[WS] message error: {e}")

def on_error(ws, error): log.error(f"[WS] {error}")
def on_close(ws, *_):    log.warning("[WS] closed — will reconnect")

def ws_order_worker():
    """Drain the WS momentum signal queue off the WS thread to avoid blocking heartbeats."""
    while True:
        try:
            tid, price, mkt = _ws_order_queue.get(timeout=5)
            place_order(tid, price, CFG["max_usd"], mkt, "MTUM")
        except _queue.Empty:
            pass
        except Exception as e:
            log.error(f"[MTUM] order worker: {e}")

def ws_loop():
    while True:
        try:
            WATCHED_TOKENS.clear()
            tokens = top_tokens(50)
            WATCHED_TOKENS.extend(tokens)
            log.info(f"[WS] Pre-fetched {len(tokens)} tokens")
            saved = {k: os.environ.pop(k, None) for k in _ws_proxy_keys}
            try:
                ws = websocket.WebSocketApp(CFG["ws_url"],
                    on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close)
                ws.run_forever(ping_interval=30, ping_timeout=20)
            finally:
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v
        except Exception as e:
            log.error(f"[WS] crashed: {e}")
        log.info(f"[WS] Reconnecting in {CFG['ws_reconnect_sleep_sec']:.0f}s...")
        time.sleep(CFG["ws_reconnect_sleep_sec"])

# ── SERVER WATCHDOG ───────────────────────────────────────────────────────────
_server_proc = None

def server_watchdog():
    global _server_proc
    import subprocess as _sp
    server_path = BASE / "server.py"
    while True:
        try:
            alive = _server_proc and _server_proc.poll() is None
            if not alive:
                _server_proc = _sp.Popen(
                    ["python", str(server_path)],
                    cwd=str(BASE),
                    stdout=_sp.DEVNULL, stderr=_sp.DEVNULL
                )
                log.info(f"[SERVER] Started PID {_server_proc.pid}")
        except Exception as e:
            log.error(f"[SERVER] watchdog: {e}")
        time.sleep(CFG["server_watchdog_sleep_sec"])

# ── STATUS LOOP ───────────────────────────────────────────────────────────────
def _load_equity_history():
    try:
        if STATUS_FILE.exists():
            h = json.loads(STATUS_FILE.read_text()).get("equity_history", [])
            return [x for x in h if isinstance(x, list) and len(x) == 2][-120:]
    except Exception:
        pass
    return []

_equity_history = _load_equity_history()
_last_day = datetime.now(timezone.utc).date()

def status_loop():
    global _equity_history, _last_day, _cached_portfolio_val, _cached_portfolio_ts
    while True:
        time.sleep(CFG["status_sleep_sec"])
        # Daily reset check
        today = datetime.now(timezone.utc).date()
        if today != _last_day:
            _last_day = today
            reset_daily()

        val = portfolio_val()
        # Keep cached value fresh so check_halt() never blocks the order lock on HTTP
        if val > 0:
            _cached_portfolio_val = val
            _cached_portfolio_ts  = time.time()

        if val > 0 and _day_start_val is None:
            init_baseline()

        # Poll open position exits here (10s cadence) so UPDN wins are claimed fast
        try:
            check_profit_exits()
        except Exception:
            pass
        day_pct = 0.0
        if val and _day_start_val:
            day_pct = (val - _day_start_val) / _day_start_val * 100
        session_pnl = round(val - _session_start, 4) if val and _session_start else 0.0

        halt_str = f" HALTED:{_halt_reason}" if _bot_halted else ""
        redeem_str = f" REDEEM:{len(_redeemed_cids)}done" if _redeemed_cids else ""
        log.info(f"[STATUS] bal=${val:.2f} day={day_pct:+.1f}% session={session_pnl:+.2f} "
                 f"trades={len(trade_log)} pos={len(open_positions)} "
                 f"ws={len(WATCHED_TOKENS)}/{len(price_history)}{halt_str}{redeem_str}")

        ts = datetime.now(timezone.utc).strftime("%H:%M")
        # Only record plausible values — reject extreme outliers (API glitches)
        if val and val > 1.0:
            last_val = _equity_history[-1][1] if _equity_history else val
            if last_val <= 0 or 0.1 <= val / last_val <= 10.0:
                _equity_history.append([ts, round(val, 4)])
        if len(_equity_history) > 120:
            _equity_history = _equity_history[-120:]

        try:
            pos_details = []
            for tid, p in open_positions.items():
                mid = clob_mid(tid)
                entry = float(p.get("entry", 0))
                pnl_pct = round((mid - entry) / max(entry, 0.001) * 100, 1) if mid > 0 and entry > 0 else 0
                pos_details.append({
                    "token_id": tid[:20],
                    "market":   (p.get("market") or "")[:45],
                    "source":   p.get("source", "?"),
                    "entry":    round(entry, 3),
                    "current":  round(mid, 3),
                    "pnl_pct":  pnl_pct,
                    "size":     round(float(p.get("size", 0)), 2),
                })
            with _consensus_stats_lock:
                consensus_snapshot = json.loads(json.dumps(_consensus_stats))
            with _skip_stats_lock:
                skip_snapshot = json.loads(json.dumps(_skip_stats))
            with _scan_stats_lock:
                scan_snapshot = json.loads(json.dumps(_scan_stats))
            STATUS_FILE.write_text(json.dumps({
                "updated":       datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "balance":       round(val or 0, 4),
                "day_pnl":       round(day_pct, 2),
                "session_pnl":   session_pnl,
                "trades":        len(trade_log),
                "positions":     len(open_positions),
                "open_positions": pos_details,
                "halted":        _bot_halted,
                "halt_reason":   _halt_reason,
                "wallet":        FUNDER,
                "mode":          "DRY RUN" if CFG["dry_run"] else "LIVE",
                "ws_subscribed": len(WATCHED_TOKENS),
                "ws_active":     len(price_history),
                "consensus":     consensus_snapshot,
                "skip_stats":    skip_snapshot,
                "scan_stats":    scan_snapshot,
                "equity_history": _equity_history,
            }, indent=2))
        except Exception:
            pass

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Single-instance lock — kills any previous instance
    if LOCK_FILE.exists():
        try:
            os.kill(int(LOCK_FILE.read_text().strip()), 9)
            log.info("[BOOT] Killed previous instance")
        except Exception:
            pass
    LOCK_FILE.write_text(str(os.getpid()))
    import atexit
    atexit.register(lambda: LOCK_FILE.unlink(missing_ok=True))

    log.info("=" * 55)
    log.info("POLY//BOT v7 — LIVE  [8 active engines]")
    log.info(f"  E1: UPDN  — 15m/1h/4h crypto, {UPDN_TF['15m']['min_edge']*100:.2f}%/{UPDN_TF['1h']['min_edge']*100:.2f}%/{UPDN_TF['4h']['min_edge']*100:.2f}% edge, price≥{CFG['updn_min_price']:.2f}")
    log.info(f"  E2: LIVE  — any binary ≤6min, leader {CFG['live_min_leader']:.0%}-{CFG['live_max_leader']:.1%}, $500 liq  [{CFG['live_scan_sleep_sec']:g}s]")
    log.info(f"  E3: SPORT — sports/UPDN 6-45min, {CFG['sports_min_leader']:.0%}-{CFG['sports_max_leader']:.1%} conviction     [{CFG['sports_scan_sleep_sec']:g}s]")
    log.info(f"  E4: NEAR  — {CFG['near_min_leader']:.0%}-{CFG['near_max_leader']:.0%} leader, ${CFG['near_min_vol24']/1000:.0f}K vol, ≤{CFG['near_max_days']:.0f}d horizon        [{CFG['near_scan_sleep_sec']/60:.2g}m]")
    log.info(f"  E5: COPY  — 46 whales, $50+ trades, {CFG['copy_scan_sleep_sec']:g}s scan x{CFG['copy_wallets_per_scan']} wallets")
    log.info("  E6: MTUM  — WebSocket momentum, top 50 markets         [live]")
    log.info(f"  E7: WX    — weather forecast edge, cheap entries        [{CFG['wx_scan_sleep_sec']:g}s]")
    log.info(f"  E8: POL   — politics directional, high liq/vol          [{CFG['politics_scan_sleep_sec']:g}s]")
    log.info(f"  Positions: {CFG['max_positions']} max (COPY≤2, UPDN≤3) | Max/trade: ${CFG['max_usd']} | Halt: {CFG['daily_halt_pct']*100:.0f}%")
    log.info("=" * 55)

    init_baseline()
    _load_targets()

    for fn, name in [
        (status_loop,    "status"),
        (updn_loop,      "updn"),        # 15m + 1h + 4h, min_edge 0.25%/0.5%/0.5%, price ≥0.55
        (live_loop,      "live"),        # ≤6min binary, leader 78-97%, $500 liq
        (sports_loop,    "sport"),       # 6-45min sports/UPDN, 90-98% conviction
        (near_loop,      "near"),        # 72-88% leader, $5K vol, ≤7 days, CLOB verified
        (copy_loop,      "copy"),        # active whales, refreshed every 4h, 15s scan
        (wx_loop,        "wx"),
        (politics_loop,  "pol"),
        (redeem_loop,    "redeem"),
        (server_watchdog,"server"),
        (ws_order_worker,"mtum-orders"),
    ]:
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        log.info(f"[BOOT] Thread [{name}] started")

    ws_loop()   # runs on main thread, reconnects forever
