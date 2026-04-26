"""
POLY//BOT v5 — Four proven edges:
  1. UPDN  — 5-min crypto markets (BTC/ETH/SOL/XRP/DOGE/BNB), enter last 30-60s (ET hours)
  2. LIVE  — Any binary market closing in ≤4 min where leader is 82-96%
  3. DRIFT — Momentum on top liquid markets: buy when price drifts 4%+ consistently
  4. WX    — Weather forecast vs Polymarket price, buy <20¢ sell at 45¢
"""

import json, time, re, threading, logging, pathlib, os, asyncio
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

try:
    import aiohttp as _aiohttp
    _AIOHTTP_OK = True
except ImportError:
    _AIOHTTP_OK = False

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
PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "0e0f279ec9fb2ff4959525cc30db040ef941e6f714ee556a8bf594a7dc8303d9")
FUNDER      = os.environ.get("POLY_FUNDER",      "0x36576E80353D35B2Fa00520cD96823861fD922DF")
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
    "max_usd":         3.00,   # ~16% of $19 bankroll per trade
    "min_free_usdc":   1.00,   # keep $1 liquid minimum — deploy the rest
    "daily_halt_pct":  -0.20,  # halt if down 20% on the day (~$3 on $15)
    "max_positions":   3,      # max 3 concurrent positions at this balance

    # UPDN — only trade when market AGREES with our signal (proven from trade history)
    # Losses at 0.30-0.44, wins at 0.55+. Market calibration > Binance signal alone.
    "updn_max_price":  0.88,   # soft cap guidance; timeframe caps below are authoritative
    "updn_min_price":  0.30,   # allow market slight disagreement — Binance signal is the edge

    # WEATHER
    "wx_buy_under":    0.25,   # buy YES when price < 25¢
    "wx_sell_at":      0.45,   # target exit price
    "wx_min_edge":     0.06,   # 6% gap between forecast and market price
    "wx_max_days":     2,      # only markets resolving within 2 days
    "wx_min_liq":      80,

    # LIVE (6-min rule)
    "live_max_mins":   10.0,
    "live_min_leader": 0.72,   # tightened from 0.60 — sub-72% has ~52% win rate
    "live_max_leader": 0.985,
    "live_min_liq":    300,

    # SPORT — dynamic threshold applied in code by mins_left
    "sports_min_leader": 0.80,
    "sports_max_leader": 0.985,

    # NEAR
    "near_min_vol24":  200,
    "near_prefilter_vol24": 100,
    "near_min_liq":    150,
    "near_min_leader": 0.70,   # tightened from 0.60 — 60-70% is coin-flip after fees
    "near_max_leader": 0.92,
    "near_min_days":   0.1,
    "near_max_days":   30.0,
    "near_prefilter_limit": 150,
    "near_blacklist_sec": 900,
    "near_vol_skip_blacklist_hits": 8,

    # WEATHER / POLITICS category expansion
    "wx_scan_sleep_sec": _pace(120, 45),
    "politics_min_liq":  150,
    "politics_min_vol24": 300,
    "politics_min_leader": 0.58,
    "politics_max_leader": 0.93,
    "politics_min_hours": 1,
    "politics_max_hours": 30 * 24,
    "politics_scan_sleep_sec": _pace(20, 8),
    "politics_cooldown_sec": 3600,

    # DRIFT — momentum on top liquid markets
    "drift_min_move":  0.012,  # 1.2% price drift required
    "drift_readings":  1,
    "drift_max_price": 0.90,
    "drift_min_price": 0.08,
    "drift_exit_pct":  0.20,   # exit at +20% profit

    # M30 — top 20 highest 30d percentage-to-price ratio, fed into momentum watchlist
    "m30_top_n":       20,
    "m30_min_change":  0.0,
    "m30_min_liq":     500,
    "m30_min_vol1mo":  5000,
    "m30_min_price":   0.05,
    "m30_max_price":   0.80,
    "m30_max_days":    365,
    "m30_cache_sec":   _pace(300, 180),

    # WebSocket
    "ws_url":          "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "lookback":        20,
    "momentum_thresh": 0.03,

    # Loop pacing (faster polling does NOT change risk sizing/halts)
    "updn_cache_refresh_sec": _pace(30, 15),
    "updn_slug_fetch_gap_sec": _pace(0.05, 0.02),
    "updn_scan_sleep_sec": _pace(3, 1),        # faster UPDN — catch 5m windows
    "live_scan_sleep_sec": _pace(3, 1),        # faster LIVE — catch closing markets
    "sports_scan_sleep_sec": _pace(6, 2),
    "near_scan_sleep_sec": _pace(30, 10),      # scan NEAR every 10s in fast mode
    "copy_scan_sleep_sec": _pace(5, 2),        # faster whale copy — 2s in fast mode
    "copy_wallets_per_scan": max(1, int(_pace(4, 8))),  # check more wallets per scan for volume
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
_cached_free_usdc:     float = 0.0   # liquid USDC — updated every status tick, read in place_order
_last_tier: int = -1   # tracks tier changes for level-up alerts
_last_heartbeat: float = time.time()  # watchdog: updated every status tick
_m30_cache_lock = threading.Lock()
_m30_cache: list = []
_m30_cache_ts: float = 0.0

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

# ── Portfolio tier table — auto-scales as balance grows ───────────────────────
# (min_bal, trade_pct, max_trade, max_pos, src_caps, kelly_fracs, min_free)
_TIER_TABLE = [
    (1000, 0.08, 200.0, 35, {"COPY": 8, "UPDN": 12, "HOLD": 8, "SNIPER": 10, "BLITZ": 8}, {1: 0.04, 2: 0.10, 3: 0.18}, 20.0),
    (500,  0.09, 80.0,  25, {"COPY": 6, "UPDN": 9,  "HOLD": 7, "SNIPER": 8,  "BLITZ": 7}, {1: 0.04, 2: 0.10, 3: 0.18}, 10.0),
    (200,  0.10, 35.0,  18, {"COPY": 4, "UPDN": 7,  "HOLD": 5, "SNIPER": 7,  "BLITZ": 5}, {1: 0.04, 2: 0.10, 3: 0.18}, 5.0),
    (75,   0.12, 14.0,  12, {"COPY": 3, "UPDN": 5,  "HOLD": 4, "SNIPER": 5,  "BLITZ": 4}, {1: 0.04, 2: 0.10, 3: 0.18}, 3.0),
    (30,   0.13, 7.0,   5,  {"COPY": 1, "UPDN": 2,  "HOLD": 2, "SNIPER": 2,  "BLITZ": 2}, {1: 0.04, 2: 0.10, 3: 0.18}, 2.0),
    (0,    0.15, 4.0,   3,  {"COPY": 1, "UPDN": 1,  "HOLD": 1, "SNIPER": 1,  "BLITZ": 1}, {1: 0.04, 2: 0.10, 3: 0.18}, 1.0),
]
_TIER_NAMES = ["$0", "$30", "$75", "$200", "$500", "$1k"]

def portfolio_tier(bal: float = 0.0) -> dict:
    global _last_tier
    bal = bal or _cached_portfolio_val or 20.0  # never call free_usdc() — could be inside _order_lock
    for idx, (min_bal, trade_pct, max_trade, max_pos, src_caps, kelly_fracs, min_free) in enumerate(_TIER_TABLE):
        if bal >= min_bal:
            tier_idx = len(_TIER_TABLE) - 1 - idx
            if tier_idx != _last_tier and _last_tier >= 0:
                direction = "UP" if tier_idx > _last_tier else "DOWN"
                log.info(f"[SCALE] Portfolio tier {direction}: ${bal:.2f} -> Tier {tier_idx+1} "
                         f"(max/trade=${max_trade}, max_pos={max_pos})")
                tg(f"Portfolio scaled {direction} to Tier {tier_idx+1} — balance ${bal:.2f}. "
                   f"Max/trade ${max_trade}, {max_pos} positions.", "INFO")
            _last_tier = tier_idx
            return dict(trade_pct=trade_pct, max_trade=max_trade, max_pos=max_pos,
                        src_caps=src_caps, kelly_fracs=kelly_fracs, min_free=min_free)
    # fallback (should never hit)
    return dict(trade_pct=0.15, max_trade=4.0, max_pos=7,
                src_caps={"COPY": 2, "UPDN": 4, "HOLD": 3, "SNIPER": 4, "BLITZ": 3},
                kelly_fracs={1: 0.04, 2: 0.10, 3: 0.18}, min_free=1.0)

def dynamic_max_usd() -> float:
    """Max trade size auto-scales with portfolio — compounds wins, expands as we grow."""
    bal = _cached_portfolio_val if _cached_portfolio_val > 0 else free_usdc()
    t = portfolio_tier(bal)
    return round(max(1.00, min(t["max_trade"], bal * t["trade_pct"])), 2)

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
        log.info(f"[RISK] Baseline ${val:.2f} | halt@{CFG['daily_halt_pct']*100:.0f}%=${val*(1+CFG['daily_halt_pct']):.2f}")

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
    global _cached_portfolio_val, _cached_free_usdc
    free_now = _cached_free_usdc   # local snapshot — avoids UnboundLocalError in nested loops
    # Brain check runs OUTSIDE the lock — it makes HTTP calls (whale lookup)
    # and must not block other threads while waiting for network responses.
    # UPDN and BLITZ have their own signal logic — brain_check adds no value and kills valid entries
    if p_win > 0 and not source.startswith("UPDN") and source not in ("BLITZ",):
        mkt_stub = {"question": market, "estimate": p_win}
        adj_p, brain_reason = brain_check(token_id, mkt_stub, p_win, source)
        if adj_p == 0.0:
            log.info(f"[{source}] BRAIN KILL — {brain_reason[:80]}")
            return
        if "HALF" in brain_reason:
            size_usd = size_usd * 0.5
        p_win = adj_p
    with _order_lock:
        if not check_halt():
            return
        # Cross-engine dedup: never double-enter the same token
        if token_id in open_positions:
            log.debug(f"[{source}] SKIP — already holding {token_id[:16]}")
            return
        # Tier-aware caps — scale with portfolio
        _tier = portfolio_tier()
        _max_pos   = _tier["max_pos"]
        _src_caps  = _tier["src_caps"]
        _min_free  = _tier["min_free"]
        if len(open_positions) >= _max_pos:
            log.warning(f"[{source}] SKIP — max {_max_pos} positions reached")
            return
        src_group = source.split("/")[0]
        if src_group in _src_caps:
            src_count = sum(1 for p in open_positions.values()
                            if p.get("source", "").startswith(src_group))
            if src_count >= _src_caps[src_group]:
                log.debug(f"[{source}] SKIP — {src_group} cap {_src_caps[src_group]} reached")
                return
        # Use cached free USDC — NEVER call free_usdc() inside _order_lock (HTTP = deadlock)
        f = free_now if free_now > 0 else (_cached_portfolio_val if _cached_portfolio_val > 0 else 1.0)
        if f < _min_free:
            log.warning(f"[{source}] SKIP — free USDC ~${f:.2f}")
            return
        # Drawdown protection: scale sizes down if losing badly today (no halt, just smaller bets)
        dd_mult = 1.0
        if _day_start_val and _cached_portfolio_val:
            day_loss_pct = (_cached_portfolio_val - _day_start_val) / _day_start_val
            if day_loss_pct < -0.25:
                dd_mult = 0.25   # down >25% today → quarter size
            elif day_loss_pct < -0.15:
                dd_mult = 0.50   # down >15% today → half size
        # Dynamic sizing — bankroll = total portfolio for Kelly; f = liquid USDC for caps
        bankroll = _cached_portfolio_val or f
        port_cap = max(0.50, bankroll * 0.08 * dd_mult)
        live_max = dynamic_max_usd() * dd_mult   # scales down in drawdown
        if p_win > 0.5:
            kelly = kelly_size(p_win, price, bankroll)
            size_usd = min(kelly if kelly > 0 else size_usd, f * 0.9, live_max, port_cap)
        else:
            size_usd = min(size_usd * dd_mult, f * 0.9, live_max, port_cap)
        shares = max(5, round(size_usd / max(price, 0.01)))  # Polymarket min = 5 shares
        size_usd = round(shares * price, 2)
        if size_usd > f * 0.9:
            # Try floor to 5 shares minimum before giving up
            shares = 5
            size_usd = round(5 * price, 2)
        if size_usd > f * 0.9:
            log.warning(f"[{source}] SKIP — {shares}sh @ {price:.3f} costs ${size_usd:.2f}, only ${f:.2f} free")
            return
        cost = round(shares * price, 2)
        if CFG["dry_run"]:
            log.info(f"[DRY] {source} BUY {shares}sh @ {price:.3f} (${cost}) | {market[:50]}")
            open_positions[token_id] = {"size": shares, "entry": price, "source": source}
            trade_log.append({"source": source, "token_id": token_id, "price": price, "size": shares})
            return
        # Snapshot args — release lock BEFORE HTTP call so a slow API never freezes all threads
        _order_args = (token_id, price, shares, market, source, cost, expected_gap)

    # ── HTTP order call is OUTSIDE _order_lock ──────────────────────────────────
    token_id, price, shares, market, source, cost, expected_gap = _order_args
    # If order goes "live" (on-book, unfilled), cross the spread by +2¢ to force a match
    for attempt, bid_price in enumerate([price, min(round(price + 0.02, 3), 0.97)]):
        try:
            order  = client.create_and_post_order(OrderArgs(token_id=token_id, price=bid_price, size=shares, side=BUY))
            status = order.get("status", "?")
            actual_cost = round(shares * bid_price, 2)
            tg(f"ORDER [{source}] BUY {shares}sh @ {bid_price:.3f} (${actual_cost}) | {market[:50]} | {status}")
            log.info(f"[{source}] ORDER: {shares}sh @ {bid_price:.3f} = ${actual_cost} | {status}")
            with _order_lock:
                if token_id not in open_positions and status in ("matched", "delayed"):
                    open_positions[token_id] = {
                        "size": shares, "entry": bid_price, "source": source, "market": market,
                        "entry_time": time.time(), "expected_gap": expected_gap,
                    }
                    trade_log.append({"source": source, "token_id": token_id, "price": bid_price, "size": shares})
                    if len(trade_log) > 500:
                        del trade_log[:250]
                    _cached_free_usdc = max(0, _cached_free_usdc - actual_cost)
                    break  # filled — stop retrying
                elif status == "live" and attempt == 0:
                    log.info(f"[{source}] ORDER live @ {bid_price:.3f} — retrying +2¢ to cross spread")
                    continue
                else:
                    break
        except Exception as e:
            log.error(f"[{source}] Order failed: {e}")
            break
    return

def sell_position(token_id: str, price: float, market: str, source: str):
    # Read position snapshot inside lock, then make HTTP call OUTSIDE lock to prevent deadlock
    with _order_lock:
        pos = open_positions.get(token_id)
        if not pos:
            return
        shares = float(pos.get("size", 0))
        if shares <= 0:
            return
    # HTTP call is outside the lock — API hang won't freeze other threads
    for attempt_shares in [shares, max(1, int(shares) - 1), max(1, int(shares) - 2)]:
        try:
            order = client.create_and_post_order(OrderArgs(token_id=token_id, price=price, size=attempt_shares, side=SELL))
            status = order.get("status", "?")
            with _order_lock:
                if status in ("matched", "delayed", "live"):
                    open_positions.pop(token_id, None)
                elif status in ("unmatched", "cancelled"):
                    log.warning(f"[{source}] SELL unmatched — still holding {attempt_shares}sh @ {price:.3f}")
            tg(f"SELL [{source}] {attempt_shares}sh @ {price:.3f} | {market[:50]} | {status}")
            log.info(f"[{source}] SELL {attempt_shares}sh @ {price:.3f} | {status}")
            break  # success — stop retrying
        except Exception as e:
            err = str(e)
            if "not enough balance" in err and "balance: 0" in err:
                # Phantom position — BUY order was never actually filled (was "live"/pending)
                log.warning(f"[{source}] PHANTOM position {token_id[:16]} — no tokens in CLOB, dropping")
                with _order_lock:
                    open_positions.pop(token_id, None)
                break
            if "not enough balance" in err and attempt_shares > 1:
                log.warning(f"[{source}] Sell {attempt_shares}sh failed (balance), retrying smaller...")
                continue
            log.error(f"[{source}] Sell failed: {e}")
            break

# ── ENGINE 1: MULTI-TIMEFRAME CRYPTO UP/DOWN ──────────────────────────────────
# Four timeframes — 5m is enabled only in a stricter, higher-selectivity mode.
#   5-min : conservative variant, tighter edge/price caps than the original
#   15-min: enter last 2-5min    → 10-13min confirmed   → ~68% base win rate
#   4-hour: enter last 30-60min  → 180-210min confirmed → ~75%+ base win rate

CRYPTO_MAP = {
    "bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL", "xrp": "XRP", "ripple": "XRP",
    "dogecoin": "DOGE", "doge": "DOGE", "bnb": "BNB", "binance": "BNB",
}
SYM = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT",
       "DOGE": "DOGEUSDT", "BNB": "BNBUSDT"}

# Per-timeframe config — slug_interval is Binance kline interval for lookback
UPDN_TF = {
    "5m": {
        "interval_min":  5,
        "window_min":    2.0,   # 120s before expiry = 180s since open (end of sweet spot)
        "window_max":    4.0,   # 240s before expiry = 60s since open (start of sweet spot)
        "bin_interval":  "1m",
        "min_edge":      0.0001,
        "max_price":     0.88,
        "slug_rnd":      5,
    },

    # 15m: loosened for a low-vol regime while preserving market-agreement price checks.
    "15m": {
        "interval_min":  15,
        "window_min":    0.3,
        "window_max":    8.0,
        "bin_interval":  "1m",
        "min_edge":      0.0001,
        "max_price":     0.92,
        "slug_rnd":      15,
    },
    "4h": {
        "interval_min":  240,
        "window_min":    10,
        "window_max":    120,
        "bin_interval":  "15m",
        "min_edge":      0.0005,
        "max_price":     0.92,
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
    "link": "LINK", "avax": "AVAX", "matic": "MATIC",
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
    tfs_traded_this_scan: set = set()
    scan_total = scan_in_window = scan_token_ready = scan_nonflat = 0

    for cid, entry in list(_updn_market_cache.items()):
        try:
            if cid in _updn_traded:
                continue
            scan_total += 1
            end_dt = entry.get("end_dt")
            if not end_dt:
                continue
            tf_key = entry.get("tf", "15m")
            if tf_key in tfs_traded_this_scan:
                continue
            tf_cfg = UPDN_TF.get(tf_key)
            if tf_cfg is None:
                continue
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
                continue
            scan_nonflat += 1
            direction = "Up" if change > 0 else "Down"
            bet_i     = up_i if direction == "Up" else down_i
            token_id  = toks[bet_i]

            # ── 5m: 2-signal gate — price divergence + order book ───────────
            # Based on 2000-window analysis: both aligned = 71% win rate
            # Single signal = 57-63%, signals disagree = 47% (worse than random)
            if tf_key == "5m":
                track_imbalance(token_id)   # build history for detect_smart_entry
                # Dead zone: <60s left — spreads blow out, can't exit
                if mins_left < 1.0:
                    _skip_record("UPDN/5m", "dead_zone")
                    continue
                ref_price = updn_reference_price(sym)
                signal = should_trade_updn(token_id, sym, ref_price) if ref_price > 0 else None
                if signal is None:
                    _skip_record("UPDN/5m", "no_signal_convergence")
                    continue
                # Gate passed — use signal direction (more accurate than Binance-only)
                direction = signal["direction"]
                bet_i     = up_i if direction == "UP" else down_i
                token_id  = toks[bet_i]
                live_price = clob_mid(token_id)
                if live_price <= 0:
                    live_price = float(entry["prices"][bet_i]) if len(entry["prices"]) > bet_i else 0.5
                if not (CFG["updn_min_price"] <= live_price <= tf_cfg["max_price"]):
                    _skip_record("UPDN/5m", "price_out_of_range")
                    continue
                p_win    = signal["confidence"]
                bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else 20.0
                size_usd = min(kelly_signals(p_win, live_price, bankroll, signals=2), CFG["max_usd"])
                q        = (entry["market"].get("question") or "")[:60]
                roi      = (1 - live_price) / live_price * 100
                log.info(
                    f"[UPDN/5m] {sym} {direction} | "
                    f"Bn={signal['bn_delta']:+.0f} Cb={signal['cb_delta']:+.0f} "
                    f"OB={signal['imbalance']:.2f} CL={signal['cl_lag_sec']:.0f}s lag | "
                    f"conf={p_win:.0%} ${size_usd:.2f} @ {live_price:.2f} +{roi:.1f}% | {q}"
                )
                tg(
                    f"UPDN/5m {sym} {direction} | Bn={signal['bn_delta']:+.0f} "
                    f"OB={signal['imbalance']:.2f} conf={p_win:.0%} ${size_usd:.2f} @ {live_price:.0%}", "UPDN"
                )
                place_order(token_id, live_price, size_usd, q, "UPDN/5m",
                            p_win=p_win, expected_gap=0.27)
                _updn_traded.add(cid)
                _save_updn_traded(_updn_traded)
                tfs_traded_this_scan.add(tf_key)
                continue

            # ── 15m / 4h: Binance change + order book confirmation ───────────
            if abs(change) < tf_cfg["min_edge"]:
                _skip_record(f"UPDN/{tf_key}", "edge_low")
                log.info(f"[UPDN/{tf_key}] {sym} {direction} {change*100:+.3f}% < {tf_cfg['min_edge']*100:.2f}% — skip")
                continue
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = float(entry["prices"][bet_i]) if len(entry["prices"]) > bet_i else 0.5
            if live_price > tf_cfg["max_price"]:
                _skip_record(f"UPDN/{tf_key}", "priced_in")
                continue
            if live_price < CFG["updn_min_price"]:
                _skip_record(f"UPDN/{tf_key}", "market_disagrees")
                continue
            book_raw = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
            if book_raw:
                ib = calculate_imbalance(book_raw)
                if direction == "Up" and ib < 1.5:
                    _skip_record(f"UPDN/{tf_key}", "ob_disagrees")
                    continue
                if direction == "Down" and ib > 0.67:
                    _skip_record(f"UPDN/{tf_key}", "ob_disagrees")
                    continue
            q          = (entry["market"].get("question") or "")[:60]
            roi        = (1 - live_price) / live_price * 100
            magnitude  = min(abs(change), 0.02)
            p_win_updn = 0.55 + (magnitude / 0.02) * 0.18
            log.info(f"[UPDN/{tf_key}] {sym} {change*100:+.3f}% → {direction} @ {live_price:.2f} +{roi:.1f}% | {mins_left:.1f}min | p={p_win_updn:.0%}")
            tg(f"UPDN/{tf_key} {sym}: {change*100:+.3f}% → {direction} @ {live_price:.0%} +{roi:.1f}% | {mins_left:.1f}min", "UPDN")
            place_order(token_id, live_price, CFG["max_usd"], q, f"UPDN/{tf_key}",
                        p_win=p_win_updn, expected_gap=abs(change) * 5)
            _updn_traded.add(cid)
            _save_updn_traded(_updn_traded)
            tfs_traded_this_scan.add(tf_key)

        except Exception as e:
            import traceback
            log.error(f"[UPDN] {e}\n{traceback.format_exc()}")

    _scan_record("UPDN", {
        "cache_entries": scan_total, "in_window": scan_in_window,
        "token_ready": scan_token_ready, "nonflat": scan_nonflat,
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

# ── MARKET SCORER + BRAIN ─────────────────────────────────────────────────────
# score_market(): universal pre-filter — any engine can call this before trading.
# Brain: 4-check gate (base rate, news, whale, disposition) → 3/4 → execute.

def score_market(market: dict, claude_estimate: float, depth_info: dict | None = None) -> dict | None:
    """
    Three-factor universal market filter. Returns scored dict or None if killed.
      gap   > 0.07  — need ≥7% edge (edge too thin = skip)
      depth > 200   — $200 min liquidity each side (budget-scaled from $500)
      hours in 4-168 — sweet spot: 4h to 7 days
    EV proxy = gap × depth × 0.001
    """
    price      = float(market.get("midpoint") or market.get("bestAsk") or 0.5)
    gap        = abs(claude_estimate - price)
    hours_left = float(market.get("hours_to_resolution") or
                       market.get("hoursLeft") or 0)

    # Parse depth from depth_info or market dict
    if depth_info:
        depth = min(depth_info.get("bids_depth", 0), depth_info.get("asks_depth", 0))
    else:
        liq = float(market.get("liquidity") or market.get("liquidityNum") or 0)
        depth = liq / 2   # rough split

    if gap     < 0.07:  return None   # edge too thin
    if depth   < 200:   return None   # can't fill without slippage
    if hours_left < 4:  return None   # too late — spreads blow out
    if hours_left > 168: return None  # too slow — capital locked 7+ days

    ev = round(gap * depth * 0.001, 2)
    return {
        "question": (market.get("question") or "")[:60],
        "gap":      round(gap, 3),
        "depth":    round(depth, 0),
        "hours":    round(hours_left, 1),
        "ev":       ev,
        "estimate": claude_estimate,
        "price":    price,
    }


_brain_base_rates: dict = {
    # Category → historical win rate when price is in 60-80% range
    "crypto":    0.71,
    "politics":  0.65,
    "sports":    0.63,
    "weather":   0.68,
    "economics": 0.66,
    "default":   0.65,
}

def brain_check(token_id: str, market: dict, p_win: float,
                source: str = "") -> tuple[float, str]:
    """
    4-check gate before any trade.
    Returns (adjusted_p_win, reason).
    Boosts or reduces confidence based on evidence weight.

    Check 1: Base rate — category historical win rate
    Check 2: Price drift — is crowd moving toward or away from estimate?
    Check 3: Whale presence — elite wallets in this market?
    Check 4: Disposition bias — is market anchored to a stale prior?
    """
    votes    = 0
    max_vote = 4
    factors  = []

    question = (market.get("question") or "").lower()

    # Check 1: base rate for category
    if any(k in question for k in ["btc","eth","sol","xrp","crypto","bitcoin","ethereum"]):
        cat = "crypto"
    elif any(k in question for k in ["trump","biden","election","senate","congress","president"]):
        cat = "politics"
    elif any(k in question for k in ["nba","nfl","mlb","soccer","ufc","match","game","win"]):
        cat = "sports"
    else:
        cat = "default"
    base = _brain_base_rates[cat]
    if p_win >= base * 1.05:   # need 5% premium over historical base — prevents rubber-stamping
        votes += 1
        factors.append(f"base_rate✓({cat}={base:.0%})")
    else:
        factors.append(f"base_rate✗({cat}={base:.0%})")

    # Check 2: price drift — recent price history trending toward our estimate
    hist = list(price_history.get(token_id, []))
    if len(hist) >= 5:
        trend = hist[-1] - hist[-5]
        estimate = market.get("estimate") or p_win
        if (estimate > 0.5 and trend > 0) or (estimate < 0.5 and trend < 0):
            votes += 1
            factors.append(f"drift✓({trend:+.3f})")
        else:
            factors.append(f"drift✗({trend:+.3f})")
    else:
        votes += 1   # no history = neutral pass (not enough data to vote against)
        factors.append("drift~(no_hist)")

    # Check 3: whale presence — elite wallet = bonus vote; absence = neutral (whales are rare)
    if whale_in_market_cached(token_id):
        votes += 1
        factors.append("whale✓")

    # Check 4: disposition bias — price hasn't moved despite strong signal
    # If market is at 0.65 but has been there for >30 ticks → anchored crowd
    if len(hist) >= 15:
        price_range = max(hist[-15:]) - min(hist[-15:])
        if price_range < 0.03 and p_win > 0.70:
            votes += 1   # stale anchoring = contrarian opportunity
            factors.append("disposition✓(anchored)")
        else:
            factors.append("disposition✗")
    else:
        votes += 1
        factors.append("disposition~")

    reason = " | ".join(factors)

    if votes >= 3:
        # 3-4/4 agree → full kelly, slight confidence boost
        adj = min(0.95, p_win + 0.03)
        return adj, f"BRAIN 3+/4 PASS [{reason}]"
    elif votes == 2:
        # 2/4 agree → half position (caller must halve size)
        adj = p_win
        return adj, f"BRAIN 2/4 HALF [{reason}]"
    else:
        # 0-1/4 → kill trade
        return 0.0, f"BRAIN KILL {votes}/4 [{reason}]"


# ── 4-AGENT SIGNAL SYSTEM ─────────────────────────────────────────────────────
# Proven approach (30-day live results: 71% win rate, 85% filter rate):
#   What works:    price divergence + order book imbalance aligned
#   What doesn't:  RSI/MACD, single-source price, last-60s entry, holding to resolution
#
# Agent 1 — Wallet Scorer:  rank wallets by win rate, feed best to COPY engine
# Agent 2 — Entry Timing:   60-180s after market open is the sweet spot (67-71%)
# Agent 3 — Order Book:     imbalance >1.8 UP / <0.55 DOWN confirms direction
# Agent 4 — Kelly:          size scales with signal count, no overbet

# ── Chainlink Oracle (Polygon) ────────────────────────────────────────────────
_CHAINLINK_ABI = [{"inputs": [], "name": "latestRoundData",
    "outputs": [{"type": "uint80", "name": "roundId"},
                {"type": "int256",  "name": "answer"},
                {"type": "uint256", "name": "startedAt"},
                {"type": "uint256", "name": "updatedAt"},
                {"type": "uint80",  "name": "answeredInRound"}],
    "stateMutability": "view", "type": "function"}]

_CHAINLINK_FEEDS = {
    "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
    "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
}
_cl_cache: dict = {}   # symbol -> (fetched_ts, price_usd, oracle_updated_at)

def chainlink_price(symbol: str) -> tuple:
    """Returns (price_usd, seconds_since_update). Reads Polygon Chainlink feed."""
    addr = _CHAINLINK_FEEDS.get(symbol.upper())
    if not addr or not _WEB3_OK:
        return 0.0, 999.0
    now = time.time()
    cached = _cl_cache.get(symbol)
    if cached and now - cached[0] < 8:
        return cached[1], now - cached[2]
    try:
        w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com", request_kwargs={"timeout": 4}))
        feed = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=_CHAINLINK_ABI)
        _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
        price = float(answer) / 1e8
        _cl_cache[symbol] = (now, price, float(updated_at))
        return price, now - float(updated_at)
    except Exception:
        return 0.0, 999.0

# ── Multi-source price (Binance + Coinbase + Chainlink in parallel) ────────────
async def _async_json(session, url: str) -> dict:
    try:
        async with session.get(url, timeout=_aiohttp.ClientTimeout(total=3)) as r:
            return await r.json(content_type=None)
    except Exception:
        return {}

_KRAKEN_SYM = {"BTC": "XXBTZUSD", "ETH": "XETHZUSD", "SOL": "SOLUSD",
               "XRP": "XXRPZUSD", "DOGE": "XDGUSD", "BNB": "BNBUSD"}

async def _get_prices_async(symbol: str) -> dict:
    sym = symbol.upper()
    kr_sym = _KRAKEN_SYM.get(sym, f"{sym}USD")
    async with _aiohttp.ClientSession() as session:
        bn_d, cb_d, kr_d = await asyncio.gather(
            _async_json(session, f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT"),
            _async_json(session, f"https://api.coinbase.com/v2/prices/{sym}-USD/spot"),
            _async_json(session, f"https://api.kraken.com/0/public/Ticker?pair={kr_sym}"),
        )
    cl, cl_lag = chainlink_price(symbol)
    kr_result = (kr_d.get("result") or {})
    kr_price  = float(next(iter(kr_result.values()), {}).get("c", [0])[0] or 0) if kr_result else 0.0
    return {
        "binance":           float(bn_d.get("price", 0) or 0),
        "coinbase":          float((cb_d.get("data") or {}).get("amount", 0) or 0),
        "kraken":            kr_price,
        "chainlink":         cl,
        "chainlink_lag_sec": round(cl_lag, 1),
    }

_mprice_cache: dict = {}   # symbol -> (ts, dict)

def get_multi_prices(symbol: str) -> dict:
    """Parallel Binance+Coinbase+Chainlink. Cache 4s."""
    now = time.time()
    cached = _mprice_cache.get(symbol)
    if cached and now - cached[0] < 4:
        return cached[1]
    if _AIOHTTP_OK:
        try:
            d = asyncio.run(_get_prices_async(symbol))
            if d["binance"] > 0:
                _mprice_cache[symbol] = (now, d)
            return d
        except Exception:
            pass
    # Sequential fallback
    sym = symbol.upper()
    kr_sym = _KRAKEN_SYM.get(sym, f"{sym}USD")
    bn  = fetch(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT") or {}
    cb  = fetch(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot") or {}
    kr  = fetch(f"https://api.kraken.com/0/public/Ticker?pair={kr_sym}") or {}
    cl, cl_lag = chainlink_price(symbol)
    kr_result = (kr.get("result") or {})
    kr_price  = float(next(iter(kr_result.values()), {}).get("c", [0])[0] or 0) if kr_result else 0.0
    d = {
        "binance":           float(bn.get("price", 0) or 0),
        "coinbase":          float((cb.get("data") or {}).get("amount", 0) or 0),
        "kraken":            kr_price,
        "chainlink":         cl,
        "chainlink_lag_sec": round(cl_lag, 1),
    }
    if d["binance"] > 0:
        _mprice_cache[symbol] = (now, d)
    return d

def updn_reference_price(symbol: str) -> float:
    """Open price of the current 5-min candle = Polymarket's price to beat."""
    sym = f"{symbol.upper()}USDT"
    data = fetch(f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=5m&limit=2")
    if data and len(data) >= 1:
        return float(data[-1][1])   # index 1 = open of in-progress candle
    return 0.0

# ── Agent 3: Order Book Imbalance ─────────────────────────────────────────────
def calculate_imbalance(order_book_raw: dict) -> float:
    """
    Bid/ask depth ratio using top 10 levels.
    >1.8 = strong buy pressure (63-72% accurate).
    <0.55 = strong sell pressure.
    0.55-1.8 = neutral, no trade.
    """
    bids = order_book_raw.get("bids", [])[:10]
    asks = order_book_raw.get("asks", [])[:10]
    bid_depth = sum(float(b.get("size", 0)) for b in bids)
    ask_depth = sum(float(a.get("size", 0)) for a in asks)
    if ask_depth == 0:
        return 9.99
    return round(bid_depth / ask_depth, 3)

# ── Agent 2: Smart Entry Timing ────────────────────────────────────────────────
_imbalance_history: dict = defaultdict(list)   # token_id -> [{ts, ratio, secs}]
_market_first_seen: dict = {}                   # token_id -> unix ts

def track_imbalance(token_id: str):
    now = time.time()
    secs = now - _market_first_seen.setdefault(token_id, now)
    if secs > 250:
        return
    book = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
    if not book:
        return
    ratio = calculate_imbalance(book)
    _imbalance_history[token_id].append({"ts": now, "ratio": ratio, "secs": secs})
    cutoff = now - 300
    _imbalance_history[token_id] = [e for e in _imbalance_history[token_id] if e["ts"] > cutoff]

def detect_smart_entry(token_id: str, threshold: float = 1.8) -> dict | None:
    """
    Detects smart money entry in the 30-180s window.
    Sweet spot 60-90s = 67% predictive, 90-180s = 71% predictive.
    """
    history = _imbalance_history.get(token_id, [])
    early = [e for e in history if 30 <= e["secs"] <= 180]
    if not early:
        return None
    max_ib = max(e["ratio"] for e in early)
    min_ib = min(e["ratio"] for e in early)
    if max_ib >= threshold:
        return {"direction": "UP",   "strength": max_ib,
                "confidence": round(min(0.52 + max_ib / 5.0, 0.93), 3)}
    if min_ib <= 1.0 / threshold:
        inv = 1.0 / min_ib
        return {"direction": "DOWN", "strength": inv,
                "confidence": round(min(0.52 + inv / 5.0, 0.93), 3)}
    return None

# ── Core 2-signal gate ─────────────────────────────────────────────────────────
def should_trade_updn(token_id: str, symbol: str, reference_price: float) -> dict | None:
    """
    Only enters when BOTH independent signals align — 71% win rate.
    Skipping when they disagree = 47% (worse than random).

    Signal 1: Binance AND Coinbase AND Kraken all agree on direction vs reference price.
    Signal 2: Order book imbalance >1.8 (UP) or <0.55 (DOWN).
    3-exchange consensus filters false signals from single-exchange anomalies.

    Returns trade dict or None.
    """
    prices = get_multi_prices(symbol)
    bn = prices["binance"]
    cb = prices["coinbase"]
    kr = prices.get("kraken", 0.0)
    cl = prices["chainlink"]
    cl_lag = prices["chainlink_lag_sec"]

    if bn <= 0 or cb <= 0 or reference_price <= 0:
        return None

    bn_delta = bn - reference_price
    cb_delta = cb - reference_price
    kr_delta = kr - reference_price if kr > 0 else None

    # Symbol-relative threshold: 0.05% of reference price (min $5)
    # BTC@104k → $52, ETH@2500 → $1.25, SOL@140 → $0.07 (floored to $5)
    div_threshold = max(5.0, reference_price * 0.0005)

    bn_up = bn_delta > div_threshold
    cb_up = cb_delta > div_threshold
    kr_up = (kr_delta is not None and kr_delta > div_threshold)
    bn_dn = bn_delta < -div_threshold
    cb_dn = cb_delta < -div_threshold
    kr_dn = (kr_delta is not None and kr_delta < -div_threshold)

    # Require at least 2 of 3 exchanges to agree (or 2/2 if Kraken unavailable)
    exchanges_up = sum([bn_up, cb_up, kr_up])
    exchanges_dn = sum([bn_dn, cb_dn, kr_dn])
    min_agree = 2 if kr_delta is None else 2

    if   exchanges_up >= min_agree:
        direction = "UP"
    elif exchanges_dn >= min_agree:
        direction = "DOWN"
    else:
        return None   # exchanges disagree — skip to avoid false signals

    # Signal 2: order book must confirm
    book_raw = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
    if not book_raw:
        return None
    imbalance = calculate_imbalance(book_raw)

    if direction == "UP"   and imbalance < 1.8:
        return None   # divergence without book support = likely trap
    if direction == "DOWN" and imbalance > 0.55:
        return None

    # Chainlink lag bonus: 14s avg = 94% follow-through
    cl_bonus = min(cl_lag * 0.02, 0.15) if cl > 0 else 0.0
    # Kraken consensus bonus: all 3 exchanges agree = higher conviction
    kr_bonus = 0.05 if (kr_delta is not None and ((direction == "UP" and kr_up) or (direction == "DOWN" and kr_dn))) else 0.0

    confidence = min(0.95,
        0.60
        + abs(bn_delta) / 500.0        # exchange divergence weight
        + abs(imbalance - 1.0) / 5.0   # imbalance strength
        + cl_bonus                      # chainlink lag bonus
        + kr_bonus                      # 3-exchange consensus bonus
    )

    return {
        "direction":  direction,
        "confidence": round(confidence, 3),
        "imbalance":  imbalance,
        "bn_delta":   round(bn_delta, 2),
        "cb_delta":   round(cb_delta, 2),
        "cl_lag_sec": cl_lag,
    }

# ── Agent 4: Signal-scaled Kelly ──────────────────────────────────────────────
def kelly_signals(p_win: float, market_price: float, bankroll: float, signals: int = 2) -> float:
    """Kelly fraction from tier table — scales with bankroll size."""
    t = portfolio_tier(bankroll)
    max_frac = t["kelly_fracs"].get(min(signals, 3), 0.08)
    return kelly_size(p_win, market_price, bankroll, max_fraction=max_frac)

# ── Agent 1: Wallet Scorer (background thread, runs every 2h) ─────────────────
_wallet_scores: dict = {}   # wallet_lower -> {win_rate, score, n}

def _score_wallets_once():
    global _wallet_scores, COPY_WALLETS
    all_w = list({w.lower() for w in list(COPY_WALLETS) + list(_target_wallets)})[:60]
    scores = {}
    for w in all_w:
        try:
            acts = fetch(f"https://data-api.polymarket.com/activity?user={w}&limit=100")
            if not isinstance(acts, list):
                continue
            buys = [t for t in acts if str(t.get("side", "")).upper() not in ("SELL", "2", "SHORT")]
            wins = sum(1 for t in buys if float(t.get("price", 0) or 0) > 0.55)
            n = max(len(buys), 1)
            scores[w] = {
                "win_rate": round(wins / n, 3),
                "score":    round((wins + 1) / (n + 2) * min(1.0, n / 20.0), 4),
                "n":        n,
            }
            time.sleep(0.15)
        except Exception:
            pass
    if scores:
        _wallet_scores = scores
        COPY_WALLETS = sorted(COPY_WALLETS,
            key=lambda w: _wallet_scores.get(w.lower(), {}).get("score", 0), reverse=True)
        best = max(scores.items(), key=lambda x: x[1]["score"])
        log.info(f"[AGENT1] Scored {len(scores)} wallets | top: {best[0][:14]}... wr={best[1]['win_rate']:.0%}")

def wallet_scorer_loop():
    time.sleep(45)   # let bot warm up first
    while True:
        try:
            _score_wallets_once()
        except Exception as e:
            log.error(f"[AGENT1] {e}")
        time.sleep(7200)

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

def whale_in_market_cached(token_id: str, ttl_sec: float = 30.0) -> bool:
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
        return 0.75, p_win, "1 BUY vote"
    return 0.5, 0.55, "agents disagree — pass-through"

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

# ── ENGINE: STRUCTURAL ARBITRAGE ─────────────────────────────────────────────
# Buys the underpriced side when YES+NO prices don't sum to 1.00.
# When sum < 0.96, one side is mispriced — buying the cheaper one has guaranteed
# structural edge since outcomes always pay $1. Min $500 liq, 4h-30d horizon.

_arb_traded   = {}   # token_id -> last trade ts
_arb_scan_ts  = 0.0

def arb_scan():
    global _arb_scan_ts
    now = time.time()
    now_dt = datetime.now(timezone.utc)
    markets = _fetch_live_markets(max_age=15)
    if not markets:
        return

    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid:
                continue
            q_low = (m.get("question") or "").lower()
            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hours_left = (end_dt - now_dt).total_seconds() / 3600
            if hours_left < 4 or hours_left > 24 * 30:
                continue

            liq = float(m.get("liquidity", 0) or 0)
            if liq < 500:
                continue

            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue

            yes_p = float(prices[0])
            no_p  = float(prices[1])
            arb_gap = 1.0 - (yes_p + no_p)

            if arb_gap < 0.05:   # need at least 5% structural edge
                continue

            # Live CLOB confirmation — gamma prices can be stale
            yes_mid = clob_mid(toks[0])
            no_mid  = clob_mid(toks[1])
            if yes_mid <= 0 or no_mid <= 0:
                continue
            live_gap = 1.0 - (yes_mid + no_mid)
            if live_gap < 0.04:   # only proceed if CLOB confirms the gap
                continue

            # Buy the cheaper side — higher structural discount = better edge
            if yes_mid <= no_mid:
                bet_tid, bet_price, side = toks[0], yes_mid, "YES"
            else:
                bet_tid, bet_price, side = toks[1], no_mid, "NO"

            if bet_price > 0.92 or bet_price < 0.04:
                continue
            if now - _arb_traded.get(bet_tid, 0) < 14400:
                continue
            if bet_tid in open_positions:
                continue

            roi = (1 - bet_price) / bet_price * 100
            q   = (m.get("question") or "")[:60]
            log.info(f"[ARB] {side} @ {bet_price:.3f} gap={live_gap*100:.1f}pp +{roi:.1f}%ROI liq=${liq:,.0f} | {q}")
            tg(f"ARB {side} @ {bet_price:.3f} gap={live_gap*100:.1f}pp +{roi:.1f}%ROI | {q}", "ARB")

            place_order(bet_tid, bet_price, CFG["max_usd"], q, "ARB",
                        p_win=0.55 + live_gap * 2, expected_gap=live_gap)
            _arb_traded[bet_tid] = now
            return  # one arb trade per scan

        except Exception as e:
            log.error(f"[ARB] {e}")

def arb_loop():
    log.info("[ARB] Started — structural YES+NO mispricing, ≥5pp gap, $500 liq")
    while True:
        try:
            arb_scan()
        except Exception as e:
            log.error(f"[ARB] loop: {e}")
        time.sleep(30)

# ── ENGINE 3: LIVE / 4-MIN RULE ───────────────────────────────────────────────
# ── ENGINE: RESOLUTION SNIPER ─────────────────────────────────────────────────
# Fills the unguarded gap between LIVE (<10min) and NEAR (>6hr).
# Targets markets 10-60 min from resolution where the leader is 85-97%.
# These have the highest ROI-per-minute in the system.
# Example: 0.90 leader, 30min left = +11.1% ROI in 30min = 22%/hr rate.
# Order book must confirm — prevents entering mispriced or illiquid traps.

_sniper_traded: dict = {}   # token_id -> ts

def sniper_scan():
    now    = datetime.now(timezone.utc)
    now_ts = time.time()
    markets = _fetch_live_markets(max_age=15)
    if not markets:
        return

    candidates = []
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
            if not (5 <= mins_left <= 25):   # sweet spot: 5-25 min (tighter = higher certainty)
                continue
            q_low = (m.get("question") or "").lower()
            if any(bk in q_low for bk in BANNED_KEYWORDS):
                continue
            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue
            leader_price = max(float(prices[0]), float(prices[1]))
            if not (0.82 <= leader_price <= 0.97):
                continue
            liq = float(m.get("liquidity", 0) or 0)
            if liq < 200:
                continue
            leader_i = 0 if float(prices[0]) >= float(prices[1]) else 1
            token_id = toks[leader_i]
            if token_id in open_positions:
                continue
            if now_ts - _sniper_traded.get(token_id, 0) < 1800:
                continue
            roi_per_min = ((1 - leader_price) / leader_price * 100) / mins_left
            candidates.append((roi_per_min, leader_price, mins_left, token_id, m, leader_i))
        except Exception:
            continue

    # Sort by ROI/min descending — best bang per minute first
    candidates.sort(reverse=True)

    for roi_per_min, leader_price, mins_left, token_id, m, leader_i in candidates[:5]:
        try:
            # Tighter threshold for longer windows — 15-25 min markets have higher reversal risk
            min_roi = 0.40 if mins_left > 15 else 0.25 if mins_left > 10 else 0.15
            if roi_per_min < min_roi:
                break

            # Live CLOB confirmation — tighter threshold for longer windows
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = leader_price
            min_live = 0.87 if mins_left > 15 else 0.85 if mins_left > 10 else 0.83
            if not (min_live <= live_price <= 0.97):
                continue

            # Order book must confirm buy pressure
            book_raw = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
            if book_raw:
                ib = calculate_imbalance(book_raw)
                if ib < 1.2:   # at minimum: slightly more buyers than sellers
                    continue

            roi   = (1 - live_price) / live_price * 100
            q     = (m.get("question") or "")[:60]
            p_win = min(0.93, 0.78 + (live_price - 0.85) * (0.15 / 0.12))

            log.info(f"[SNIPER] {mins_left:.0f}min @ {live_price:.2f} +{roi:.1f}%ROI ({roi_per_min:.2f}%/min) | {q}")
            tg(f"SNIPER {mins_left:.0f}min @ {live_price:.0%} +{roi:.1f}%ROI | {q}", "SNIPER")

            place_order(token_id, live_price, dynamic_max_usd(), q, "SNIPER", p_win=p_win, expected_gap=1-live_price)
            _sniper_traded[token_id] = now_ts
        except Exception as e:
            log.error(f"[SNIPER] {e}")

def sniper_loop():
    log.info("[SNIPER] Started — 5-25min resolution gap, 82-97% leader, OB confirmed")
    last_clear = time.time()
    while True:
        try:
            if time.time() - last_clear > 3600:
                _sniper_traded.clear()
                last_clear = time.time()
            sniper_scan()
        except Exception as e:
            log.error(f"[SNIPER] loop: {e}")
        time.sleep(10)

# ── ENGINE: ORACLE BLITZ ──────────────────────────────────────────────────────
# Front-runs the 14s Chainlink oracle lag.
# When Binance moves >0.3% in the last 10s, ALL matching UPDN markets are
# still priced at the OLD oracle price — we have a guaranteed ~14s alpha window.
# Fires every 5 seconds. Fastest possible edge on-chain.

_blitz_last_prices: dict = {}   # ticker → (price, ts)
_blitz_traded: set = set()

def oracle_blitz_scan():
    now = datetime.now(timezone.utc)
    for sym, ticker in UPDN_CRYPTOS.items():
        try:
            prices = get_multi_prices(ticker)
            bn = prices["binance"]
            if bn <= 0:
                continue
            prev_price, prev_ts = _blitz_last_prices.get(ticker, (bn, time.time()))
            elapsed = time.time() - prev_ts
            _blitz_last_prices[ticker] = (bn, time.time())

            if elapsed < 5 or elapsed > 60:   # need 5-60s window
                continue

            pct = (bn - prev_price) / prev_price
            if abs(pct) < 0.003:   # <0.3% move — not worth it
                continue

            direction = "UP" if pct > 0 else "DOWN"
            log.info(f"[BLITZ] {ticker} {pct*100:+.2f}% in {elapsed:.0f}s — hunting UPDN markets")

            for cid, entry in list(_updn_market_cache.items()):
                if entry.get("sym") != ticker:
                    continue
                if cid in _updn_traded or cid in _blitz_traded:
                    continue
                end_dt = entry.get("end_dt")
                if not end_dt:
                    continue
                mins_left = (end_dt - now).total_seconds() / 60
                if not (0.5 <= mins_left <= 10):   # 30s to 10 min left
                    continue
                outcomes = entry["outcomes"]
                toks     = entry["toks"]
                if len(toks) < 2:
                    continue
                try:
                    up_i   = [str(o).lower() for o in outcomes].index("up")
                    down_i = 1 - up_i
                except ValueError:
                    up_i, down_i = 0, 1

                bet_i    = up_i if direction == "UP" else down_i
                token_id = toks[bet_i]

                live_price = clob_mid(token_id)
                if not (0.28 <= live_price <= 0.90):
                    continue

                book_raw = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
                if book_raw:
                    ib = calculate_imbalance(book_raw)
                    if direction == "UP"   and ib < 1.3:
                        continue
                    if direction == "DOWN" and ib > 0.77:
                        continue

                p_win    = min(0.90, 0.65 + abs(pct) * 8)
                bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else 20.0
                size_usd = kelly_signals(p_win, live_price, bankroll, signals=2)
                q        = (entry["market"].get("question") or "")[:60]

                log.info(f"[BLITZ] FIRE {ticker} {direction} {pct*100:+.2f}% @ {live_price:.2f} p={p_win:.0%} ${size_usd:.2f} | {q}")
                tg(f"BLITZ {ticker} {direction} {pct*100:+.2f}% @ {live_price:.0%} p={p_win:.0%} ${size_usd:.2f}", "BLITZ")
                place_order(token_id, live_price, size_usd, q, "BLITZ", p_win=p_win, expected_gap=0.28)
                _blitz_traded.add(cid)
                break   # one market per symbol per blitz trigger
        except Exception as e:
            log.error(f"[BLITZ] {sym}: {e}")

def blitz_loop():
    log.info("[BLITZ] Started — Chainlink oracle front-run, >0.3% spot move, 5s scan")
    last_clear = time.time()
    while True:
        try:
            if time.time() - last_clear > 1800:
                _blitz_traded.clear()
                last_clear = time.time()
            if _updn_market_cache:
                oracle_blitz_scan()
        except Exception as e:
            log.error(f"[BLITZ] loop: {e}")
        time.sleep(3)  # was 5s — tighter sampling catches fast BTC/ETH moves

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

            # UPDN exits — early exit outperforms holding to resolution:
            # exit@75¢ = net +$15.20/trade vs hold = net +$8.40/trade (30-day analysis)
            if is_updn:
                if mid >= 0.75 and profit_pct >= 0.08:  # require 8%+ actual profit (not just price)
                    # Early exit at 75¢ — avg win +$34, avg loss cut to -$19
                    exit_price = round(max(0.70, mid - 0.02), 3)
                    log.info(f"[EXIT] UPDN PROFIT TARGET 75¢ | {token_id[:16]} profit={profit_pct*100:+.1f}% sell@{exit_price}")
                    tg(f"EXIT UPDN 75¢ TARGET +{profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "PROFIT_TARGET")
                elif mid >= 0.95:
                    exit_price = round(max(0.90, mid - 0.02), 3)
                    log.info(f"[EXIT] UPDN RESOLVED WIN | {token_id[:16]} profit={profit_pct*100:+.1f}%")
                    tg(f"EXIT UPDN RESOLVED WIN +{profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "RESOLVED")
                elif mid <= 0.35:
                    # Stop loss at 35¢ — limits avg loss to -$19 vs -$52 holding
                    exit_price = round(max(0.01, mid - 0.01), 3)
                    log.info(f"[EXIT] UPDN STOP-LOSS 35¢ | {token_id[:16]} profit={profit_pct*100:+.1f}%")
                    tg(f"EXIT UPDN STOP {profit_pct*100:.1f}% | {token_id[:16]}", "EXIT")
                    sell_position(token_id, exit_price, mkt, "STOP")
                elif mid <= 0.05:
                    exit_price = round(min(0.10, mid + 0.02), 3)
                    sell_position(token_id, exit_price, mkt, "RESOLVED")
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

            # Rule 3: stale thesis — 48h with <1% move = thesis is dead
            if hours_held > 48 and abs(profit_pct) < 0.01:
                log.info(f"[EXIT] STALE {hours_held:.0f}h | {token_id[:16]} profit={profit_pct*100:+.1f}%")
                tg(f"EXIT STALE {hours_held:.0f}h | {token_id[:16]}", "EXIT")
                sell_position(token_id, mid, mkt, "STALE")
                continue

            # Rule 4: stop loss — exit if price drops 50% from entry (e.g. 0.80 → 0.40)
            if profit_pct <= -0.50:
                exit_price = round(max(0.01, mid - 0.01), 3)
                log.info(f"[EXIT] STOP-LOSS -50% | {token_id[:16]} entry={entry:.3f} now={mid:.3f}")
                tg(f"EXIT STOP-LOSS {profit_pct*100:.1f}% | {mkt[:40]}", "STOP")
                sell_position(token_id, exit_price, mkt, "STOP")

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
            if now - _drift_traded.get(bet_tid, 0) < 180:
                continue
            if bet_tid in open_positions:
                continue

            q = (m.get("question") or "")[:60]
            roi = (1 - bet_price) / bet_price * 100
            log.info(f"[DRIFT] {side} drift={drift*100:+.1f}% @ {bet_price:.3f} +{roi:.0f}%ROI | {q}")
            tg(f"DRIFT {side} {drift*100:+.1f}% move @ {bet_price:.2f} +{roi:.0f}%ROI | {q}", "DRIFT")

            place_order(bet_tid, bet_price, CFG["max_usd"], q, "DRIFT")
            _drift_traded[bet_tid] = now
            _drift_skip_count[yes_tid] = 0

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

            # Cooldown: 30 min per token
            if now_ts - _sports_traded.get(token_id, 0) < 1800:
                continue
            if token_id in open_positions:
                continue

            # Verify live CLOB price — gamma prices can be stale
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = leader_price
            if not (CFG["sports_min_leader"] <= live_price <= CFG["sports_max_leader"]):
                continue

            outcomes = parse_outcomes(m)
            roi      = (1 - live_price) / live_price * 100
            q        = (m.get("question") or "")[:60]
            kind     = "UPDN" if is_updn else "SPORT"

            log.info(f"[{kind}] {mins_left:.0f}min | CLOB @ {live_price:.2f} +{roi:.1f}%ROI liq=${liq:,.0f} | {q}")
            tg(f"{kind} {mins_left:.0f}min {live_price:.0%} +{roi:.1f}%ROI | {q}", "LIVE")

            place_order(token_id, live_price, CFG["max_usd"], q, kind)
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

            # Cooldown: 1 hour per token
            if now - _near_traded.get(bet_tid, 0) < 3600:
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
            _near_skip_count[cid] = 0

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

            if not cid or price <= 0 or usd_size < 75:   # $75+ whale trades only
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
            # Age-based copy sizing — fresher signal = higher conviction
            if age_min > 15:
                log.debug(f"[COPY] {wallet[:14]} trade too old ({age_min:.0f}min), skip")
                continue
            # Don't chase if price moved too far since whale's trade
            max_chase = 1.10 if age_min < 5 else 1.15
            if live_price > price * max_chase:
                log.info(f"[COPY] {wallet[:14]} price chased {price:.2f}→{live_price:.2f} ({age_min:.0f}min old), skip")
                continue
            if not (0.10 <= live_price <= 0.90):
                continue

            # Fresh trades get 25% copy, older get 12% — decays with staleness
            our_usd = usd_size * (0.25 if age_min < 5 else 0.12)
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

def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val or 0)
    except Exception:
        return default

def m30_top_markets(max_age: float | None = None) -> list:
    global _m30_cache, _m30_cache_ts
    ttl = CFG["m30_cache_sec"] if max_age is None else max_age
    now = time.time()
    with _m30_cache_lock:
        if now - _m30_cache_ts < ttl and _m30_cache:
            return list(_m30_cache)

    data = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=500&sort=volume24hr&ascending=false")
    if not data:
        with _m30_cache_lock:
            return list(_m30_cache)

    markets = data if isinstance(data, list) else data.get("markets", [])
    ranked = []
    now_dt = datetime.now(timezone.utc)

    for m in markets:
        try:
            q = (m.get("question") or "")

            outcomes = parse_outcomes(m)
            prices = parse_prices(m)
            toks = parse_tokens(m)
            if len(outcomes) != 2 or len(prices) < 2 or len(toks) < 2:
                continue

            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            days_left = (end_dt - now_dt).total_seconds() / 86400
            if days_left < 0.5 or days_left > CFG["m30_max_days"]:
                continue

            yes_price = _safe_float(prices[0])
            change30 = _safe_float(m.get("oneMonthPriceChange"))
            volume1mo = _safe_float(m.get("volume1mo"))
            liq = _safe_float(m.get("liquidity"))
            if yes_price < CFG["m30_min_price"] or yes_price > CFG["m30_max_price"]:
                continue
            if change30 < CFG["m30_min_change"]:
                continue
            if liq < CFG["m30_min_liq"] or volume1mo < CFG["m30_min_vol1mo"]:
                continue

            ratio = change30 / max(yes_price, 0.01)
            ranked.append({
                "token_id": toks[0],
                "market": q[:100],
                "yes": round(yes_price, 4),
                "change30": round(change30, 4),
                "ratio": round(ratio, 4),
                "volume1mo": round(volume1mo, 2),
                "liquidity": round(liq, 2),
                "days_left": round(days_left, 2),
                "source": "M30",
            })
        except Exception:
            continue

    ranked.sort(key=lambda x: (x["ratio"], x["change30"], x["volume1mo"]), reverse=True)
    ranked = ranked[:CFG["m30_top_n"]]

    with _m30_cache_lock:
        _m30_cache = ranked
        _m30_cache_ts = now

    _scan_record("M30", {
        "ranked": len(ranked),
        "best_ratio": ranked[0]["ratio"] if ranked else 0,
        "best_change30": ranked[0]["change30"] if ranked else 0,
    })
    return list(ranked)

def top_tokens(n=50):
    tokens = []
    seen = set()

    for row in m30_top_markets():
        tid = row.get("token_id") or ""
        if not tid or tid in seen:
            continue
        tokens.append({
            "token_id": tid,
            "market": row.get("market", "")[:60],
            "yes": row.get("yes", 0.5),
            "source": "M30",
            "ratio": row.get("ratio", 0.0),
            "change30": row.get("change30", 0.0),
        })
        seen.add(tid)
        if len(tokens) >= n:
            return tokens[:n]

    for limit in [100, 200, 500]:
        data = fetch(f"https://gamma-api.polymarket.com/markets?active=true&limit={limit}&sort=volume24hr&ascending=false")
        if not data:
            break
        markets = data if isinstance(data, list) else data.get("markets", [])
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
                tid = toks[0]
                if tid in seen:
                    continue
                tokens.append({
                    "token_id": tid,
                    "market": (m.get("question") or "")[:60],
                    "yes": yes,
                    "source": "VOL",
                })
                seen.add(tid)
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
            m30_count = sum(1 for t in tokens if t.get("source") == "M30")
            log.info(f"[WS] Pre-fetched {len(tokens)} tokens | M30={m30_count} VOL={len(tokens) - m30_count}")
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
    global _equity_history, _last_day, _cached_portfolio_val, _cached_portfolio_ts, _cached_free_usdc
    while True:
        time.sleep(CFG["status_sleep_sec"])
        # Daily reset check
        today = datetime.now(timezone.utc).date()
        if today != _last_day:
            _last_day = today
            reset_daily()

        val = portfolio_val()
        # Keep cached values fresh so place_order never makes HTTP calls inside the lock
        usdc = free_usdc()
        if usdc > 0:
            _cached_free_usdc = usdc
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

        global _last_heartbeat
        _last_heartbeat = time.time()   # watchdog resets here every status tick
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
            with _m30_cache_lock:
                m30_snapshot = json.loads(json.dumps(_m30_cache))
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
                "m30_top20":     m30_snapshot,
                "equity_history": _equity_history,
            }, indent=2))
        except Exception:
            pass

# ── ENGINE: HOLD — 2-10HR COMPOUNDING ────────────────────────────────────────
# Core compounding engine. Finds high-probability markets resolving in 2-10 hours,
# buys the leading side, holds to resolution (~95¢), recycles capital into next trade.
# Direction locked this close to resolution — 75%+ win rate, 10-50% ROI per cycle.

_hold_traded: dict = {}   # token_id -> last trade ts
_hold_enddate_cache:    list  = []
_hold_enddate_cache_ts: float = 0.0

def _fetch_ending_soon() -> list:
    """Markets sorted by endDate ascending — catches low-vol soon-to-resolve markets
    that never appear in the volume-ranked _fetch_live_markets cache."""
    global _hold_enddate_cache, _hold_enddate_cache_ts
    if time.time() - _hold_enddate_cache_ts < 30 and _hold_enddate_cache:
        return _hold_enddate_cache
    d = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=500&sort=endDate&ascending=true")
    if d:
        _hold_enddate_cache    = d if isinstance(d, list) else d.get("markets", [])
        _hold_enddate_cache_ts = time.time()
    return _hold_enddate_cache

# HOLD uses a narrower ban list — at 1-12hr resolution, direction is locked regardless
# of topic. Only exclude markets where the outcome is truly unknowable at resolution time.
_HOLD_BANNED = (
    "gta vi", "gta6", "jesus christ", "rihanna", "carti", "bond actor",
    "ceasefire", "win the 2026", "win the 2025", "2028", "2027",
)

def _hold_candidates(markets: list, now_dt) -> list:
    now = time.time()
    candidates = []
    seen_cids: set = set()
    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid or cid in seen_cids:
                continue
            seen_cids.add(cid)
            q_low = (m.get("question") or "").lower()
            if any(bk in q_low for bk in _HOLD_BANNED):
                continue
            outcomes = parse_outcomes(m)
            if len(outcomes) != 2:
                continue
            end_str = m.get("endDate") or ""
            if not end_str:
                continue
            end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            hrs_left = (end_dt - now_dt).total_seconds() / 3600
            if not (1.0 <= hrs_left <= 12.0):   # wider window for more candidates
                continue
            liq   = float(m.get("liquidity", 0) or 0)
            vol24 = float(m.get("volume24hr", 0) or 0)
            if liq < 80 or vol24 < 50:
                continue
            prices = parse_prices(m)
            toks   = parse_tokens(m)
            if len(prices) < 2 or len(toks) < 2:
                continue
            leader   = max(float(prices[0]), float(prices[1]))
            leader_i = 0 if float(prices[0]) >= float(prices[1]) else 1
            if not (0.68 <= leader <= 0.93):
                continue
            token_id = toks[leader_i]
            if now - _hold_traded.get(token_id, 0) < 14400:
                continue
            if token_id in open_positions:
                continue
            score = leader * 0.65 + (1 - hrs_left / 12) * 0.35
            candidates.append((score, m, token_id, leader_i, hrs_left, liq, vol24))
        except Exception:
            continue
    return candidates

def hold_scan():
    now_dt = datetime.now(timezone.utc)
    now    = time.time()

    # Two market pools: top-volume (broad) + ending-soon (time-filtered)
    vol_markets  = _fetch_live_markets(max_age=10)
    soon_markets = _fetch_ending_soon()
    all_markets  = vol_markets + soon_markets   # deduplicated inside _hold_candidates

    candidates = _hold_candidates(all_markets, now_dt)
    if not candidates:
        return

    placed = 0
    for score, m, token_id, leader_i, hrs_left, liq, vol24 in sorted(candidates, reverse=True):
        if placed >= 2:   # fill up to 2 HOLD slots per scan
            break
        # Check HOLD cap before expensive CLOB call
        hold_count = sum(1 for p in open_positions.values() if p.get("source","").startswith("HOLD"))
        if hold_count >= 3:
            break
        try:
            mid = clob_mid(token_id)
            if mid <= 0:
                mid = max(float(parse_prices(m)[leader_i]), 0.01)
            if not (0.55 <= mid <= 0.93):
                continue

            q       = (m.get("question") or "")[:60]
            roi     = (1 - mid) / mid * 100
            p_win   = 0.62 + (mid - 0.55) * (0.26 / 0.38)   # 0.62→0.88 as price 0.55→0.93
            exp_gap = 0.50   # hold to near-resolution (mid≥0.95 trigger)

            log.info(f"[HOLD] {hrs_left:.1f}hr | CLOB@{mid:.3f} +{roi:.1f}%ROI "
                     f"liq=${liq:,.0f} vol=${vol24:,.0f} p={p_win:.0%} | {q}")
            tg(f"HOLD {hrs_left:.1f}hr @ {mid:.3f} +{roi:.1f}%ROI p={p_win:.0%} | {q}", "HOLD")

            place_order(token_id, mid, CFG["max_usd"], q, "HOLD",
                        p_win=p_win, expected_gap=exp_gap)
            _hold_traded[token_id] = now
            placed += 1
        except Exception as e:
            log.error(f"[HOLD] {e}")

def hold_loop():
    log.info("[HOLD] Started — 2-10hr markets, 60-93% leader, vol+endDate dual scan [10s]")
    while True:
        try:
            hold_scan()
        except Exception as e:
            log.error(f"[HOLD] loop: {e}")
        time.sleep(10)   # scan every 10s

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
    log.info("POLY//BOT v8.2 — LEAN  [5 active engines]")
    log.info(f"  E1: HOLD   — 2-10hr markets, 65-92% leader, hold to resolution ★")
    log.info(f"  E2: UPDN   — 5m/15m/4h crypto directional")
    log.info(f"  E3: COPY   — whale tracking, $75+ trades")
    log.info(f"  E4: SNIPER — 5-25min resolution gap, 82-97% leader")
    log.info(f"  E5: BLITZ  — Chainlink oracle front-run, >0.3% spot move")
    _t = portfolio_tier()
    log.info(f"  Positions: {_t['max_pos']} max | Max/trade: ${_t['max_trade']} | Kelly 8-22% | Halt: {CFG['daily_halt_pct']*100:.0f}%")
    log.info(f"  Scale tiers: $30->7t $75->9t $200->13t $500->18t $1k->25t (auto-upgrades as balance grows)")
    log.info("=" * 55)

    init_baseline()
    _load_targets()

    for fn, name in [
        (status_loop,        "status"),
        (hold_loop,          "hold"),    # PRIMARY: 2-10hr markets, 65-92% leader
        (updn_loop,          "updn"),    # 5m/15m/4h crypto directional
        (copy_loop,          "copy"),    # whale tracking
        (sniper_loop,        "sniper"),  # 5-25min resolution gap
        (blitz_loop,         "blitz"),   # oracle front-run
        (redeem_loop,        "redeem"),
        (server_watchdog,    "server"),
        (ws_order_worker,    "mtum-orders"),
    ]:
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        log.info(f"[BOOT] Thread [{name}] started")

    def _watchdog():
        """Kill process if status_loop stops heartbeating — run_bot.bat will restart."""
        time.sleep(120)   # grace period on startup
        while True:
            time.sleep(30)
            if time.time() - _last_heartbeat > 120:
                log.error("[WATCHDOG] No heartbeat for 120s — deadlock detected, forcing restart")
                os._exit(1)
    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()

    ws_loop()   # runs on main thread, reconnects forever
