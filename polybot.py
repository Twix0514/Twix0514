"""
POLY//MULTI-ENGINE — Prediction Market Bot
==========================================
Engines:
  E0  All-Market OB  OB imbalance sweep across ALL active markets
  E1  BTC UPDN       5 / 10 / 15 / 20 / 25 / 30 / 35 / 40m
  E2  Pairs Arb      YES+NO < 0.99 → instant dual-leg entry
  E3  Sentiment      RSS/news headline → early entry before CLOB reprices
  E4  Volatility     wide-range markets approaching resolution
  E5  Sports         NFL/NBA/Soccer/Tennis/Golf
Platforms: Polymarket CLOB  +  Kalshi REST API
"""

import json, time, re, threading, logging, pathlib, os
import urllib.request, urllib.parse, xml.etree.ElementTree as ET
from collections import defaultdict
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

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("POLY")

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
PRIVATE_KEY       = os.environ.get("POLY_PRIVATE_KEY", "")
FUNDER            = os.environ.get("POLY_FUNDER",      "")
CHAIN_ID          = 137
PROXY             = os.environ.get("POLY_PROXY", "")
BANKROLL_OVERRIDE = float(os.environ.get("POLY_BANKROLL", "0"))

KALSHI_EMAIL    = os.environ.get("KALSHI_EMAIL", "")
KALSHI_PASSWORD = os.environ.get("KALSHI_PASSWORD", "")
KALSHI_API_KEY  = os.environ.get("KALSHI_API_KEY", "")

NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")

try:
    from secrets_local import PRIVATE_KEY, FUNDER, PROXY, BANKROLL_OVERRIDE  # type: ignore
except ImportError:
    try:
        from secrets_local import PRIVATE_KEY, FUNDER, PROXY  # type: ignore
    except Exception:
        pass
except Exception:
    pass

try:
    from secrets_local import KALSHI_EMAIL, KALSHI_PASSWORD, KALSHI_API_KEY, NEWSAPI_KEY  # type: ignore
except Exception:
    pass

# ── CONFIG ────────────────────────────────────────────────────────────────────
CFG = {
    "dry_run":         False,
    "stake_pct":       0.10,        # 10% per trade — sized for small bankroll
    "stake_min_usd":   1.00,        # $1 floor to stretch $9.91 across more trades
    "free_usdc_min":   0.50,        # trade as long as 50¢ free
    "daily_halt_pct":  -1.00,       # disabled
    "max_positions":   12,

    # Crypto UPDN multi-timeframe (BTC + ETH, minutes; 60=1h, 240=4h)
    "btc_timeframes":  [5, 15, 60, 240],
    "btc_entry_frac":  (0.10, 0.90),  # wide window — enter almost any time
    "btc_confluence_min": 1.0,        # fire on any signal at all
    "updn_min_price":  0.10,          # accept any price — more opportunities
    "updn_max_price":  0.95,
    "updn_ttl_sec":    60,            # re-enter same market every 60s
    "updn_refresh_sec": 10,
    "updn_scan_sleep_sec": 1,

    # Pairs arb
    "arb_threshold":   0.990,       # catch even tight mispricings
    "arb_min_profit":  0.005,       # 0.5¢ edge is enough
    "arb_scan_sleep":  3,
    "arb_market_limit": 200,

    # Sentiment
    "sent_scan_sleep": 30,
    "sent_entry_ttl":  60,
    "sent_min_conf":   0.40,        # fire on weaker signals

    # Volatility timing
    "vol_mid_range":   (0.20, 0.80),  # wide range
    "vol_event_hours": (0.1, 72.0),   # any upcoming resolution
    "vol_scan_sleep":  15,

    # Sports
    "sports_tags":     ["sports", "nfl", "nba", "mlb", "soccer", "tennis", "golf"],
    "sports_min_liq":  50,          # very low liquidity threshold
    "sports_scan_sleep": 20,

    # Whale flow
    "whale_min_usd":       1_000,   # track smaller whale trades too
    "whale_lookback_sec":  600,     # 10-minute window
    "whale_scan_interval": 20,

    # Misc
    "status_sleep_sec": 5,
    "cache_refresh_sleep": 10,
}

# ── PROXY SETUP ───────────────────────────────────────────────────────────────
_direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
if PROXY:
    os.environ["HTTP_PROXY"]  = PROXY
    os.environ["HTTPS_PROXY"] = PROXY
    os.environ["ALL_PROXY"]   = PROXY
    log.info(f"[PROXY] {PROXY}")

_PROXY_ENV_KEYS = ("HTTP_PROXY","HTTPS_PROXY","ALL_PROXY","http_proxy","https_proxy","all_proxy")

# ── POLYMARKET CLOB CLIENT ────────────────────────────────────────────────────
client = ClobClient(
    "https://clob.polymarket.com",
    key=PRIVATE_KEY, chain_id=CHAIN_ID,
    signature_type=1, funder=FUNDER,   # 1=POLY_PROXY (standard Polymarket wallet)
)
_eoa_client = ClobClient(
    "https://clob.polymarket.com",
    key=PRIVATE_KEY, chain_id=CHAIN_ID,
    signature_type=0,
)
try:
    _creds = client.create_or_derive_api_creds()
    client.set_api_creds(_creds)
    _eoa_client.set_api_creds(_creds)
    log.info("[AUTH] Polymarket API creds derived")
except Exception as e:
    log.warning(f"[AUTH] {e}")

# ── KALSHI CLIENT ─────────────────────────────────────────────────────────────
KALSHI_BASE  = "https://trading-api.kalshi.com/trade-api/v2"
_kalshi_token: str = ""
_kalshi_lock  = threading.Lock()

def kalshi_login():
    global _kalshi_token
    if not KALSHI_EMAIL or not KALSHI_PASSWORD:
        return False
    try:
        payload = json.dumps({"email": KALSHI_EMAIL, "password": KALSHI_PASSWORD}).encode()
        req = urllib.request.Request(
            f"{KALSHI_BASE}/login",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with _direct.open(req, timeout=10) as r:
            data = json.loads(r.read())
        _kalshi_token = data.get("token", "")
        if _kalshi_token:
            log.info("[KALSHI] Authenticated")
            return True
    except Exception as e:
        log.warning(f"[KALSHI] Login failed: {e}")
    return False

def kalshi_get(path: str, params: dict = None) -> dict | list | None:
    with _kalshi_lock:
        tok = _kalshi_token or KALSHI_API_KEY
    if not tok:
        return None
    try:
        url = f"{KALSHI_BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        with _direct.open(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.debug(f"[KALSHI] GET {path}: {e}")
        return None

# ── FILES ─────────────────────────────────────────────────────────────────────
BASE              = pathlib.Path(__file__).parent
ALERTS_FILE       = BASE / "alerts.json"
STATUS_FILE       = BASE / "status.json"
LOCK_FILE         = BASE / "bot.lock"
TRADED_FILE       = BASE / "multi_traded.json"

# ── STATE ─────────────────────────────────────────────────────────────────────
_order_lock           = threading.Lock()
_bot_halted           = False
_halt_reason          = ""
_day_start_val        = None
_session_start        = None
trade_log             = []
open_positions        = {}
_cached_portfolio_val: float = max(BANKROLL_OVERRIDE, 0.0)
_cached_free_usdc:     float = max(BANKROLL_OVERRIDE, 0.0)
_traded: set          = set()
_traded_lock          = threading.Lock()
_bad_tokens: set      = set()   # permanent per-run blacklist (version mismatch / expired)

# ── WHALE WALLETS — top BTC/crypto directional traders ───────────────────────
# Conviction = composite score 0-10 (PnL rank × accuracy × avg size).
# Auto-discovery adds new wallets at runtime — see _discover_whales().
BTC_WHALE_WALLETS: dict = {
    # Tier 1 — confirmed top-month BTC specialists (polyscope / polymarketanalytics)
    "0x55be7aa03ecfbe37aa5460db791205f7ac9ddca3": {"name": "coinman2",       "conviction": 9.8},
    "0x2a2c53bd278c04da9962fcf96490e17f3dfb9bc1": {"name": "anon-T1",        "conviction": 9.3},
    "0xefbc5fec8d7b0acdc8911bdd9a98d6964308f9a2": {"name": "reachingthesky", "conviction": 8.5},
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7": {"name": "HorizonSplend",  "conviction": 8.2},
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51": {"name": "beachboy4",      "conviction": 8.0},
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a": {"name": "0x8dxd",         "conviction": 8.0},
    "0x019782cab5d844f02bafb71f512758be78579f3c": {"name": "majorexploit",   "conviction": 7.9},
    "0xde17f7144fbd0eddb2679132c10ff5e74b120988": {"name": "crypto-top1",    "conviction": 7.5},
    "0xd84c2b6d65dc596f49c7b6aadd6d74ca91e407b9": {"name": "BoneReader",     "conviction": 7.3},
    "0xd0d6053c3c37e727402d84c14069780d360993aa": {"name": "k9Q2mX",         "conviction": 7.1},
    "0xe1d6b51521bd4365769199f392f9818661bd907c": {"name": "crypto-top5",    "conviction": 6.9},
    "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30": {"name": "Bonereaper",     "conviction": 6.8},
    "0x6e1d5040d0ac73709b0621f620d2a60b80d2d0fa": {"name": "crypto-top8",    "conviction": 6.5},
    "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a": {"name": "vidarx",         "conviction": 6.3},

    # Tier 2 — additional known high-volume BTC updown traders
    "0xfc85b7a7e622e3ed0de319507bb950b750c6648e": {"name": "bejyvqkri",      "conviction": 6.2},
    "0x43b4e4bfbab5ba1b70d2cc223ff6f412572aec9c": {"name": "budu100",        "conviction": 6.0},
    "0xcfb103c37c0234f524c632d964ed31f117b5f694": {"name": "xuanxuan008",    "conviction": 5.9},
    "0xfbc3eab55db8c7e4d35b2d556bc33737d5d037b9": {"name": "lfg2048",        "conviction": 5.8},
    "0xa8ccda0419f357f819271437efb61022f01341a9": {"name": "UUDDLRLR",       "conviction": 5.7},
    "0x6676515d15697e60f9b8199f1b5884fd62af22f3": {"name": "canaanrain2",    "conviction": 5.6},
}

# Dynamically discovered whales (populated at runtime by _discover_whales)
_discovered_whales: dict = {}
_whale_discovery_lock     = threading.Lock()
_last_discovery:    float = 0.0

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch(url: str, headers: dict = None) -> dict | list | None:
    try:
        h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        if headers:
            h.update(headers)
        req = urllib.request.Request(url, headers=h)
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
        ALERTS_FILE.write_text(json.dumps(existing[:300], indent=2))
    except Exception:
        pass
    log.info(f"[ALERT] {msg}")

def parse_prices(m):
    p = m.get("outcomePrices", "[0.5,0.5]")
    return [float(x) for x in (json.loads(p) if isinstance(p, str) else p)]

def parse_tokens(m):
    t   = m.get("clobTokenIds", "[]")
    raw = json.loads(t) if isinstance(t, str) else t
    return [x if isinstance(x, str) else x.get("token_id","") for x in raw]

def parse_outcomes(m):
    o = m.get("outcomes", '["Yes","No"]')
    return json.loads(o) if isinstance(o, str) else o

def already_traded(key: str) -> bool:
    with _traded_lock:
        return key in _traded

def mark_traded(key: str):
    with _traded_lock:
        _traded.add(key)
    try:
        existing = {}
        try:
            existing = json.loads(TRADED_FILE.read_text())
        except Exception:
            pass
        existing[key] = time.time()
        existing = {k: v for k, v in existing.items() if time.time() - v < CFG["updn_ttl_sec"]}
        TRADED_FILE.write_text(json.dumps(existing))
    except Exception:
        pass

def _load_traded():
    try:
        now = time.time()
        data = json.loads(TRADED_FILE.read_text())
        return {k for k, v in data.items() if now - v < CFG["updn_ttl_sec"]}
    except Exception:
        return set()

# ── PORTFOLIO ─────────────────────────────────────────────────────────────────
def _parse_bal(raw) -> float:
    v = float(raw or 0)
    return v / 1_000_000 if v > 1000 else v

def free_usdc() -> float:
    for c in [client, _eoa_client]:
        try:
            bal = c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            v   = _parse_bal(bal.get("balance", 0))
            if v > 0:
                return v
        except Exception:
            pass
    try:
        data = fetch(f"https://gamma-api.polymarket.com/portfolio?user={FUNDER}")
        if data:
            item = data[0] if isinstance(data, list) else data
            v = float(item.get("cashBalance") or item.get("balance") or 0)
            if v > 0:
                return v
    except Exception:
        pass
    return BANKROLL_OVERRIDE if BANKROLL_OVERRIDE > 0 else 0.0

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
    usdc    = free_usdc()
    pos_val = sum(
        float(p.get("size", 0)) * (clob_mid(tid) or float(p.get("entry", 0.5)))
        for tid, p in list(open_positions.items())
    )
    return usdc + pos_val

# ── RISK ──────────────────────────────────────────────────────────────────────
def init_baseline():
    global _day_start_val, _session_start
    val = portfolio_val()
    if val > 0:
        _day_start_val = val
        _session_start = val
        log.info(f"[RISK] Baseline ${val:.2f}")

def check_halt() -> bool:
    if _bot_halted:
        return False
    if _day_start_val:
        val = _cached_portfolio_val if _cached_portfolio_val > 0 else _day_start_val
        if (val - _day_start_val) / _day_start_val < CFG["daily_halt_pct"]:
            return False
    return True

def reset_daily():
    global _day_start_val, _bot_halted, _halt_reason
    val = portfolio_val()
    if val > 0:
        _day_start_val = val
    if _bot_halted and "daily" in _halt_reason:
        _bot_halted  = False
        _halt_reason = ""

# ── CLOB HELPERS ──────────────────────────────────────────────────────────────
def clob_mid(token_id: str) -> float:
    data = fetch(f"https://clob.polymarket.com/midpoint?token_id={token_id}")
    return float(data.get("mid", 0) or 0) if data else 0.0

def clob_book(token_id: str) -> dict:
    return fetch(f"https://clob.polymarket.com/book?token_id={token_id}") or {}

def book_depth(book: dict) -> float:
    return sum(float(b.get("size",0)) for b in book.get("bids",[])[:10])

def calculate_imbalance(book: dict) -> float:
    bids = sum(float(b.get("size",0)) for b in book.get("bids",[])[:10])
    asks = sum(float(a.get("size",0)) for a in book.get("asks",[])[:10])
    return round(bids / asks, 3) if asks > 0 else 9.99

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def kelly_size(p_win: float, price: float, bankroll: float) -> float:
    if price <= 0 or price >= 1:
        return 0.0
    b      = (1 / price) - 1
    f_star = (p_win * b - (1 - p_win)) / b
    if f_star <= 0:
        return 0.0
    f = min(f_star * 0.5, 0.05)
    return round(max(bankroll * CFG["stake_pct"], bankroll * f, CFG["stake_min_usd"]), 2)

_neg_risk_cache: dict = {}
_neg_risk_lock        = threading.Lock()

def _is_neg_risk(token_id: str) -> bool:
    with _neg_risk_lock:
        if token_id in _neg_risk_cache:
            return _neg_risk_cache[token_id]
    try:
        data = fetch(f"https://clob.polymarket.com/neg-risk?token_id={token_id}")
        val  = bool(data.get("neg_risk", False)) if data else False
    except Exception:
        val = False
    with _neg_risk_lock:
        _neg_risk_cache[token_id] = val
    return val

def place_order(token_id: str, price: float, size_usd: float,
                market: str, source: str, p_win: float = 0.6,
                side: str = BUY) -> bool:
    global _cached_free_usdc
    with _order_lock:
        if not check_halt():
            return False
        if token_id in open_positions and side == BUY:
            return False
        if len(open_positions) >= CFG["max_positions"]:
            log.warning(f"[{source}] SKIP — max {CFG['max_positions']} positions open")
            return False
        f = _cached_free_usdc if _cached_free_usdc > 0 else max(_cached_portfolio_val, 1.0)
        if f < CFG["free_usdc_min"]:
            log.warning(f"[{source}] SKIP — free USDC ${f:.2f} < ${CFG['free_usdc_min']}")
            return False
        shares   = max(5, round(size_usd / max(price, 0.01)))
        size_usd = round(shares * price, 2)
        if size_usd > f * 0.9:
            shares   = max(1, round(f * 0.85 / max(price, 0.01)))
            size_usd = round(shares * price, 2)
        if size_usd < 0.50:
            return False
        _args = (token_id, price, shares, market, source, size_usd, side)

    token_id, price, shares, market, source, cost, side = _args
    if token_id in _bad_tokens:
        return False

    for attempt, bid in enumerate([price, min(round(price + 0.02, 3), 0.97)]):
        try:
            if CFG["dry_run"]:
                log.info(f"[DRY] {side} {shares}sh @ {bid:.3f} (${cost:.2f}) | {market[:45]}")
                status = "dry"
            else:
                order  = client.create_and_post_order(
                    OrderArgs(token_id=token_id, price=bid, size=shares, side=side)
                )
                status = order.get("status", "?")
            actual = round(shares * bid, 2)
            tg(f"ORDER [{source}] {side} {shares}sh @ {bid:.3f} (${actual:.2f}) | {market[:45]} | {status}")
            with _order_lock:
                if side == BUY and token_id not in open_positions:
                    if status in ("matched", "delayed", "live", "dry"):
                        open_positions[token_id] = {
                            "size": shares, "entry": bid, "source": source,
                            "market": market, "entry_time": time.time(),
                        }
                        trade_log.append({"source": source, "token_id": token_id,
                                          "price": bid, "size": shares, "ts": time.time()})
                        _cached_free_usdc = max(0, _cached_free_usdc - actual)
                        return True
            return False
        except Exception as e:
            err = str(e)
            if "order_version_mismatch" in err or "Invalid token" in err:
                _bad_tokens.add(token_id)
                log.error(f"[{source}] ORDER 400 err={err[:80]} token={token_id[:20]}")
            else:
                log.error(f"[{source}] Order error: {e}")
            break
    return False

def place_arb_pair(yes_tid: str, no_tid: str, yes_price: float, no_price: float,
                   size_usd: float, market: str) -> bool:
    """Buy YES and NO simultaneously. Returns True if both legs filled."""
    filled = [False, False]
    def _leg(idx, tid, price):
        shares = max(1, round(size_usd / max(price, 0.01)))
        try:
            if CFG["dry_run"]:
                filled[idx] = True
                return
            order = client.create_and_post_order(
                OrderArgs(token_id=tid, price=price, size=shares, side=BUY)
            )
            filled[idx] = order.get("status") in ("matched", "delayed", "live")
        except Exception as e:
            log.debug(f"[ARB] leg{idx}: {e}")

    t0 = threading.Thread(target=_leg, args=(0, yes_tid, yes_price), daemon=True)
    t1 = threading.Thread(target=_leg, args=(1, no_tid, no_price), daemon=True)
    t0.start(); t1.start()
    t0.join(timeout=4); t1.join(timeout=4)

    if all(filled):
        gross   = 1.0 - (yes_price + no_price)
        tg(f"ARB PAIR ✓ YES@{yes_price:.3f}+NO@{no_price:.3f} edge=${gross:.3f} | {market[:50]}", "ARB")
        trade_log.append({"source": "ARB", "market": market,
                          "yes_price": yes_price, "no_price": no_price, "edge": gross})
        return True
    log.warning(f"[ARB] Partial fill — legs: {filled}")
    return False

def sell_position(token_id: str, price: float, market: str, source: str):
    with _order_lock:
        pos = open_positions.get(token_id)
        if not pos:
            return
        shares = float(pos.get("size", 0))
    if shares <= 0:
        return
    for sh in [shares, max(1, int(shares) - 1)]:
        try:
            if CFG["dry_run"]:
                with _order_lock:
                    open_positions.pop(token_id, None)
                tg(f"SELL [DRY/{source}] {sh}sh @ {price:.3f} | {market[:45]}")
                return
            order  = client.create_and_post_order(
                OrderArgs(token_id=token_id, price=price, size=sh, side=SELL)
            )
            status = order.get("status","?")
            with _order_lock:
                if status in ("matched","delayed","live"):
                    open_positions.pop(token_id, None)
            tg(f"SELL [{source}] {sh}sh @ {price:.3f} | {market[:45]} | {status}")
            return
        except Exception as e:
            err = str(e)
            if "not enough balance" in err:
                with _order_lock:
                    open_positions.pop(token_id, None)
                return
            if sh > 1:
                continue
            log.error(f"[{source}] Sell error: {e}")
            return

# ── BINANCE WEBSOCKET ─────────────────────────────────────────────────────────
_WS_PRICES: dict = {}
_WS_LOCK         = threading.Lock()

def _start_binance_ws():
    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"
    def on_msg(ws, msg):
        try:
            d = json.loads(msg)["data"]
            with _WS_LOCK:
                _WS_PRICES[d["s"]] = {"price": float(d["p"]), "ts": float(d["T"]) / 1000}
        except Exception:
            pass
    def run():
        while True:
            saved = {k: os.environ.pop(k, None) for k in _PROXY_ENV_KEYS}
            try:
                websocket.WebSocketApp(url, on_message=on_msg).run_forever(
                    ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error(f"[BNS-WS] {e}")
            finally:
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v
            time.sleep(3)
    threading.Thread(target=run, daemon=True, name="binance-ws").start()
    log.info("[BNS-WS] Binance WebSocket starting")

def ws_price(symbol: str) -> tuple:
    with _WS_LOCK:
        d = _WS_PRICES.get(f"{symbol.upper()}USDT")
    if not d:
        return 0.0, 999.0
    return d["price"], time.time() - d["ts"]

def get_btc_price() -> float:
    p, age = ws_price("BTC")
    if p > 0 and age < 3:
        return p
    data = fetch("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT") or {}
    return float(data.get("price", 0) or 0)

def btc_candle_open(interval: str = "5m") -> float:
    data = fetch(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval={interval}&limit=2")
    return float(data[-1][1]) if data and len(data) >= 1 else 0.0

def multi_exchange_agree(delta: float, ref: float, threshold: float) -> int:
    if ref <= 0 or delta == 0:
        return 0
    up = delta > threshold
    dn = delta < -threshold
    score = 0
    for sym_url in [
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        "https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD",
    ]:
        try:
            d = fetch(sym_url) or {}
            if "coinbase" in sym_url:
                p = float((d.get("data") or {}).get("amount", 0) or 0)
            else:
                res = d.get("result") or {}
                p   = float(next(iter(res.values()), {}).get("c", [0])[0] or 0) if res else 0
            if p <= 0:
                continue
            d2 = p - ref
            if (up and d2 > threshold) or (dn and d2 < -threshold):
                score += 1
        except Exception:
            pass
    return score

# ── CHAINLINK ORACLE ──────────────────────────────────────────────────────────
_CL_ABI = [{"inputs":[], "name":"latestRoundData",
    "outputs":[{"type":"uint80"},{"type":"int256"},{"type":"uint256"},
               {"type":"uint256"},{"type":"uint80"}],
    "stateMutability":"view","type":"function"}]
_CL_BTC   = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
_cl_cache: dict = {}

def chainlink_btc() -> tuple:
    if not _WEB3_OK:
        return 0.0, 999.0
    now = time.time()
    c = _cl_cache.get("BTC")
    if c and now - c[0] < 8:
        return c[1], now - c[2]
    try:
        w3   = Web3(Web3.HTTPProvider("https://polygon-rpc.com", request_kwargs={"timeout":4}))
        feed = w3.eth.contract(address=Web3.to_checksum_address(_CL_BTC), abi=_CL_ABI)
        _, ans, _, updated_at, _ = feed.functions.latestRoundData().call()
        price = float(ans) / 1e8
        _cl_cache["BTC"] = (now, price, float(updated_at))
        return price, now - float(updated_at)
    except Exception:
        return 0.0, 999.0

# ── WHALE SCANNER ─────────────────────────────────────────────────────────────
_whale_flow: dict = {}
_whale_lock       = threading.Lock()

def _scan_whale_flow_with(wallets: dict):
    now_ts = time.time()
    cutoff = now_ts - CFG["whale_lookback_sec"]
    for wallet, info in wallets.items():
        try:
            activity = fetch(f"https://data-api.polymarket.com/activity?user={wallet}&limit=20")
            if not isinstance(activity, list):
                continue
            for trade in activity:
                raw_ts = int(trade.get("timestamp", 0) or 0)
                if raw_ts > 1e12:
                    raw_ts //= 1000
                if raw_ts < cutoff or trade.get("type") != "TRADE":
                    continue
                if str(trade.get("side","")).upper() in ("SELL","2"):
                    continue
                usd = float(trade.get("usdcSize", 0) or 0)
                if usd < CFG["whale_min_usd"]:
                    continue
                cid     = trade.get("conditionId","")
                outcome = str(trade.get("outcome","")).strip()
                if not cid:
                    continue
                mkt_data = fetch(f"https://gamma-api.polymarket.com/markets?conditionId={cid}")
                if not mkt_data:
                    continue
                ml = mkt_data if isinstance(mkt_data, list) else mkt_data.get("markets",[])
                if not ml:
                    continue
                q = (ml[0].get("question") or "").lower()
                if not re.search(r"btc|bitcoin", q) or not re.search(r"up|down", q):
                    continue
                out_lc = outcome.lower()
                if out_lc in ("up","yes") and "up" in q:
                    direction = "UP"
                elif out_lc in ("down","yes") and "down" in q:
                    direction = "DOWN"
                elif out_lc == "up":
                    direction = "UP"
                elif out_lc == "down":
                    direction = "DOWN"
                else:
                    continue
                rec = {"wallet": wallet, "name": info["name"], "conviction": info["conviction"],
                       "usd": usd, "direction": direction, "ts": raw_ts, "market": q[:60]}
                with _whale_lock:
                    bucket = _whale_flow.setdefault(cid, [])
                    if not any(r["wallet"] == wallet and r["ts"] == raw_ts for r in bucket):
                        bucket.append(rec)
                    _whale_flow[cid] = [r for r in bucket if r["ts"] > cutoff]
                log.info(f"[WHALE] {info['name']} ${usd:,.0f} → {direction} | {q[:50]}")
        except Exception as e:
            log.debug(f"[WHALE] {wallet[:14]}: {e}")
    with _whale_lock:
        for k in [k for k, v in _whale_flow.items() if not v]:
            del _whale_flow[k]

def whale_consensus(cid: str) -> tuple:
    with _whale_lock:
        records = list(_whale_flow.get(cid, []))
    if not records:
        return None, 0.0, 0, 0.0
    up_w = down_w = 0.0
    total_usd = sum(r["usd"] for r in records)
    for r in records:
        w = r["conviction"] * (r["usd"] / 100_000)
        if r["direction"] == "UP":
            up_w += w
        else:
            down_w += w
    total_w = up_w + down_w
    if total_w == 0:
        return None, 0.0, 0, 0.0
    if up_w >= down_w:
        return "UP",   round(up_w   / total_w * 10, 2), len(records), total_usd
    return     "DOWN", round(down_w / total_w * 10, 2), len(records), total_usd

def _all_whales() -> dict:
    with _whale_discovery_lock:
        return {**BTC_WHALE_WALLETS, **_discovered_whales}

def _discover_whales():
    """
    Scans recent large BTC updown trades to auto-discover new whale wallets.
    Runs every 6 hours. Adds wallets making ≥3 BTC trades with avg size ≥$500
    (proxy for $10k+ intent; the USDC size we see is often partial fills).
    """
    global _last_discovery, _discovered_whales
    if time.time() - _last_discovery < 21600:  # 6h cooldown
        return
    _last_discovery = time.time()
    log.info("[WHALE] Auto-discovery scan starting...")

    from collections import defaultdict
    candidates: dict = defaultdict(lambda: {"trades": 0, "vol": 0.0, "name": ""})
    known = set(BTC_WHALE_WALLETS.keys())

    for offset in range(0, 500, 100):
        try:
            trades = fetch(
                f"https://data-api.polymarket.com/trades?market=btc&limit=100&offset={offset}"
            )
            if not isinstance(trades, list):
                break
            for t in trades:
                w = t.get("proxyWallet","")
                if not w or w.lower() in known:
                    continue
                usdc = float(t.get("size",0) or 0) * float(t.get("price",0) or 0)
                candidates[w.lower()]["trades"]  += 1
                candidates[w.lower()]["vol"]     += usdc
                if t.get("name") and not candidates[w.lower()]["name"]:
                    candidates[w.lower()]["name"] = t["name"]
        except Exception as e:
            log.debug(f"[WHALE/disc] offset={offset}: {e}")

    # Qualify: ≥3 trades in sample or single trade > $300 USDC equivalent
    added = 0
    new_dict: dict = {}
    for addr, info in sorted(candidates.items(), key=lambda x: x[1]["vol"], reverse=True):
        if addr in known:
            continue
        if info["trades"] < 3 and info["vol"] < 300:
            continue
        conviction = max(4.0, min(6.5, 4.0 + info["vol"] / 500))
        new_dict[addr] = {
            "name":       info["name"] or addr[:10],
            "conviction": round(conviction, 1),
        }
        added += 1
        if added >= 20:  # cap auto list at 20 extra wallets
            break

    with _whale_discovery_lock:
        _discovered_whales = new_dict

    total = len(BTC_WHALE_WALLETS) + len(new_dict)
    log.info(f"[WHALE] Discovery complete — {added} new wallets found | total tracking: {total}")

def whale_flow_loop():
    log.info(f"[WHALE] Started — {len(BTC_WHALE_WALLETS)} seed wallets | ${CFG['whale_min_usd']:,} threshold | auto-discovery ON")
    _discover_whales()  # run immediately on start
    while True:
        try:
            all_w = _all_whales()
            _scan_whale_flow_with(all_w)
            # Re-discover every 6h in background
            if time.time() - _last_discovery > 21600:
                threading.Thread(target=_discover_whales, daemon=True).start()
        except Exception as e:
            log.error(f"[WHALE] {e}")
        time.sleep(CFG["whale_scan_interval"])

# ── CONFLUENCE SCORER ─────────────────────────────────────────────────────────
_WEIGHTS = {
    "whale_consensus": 0.32, "price_delta": 0.24, "chainlink_lag": 0.18,
    "ob_imbalance": 0.14,    "exchange_agree": 0.08, "clob_spread": 0.04,
}

def confluence_score(cid, token_id, direction, bn_delta, ref_price,
                     imbalance, cl_lag, exchange_agree, book) -> float:
    score = 0.0
    w_dir, w_sc, w_cnt, _ = whale_consensus(cid)
    if w_dir == direction and w_cnt > 0:
        score += _WEIGHTS["whale_consensus"] * min(w_sc, 10)
    elif w_dir and w_dir != direction:
        score -= _WEIGHTS["whale_consensus"] * 3
    if ref_price > 0 and bn_delta != 0:
        aligned = (bn_delta > 0 and direction == "UP") or (bn_delta < 0 and direction == "DOWN")
        if aligned:
            score += _WEIGHTS["price_delta"] * 10 * min(abs(bn_delta) / ref_price / 0.005, 1.0)
    if 5 <= cl_lag < 60:
        score += _WEIGHTS["chainlink_lag"] * 10 * min(cl_lag / 15.0, 1.0)
    if direction == "UP" and imbalance >= 1.8:
        score += _WEIGHTS["ob_imbalance"] * 10 * min((imbalance - 1.8) / 3.2 + 0.5, 1.0)
    elif direction == "DOWN" and imbalance <= 0.55:
        score += _WEIGHTS["ob_imbalance"] * 10 * min((0.55 - imbalance) / 0.45 + 0.5, 1.0)
    if exchange_agree >= 2:
        score += _WEIGHTS["exchange_agree"] * 10 * min(exchange_agree / 3.0, 1.0)
    bids = book.get("bids",[]); asks = book.get("asks",[])
    if bids and asks:
        try:
            spread = min(float(a["price"]) for a in asks[:3]) - max(float(b["price"]) for b in bids[:3])
            if spread < 0.05:
                score += _WEIGHTS["clob_spread"] * 10
        except Exception:
            pass
    return round(max(0.0, score), 2)

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 1 — BTC UPDN MULTI-TIMEFRAME (5 / 10 / 15 / 20 / 25 / 30 / 35 / 40m)
# ═══════════════════════════════════════════════════════════════════════════════
_btc_market_cache: dict = {}
_btc_cache_ts:     float = 0.0
_btc_refresh_lock         = threading.Lock()

def _refresh_btc_cache():
    global _btc_market_cache, _btc_cache_ts
    now   = datetime.now(timezone.utc)
    cache = {}
    for tf in CFG["btc_timeframes"]:
        base  = now.replace(minute=(now.minute // tf) * tf, second=0, microsecond=0)
        slots = range(-2, max(4, 60 // max(tf, 1) + 2))
        for asset in ["btc", "eth"]:
            for window in [base + timedelta(minutes=i * tf) for i in slots]:
                ts   = int(window.timestamp())
                slug = f"{asset}-updown-{tf}m-{ts}"
                d    = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}")
                if not d:
                    continue
                for m in (d if isinstance(d, list) else d.get("markets",[])):
                    cid     = m.get("conditionId") or m.get("id") or ""
                    end_str = m.get("endDate","")
                    end_dt  = datetime.fromisoformat(end_str.replace("Z","+00:00")) if end_str else None
                    if cid:
                        cache[cid] = {
                            "market": m, "end_dt": end_dt, "tf": tf, "asset": asset,
                            "prices": parse_prices(m), "outcomes": parse_outcomes(m),
                            "toks": parse_tokens(m),
                        }
    if cache:
        tf_counts = {}
        for v in cache.values():
            tf_counts[v["tf"]] = tf_counts.get(v["tf"], 0) + 1
        summary = " ".join(f"{k}m×{v}" for k, v in sorted(tf_counts.items()))
        log.info(f"[E1/CRYPTO] Cache: {len(cache)} markets — {summary}")
        _btc_market_cache = cache
    _btc_cache_ts = time.time()

def btc_updn_scan():
    now = datetime.now(timezone.utc)
    for cid, entry in list(_btc_market_cache.items()):
        try:
            if already_traded(f"btc:{cid}"):
                continue
            end_dt = entry.get("end_dt")
            if not end_dt:
                continue
            tf          = entry["tf"]
            mins_left   = (end_dt - now).total_seconds() / 60
            lo, hi      = CFG["btc_entry_frac"]
            if not (tf * lo <= mins_left <= tf * hi):
                continue
            if mins_left < 1.0:
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

            # Price signals
            interval  = f"{tf}m" if tf <= 30 else "30m"
            ref_price = btc_candle_open(interval)
            bn        = get_btc_price()
            cl, cl_lag = chainlink_btc()
            if bn <= 0 or ref_price <= 0:
                continue

            threshold    = max(5.0, ref_price * 0.0005)
            bn_delta     = bn - ref_price
            exc_up       = int(bn_delta > threshold) + multi_exchange_agree(bn_delta, ref_price, threshold)
            exc_dn       = int(bn_delta < -threshold) + multi_exchange_agree(bn_delta, ref_price, -threshold)

            if exc_up >= 2:
                direction, exchange_agree = "UP", exc_up
            elif exc_dn >= 2:
                direction, exchange_agree = "DOWN", exc_dn
            else:
                w_dir, w_sc, w_cnt, _ = whale_consensus(cid)
                if w_dir and w_sc >= 8.0 and w_cnt >= 2:
                    direction, exchange_agree = w_dir, 0
                    bn_delta = 0.0
                else:
                    continue

            bet_i    = up_i if direction == "UP" else down_i
            token_id = toks[bet_i]
            book      = clob_book(token_id)
            imbalance = calculate_imbalance(book) if book else 1.0
            conf      = confluence_score(cid, token_id, direction, bn_delta, ref_price,
                                         imbalance, cl_lag, exchange_agree, book or {})
            if conf < CFG["btc_confluence_min"]:
                continue

            live_price = clob_mid(token_id) or float(entry["prices"][bet_i] if len(entry["prices"]) > bet_i else 0.5)
            if not (CFG["updn_min_price"] <= live_price <= CFG["updn_max_price"]):
                continue

            p_win    = min(0.93, 0.55 + (conf - CFG["btc_confluence_min"]) / 10)
            bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
            size_usd = kelly_size(p_win, live_price, bankroll)
            q        = (entry["market"].get("question") or "")[:60]

            log.info(
                f"[E1/{tf}m] BTC {direction} | Bn={bn_delta:+.0f} OB={imbalance:.2f} "
                f"CL={cl_lag:.0f}s Conf={conf:.1f}/10 ${size_usd:.2f}@{live_price:.2f}"
            )
            tg(f"E1/{tf}m BTC {direction} Conf={conf:.1f}/10 ${size_usd:.2f}@{live_price:.0%}", "E1")

            if place_order(token_id, live_price, size_usd, q, f"E1/{tf}m", p_win=p_win):
                mark_traded(f"btc:{cid}")
        except Exception as e:
            import traceback; log.error(f"[E1] {e}\n{traceback.format_exc()}")

def btc_updn_loop():
    log.info(f"[E1/BTC] Started — timeframes: {CFG['btc_timeframes']}m")
    _refresh_btc_cache()
    while True:
        try:
            if time.time() - _btc_cache_ts > CFG["updn_refresh_sec"]:
                if not _btc_refresh_lock.locked():
                    threading.Thread(target=lambda: _btc_refresh_lock.locked() or
                                     (_btc_refresh_lock.acquire() or _refresh_btc_cache() or _btc_refresh_lock.release()),
                                     daemon=True).start()
            btc_updn_scan()
        except Exception as e:
            log.error(f"[E1] loop: {e}")
        time.sleep(CFG["updn_scan_sleep_sec"])

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 2 — PAIRS ARBITRAGE  (YES + NO < threshold → buy both legs)
# ═══════════════════════════════════════════════════════════════════════════════
def _scan_arb_batch(markets: list):
    for m in markets:
        try:
            cid = m.get("conditionId") or m.get("id") or ""
            if not cid or already_traded(f"arb:{cid}"):
                continue
            if m.get("closed") or not m.get("active", True):
                continue
            toks     = parse_tokens(m)
            outcomes = parse_outcomes(m)
            if len(toks) < 2:
                continue

            # Find YES and NO token indices
            yes_i = no_i = -1
            for i, o in enumerate(outcomes):
                ol = str(o).lower()
                if ol in ("yes","up","true","1"):
                    yes_i = i
                elif ol in ("no","down","false","0"):
                    no_i = i
            if yes_i == -1:
                yes_i, no_i = 0, 1
            if no_i == -1:
                no_i = 1 - yes_i

            yes_mid = clob_mid(toks[yes_i])
            no_mid  = clob_mid(toks[no_i])
            if yes_mid <= 0 or no_mid <= 0:
                continue

            total = yes_mid + no_mid
            edge  = 1.0 - total
            if total > CFG["arb_threshold"] or edge < CFG["arb_min_profit"]:
                continue

            # Check liquidity
            yes_book = clob_book(toks[yes_i])
            no_book  = clob_book(toks[no_i])
            if book_depth(yes_book) < 50 or book_depth(no_book) < 50:
                continue

            bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
            size_usd = max(CFG["stake_min_usd"], bankroll * 0.02)
            q        = (m.get("question") or "")[:60]

            log.info(f"[E2/ARB] YES@{yes_mid:.3f}+NO@{no_mid:.3f}={total:.3f} edge={edge:.3f} | {q}")
            tg(f"E2/ARB YES@{yes_mid:.3f}+NO@{no_mid:.3f} edge=${edge:.3f} | {q[:45]}", "ARB")

            if place_arb_pair(toks[yes_i], toks[no_i], yes_mid, no_mid, size_usd, q):
                mark_traded(f"arb:{cid}")
        except Exception as e:
            log.debug(f"[E2/ARB] {e}")

def pairs_arb_loop():
    log.info(f"[E2/ARB] Started — threshold={CFG['arb_threshold']} min_edge={CFG['arb_min_profit']}")
    offset = 0
    while True:
        try:
            markets = fetch(
                f"https://gamma-api.polymarket.com/markets?"
                f"active=true&closed=false&limit={CFG['arb_market_limit']}&offset={offset}"
            )
            if isinstance(markets, dict):
                markets = markets.get("markets", [])
            if markets:
                _scan_arb_batch(markets)
                offset = (offset + CFG["arb_market_limit"]) % 5000
        except Exception as e:
            log.error(f"[E2/ARB] {e}")
        time.sleep(CFG["arb_scan_sleep"])

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 3 — SENTIMENT AI  (RSS / news → keyword match → early entry)
# ═══════════════════════════════════════════════════════════════════════════════
_RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://rss.reuters.com/reuters/topNews",
    "https://feeds.skynews.com/feeds/rss/home.xml",
    "https://www.espn.com/espn/rss/news",
    "https://sports.yahoo.com/rss/",
]

_BULLISH_WORDS = {"win","beat","surge","jump","rally","record","high","gain","rise","above","leads"}
_BEARISH_WORDS = {"lose","drop","fall","crash","below","miss","decline","cut","halt","suspend"}


_sent_markets: list = []
_sent_mkt_ts: float = 0.0

def _fetch_rss_headlines() -> list:
    headlines = []
    for url in _RSS_FEEDS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
            with _direct.open(req, timeout=8) as r:
                root = ET.fromstring(r.read())
            for item in root.iter("item"):
                title = (item.findtext("title") or "").strip()
                desc  = (item.findtext("description") or "").strip()
                if title:
                    headlines.append(f"{title} {desc}"[:200])
        except Exception as e:
            log.debug(f"[E3/RSS] {url[:50]}: {e}")
    return headlines

def _match_headline_to_market(headline: str, question: str) -> tuple:
    """Returns (direction, confidence) or (None, 0)."""
    hl = headline.lower()
    q  = question.lower()

    # Extract key nouns from market question (teams, assets, people)
    q_words = set(re.findall(r"\b[a-z]{3,}\b", q)) - {
        "will","the","who","what","when","does","that","this","with","for","from",
        "have","has","are","was","were","is","be","been","by","at","an","a","to","in","on","of"
    }
    matched = q_words & set(re.findall(r"\b[a-z]+\b", hl))
    if len(matched) < 2:
        return None, 0.0

    bull = len([w for w in _BULLISH_WORDS if w in hl])
    bear = len([w for w in _BEARISH_WORDS if w in hl])

    if bull > bear and bull >= 1:
        direction = "YES" if re.search(r"\bwill\b.*\b(win|beat|be above|exceed|reach)", q) else "UP"
        conf = min(0.55 + bull * 0.05, 0.75)
        return direction, conf
    elif bear > bull and bear >= 1:
        direction = "NO" if re.search(r"\bwill\b.*\b(win|beat|be above|exceed|reach)", q) else "DOWN"
        conf = min(0.55 + bear * 0.05, 0.75)
        return direction, conf
    return None, 0.0

def sentiment_loop():
    global _sent_markets, _sent_mkt_ts
    log.info("[E3/SENT] Started — RSS news scanner")
    while True:
        try:
            # Refresh active markets list every 5 minutes
            if time.time() - _sent_mkt_ts > 300:
                raw = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=100") or []
                _sent_markets = raw if isinstance(raw, list) else raw.get("markets",[])
                _sent_mkt_ts  = time.time()

            headlines = _fetch_rss_headlines()
            log.debug(f"[E3/SENT] {len(headlines)} headlines fetched")

            for m in _sent_markets:
                cid = m.get("conditionId") or m.get("id") or ""
                if not cid or already_traded(f"sent:{cid}"):
                    continue
                q = m.get("question","")
                if not q:
                    continue

                toks     = parse_tokens(m)
                outcomes = parse_outcomes(m)
                if len(toks) < 2:
                    continue

                for hl in headlines:
                    direction, conf = _match_headline_to_market(hl, q)
                    if not direction or conf < CFG["sent_min_conf"]:
                        continue

                    # Pick token for direction
                    out_lower = [str(o).lower() for o in outcomes]
                    tok_i = 0
                    if direction.lower() in out_lower:
                        tok_i = out_lower.index(direction.lower())
                    elif "yes" in out_lower and direction in ("YES","UP"):
                        tok_i = out_lower.index("yes")
                    elif "no" in out_lower and direction in ("NO","DOWN"):
                        tok_i = out_lower.index("no")

                    token_id   = toks[tok_i]
                    if token_id in _bad_tokens:
                        break

                    live_price = clob_mid(token_id)
                    if not live_price or live_price < 0.15 or live_price > 0.88:
                        continue

                    bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
                    size_usd = kelly_size(conf, live_price, bankroll)

                    log.info(f"[E3/SENT] {direction} conf={conf:.2f} ${size_usd:.2f} | {q[:55]}")
                    tg(f"E3/SENT {direction} conf={conf:.0%} ${size_usd:.2f} | {q[:50]}", "SENT")

                    place_order(token_id, live_price, size_usd, q, "E3/SENT", p_win=conf)
                    mark_traded(f"sent:{cid}")
                    break
        except Exception as e:
            log.error(f"[E3/SENT] {e}")
        time.sleep(CFG["sent_scan_sleep"])

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 4 — VOLATILITY TIMING  (50/50 markets before resolution events)
# ═══════════════════════════════════════════════════════════════════════════════
def volatility_loop():
    log.info("[E4/VOL] Started — 50/50 markets before resolution events")
    while True:
        try:
            markets = fetch("https://gamma-api.polymarket.com/markets?active=true&limit=150") or []
            if isinstance(markets, dict):
                markets = markets.get("markets",[])
            now = datetime.now(timezone.utc)

            for m in markets:
                try:
                    cid = m.get("conditionId") or m.get("id") or ""
                    if not cid or already_traded(f"vol:{cid}"):
                        continue
                    toks     = parse_tokens(m)
                    prices   = parse_prices(m)
                    outcomes = parse_outcomes(m)
                    if len(toks) < 2 or len(prices) < 2:
                        continue

                    # Market must be near 50/50
                    lo, hi = CFG["vol_mid_range"]
                    if not (lo <= prices[0] <= hi):
                        continue

                    # Resolution must be within target window
                    end_str = m.get("endDate","")
                    if not end_str:
                        continue
                    end_dt    = datetime.fromisoformat(end_str.replace("Z","+00:00"))
                    hrs_left  = (end_dt - now).total_seconds() / 3600
                    vol_lo, vol_hi = CFG["vol_event_hours"]
                    if not (vol_lo <= hrs_left <= vol_hi):
                        continue

                    # Prefer markets with some liquidity
                    book0 = clob_book(toks[0])
                    depth = book_depth(book0)
                    if depth < 100:
                        continue

                    # Pick the side with slight OB advantage
                    imb = calculate_imbalance(book0)
                    if imb >= 1.5:
                        tok_i, p_win = 0, 0.55
                    elif imb <= 0.67:
                        tok_i, p_win = 1, 0.55
                    else:
                        continue

                    token_id   = toks[tok_i]
                    if token_id in _bad_tokens:
                        continue
                    live_price = clob_mid(token_id) or prices[tok_i]
                    if not (0.35 <= live_price <= 0.65):
                        continue

                    bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
                    size_usd = kelly_size(p_win, live_price, bankroll)
                    q        = (m.get("question",""))[:60]

                    log.info(f"[E4/VOL] {outcomes[tok_i]} OB={imb:.2f} {hrs_left:.1f}h | {q}")
                    tg(f"E4/VOL {outcomes[tok_i]} OB={imb:.2f} {hrs_left:.1f}h left | {q[:45]}", "VOL")

                    if place_order(token_id, live_price, size_usd, q, "E4/VOL", p_win=p_win):
                        mark_traded(f"vol:{cid}")
                    else:
                        mark_traded(f"vol:{cid}")
                except Exception:
                    pass
        except Exception as e:
            log.error(f"[E4/VOL] {e}")
        time.sleep(CFG["vol_scan_sleep"])

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 5 — SPORTS MARKETS  (slow-repricing NFL/NBA/Soccer/MLB)
# ═══════════════════════════════════════════════════════════════════════════════
_SPORTS_KEYWORDS = re.compile(
    r"\b(nfl|nba|mlb|nhl|soccer|football|basketball|baseball|tennis|golf|"
    r"championship|league|cup|tournament|series|super bowl|playoff|finals|match|game)\b",
    re.I
)

def sports_loop():
    log.info(f"[E5/SPORTS] Started — tags: {', '.join(CFG['sports_tags'])}")
    while True:
        try:
            all_markets = []
            for tag in CFG["sports_tags"]:
                raw = fetch(f"https://gamma-api.polymarket.com/markets?tag_slug={tag}&active=true&limit=50")
                if isinstance(raw, list):
                    all_markets.extend(raw)
                elif isinstance(raw, dict):
                    all_markets.extend(raw.get("markets",[]))
                time.sleep(0.1)

            # Deduplicate
            seen = set()
            markets = []
            for m in all_markets:
                cid = m.get("conditionId","")
                if cid and cid not in seen:
                    seen.add(cid); markets.append(m)

            now = datetime.now(timezone.utc)
            for m in markets:
                try:
                    cid = m.get("conditionId") or m.get("id") or ""
                    if not cid or already_traded(f"sport:{cid}"):
                        continue
                    q = m.get("question","")
                    if not _SPORTS_KEYWORDS.search(q):
                        continue

                    toks   = parse_tokens(m)
                    prices = parse_prices(m)
                    if len(toks) < 2 or len(prices) < 2:
                        continue

                    # Skip resolved / near-resolved markets
                    end_str = m.get("endDate","")
                    if end_str:
                        end_dt   = datetime.fromisoformat(end_str.replace("Z","+00:00"))
                        hrs_left = (end_dt - now).total_seconds() / 3600
                        if hrs_left < 0.5 or hrs_left > 72:
                            continue

                    # Find meaningful mispricing: any outcome priced 0.30–0.70
                    best_i, best_edge = -1, 0.0
                    for i, price in enumerate(prices[:2]):
                        if 0.30 <= price <= 0.70:
                            book  = clob_book(toks[i])
                            depth = book_depth(book)
                            imb   = calculate_imbalance(book)
                            # Edge: OB skewed AND price hasn't moved yet
                            edge  = abs(imb - 1.0) * (1 - abs(price - 0.5) * 2)
                            if depth >= CFG["sports_min_liq"] and edge > best_edge:
                                best_i, best_edge = i, edge

                    if best_i < 0 or best_edge < 0.15:
                        continue

                    token_id   = toks[best_i]
                    if token_id in _bad_tokens:
                        continue
                    live_price = clob_mid(token_id) or prices[best_i]
                    if not (0.30 <= live_price <= 0.75):
                        continue

                    book  = clob_book(token_id)
                    imb   = calculate_imbalance(book)
                    p_win = min(0.65, 0.50 + best_edge * 0.2)

                    bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
                    size_usd = kelly_size(p_win, live_price, bankroll)

                    log.info(f"[E5/SPORT] edge={best_edge:.3f} OB={imb:.2f} ${size_usd:.2f}@{live_price:.2f} | {q[:55]}")
                    tg(f"E5/SPORT edge={best_edge:.3f} OB={imb:.2f} ${size_usd:.2f} | {q[:50]}", "SPORT")

                    if place_order(token_id, live_price, size_usd, q, "E5/SPORT", p_win=p_win):
                        mark_traded(f"sport:{cid}")
                    else:
                        mark_traded(f"sport:{cid}")
                except Exception:
                    pass
        except Exception as e:
            log.error(f"[E5/SPORTS] {e}")
        time.sleep(CFG["sports_scan_sleep"])

# ═══════════════════════════════════════════════════════════════════════════════
# POSITION EXIT MANAGER
# ═══════════════════════════════════════════════════════════════════════════════
def check_profit_exits():
    now = time.time()
    for token_id, pos in list(open_positions.items()):
        try:
            entry      = float(pos.get("entry", 0))
            if entry <= 0:
                continue
            mid        = clob_mid(token_id)
            if mid <= 0:
                continue
            profit_pct = (mid - entry) / entry
            mins_held  = (now - float(pos.get("entry_time", now))) / 60
            mkt        = pos.get("market", token_id[:20])

            if profit_pct <= -0.50:
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "STOP")
            elif mid >= 0.75:
                sell_position(token_id, round(max(0.70, mid - 0.02), 3), mkt, "PROFIT")
            elif mid >= 0.95:
                sell_position(token_id, round(max(0.90, mid - 0.02), 3), mkt, "WIN")
            elif mid <= 0.30:
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "STOP_30")
            elif mins_held > 60:
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "EXPIRED")
        except Exception:
            pass

# ── ON-CHAIN REDEMPTION ───────────────────────────────────────────────────────
_CT_ADDR   = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_USDC_POLY = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_ZERO_ADDR = "0x0000000000000000000000000000000000000000"
_CT_ABI    = [{"name":"redeemPositions","type":"function","outputs":[],"inputs":[
    {"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},
    {"name":"conditionId","type":"bytes32"},{"name":"indexSets","type":"uint256[]"}]}]
_SAFE_ABI  = [
    {"name":"nonce","type":"function","inputs":[],"outputs":[{"type":"uint256"}]},
    {"name":"getTransactionHash","type":"function","outputs":[{"type":"bytes32"}],
     "inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},
               {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
               {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
               {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
               {"name":"refundReceiver","type":"address"},{"name":"_nonce","type":"uint256"}]},
    {"name":"execTransaction","type":"function","outputs":[{"type":"bool"}],
     "inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},
               {"name":"data","type":"bytes"},{"name":"operation","type":"uint8"},
               {"name":"safeTxGas","type":"uint256"},{"name":"baseGas","type":"uint256"},
               {"name":"gasPrice","type":"uint256"},{"name":"gasToken","type":"address"},
               {"name":"refundReceiver","type":"address"},{"name":"signatures","type":"bytes"}]},
]
_redeemed: set = set()

def redeem_loop():
    if not _WEB3_OK:
        return
    try:
        w3   = Web3(Web3.HTTPProvider("https://polygon-bor-rpc.publicnode.com", request_kwargs={"timeout":15}))
        acct = w3.eth.account.from_key("0x" + PRIVATE_KEY)
    except Exception as e:
        log.error(f"[REDEEM] {e}"); return
    log.info(f"[REDEEM] EOA={acct.address[:16]}...")
    while True:
        try:
            positions = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.01")
            if isinstance(positions, list):
                for pos in positions:
                    cid = pos.get("conditionId","")
                    if not bool(pos.get("redeemable")) or not cid or cid in _redeemed:
                        continue
                    size = float(pos.get("size",0) or 0)
                    if size < 0.1:
                        continue
                    try:
                        fcs    = Web3.to_checksum_address(FUNDER)
                        ct     = w3.eth.contract(address=Web3.to_checksum_address(_CT_ADDR), abi=_CT_ABI)
                        sf     = w3.eth.contract(address=fcs, abi=_SAFE_ABI)
                        cid_b  = bytes.fromhex(cid.replace("0x",""))
                        idx    = [1] if pos.get("outcome","").lower() in ("yes","up") else [2]
                        call_d = ct.encode_abi("redeemPositions",
                                               [Web3.to_checksum_address(_USDC_POLY),
                                                b"\x00"*32, cid_b, idx])
                        n      = sf.functions.nonce().call()
                        txh    = sf.functions.getTransactionHash(
                            Web3.to_checksum_address(_CT_ADDR),0,call_d,0,0,0,0,
                            _ZERO_ADDR,_ZERO_ADDR,n).call()
                        sig    = acct.sign_message(encode_defunct(txh))
                        sigb   = sig.r.to_bytes(32,"big")+sig.s.to_bytes(32,"big")+bytes([sig.v+4])
                        gp     = w3.eth.gas_price
                        if w3.eth.get_balance(acct.address) < gp * 200_000:
                            continue
                        tx     = sf.functions.execTransaction(
                            Web3.to_checksum_address(_CT_ADDR),0,call_d,0,0,0,0,
                            _ZERO_ADDR,_ZERO_ADDR,sigb
                        ).build_transaction({"from":acct.address,
                                             "nonce":w3.eth.get_transaction_count(acct.address),
                                             "gas":250_000,"gasPrice":gp,"chainId":137})
                        rx = w3.eth.wait_for_transaction_receipt(
                            w3.eth.send_raw_transaction(acct.sign_transaction(tx).raw_transaction),
                            timeout=60)
                        if rx.status == 1:
                            _redeemed.add(cid)
                            tg(f"REDEEMED {size:.2f}sh | {pos.get('title','?')[:40]}", "INFO")
                    except Exception as e:
                        log.error(f"[REDEEM] {cid[:16]}: {e}")
        except Exception as e:
            log.error(f"[REDEEM] {e}")
        time.sleep(300)

# ── STATUS LOOP ───────────────────────────────────────────────────────────────
_equity_history: list = []
_last_day             = datetime.now(timezone.utc).date()

def status_loop():
    global _equity_history, _last_day, _cached_portfolio_val, _cached_free_usdc
    while True:
        time.sleep(CFG["status_sleep_sec"])
        today = datetime.now(timezone.utc).date()
        if today != _last_day:
            _last_day = today
            reset_daily()

        val  = portfolio_val()
        usdc = free_usdc()
        if usdc > 0:
            _cached_free_usdc     = usdc
        if val > 0:
            _cached_portfolio_val = val
        if val > 0 and _day_start_val is None:
            init_baseline()

        check_profit_exits()

        day_pct     = round((val - _day_start_val) / _day_start_val * 100, 2) if val and _day_start_val else 0.0
        session_pnl = round(val - _session_start, 4) if val and _session_start else 0.0

        btc_ws, btc_age = ws_price("BTC")
        with _whale_lock:
            active_signals = sum(len(v) for v in _whale_flow.values())

        log.info(
            f"[STATUS] ${val:.2f} day={day_pct:+.1f}% session={session_pnl:+.2f} "
            f"pos={len(open_positions)} whale={active_signals} btc=${btc_ws:,.0f}({btc_age:.1f}s)"
        )

        if val and val > 1.0:
            ts = datetime.now(timezone.utc).strftime("%H:%M")
            _equity_history.append([ts, round(val, 4)])
        if len(_equity_history) > 120:
            _equity_history = _equity_history[-120:]

        try:
            pos_details = []
            for tid, p in open_positions.items():
                mid   = clob_mid(tid)
                entry = float(p.get("entry", 0))
                pnl   = round((mid - entry) / max(entry,0.001) * 100, 1) if mid > 0 and entry > 0 else 0
                pos_details.append({
                    "token_id": tid[:20], "market": (p.get("market",""))[:45],
                    "source": p.get("source","?"), "entry": round(entry,3),
                    "current": round(mid,3), "pnl_pct": pnl,
                    "size": round(float(p.get("size",0)),2),
                })
            with _whale_lock:
                whale_snap = {
                    cid: [{"name":r["name"],"dir":r["direction"],"usd":r["usd"]} for r in recs]
                    for cid, recs in _whale_flow.items()
                }
            with _whale_discovery_lock:
                disc_count = len(_discovered_whales)
            STATUS_FILE.write_text(json.dumps({
                "updated":        datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "balance":        round(val or 0, 4),
                "day_pnl":        day_pct,
                "session_pnl":    session_pnl,
                "trades":         len(trade_log),
                "positions":      len(open_positions),
                "open_positions": pos_details,
                "halted":         _bot_halted,
                "halt_reason":    _halt_reason,
                "wallet":         FUNDER,
                "mode":           "DRY RUN" if CFG["dry_run"] else "LIVE",
                "btc_ws_price":   btc_ws,
                "btc_ws_age":     round(btc_age, 1),
                "whale_signals":  active_signals,
                "whale_tracked":  len(BTC_WHALE_WALLETS) + disc_count,
                "whale_detail":   whale_snap,
                "equity_history": _equity_history[-30:],
                "engines": {
                    "E0_ALL_SWEEP": {"status": "ACTIVE", "mode": "all-markets OB+arb"},
                    "E1_BTC_UPDN":  {"status": "ACTIVE", "timeframes": CFG["btc_timeframes"]},
                    "E2_PAIRS_ARB": {"status": "ACTIVE", "threshold": CFG["arb_threshold"]},
                    "E3_SENTIMENT": {"status": "ACTIVE", "feeds": len(_RSS_FEEDS)},
                    "E4_VOLATILITY":{"status": "ACTIVE"},
                    "E5_SPORTS":    {"status": "ACTIVE", "tags": CFG["sports_tags"]},
                },
            }, indent=2))
        except Exception:
            pass

# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 0 — ALL-MARKET OB SWEEP  (exploit every active market with edge)
# ═══════════════════════════════════════════════════════════════════════════════
_e0_offset = 0

def all_market_sweep_loop():
    """
    Continuously sweeps ALL active Polymarket markets.
    Fires on any of three conditions:
      a) OB imbalance > 1.6 or < 0.625  (strong directional pressure)
      b) YES+NO sum < 0.995             (pairs arb edge)
      c) Single outcome priced 0.05-0.30 near resolution (longshot value)
    """
    global _e0_offset
    log.info("[E0/SWEEP] Started — scanning all active markets")
    while True:
        try:
            raw = fetch(
                f"https://gamma-api.polymarket.com/markets?"
                f"active=true&closed=false&limit=100&offset={_e0_offset}"
            )
            markets = raw if isinstance(raw, list) else (raw or {}).get("markets", [])
            if not markets:
                _e0_offset = 0
                time.sleep(5)
                continue

            _e0_offset += 100

            for m in markets:
                try:
                    cid = m.get("conditionId") or m.get("id") or ""
                    if not cid or already_traded(f"e0:{cid}"):
                        continue

                    toks   = parse_tokens(m)
                    prices = parse_prices(m)
                    if len(toks) < 2 or len(prices) < 2:
                        continue

                    q = (m.get("question") or "")[:65]

                    # ── Signal A: pairs arb ───────────────────────────────────
                    if prices[0] + prices[1] < 0.995:
                        yes_mid = clob_mid(toks[0])
                        no_mid  = clob_mid(toks[1])
                        if yes_mid > 0 and no_mid > 0:
                            total = yes_mid + no_mid
                            if total < 0.995:
                                bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
                                size_usd = max(CFG["stake_min_usd"], bankroll * 0.05)
                                tg(f"E0/ARB {total:.3f} edge={1-total:.3f} | {q[:50]}", "ARB")
                                if place_arb_pair(toks[0], toks[1], yes_mid, no_mid, size_usd, q):
                                    mark_traded(f"e0:{cid}")
                                continue

                    # ── Signal B: OB imbalance momentum ──────────────────────
                    best_i = -1
                    for i in range(min(2, len(toks))):
                        if toks[i] in _bad_tokens:
                            continue
                        live = clob_mid(toks[i])
                        if not live or live < 0.05 or live > 0.95:
                            continue
                        book = clob_book(toks[i])
                        if not book:
                            continue
                        imb = calculate_imbalance(book)
                        depth = book_depth(book)
                        if depth < 20:
                            continue
                        if imb > 1.6 or imb < 0.625:
                            best_i     = i
                            best_imb   = imb
                            best_price = live
                            break

                    if best_i >= 0:
                        p_win    = 0.58 if best_imb > 1.6 else 0.42
                        bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
                        size_usd = kelly_size(p_win, best_price, bankroll)
                        tg(f"E0/OB imb={best_imb:.2f} ${size_usd:.2f}@{best_price:.2f} | {q[:50]}", "E0")
                        if place_order(toks[best_i], best_price, size_usd, q, "E0/OB", p_win=p_win):
                            mark_traded(f"e0:{cid}")

                except Exception:
                    pass

        except Exception as e:
            log.error(f"[E0/SWEEP] {e}")
            _e0_offset = 0
        time.sleep(0.5)  # fast sweep — 0.5s per batch

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  POLY//MULTI-ENGINE")
    log.info(f"  Wallet  : {FUNDER}")
    log.info(f"  Engines : E1=BTC({CFG['btc_timeframes']}m) E2=PairsArb E3=Sentiment E4=Vol E5=Sports")
    log.info(f"  Whales  : {len(BTC_WHALE_WALLETS)} seed + auto-discovery (target ≥20)")
    log.info(f"  Kalshi  : {'enabled' if KALSHI_EMAIL or KALSHI_API_KEY else 'disabled (no creds)'}")
    log.info(f"  Mode    : {'DRY RUN' if CFG['dry_run'] else 'LIVE'}")
    log.info(f"  Stake   : {CFG['stake_pct']*100:.1f}% / trade  floor=${CFG['stake_min_usd']:.2f}")
    log.info("=" * 60)

    global _traded
    _traded = _load_traded()

    if KALSHI_EMAIL and KALSHI_PASSWORD:
        kalshi_login()

    _start_binance_ws()

    threads = [
        threading.Thread(target=whale_flow_loop,        daemon=True, name="whale"),
        threading.Thread(target=all_market_sweep_loop,  daemon=True, name="E0-sweep"),
        threading.Thread(target=btc_updn_loop,          daemon=True, name="E1-btc"),
        threading.Thread(target=pairs_arb_loop,         daemon=True, name="E2-arb"),
        threading.Thread(target=sentiment_loop,         daemon=True, name="E3-sent"),
        threading.Thread(target=volatility_loop,        daemon=True, name="E4-vol"),
        threading.Thread(target=sports_loop,            daemon=True, name="E5-sports"),
        threading.Thread(target=status_loop,            daemon=True, name="status"),
        threading.Thread(target=redeem_loop,            daemon=True, name="redeem"),
    ]
    for t in threads:
        t.start()
    log.info(f"[MAIN] {len(threads)} threads running")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("[MAIN] Shutting down")

if __name__ == "__main__":
    main()
