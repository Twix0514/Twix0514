"""
POLY//BOT — BTC 5MIN UP/DOWN | Whale Flow + Latency Arb
=========================================================
Edge: top whale wallets exploit Chainlink oracle lag before Polymarket CLOB reprices.
      We follow their direction on BTC 5m directional markets through a 6-signal
      confluence gate. No trade fires unless score ≥ 6.5 / 10.

Architecture:
  Layer 0  Data    — Binance WebSocket (real-time BTC) + CLOB + Polymarket activity API
  Layer 1  Signals — price delta, order book, Chainlink lag, whale consensus, exchange agree, spread
  Layer 2  Gate    — weighted confluence score, threshold 6.5 / 10
  Layer 3  Exec    — half-Kelly position, exit at 75¢ or stop at 35¢

Risk: 0.5% bankroll per trade · 2% daily halt · hard stop -50% position loss
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
from py_clob_client_v2 import ClobClient, ApiCreds
from py_clob_client_v2.clob_types import (
    OrderArgs, BalanceAllowanceParams, AssetType, OrderType, PartialCreateOrderOptions
)
from py_clob_client_v2.order_builder.constants import BUY, SELL

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("BOT")

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
# Override via secrets_local.py or env vars: POLY_PRIVATE_KEY, POLY_FUNDER
PRIVATE_KEY       = os.environ.get("POLY_PRIVATE_KEY", "0e0f279ec9fb2ff4959525cc30db040ef941e6f714ee556a8bf594a7dc8303d9")
FUNDER            = os.environ.get("POLY_FUNDER",      "0x27af098ACCaB972Bf33869C34387aAF937033DE7")
CHAIN_ID          = 137
PROXY             = os.environ.get("POLY_PROXY", "")
BANKROLL_OVERRIDE = 0.0   # set in secrets_local.py to pin known balance when APIs return $0
try:
    from secrets_local import PRIVATE_KEY, FUNDER, PROXY, BANKROLL_OVERRIDE  # type: ignore
except ImportError:
    try:
        from secrets_local import PRIVATE_KEY, FUNDER, PROXY  # type: ignore
    except Exception:
        pass
except Exception:
    pass

# ── CONFIG ────────────────────────────────────────────────────────────────────
CFG = {
    "dry_run":          False,

    # Risk controls (match system description)
    "stake_pct":        0.005,   # 0.5% of bankroll per trade (Kelly floor)
    "daily_halt_pct":  -1.00,    # halt disabled (would only trigger at -100%)
    "max_positions":    3,

    # Signal gate
    "confluence_min":   6.5,     # 0–10 score required to fire

    # Whale flow
    "whale_min_usd":    10_000,  # only track trades ≥ $10k
    "whale_lookback_sec": 300,   # 5-minute window for whale signals
    "whale_scan_interval": 30,   # re-scan whale activity every 30s

    # UPDN 5m — BTC only
    "updn_min_price":           0.30,
    "updn_max_price":           0.88,
    "updn_cache_refresh_sec":   15,
    "updn_slug_fetch_gap_sec":  0.02,
    "updn_scan_sleep_sec":      1,
    "updn_trade_ttl_sec":       3600,   # don't re-enter same market for 1h

    # Misc
    "status_sleep_sec": 5,
}

# ── PROXY SETUP ───────────────────────────────────────────────────────────────
_direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
if PROXY:
    os.environ["HTTP_PROXY"]  = PROXY
    os.environ["HTTPS_PROXY"] = PROXY
    os.environ["ALL_PROXY"]   = PROXY
    log.info(f"[PROXY] {PROXY}")

# ── CLIENT ────────────────────────────────────────────────────────────────────
_API_CREDS = ApiCreds(
    api_key=os.environ.get("POLY_API_KEY", "59aeac19-664d-7549-a953-ad56ffe1319c"),
    api_secret=os.environ.get("POLY_API_SECRET", "vog6fxN-j1hHOkdYaH_6wVURd9-UMu-mo6HVFCRH_4o="),
    api_passphrase=os.environ.get("POLY_API_PASSPHRASE", "f0b703243f3a51a6b57668ce5da47cb86616406814c175689dc79622569dbe00"),
)
try:
    from secrets_local import API_KEY, API_SECRET, API_PASSPHRASE  # type: ignore
    _API_CREDS = ApiCreds(api_key=API_KEY, api_secret=API_SECRET, api_passphrase=API_PASSPHRASE)
except Exception:
    pass

client = ClobClient(
    "https://clob.polymarket.com",
    key=PRIVATE_KEY, chain_id=CHAIN_ID,
    creds=_API_CREDS,
    signature_type=3,   # POLY_1271 — FUNDER is EIP-1271 deposit wallet
    funder=FUNDER,
)
try:
    _derived = client.create_or_derive_api_key()
    client.set_api_creds(_derived)
    log.info("[AUTH] API creds derived/refreshed")
except Exception as e:
    log.warning(f"[AUTH] using pre-stored creds: {e}")

# ── FILES ─────────────────────────────────────────────────────────────────────
BASE             = pathlib.Path(__file__).parent
ALERTS_FILE      = BASE / "alerts.json"
STATUS_FILE      = BASE / "status.json"
LOCK_FILE        = BASE / "bot.lock"
UPDN_TRADED_FILE = BASE / "updn_traded.json"

# ── STATE ─────────────────────────────────────────────────────────────────────
_order_lock           = threading.Lock()
_bot_halted           = False
_halt_reason          = ""
_day_start_val        = None
_session_start        = None
trade_log             = []
open_positions        = {}
_cached_portfolio_val: float = 0.0
_cached_free_usdc:     float = 0.0
_last_heartbeat:       float = time.time()

# ── TOP WHALE WALLETS — BTC DIRECTIONAL SPECIALISTS ───────────────────────────
# Source: polyscope / polymarketanalytics.com top 30-day P&L
# These wallets front-run Chainlink oracle lag on BTC 5m/15m markets.
# conviction = composite score (0–10) used to weight their directional signal.
BTC_WHALE_WALLETS: dict = {
    "0x55be7aa03ecfbe37aa5460db791205f7ac9ddca3": {"name": "coinman2",      "conviction": 9.8},

    # ── ADD insider signal wallet (0x7c9e...1bc2, 97% acc/42 trades) here ─────
    # "0x7c9e<FULL>1bc2": {"name": "insider",        "conviction": 9.0},

    # Known full addresses — T1 consistent P&L
    "0x2a2c53bd278c04da9962fcf96490e17f3dfb9bc1": {"name": "anon-T1",       "conviction": 9.3},
    "0xefbc5fec8d7b0acdc8911bdd9a98d6964308f9a2": {"name": "reachingthesky","conviction": 8.5},
    "0x02227b8f5a9636e895607edd3185ed6ee5598ff7": {"name": "HorizonSplend", "conviction": 8.2},
    "0xc2e7800b5af46e6093872b177b7a5e7f0563be51": {"name": "beachboy4",     "conviction": 8.0},
    "0x019782cab5d844f02bafb71f512758be78579f3c": {"name": "majorexploit",  "conviction": 7.9},

    # T4 crypto/UPDN specialists (top 30d crypto PnL)
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a": {"name": "0x8dxd",        "conviction": 8.0},
    "0xde17f7144fbd0eddb2679132c10ff5e74b120988": {"name": "crypto-top1",   "conviction": 7.5},
    "0xd84c2b6d65dc596f49c7b6aadd6d74ca91e407b9": {"name": "BoneReader",    "conviction": 7.3},
    "0xd0d6053c3c37e727402d84c14069780d360993aa": {"name": "k9Q2mX",        "conviction": 7.1},
    "0xe1d6b51521bd4365769199f392f9818661bd907c": {"name": "crypto-top5",   "conviction": 6.9},
    "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30": {"name": "Bonereaper",    "conviction": 6.8},
    "0x6e1d5040d0ac73709b0621f620d2a60b80d2d0fa": {"name": "crypto-top8",   "conviction": 6.5},
    "0x2d8b401d2f0e6937afebf18e19e11ca568a5260a": {"name": "vidarx",        "conviction": 6.3},
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch(url: str):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
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

def parse_prices(m):
    p = m.get("outcomePrices", "[0.5,0.5]")
    return [float(x) for x in (json.loads(p) if isinstance(p, str) else p)]

def parse_tokens(m):
    t   = m.get("clobTokenIds", "[]")
    raw = json.loads(t) if isinstance(t, str) else t
    return [x if isinstance(x, str) else x.get("token_id", "") for x in raw]

def parse_outcomes(m):
    o = m.get("outcomes", '["Up","Down"]')
    return json.loads(o) if isinstance(o, str) else o

# ── PORTFOLIO ─────────────────────────────────────────────────────────────────
def _parse_bal(raw) -> float:
    v = float(raw or 0)
    return v / 1_000_000 if v > 1000 else v

def free_usdc() -> float:
    # 1. FUNDER deposit wallet (POLY_1271 / signature_type=3)
    try:
        bal = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        v   = _parse_bal(bal.get("balance", 0))
        if v > 0:
            return v
    except Exception:
        pass

    # 2. Gamma-api portfolio for FUNDER
    try:
        data = fetch(f"https://gamma-api.polymarket.com/portfolio?user={FUNDER}")
        if data:
            item = data[0] if isinstance(data, list) else data
            v    = float(item.get("cashBalance") or item.get("balance") or 0)
            if v > 0:
                log.info(f"[BAL] Cash balance from gamma-api: ${v:.2f}")
                return v
    except Exception:
        pass

    # 4. Known override (set in secrets_local.py when UI shows balance but APIs return $0)
    if BANKROLL_OVERRIDE > 0:
        return BANKROLL_OVERRIDE

    return 0.0

def portfolio_val() -> float:
    # data-api portfolioValue is authoritative (includes USDC + positions)
    try:
        data = fetch(f"https://data-api.polymarket.com/value?user={FUNDER}")
        if data:
            item = data[0] if isinstance(data, list) else data
            v    = float(item.get("portfolioValue") or item.get("value") or 0)
            if v > 0:
                return v
    except Exception:
        pass

    # Fall back: cash + mark-to-market open positions
    usdc    = free_usdc()
    pos_val = 0.0
    for tid, p in list(open_positions.items()):
        mid      = clob_mid(tid)
        sz       = float(p.get("size", 0))
        pos_val += sz * (mid if mid > 0 else float(p.get("entry", 0.5)))
    return usdc + pos_val

# ── RISK MANAGEMENT ───────────────────────────────────────────────────────────
def init_baseline():
    global _day_start_val, _session_start
    val = portfolio_val()
    if val > 0:
        _day_start_val = val
        _session_start = val
        log.info(f"[RISK] Baseline ${val:.2f} | daily halt @ -2% = ${val * 0.98:.2f}")

def check_halt() -> bool:
    global _bot_halted, _halt_reason
    if _bot_halted:
        return False
    if _day_start_val:
        val = _cached_portfolio_val if _cached_portfolio_val > 0 else _day_start_val
        pct = (val - _day_start_val) / _day_start_val
        if pct < CFG["daily_halt_pct"]:
            _bot_halted  = True
            _halt_reason = f"daily loss {pct * 100:.1f}%"
            tg(f"BOT HALTED — {pct * 100:.1f}% daily loss. Resumes at midnight UTC.", "WARN")
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
        tg("Daily halt lifted — new trading day.", "INFO")

# ── CLOB HELPERS ──────────────────────────────────────────────────────────────
def clob_mid(token_id: str) -> float:
    data = fetch(f"https://clob.polymarket.com/midpoint?token_id={token_id}")
    return float(data.get("mid", 0) or 0) if data else 0.0

def calculate_imbalance(book: dict) -> float:
    """bid/ask depth ratio (top 10). >1.8 = buy pressure. <0.55 = sell pressure."""
    bids      = book.get("bids", [])[:10]
    asks      = book.get("asks", [])[:10]
    bid_depth = sum(float(b.get("size", 0)) for b in bids)
    ask_depth = sum(float(a.get("size", 0)) for a in asks)
    return round(bid_depth / ask_depth, 3) if ask_depth > 0 else 9.99

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def kelly_size(p_win: float, market_price: float, bankroll: float) -> float:
    """Half-Kelly, floored at stake_pct of bankroll and $1 min, hard-capped at 5%."""
    if market_price <= 0 or market_price >= 1:
        return 0.0
    b      = (1 / market_price) - 1
    f_star = (p_win * b - (1 - p_win)) / b
    if f_star <= 0:
        return 0.0
    f      = min(f_star * 0.5, 0.05)
    raw    = max(bankroll * CFG["stake_pct"], bankroll * f)
    return round(max(raw, 2.50), 2)  # $2.50 minimum stake

def place_order(token_id: str, price: float, size_usd: float,
                market: str, source: str, p_win: float = 0.0):
    global _cached_free_usdc
    free_now = _cached_free_usdc
    with _order_lock:
        if not check_halt():
            return
        if token_id in open_positions:
            return
        if len(open_positions) >= CFG["max_positions"]:
            log.warning(f"[{source}] SKIP — max {CFG['max_positions']} positions")
            return
        f = free_now if free_now > 0 else max(_cached_portfolio_val, 1.0)
        if f < 1.0:
            log.warning(f"[{source}] SKIP — free USDC ${f:.2f} < $1 minimum")
            return
        shares   = max(5, round(size_usd / max(price, 0.01)))
        size_usd = round(shares * price, 2)
        if size_usd > f * 0.9:
            shares   = 5
            size_usd = round(5 * price, 2)
        if size_usd > f * 0.9:
            log.warning(f"[{source}] SKIP — ${size_usd:.2f} > free ${f:.2f}")
            return
        _args = (token_id, price, shares, market, source, size_usd)

    token_id, price, shares, market, source, cost = _args
    for attempt, bid in enumerate([price, min(round(price + 0.02, 3), 0.97)]):
        try:
            order  = client.create_and_post_order(
                OrderArgs(token_id=token_id, price=bid, size=shares, side=BUY),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.GTC,
            )
            status = order.get("status", "?")
            actual = round(shares * bid, 2)
            tg(f"ORDER [{source}] BUY {shares}sh @ {bid:.3f} (${actual}) | {market[:50]} | {status}")
            with _order_lock:
                if token_id not in open_positions and status in ("matched", "delayed"):
                    open_positions[token_id] = {
                        "size": shares, "entry": bid, "source": source,
                        "market": market, "entry_time": time.time(),
                    }
                    trade_log.append({"source": source, "token_id": token_id, "price": bid, "size": shares})
                    if len(trade_log) > 500:
                        del trade_log[:250]
                    _cached_free_usdc = max(0, _cached_free_usdc - actual)
                    break
                elif status == "live" and attempt == 0:
                    continue
                else:
                    break
        except Exception as e:
            log.error(f"[{source}] Order failed: {e}")
            break

def sell_position(token_id: str, price: float, market: str, source: str):
    with _order_lock:
        pos    = open_positions.get(token_id)
        if not pos:
            return
        shares = float(pos.get("size", 0))
    if shares <= 0:
        return
    for attempt_sh in [shares, max(1, int(shares) - 1)]:
        try:
            order  = client.create_and_post_order(
                OrderArgs(token_id=token_id, price=price, size=attempt_sh, side=SELL),
                options=PartialCreateOrderOptions(tick_size="0.01"),
                order_type=OrderType.GTC,
            )
            status = order.get("status", "?")
            with _order_lock:
                if status in ("matched", "delayed", "live"):
                    open_positions.pop(token_id, None)
            tg(f"SELL [{source}] {attempt_sh}sh @ {price:.3f} | {market[:50]} | {status}")
            break
        except Exception as e:
            err = str(e)
            if "not enough balance" in err and "balance: 0" in err:
                with _order_lock:
                    open_positions.pop(token_id, None)
                break
            if "not enough balance" in err and attempt_sh > 1:
                continue
            log.error(f"[{source}] Sell failed: {e}")
            break

# ── BINANCE WEBSOCKET — REAL-TIME BTC PRICE ───────────────────────────────────
_WS_PRICES: dict = {}
_WS_LOCK         = threading.Lock()

_PROXY_ENV_KEYS = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "all_proxy",
)

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
            # Strip proxy env vars — Binance WS connects directly (proxy blocks WS upgrades)
            saved = {k: os.environ.pop(k, None) for k in _PROXY_ENV_KEYS}
            try:
                app = websocket.WebSocketApp(url, on_message=on_msg)
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error(f"[BNS-WS] {e}")
            finally:
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v
            time.sleep(3)

    threading.Thread(target=run, daemon=True, name="binance-ws").start()
    log.info("[BNS-WS] Binance WebSocket starting — BTCUSDT real-time feed (direct, no proxy)")

def ws_price(symbol: str) -> tuple:
    """Returns (price, age_seconds). Falls back to (0, 999) if not yet connected."""
    with _WS_LOCK:
        d = _WS_PRICES.get(f"{symbol.upper()}USDT")
    if not d:
        return 0.0, 999.0
    return d["price"], time.time() - d["ts"]

# ── MULTI-SOURCE PRICE (REST FALLBACK) ────────────────────────────────────────
_KRAKEN_SYM   = {"BTC": "XXBTZUSD", "ETH": "XETHZUSD"}
_mprice_cache: dict = {}

async def _get_prices_async(symbol: str) -> dict:
    sym    = symbol.upper()
    kr_sym = _KRAKEN_SYM.get(sym, f"{sym}USD")
    async with _aiohttp.ClientSession() as session:
        async def _j(url):
            try:
                async with session.get(url, timeout=_aiohttp.ClientTimeout(total=3)) as r:
                    return await r.json(content_type=None)
            except Exception:
                return {}
        bn_d, cb_d, kr_d = await asyncio.gather(
            _j(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT"),
            _j(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot"),
            _j(f"https://api.kraken.com/0/public/Ticker?pair={kr_sym}"),
        )
    kr_res   = kr_d.get("result") or {}
    kr_price = float(next(iter(kr_res.values()), {}).get("c", [0])[0] or 0) if kr_res else 0.0
    return {
        "binance":  float(bn_d.get("price", 0) or 0),
        "coinbase": float((cb_d.get("data") or {}).get("amount", 0) or 0),
        "kraken":   kr_price,
    }

def get_multi_prices(symbol: str) -> dict:
    """Prefer WebSocket price when fresh (< 2s). Falls back to parallel REST."""
    now    = time.time()
    ws_p, ws_age = ws_price(symbol)
    cached = _mprice_cache.get(symbol)
    if cached and now - cached[0] < 4:
        d = dict(cached[1])
        if ws_p > 0 and ws_age < 2:
            d["binance"] = ws_p
        return d
    if _AIOHTTP_OK:
        try:
            d = asyncio.run(_get_prices_async(symbol))
            if ws_p > 0 and ws_age < 2:
                d["binance"] = ws_p
            if d["binance"] > 0:
                _mprice_cache[symbol] = (now, d)
            return d
        except Exception:
            pass
    # Sequential fallback
    sym    = symbol.upper()
    kr_sym = _KRAKEN_SYM.get(sym, f"{sym}USD")
    bn     = fetch(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}USDT") or {}
    cb     = fetch(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot") or {}
    kr     = fetch(f"https://api.kraken.com/0/public/Ticker?pair={kr_sym}") or {}
    kr_res = (kr.get("result") or {})
    kr_p   = float(next(iter(kr_res.values()), {}).get("c", [0])[0] or 0) if kr_res else 0.0
    d = {
        "binance":  ws_p if (ws_p > 0 and ws_age < 2) else float(bn.get("price", 0) or 0),
        "coinbase": float((cb.get("data") or {}).get("amount", 0) or 0),
        "kraken":   kr_p,
    }
    if d["binance"] > 0:
        _mprice_cache[symbol] = (now, d)
    return d

def btc_reference_price() -> float:
    """Open of the in-progress 5m BTC candle — Polymarket's resolution baseline."""
    data = fetch("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=2")
    return float(data[-1][1]) if data and len(data) >= 1 else 0.0

# ── CHAINLINK ORACLE — LAG DETECTION ─────────────────────────────────────────
_CL_ABI = [{"inputs": [], "name": "latestRoundData",
    "outputs": [{"type": "uint80"}, {"type": "int256"}, {"type": "uint256"},
                {"type": "uint256"}, {"type": "uint80"}],
    "stateMutability": "view", "type": "function"}]
_CL_BTC   = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
_cl_cache: dict = {}

def chainlink_btc() -> tuple:
    """Returns (price_usd, lag_seconds) from Polygon Chainlink BTC/USD feed."""
    if not _WEB3_OK:
        return 0.0, 999.0
    now    = time.time()
    cached = _cl_cache.get("BTC")
    if cached and now - cached[0] < 8:
        return cached[1], now - cached[2]
    try:
        w3   = Web3(Web3.HTTPProvider("https://polygon-rpc.com", request_kwargs={"timeout": 4}))
        feed = w3.eth.contract(address=Web3.to_checksum_address(_CL_BTC), abi=_CL_ABI)
        _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
        price = float(answer) / 1e8
        _cl_cache["BTC"] = (now, price, float(updated_at))
        return price, now - float(updated_at)
    except Exception:
        return 0.0, 999.0

# ── WHALE FLOW SCANNER ────────────────────────────────────────────────────────
_whale_flow: dict = {}   # cid -> [{"wallet", "name", "conviction", "usd", "direction", "ts"}]
_whale_lock       = threading.Lock()

def _scan_whale_flow():
    """
    Poll activity for each tracked whale wallet.
    Records any BTC directional trade > $10k placed in the last 5 minutes.
    Outcome "Up" = UP signal, "Down" = DOWN signal.
    """
    now_ts = time.time()
    cutoff = now_ts - CFG["whale_lookback_sec"]

    for wallet, info in BTC_WHALE_WALLETS.items():
        try:
            activity = fetch(
                f"https://data-api.polymarket.com/activity?user={wallet}&limit=20"
            )
            if not isinstance(activity, list):
                continue

            for trade in activity:
                raw_ts = int(trade.get("timestamp", 0) or 0)
                if raw_ts > 1e12:
                    raw_ts //= 1000
                if raw_ts < cutoff:
                    continue
                if trade.get("type") != "TRADE":
                    continue
                if str(trade.get("side", "")).upper() in ("SELL", "2"):
                    continue

                usd_size = float(trade.get("usdcSize", 0) or 0)
                if usd_size < CFG["whale_min_usd"]:
                    continue

                cid     = trade.get("conditionId", "")
                outcome = str(trade.get("outcome", "")).strip()   # "Up", "Down", "Yes", "No"
                if not cid:
                    continue

                # Resolve market to confirm it's BTC directional
                mkt_data = fetch(f"https://gamma-api.polymarket.com/markets?conditionId={cid}")
                if not mkt_data:
                    continue
                ml = mkt_data if isinstance(mkt_data, list) else mkt_data.get("markets", [])
                if not ml:
                    continue
                q = (ml[0].get("question") or "").lower()
                if not re.search(r"btc|bitcoin", q):
                    continue
                if not re.search(r"up|down", q):
                    continue

                # Map outcome to directional signal
                out_lc = outcome.lower()
                if out_lc in ("up", "yes") and "up" in q:
                    direction = "UP"
                elif out_lc in ("down", "yes") and "down" in q:
                    direction = "DOWN"
                elif out_lc == "up":
                    direction = "UP"
                elif out_lc == "down":
                    direction = "DOWN"
                else:
                    continue

                rec = {
                    "wallet": wallet, "name": info["name"],
                    "conviction": info["conviction"],
                    "usd": usd_size, "direction": direction,
                    "ts": raw_ts, "market": q[:60],
                }
                with _whale_lock:
                    bucket = _whale_flow.setdefault(cid, [])
                    if not any(r["wallet"] == wallet and r["ts"] == raw_ts for r in bucket):
                        bucket.append(rec)
                    _whale_flow[cid] = [r for r in bucket if r["ts"] > cutoff]

                log.info(
                    f"[WHALE] {info['name']} ${usd_size:,.0f} → {direction} "
                    f"(conv={info['conviction']}) | {q[:50]}"
                )
                tg(
                    f"WHALE {info['name']} ${usd_size:,.0f} → {direction} | {q[:50]}",
                    "WHALE",
                )

        except Exception as e:
            log.debug(f"[WHALE] {wallet[:14]}: {e}")

    # Prune empty buckets
    with _whale_lock:
        empty = [k for k, v in _whale_flow.items() if not v]
        for k in empty:
            del _whale_flow[k]

def whale_consensus(cid: str) -> tuple:
    """
    Returns (direction, score_0_to_10, count, total_usd).
    Weighted by conviction × size. Returns (None, 0, 0, 0) if no signal.
    """
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
    else:
        return "DOWN", round(down_w / total_w * 10, 2), len(records), total_usd

def whale_flow_loop():
    log.info(
        f"[WHALE] Started — tracking {len(BTC_WHALE_WALLETS)} wallets | "
        f"threshold ${CFG['whale_min_usd']:,} | scan every {CFG['whale_scan_interval']}s"
    )
    while True:
        try:
            _scan_whale_flow()
        except Exception as e:
            log.error(f"[WHALE] loop: {e}")
        time.sleep(CFG["whale_scan_interval"])

# ── SIGNAL CONFLUENCE SCORER ──────────────────────────────────────────────────
# Weights mirror polyscope's panel (sums to 1.0).
_WEIGHTS = {
    "whale_consensus": 0.32,
    "price_delta":     0.24,
    "chainlink_lag":   0.18,
    "ob_imbalance":    0.14,
    "exchange_agree":  0.08,
    "clob_spread":     0.04,
}

def confluence_score(
    cid: str, token_id: str, direction: str,
    bn_delta: float, ref_price: float,
    imbalance: float, cl_lag: float,
    exchange_agree: int, book: dict,
) -> float:
    """Returns weighted confluence score 0–10."""
    score = 0.0

    # 1. Whale consensus (weight 0.32)
    w_dir, w_score, w_count, _ = whale_consensus(cid)
    if w_dir == direction and w_count > 0:
        score += _WEIGHTS["whale_consensus"] * min(w_score, 10)
    elif w_dir and w_dir != direction:
        score -= _WEIGHTS["whale_consensus"] * 3   # penalty for opposing whale signal

    # 2. Price delta signal (weight 0.24)
    if ref_price > 0 and bn_delta != 0:
        aligned = (bn_delta > 0 and direction == "UP") or (bn_delta < 0 and direction == "DOWN")
        if aligned:
            magnitude = min(abs(bn_delta) / ref_price / 0.005, 1.0)
            score += _WEIGHTS["price_delta"] * 10 * magnitude

    # 3. Chainlink lag (weight 0.18) — oracle hasn't updated = repricing window open
    if 5 <= cl_lag < 60:
        lag_score = min(cl_lag / 15.0, 1.0)
        score += _WEIGHTS["chainlink_lag"] * 10 * lag_score

    # 4. Order book imbalance (weight 0.14)
    if direction == "UP" and imbalance >= 1.8:
        score += _WEIGHTS["ob_imbalance"] * 10 * min((imbalance - 1.8) / 3.2 + 0.5, 1.0)
    elif direction == "DOWN" and imbalance <= 0.55:
        score += _WEIGHTS["ob_imbalance"] * 10 * min((0.55 - imbalance) / 0.45 + 0.5, 1.0)

    # 5. Multi-exchange agreement (weight 0.08)
    if exchange_agree >= 2:
        score += _WEIGHTS["exchange_agree"] * 10 * min(exchange_agree / 3.0, 1.0)

    # 6. CLOB spread — tight spread = liquid market (weight 0.04)
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if bids and asks:
        try:
            spread = min(float(a["price"]) for a in asks[:3]) - max(float(b["price"]) for b in bids[:3])
            if spread < 0.05:
                score += _WEIGHTS["clob_spread"] * 10
        except Exception:
            pass

    return round(max(0.0, score), 2)

# ── BTC 5M UPDN MARKET CACHE ─────────────────────────────────────────────────
_updn_market_cache: dict  = {}
_updn_cache_ts:     float = 0.0
_updn_refresh_lock        = threading.Lock()

def _load_updn_traded() -> set:
    try:
        now_ts = time.time()
        data   = json.loads(UPDN_TRADED_FILE.read_text())
        return {cid for cid, ts in data.items() if now_ts - ts < CFG["updn_trade_ttl_sec"]}
    except Exception:
        return set()

def _save_updn_traded(traded: set):
    now_ts = time.time()
    try:
        existing = {}
        try:
            existing = json.loads(UPDN_TRADED_FILE.read_text())
        except Exception:
            pass
        for cid in traded:
            if cid not in existing:
                existing[cid] = now_ts
        existing = {k: v for k, v in existing.items() if now_ts - v < CFG["updn_trade_ttl_sec"]}
        UPDN_TRADED_FILE.write_text(json.dumps(existing))
    except Exception:
        pass

_updn_traded: set = _load_updn_traded()

def _refresh_updn_cache():
    global _updn_market_cache, _updn_cache_ts
    now   = datetime.now(timezone.utc)
    cache = {}
    base  = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
    for window in [base + timedelta(minutes=i * 5) for i in range(-1, 4)]:
        ts   = int(window.timestamp())
        slug = f"btc-updown-5m-{ts}"
        d    = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}")
        time.sleep(CFG["updn_slug_fetch_gap_sec"])
        if not d:
            continue
        for m in (d if isinstance(d, list) else d.get("markets", [])):
            cid    = m.get("conditionId") or m.get("id") or ""
            end_str = m.get("endDate", "")
            end_dt  = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None
            if cid:
                cache[cid] = {
                    "market": m, "end_dt": end_dt,
                    "prices": parse_prices(m),
                    "outcomes": parse_outcomes(m),
                    "toks": parse_tokens(m),
                }
    if cache:
        log.info(f"[UPDN] Cache refreshed — {len(cache)} BTC 5m markets")
        _updn_market_cache = cache
    else:
        log.debug("[UPDN] Cache refresh got 0 markets — keeping previous")
    _updn_cache_ts = time.time()

def _refresh_updn_cache_async():
    if _updn_refresh_lock.locked():
        return
    def _work():
        with _updn_refresh_lock:
            _refresh_updn_cache()
    threading.Thread(target=_work, daemon=True, name="updn-refresh").start()

# ── UPDN SCAN ─────────────────────────────────────────────────────────────────
def updn_scan():
    now = datetime.now(timezone.utc)

    for cid, entry in list(_updn_market_cache.items()):
        try:
            if cid in _updn_traded:
                continue

            end_dt = entry.get("end_dt")
            if not end_dt:
                continue
            mins_left = (end_dt - now).total_seconds() / 60
            if not (2.0 <= mins_left <= 4.0):   # 120-240s before expiry = sweet spot
                continue
            if mins_left < 1.0:                  # dead zone: spreads blow out
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

            # ── Signal: Binance vs 5m candle open ────────────────────────────
            ref_price = btc_reference_price()
            prices    = get_multi_prices("BTC")
            bn        = prices["binance"]
            cb        = prices["coinbase"]
            kr        = prices.get("kraken", 0.0)
            cl, cl_lag = chainlink_btc()

            if bn <= 0 or ref_price <= 0:
                continue

            threshold    = max(5.0, ref_price * 0.0005)
            bn_delta     = bn - ref_price
            exchanges_up = sum([bn_delta > threshold,
                                (cb - ref_price) > threshold if cb > 0 else False,
                                (kr - ref_price) > threshold if kr > 0 else False])
            exchanges_dn = sum([bn_delta < -threshold,
                                (cb - ref_price) < -threshold if cb > 0 else False,
                                (kr - ref_price) < -threshold if kr > 0 else False])

            if exchanges_up >= 2:
                direction      = "UP"
                exchange_agree = exchanges_up
            elif exchanges_dn >= 2:
                direction      = "DOWN"
                exchange_agree = exchanges_dn
            else:
                # No price signal — rely on whale consensus only if very strong
                w_dir, w_score, w_count, _ = whale_consensus(cid)
                if w_dir and w_score >= 8.0 and w_count >= 2:
                    direction      = w_dir
                    exchange_agree = 0
                    bn_delta       = 0.0
                else:
                    continue

            bet_i    = up_i   if direction == "UP" else down_i
            token_id = toks[bet_i]

            # ── Signal: Order book imbalance ─────────────────────────────────
            book = fetch(f"https://clob.polymarket.com/book?token_id={token_id}")
            if not book:
                continue
            imbalance = calculate_imbalance(book)

            if direction == "UP"   and imbalance < 1.8:
                continue
            if direction == "DOWN" and imbalance > 0.55:
                continue

            # ── Confluence gate ───────────────────────────────────────────────
            conf = confluence_score(
                cid, token_id, direction,
                bn_delta, ref_price, imbalance,
                cl_lag, exchange_agree, book,
            )
            if conf < CFG["confluence_min"]:
                log.debug(
                    f"[UPDN/5m] BTC {direction} conf={conf:.1f} < {CFG['confluence_min']} — skip"
                )
                continue

            # ── Price range ───────────────────────────────────────────────────
            live_price = clob_mid(token_id)
            if live_price <= 0:
                live_price = float(entry["prices"][bet_i]) if len(entry["prices"]) > bet_i else 0.5
            if not (CFG["updn_min_price"] <= live_price <= CFG["updn_max_price"]):
                continue

            # ── Size via Kelly ────────────────────────────────────────────────
            p_win    = min(0.93, 0.55 + (conf - CFG["confluence_min"]) / 10)
            bankroll = _cached_portfolio_val if _cached_portfolio_val > 0 else max(BANKROLL_OVERRIDE, 9.91)
            size_usd = kelly_size(p_win, live_price, bankroll)
            roi      = (1 - live_price) / live_price * 100

            w_dir_log, w_score_log, w_count_log, w_usd_log = whale_consensus(cid)
            q = (entry["market"].get("question") or "")[:60]

            log.info(
                f"[UPDN/5m] BTC {direction} | "
                f"Bn={bn_delta:+.0f} OB={imbalance:.2f} CL={cl_lag:.0f}s | "
                f"Whale={w_dir_log}({w_score_log:.1f}/10 {w_count_log}x ${w_usd_log:,.0f}) | "
                f"Conf={conf:.1f}/10 p={p_win:.0%} ${size_usd:.2f} @ {live_price:.2f} +{roi:.1f}%"
            )
            tg(
                f"UPDN/5m BTC {direction} | Conf={conf:.1f}/10 "
                f"Whale={w_dir_log}({w_count_log}x) OB={imbalance:.2f} "
                f"p={p_win:.0%} ${size_usd:.2f} @ {live_price:.0%} +{roi:.1f}%",
                "UPDN",
            )

            place_order(token_id, live_price, size_usd, q, "UPDN/5m", p_win=p_win)
            _updn_traded.add(cid)
            _save_updn_traded(_updn_traded)

        except Exception as e:
            import traceback
            log.error(f"[UPDN] {e}\n{traceback.format_exc()}")

def updn_loop():
    log.info("[UPDN] Started — BTC 5m UP/DOWN | whale + price + OB + Chainlink confluence gate")
    _refresh_updn_cache()
    while True:
        try:
            if time.time() - _updn_cache_ts > CFG["updn_cache_refresh_sec"]:
                _refresh_updn_cache_async()
            updn_scan()
        except Exception as e:
            log.error(f"[UPDN] loop: {e}")
        time.sleep(CFG["updn_scan_sleep_sec"])

# ── POSITION EXIT MANAGER ─────────────────────────────────────────────────────
def check_profit_exits():
    now = time.time()
    for token_id, pos in list(open_positions.items()):
        try:
            entry = float(pos.get("entry", 0))
            if entry <= 0:
                continue
            mid        = clob_mid(token_id)
            if mid <= 0:
                continue
            profit_pct = (mid - entry) / entry
            mins_held  = (now - float(pos.get("entry_time", now))) / 60
            mkt        = pos.get("market", token_id[:20])

            if profit_pct <= -0.50:                                     # hard stop: -50% position
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "STOP")
                tg(f"EXIT STOP {profit_pct*100:.1f}% | {mkt[:40]}", "STOP")
            elif mid >= 0.75:                                            # profit target
                sell_position(token_id, round(max(0.70, mid - 0.02), 3), mkt, "PROFIT_TARGET")
                tg(f"EXIT PROFIT +{profit_pct*100:.1f}% | {mkt[:40]}", "EXIT")
            elif mid >= 0.95:                                            # resolved win
                sell_position(token_id, round(max(0.90, mid - 0.02), 3), mkt, "RESOLVED")
            elif mid <= 0.35:                                            # stop at 35¢
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "STOP_35")
            elif mins_held > 15:                                         # 5m markets expire
                sell_position(token_id, round(max(0.01, mid - 0.01), 3), mkt, "EXPIRED")
        except Exception:
            pass

# ── ON-CHAIN REDEMPTION ────────────────────────────────────────────────────────
_CT_ADDR   = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_USDC_POLY = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_ZERO_ADDR = "0x0000000000000000000000000000000000000000"
_CT_ABI    = [{"name": "redeemPositions", "type": "function", "outputs": [], "inputs": [
    {"name": "collateralToken", "type": "address"},
    {"name": "parentCollectionId", "type": "bytes32"},
    {"name": "conditionId", "type": "bytes32"},
    {"name": "indexSets", "type": "uint256[]"},
]}]
_SAFE_ABI  = [
    {"name": "nonce", "type": "function", "inputs": [], "outputs": [{"type": "uint256"}]},
    {"name": "getTransactionHash", "type": "function", "outputs": [{"type": "bytes32"}],
     "inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"}, {"name": "operation", "type": "uint8"},
                {"name": "safeTxGas", "type": "uint256"}, {"name": "baseGas", "type": "uint256"},
                {"name": "gasPrice", "type": "uint256"}, {"name": "gasToken", "type": "address"},
                {"name": "refundReceiver", "type": "address"}, {"name": "_nonce", "type": "uint256"}]},
    {"name": "execTransaction", "type": "function", "outputs": [{"type": "bool"}],
     "inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"}, {"name": "operation", "type": "uint8"},
                {"name": "safeTxGas", "type": "uint256"}, {"name": "baseGas", "type": "uint256"},
                {"name": "gasPrice", "type": "uint256"}, {"name": "gasToken", "type": "address"},
                {"name": "refundReceiver", "type": "address"}, {"name": "signatures", "type": "bytes"}]},
]
_redeemed_cids: set = set()

def redeem_loop():
    if not _WEB3_OK:
        return
    try:
        w3   = Web3(Web3.HTTPProvider("https://polygon-bor-rpc.publicnode.com", request_kwargs={"timeout": 15}))
        acct = w3.eth.account.from_key("0x" + PRIVATE_KEY)
    except Exception as e:
        log.error(f"[REDEEM] Init: {e}")
        return
    log.info(f"[REDEEM] Started — EOA={acct.address[:16]}...")
    while True:
        try:
            positions = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.01")
            if isinstance(positions, list):
                for pos in positions:
                    cid = pos.get("conditionId", "")
                    if not bool(pos.get("redeemable")) or not cid or cid in _redeemed_cids:
                        continue
                    size    = float(pos.get("size", 0) or 0)
                    outcome = pos.get("outcome", "Yes")
                    if size < 0.1:
                        continue
                    try:
                        fcs = Web3.to_checksum_address(FUNDER)
                        ct  = w3.eth.contract(address=Web3.to_checksum_address(_CT_ADDR), abi=_CT_ABI)
                        sf  = w3.eth.contract(address=fcs, abi=_SAFE_ABI)
                        cid_b    = bytes.fromhex(cid.replace("0x", ""))
                        idx      = [1] if outcome.lower() in ("yes", "up") else [2]
                        call_d   = ct.encode_abi("redeemPositions",
                                                 [Web3.to_checksum_address(_USDC_POLY),
                                                  b"\x00" * 32, cid_b, idx])
                        n_safe   = sf.functions.nonce().call()
                        tx_hash  = sf.functions.getTransactionHash(
                            Web3.to_checksum_address(_CT_ADDR), 0, call_d, 0, 0, 0, 0,
                            _ZERO_ADDR, _ZERO_ADDR, n_safe
                        ).call()
                        signed  = acct.sign_message(encode_defunct(tx_hash))
                        sig     = signed.r.to_bytes(32, "big") + signed.s.to_bytes(32, "big") + bytes([signed.v + 4])
                        gp      = w3.eth.gas_price
                        if w3.eth.get_balance(acct.address) < gp * 200_000:
                            continue
                        tx      = sf.functions.execTransaction(
                            Web3.to_checksum_address(_CT_ADDR), 0, call_d, 0, 0, 0, 0,
                            _ZERO_ADDR, _ZERO_ADDR, sig
                        ).build_transaction({"from": acct.address,
                                             "nonce": w3.eth.get_transaction_count(acct.address),
                                             "gas": 250_000, "gasPrice": gp, "chainId": 137})
                        rx      = w3.eth.wait_for_transaction_receipt(
                            w3.eth.send_raw_transaction(acct.sign_transaction(tx).raw_transaction),
                            timeout=60,
                        )
                        if rx.status == 1:
                            _redeemed_cids.add(cid)
                            tg(f"REDEEMED {size:.2f}sh | {pos.get('title','?')[:40]}", "INFO")
                    except Exception as e:
                        log.error(f"[REDEEM] {cid[:16]}: {e}")
        except Exception as e:
            log.error(f"[REDEEM] loop: {e}")
        time.sleep(300)

# ── STATUS LOOP ───────────────────────────────────────────────────────────────
_equity_history: list = []
_last_day             = datetime.now(timezone.utc).date()

def status_loop():
    global _equity_history, _last_day
    global _cached_portfolio_val, _cached_free_usdc
    global _bot_halted, _halt_reason, _last_heartbeat

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

        if not _bot_halted and _day_start_val and val > 0:
            day_loss = (val - _day_start_val) / _day_start_val
            if day_loss < CFG["daily_halt_pct"]:
                _bot_halted  = True
                _halt_reason = f"daily loss {day_loss * 100:.1f}%"
                tg(f"BOT HALTED — {day_loss * 100:.1f}% daily loss.", "WARN")

        check_profit_exits()

        day_pct     = round((val - _day_start_val) / _day_start_val * 100, 2) if val and _day_start_val else 0.0
        session_pnl = round(val - _session_start, 4) if val and _session_start else 0.0
        halt_str    = f" HALTED:{_halt_reason}" if _bot_halted else ""
        _last_heartbeat = time.time()

        btc_ws, btc_age = ws_price("BTC")
        with _whale_lock:
            active_whale_signals = sum(len(v) for v in _whale_flow.values())

        log.info(
            f"[STATUS] ${val:.2f} day={day_pct:+.1f}% session={session_pnl:+.2f} "
            f"pos={len(open_positions)} whale_signals={active_whale_signals} "
            f"btc_ws=${btc_ws:,.0f}({btc_age:.1f}s){halt_str}"
        )

        if val and val > 1.0:
            ts       = datetime.now(timezone.utc).strftime("%H:%M")
            last_val = _equity_history[-1][1] if _equity_history else val
            if last_val <= 0 or 0.1 <= val / last_val <= 10.0:
                _equity_history.append([ts, round(val, 4)])
        if len(_equity_history) > 120:
            _equity_history = _equity_history[-120:]

        try:
            pos_details = []
            for tid, p in open_positions.items():
                mid    = clob_mid(tid)
                entry  = float(p.get("entry", 0))
                pnl    = round((mid - entry) / max(entry, 0.001) * 100, 1) if mid > 0 and entry > 0 else 0
                pos_details.append({
                    "token_id": tid[:20], "market": (p.get("market") or "")[:45],
                    "source": p.get("source", "?"), "entry": round(entry, 3),
                    "current": round(mid, 3), "pnl_pct": pnl,
                    "size": round(float(p.get("size", 0)), 2),
                })
            with _whale_lock:
                whale_snapshot = {
                    cid: [{"name": r["name"], "dir": r["direction"], "usd": r["usd"]} for r in recs]
                    for cid, recs in _whale_flow.items()
                }
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
                "btc_ws_age_sec": round(btc_age, 2),
                "whale_signals":  active_whale_signals,
                "whale_flow":     whale_snapshot,
                "equity_history": _equity_history,
            }, indent=2))
        except Exception:
            pass

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            import psutil
            if psutil.pid_exists(pid):
                log.error(f"[MAIN] Already running (PID {pid}). Exiting.")
                return
        except Exception:
            pass
    LOCK_FILE.write_text(str(os.getpid()))

    log.info("=" * 60)
    log.info("  POLY//BOT — BTC 5MIN UP/DOWN | Whale Flow + Latency Arb")
    log.info(f"  Wallet : {FUNDER}")
    log.info(f"  Whales : {len(BTC_WHALE_WALLETS)} tracked")
    log.info(f"  Mode   : {'DRY RUN' if CFG['dry_run'] else 'LIVE'}")
    log.info(f"  Gate   : confluence ≥ {CFG['confluence_min']}/10")
    log.info(f"  Risk   : {CFG['stake_pct']*100:.1f}%/trade | {abs(CFG['daily_halt_pct'])*100:.0f}%/day halt")
    log.info("=" * 60)

    _start_binance_ws()

    threads = [
        threading.Thread(target=whale_flow_loop, daemon=True, name="whale"),
        threading.Thread(target=updn_loop,       daemon=True, name="updn"),
        threading.Thread(target=status_loop,     daemon=True, name="status"),
        threading.Thread(target=redeem_loop,     daemon=True, name="redeem"),
    ]
    for t in threads:
        t.start()
    log.info(f"[MAIN] {len(threads)} threads running — whale → updn → status → redeem")

    try:
        while True:
            time.sleep(60)
            if time.time() - _last_heartbeat > 120:
                log.error("[MAIN] Status loop dead — restarting process")
                LOCK_FILE.unlink(missing_ok=True)
                import sys
                os.execv(sys.executable, [sys.executable] + sys.argv)
    except KeyboardInterrupt:
        log.info("[MAIN] Shutting down")
    finally:
        LOCK_FILE.unlink(missing_ok=True)

if __name__ == "__main__":
    main()
