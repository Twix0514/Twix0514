"""
Find ANY live CLOB market and place a minimal test order.
Tries multiple discovery strategies to avoid neg-risk/expired markets.
"""
import sys, json, time
import urllib.request
from datetime import datetime, timezone, timedelta
from secrets_local import PRIVATE_KEY, FUNDER  # type: ignore
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY

HOST = "https://clob.polymarket.com"

def make_client(sig_type, funder=None):
    kw = {"funder": funder} if funder else {}
    return ClobClient(HOST, key=PRIVATE_KEY, chain_id=137,
                      signature_type=sig_type, **kw)

def fetch(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "bot"})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except Exception:
        return None

c1 = make_client(1, FUNDER)
c0 = make_client(0)
creds = c1.create_or_derive_api_creds()
c1.set_api_creds(creds); c0.set_api_creds(creds)

print(f"Exchange contract (normal):   {c1.get_exchange_address(neg_risk=False)}")
print(f"Exchange contract (neg_risk): {c1.get_exchange_address(neg_risk=True)}")

# ── find a tradeable token via CLOB /markets (paginate until we find one with mid > 0) ──
print("\n=== Scanning CLOB /markets for a live token ===")
token_id = None
market_q = ""
cursor = ""
pages   = 0

while not token_id and pages < 15:
    url  = f"{HOST}/markets?limit=100" + (f"&next_cursor={cursor}" if cursor else "")
    data = fetch(url)
    if not data:
        break
    pages += 1
    cursor = data.get("next_cursor", "")
    for m in data.get("data", []):
        q    = m.get("question","")
        toks = m.get("tokens",[])
        for tok in toks:
            tid = tok.get("token_id","")
            if not tid:
                continue
            mid_d = fetch(f"{HOST}/midpoint?token_id={tid}")
            if not mid_d:
                continue
            mid = float(mid_d.get("mid",0) or 0)
            if 0.02 < mid < 0.98:
                token_id = tid
                market_q = q
                yes_price = mid
                print(f"  FOUND (page {pages}): {q[:65]}")
                print(f"  Token : {tid[:28]}...")
                print(f"  Mid   : {mid:.3f}")
                break
        if token_id:
            break
    if not cursor or cursor == "LTE=":
        break
    print(f"  page {pages} scanned ({len(data.get('data',[]))} markets) — still looking...")

if not token_id:
    print("  No live market found after 15 pages. Trying gamma search...")
    # Last resort: gamma keyword search
    for kw in ["will", "bitcoin", "trump", "election"]:
        d = fetch(f"https://gamma-api.polymarket.com/markets?keyword={kw}&closed=false&limit=20")
        if not d:
            continue
        for m in (d if isinstance(d,list) else []):
            for tid in (m.get("clobTokenIds") or []):
                mid_d = fetch(f"{HOST}/midpoint?token_id={tid}")
                if not mid_d: continue
                mid = float(mid_d.get("mid",0) or 0)
                if 0.02 < mid < 0.98:
                    token_id = tid
                    market_q = m.get("question","?")
                    yes_price = mid
                    print(f"  GAMMA hit: {market_q[:65]}")
                    break
            if token_id: break
        if token_id: break

if not token_id:
    print("\nCould not find any live tradeable market.")
    print("Polymarket may have no active standard CLOB markets right now,")
    print("or all markets require neg-risk ordering.")
    sys.exit(1)

# ── check neg_risk for found token ───────────────────────────────────────────
nr = c1.get_neg_risk(token_id)
print(f"  NegRisk: {nr}")

# ── place order ───────────────────────────────────────────────────────────────
price = round(min(max(yes_price, 0.02), 0.98), 2)
print(f"\n=== Order test — 1 share @ {price} ({market_q[:45]}) ===")
for c, label in [(c1,"sig=1 POLY_PROXY"), (c0,"sig=0 EOA")]:
    try:
        result = c.create_and_post_order(
            OrderArgs(token_id=token_id, price=price, size=1, side=BUY)
        )
        st  = result.get("status","?")
        oid = str(result.get("orderID",""))[:20]
        print(f"  [{label}] SUCCESS  status={st}  id={oid}")
    except Exception as e:
        print(f"  [{label}] FAIL: {e}")
    time.sleep(1)

print("\nDone.")
