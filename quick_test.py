"""Quick order post test using a hardcoded active token."""
import sys, json
sys.path.insert(0, r"C:\Users\ojeku\OneDrive\Documents\GitHub\Twix0514")
from secrets_local import PRIVATE_KEY, FUNDER  # type: ignore
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY
import urllib.request

HOST = "https://clob.polymarket.com"

# Pick a token with activity — fetch current from /markets
def find_token():
    req = urllib.request.Request(f"{HOST}/markets?limit=50", headers={"User-Agent":"bot"})
    data = json.loads(urllib.request.urlopen(req, timeout=10).read())
    for m in data.get("data", []):
        for tok in m.get("tokens", []):
            tid = tok.get("token_id","")
            if not tid: continue
            try:
                mid_r = urllib.request.urlopen(
                    urllib.request.Request(f"{HOST}/midpoint?token_id={tid}", headers={"User-Agent":"bot"}),
                    timeout=5
                ).read()
                mid = float(json.loads(mid_r).get("mid", 0) or 0)
                if 0.05 < mid < 0.95:
                    return tid, mid, m.get("question","?")[:60]
            except:
                pass
    return None, None, None

print("Finding active token...")
token_id, mid, question = find_token()
if not token_id:
    print("No active token found")
    sys.exit(1)
print(f"Token: {token_id[:30]}... mid={mid:.3f}")
print(f"Market: {question}")

# Build client and post
c = ClobClient(HOST, key=PRIVATE_KEY, chain_id=137, signature_type=1, funder=FUNDER)
creds = c.create_or_derive_api_creds()
c.set_api_creds(creds)
print(f"Exchange: {c.get_exchange_address()}")

price = round(min(max(mid, 0.02), 0.97), 2)
print(f"\nPosting BUY 1 share @ {price}...")

# Print raw order JSON first
from py_clob_client.utilities import order_to_json
from py_clob_client.clob_types import OrderType
signed = c.create_order(OrderArgs(token_id=token_id, price=price, size=1, side=BUY))
body = order_to_json(signed, creds.api_key, OrderType.GTC, False)
print("Order JSON:")
print(json.dumps(body["order"], indent=2))

# Post it
try:
    r = c.create_and_post_order(OrderArgs(token_id=token_id, price=price, size=1, side=BUY))
    print(f"RESULT: {r}")
except Exception as e:
    print(f"ERROR: {e}")
