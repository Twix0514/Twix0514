"""
Diagnostic: sign and post a single order, show full error.
Uses a known-live token from bot log.
"""
import sys, json, urllib.request
from secrets_local import PRIVATE_KEY, FUNDER
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, CreateOrderOptions
from py_clob_client.order_builder.constants import BUY
from py_clob_client.config import get_contract_config

HOST = "https://clob.polymarket.com"

# Token from recent bot log: "New Rihanna Album before GTA VI?"
TOKEN_ID = "98022490269692409998126496127597032490334070080325855126491859374983463996227"

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "diag"})
    try:
        r = urllib.request.urlopen(req, timeout=8)
        return json.loads(r.read())
    except Exception as e:
        print(f"  fetch error: {e}")
        return None

# Get midpoint
mid_d = fetch(f"{HOST}/midpoint?token_id={TOKEN_ID}")
print(f"Midpoint data: {mid_d}")

if not mid_d or not float(mid_d.get("mid", 0) or 0):
    print("No midpoint — trying alternate token")
    TOKEN_ID = "108999723207897941876452935557011604067917389120996960199512481363958770540884"
    mid_d = fetch(f"{HOST}/midpoint?token_id={TOKEN_ID}")
    print(f"Midpoint data: {mid_d}")

mid = float(mid_d.get("mid", 0.5) or 0.5)
price = round(max(0.02, min(0.98, mid)), 2)
print(f"Price: {price}")

# Build client
client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=137, signature_type=1, funder=FUNDER)
creds = client.create_or_derive_api_creds()
client.set_api_creds(creds)
print(f"API creds derived")

# Neg risk check
nr = client.get_neg_risk(TOKEN_ID)
print(f"neg_risk: {nr}")

cfg = get_contract_config(137, neg_risk=nr)
print(f"verifyingContract: {cfg.exchange}")
print(f"collateral:        {cfg.collateral}")

# Get tick size
tick = client.get_tick_size(TOKEN_ID)
print(f"tick_size: {tick}")

# Build signed order (don't post yet)
opts = CreateOrderOptions(tick_size=tick, neg_risk=nr)
order_args = OrderArgs(token_id=TOKEN_ID, price=price, size=1, side=BUY)
signed = client.builder.create_order(order_args, opts)

print(f"\nSigned order:")
print(f"  maker      = {signed.order.maker}")
print(f"  signer     = {signed.order.signer}")
print(f"  tokenId    = {signed.order.tokenId}")
print(f"  makerAmount= {signed.order.makerAmount}")
print(f"  takerAmount= {signed.order.takerAmount}")
print(f"  sigType    = {signed.order.signatureType}")
print(f"  nonce      = {signed.order.nonce}")
print(f"  expiration = {signed.order.expiration}")
print(f"  sig        = {signed.signature[:30]}...")

# Now post
print(f"\nPosting order...")
try:
    result = client.post_order(signed)
    print(f"SUCCESS: {result}")
except Exception as e:
    print(f"FAILED type={type(e).__name__}")
    print(f"  str: {str(e)}")
    print(f"  repr: {repr(e)[:300]}")
