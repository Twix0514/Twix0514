"""Find the actual Polymarket exchange contract by querying Polygon."""
import json, urllib.request
from web3 import Web3

FUNDER = "0x27af098ACCaB972Bf33869C34387aAF937033DE7"
EOA    = "0x2Cf8B7d78e6ed75cd136283127Dc9c7Daf8e00Ca"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC   = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
OLD_EXCHANGE     = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
OLD_NEG_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Try multiple Polygon RPC endpoints
rpcs = [
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.meowrpc.com",
    "https://1rpc.io/matic",
    "https://endpoints.omniatech.io/v1/matic/mainnet/public",
    "https://matic-mainnet.chainstacklabs.com",
]

w3 = None
for rpc in rpcs:
    try:
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 8}))
        if w3.is_connected():
            print(f"Connected via: {rpc}")
            break
        else:
            w3 = None
    except Exception as e:
        print(f"  {rpc}: {e}")

if not w3:
    print("No working RPC found")
    exit(1)

erc20_abi = [
    {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]

usdc   = w3.eth.contract(address=Web3.to_checksum_address(USDC),   abi=erc20_abi)
usdc_e = w3.eth.contract(address=Web3.to_checksum_address(USDC_E), abi=erc20_abi)

owner  = Web3.to_checksum_address(FUNDER)
eoa    = Web3.to_checksum_address(EOA)

print(f"\n=== FUNDER wallet balances ===")
print(f"  USDC.e : {usdc_e.functions.balanceOf(owner).call() / 1e6:.4f}")
print(f"  USDC   : {usdc.functions.balanceOf(owner).call() / 1e6:.4f}")
print(f"  POL    : {w3.eth.get_balance(owner) / 1e18:.6f}")
print(f"\n=== EOA wallet balances ===")
print(f"  USDC.e : {usdc_e.functions.balanceOf(eoa).call() / 1e6:.4f}")
print(f"  USDC   : {usdc.functions.balanceOf(eoa).call() / 1e6:.4f}")
print(f"  POL    : {w3.eth.get_balance(eoa) / 1e18:.6f}")

print(f"\n=== USDC allowances from FUNDER to exchange candidates ===")
candidates = {
    "Old CTF      ": OLD_EXCHANGE,
    "Old NegRisk  ": OLD_NEG_EXCHANGE,
}
for label, addr in candidates.items():
    spender = Web3.to_checksum_address(addr)
    a = usdc.functions.allowance(owner, spender).call() / 1e6
    b = usdc_e.functions.allowance(owner, spender).call() / 1e6
    print(f"  {label}  USDC={a:.2f}  USDC.e={b:.2f}")

# Look at recent transactions from FUNDER to find the exchange
print(f"\n=== Recent FUNDER txns (last 5 blocks) ===")
block = w3.eth.block_number
print(f"  Current block: {block}")
txns = []
for b in range(block, block - 100, -1):
    try:
        blk = w3.eth.get_block(b, full_transactions=True)
        for tx in blk.transactions:
            if tx.get("from", "").lower() == FUNDER.lower() or tx.get("to", "").lower() == FUNDER.lower():
                txns.append(tx)
                if len(txns) >= 5:
                    break
    except Exception:
        pass
    if len(txns) >= 5:
        break

for tx in txns:
    frm = tx.get("from", "?")
    to  = tx.get("to", "?") or "contract_create"
    val = tx.get("value", 0)
    print(f"  from={frm[:16]}.. to={to[:16]}.. val={val}")
