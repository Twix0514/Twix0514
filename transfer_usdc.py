"""Transfer USDC from old wallet to new wallet on Polygon."""
from web3 import Web3

# Polygon RPC
w3 = Web3(Web3.HTTPProvider("https://polygon.llamarpc.com"))

OLD_KEY  = "72f882593b660160169ee4d14165dbd3ad15626b6f45632373dd2774e7294300"
OLD_ADDR = Web3.to_checksum_address("0x361A9c14e3aD1B8Ed9ef35014fD1B5dCcB72eC07")
NEW_ADDR = Web3.to_checksum_address("0x682Df9cf2638a854a4d69Cc3a3c12fCB2B216d27")

# USDC on Polygon
USDC = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
ERC20_ABI = [
    {"name": "balanceOf", "type": "function", "inputs": [{"name": "a", "type": "address"}],
     "outputs": [{"type": "uint256"}], "stateMutability": "view"},
    {"name": "transfer",  "type": "function",
     "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable"},
]

usdc = w3.eth.contract(address=USDC, abi=ERC20_ABI)
balance_raw = usdc.functions.balanceOf(OLD_ADDR).call()
balance_usd = balance_raw / 1_000_000
print(f"Old wallet USDC balance: ${balance_usd:.6f}")

matic = w3.eth.get_balance(OLD_ADDR) / 1e18
print(f"Old wallet MATIC balance: {matic:.6f}")

if balance_raw == 0:
    print("No USDC to transfer.")
    exit()

if matic < 0.001:
    print("Not enough MATIC for gas. Send ~0.01 MATIC to the old wallet first.")
    exit()

# Send all USDC to new wallet
nonce = w3.eth.get_transaction_count(OLD_ADDR)
gas_price = w3.eth.gas_price

tx = usdc.functions.transfer(NEW_ADDR, balance_raw).build_transaction({
    "from":     OLD_ADDR,
    "nonce":    nonce,
    "gasPrice": gas_price,
    "gas":      100_000,
    "chainId":  137,
})

signed = w3.eth.account.sign_transaction(tx, OLD_KEY)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"Transfer sent: {tx_hash.hex()}")
print(f"Transferred ${balance_usd:.2f} USDC → {NEW_ADDR}")
print("Waiting for confirmation...")
receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
print(f"Confirmed in block {receipt['blockNumber']} — status: {'OK' if receipt['status'] == 1 else 'FAILED'}")
