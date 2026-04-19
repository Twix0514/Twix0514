"""Withdraw USDC from Polymarket proxy contract to funder wallet."""
from web3 import Web3
import sys

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.meowrpc.com",
    "https://polygon.drpc.org",
]

PRIVATE_KEY  = "72f882593b660160169ee4d14165dbd3ad15626b6f45632373dd2774e7294300"
FUNDER       = Web3.to_checksum_address("0x361A9c14e3aD1B8Ed9ef35014fD1B5dCcB72eC07")
PROXY        = Web3.to_checksum_address("0x4cD00E387622C35bDDB9b4c962C136462338BC31")
USDC         = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

ERC20_ABI = [
    {"name": "balanceOf", "type": "function",
     "inputs": [{"name": "a", "type": "address"}],
     "outputs": [{"type": "uint256"}], "stateMutability": "view"},
    {"name": "transfer", "type": "function",
     "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable"},
]

# Polymarket proxy withdraw ABI (calls token.transfer on behalf of owner)
PROXY_ABI = [
    {"name": "withdraw", "type": "function",
     "inputs": [
         {"name": "_token",  "type": "address"},
         {"name": "_to",     "type": "address"},
         {"name": "_amount", "type": "uint256"},
     ],
     "outputs": [], "stateMutability": "nonpayable"},
    {"name": "owner", "type": "function",
     "inputs": [], "outputs": [{"type": "address"}], "stateMutability": "view"},
]

# Connect to first working RPC
w3 = None
for rpc in RPCS:
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        _w3.eth.block_number  # test connection
        w3 = _w3
        print(f"Connected via {rpc}")
        break
    except Exception as e:
        print(f"Failed {rpc}: {e}")

if not w3:
    print("All RPCs failed. Try connecting to ProtonVPN first.")
    sys.exit(1)

usdc_contract  = w3.eth.contract(address=USDC,  abi=ERC20_ABI)
proxy_contract = w3.eth.contract(address=PROXY, abi=PROXY_ABI)

# Check balances
proxy_usdc = usdc_contract.functions.balanceOf(PROXY).call()
funder_usdc = usdc_contract.functions.balanceOf(FUNDER).call()
funder_matic = w3.eth.get_balance(FUNDER) / 1e18

print(f"\nProxy USDC:  ${proxy_usdc / 1e6:.4f}")
print(f"Funder USDC: ${funder_usdc / 1e6:.4f}")
print(f"Funder MATIC: {funder_matic:.6f}")

if proxy_usdc == 0:
    print("\nNo USDC in proxy contract.")
    sys.exit(0)

if funder_matic < 0.001:
    print(f"\nNeed MATIC for gas. Send ~0.01 MATIC to {FUNDER}")
    sys.exit(1)

print(f"\nWithdrawing ${proxy_usdc / 1e6:.4f} USDC from proxy → {FUNDER}")

nonce = w3.eth.get_transaction_count(FUNDER)
tx = proxy_contract.functions.withdraw(USDC, FUNDER, proxy_usdc).build_transaction({
    "from":     FUNDER,
    "nonce":    nonce,
    "gasPrice": w3.eth.gas_price,
    "gas":      150_000,
    "chainId":  137,
})
signed = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"TX sent: {tx_hash.hex()}")
receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
print(f"Status: {'OK' if receipt['status'] == 1 else 'FAILED'} — block {receipt['blockNumber']}")
