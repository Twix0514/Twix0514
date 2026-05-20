"""Sweep all USDC from old bot wallet → new Polymarket wallet."""
from web3 import Web3
import sys

RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.meowrpc.com",
    "https://polygon.drpc.org",
]

OLD_KEY  = "0e0f279ec9fb2ff4959525cc30db040ef941e6f714ee556a8bf594a7dc8303d9"
OLD_ADDR = Web3.to_checksum_address("0x36576E80353D35B2Fa00520cD96823861fD922DF")
NEW_ADDR = Web3.to_checksum_address("0x2Fd2208D96109f2746cE6Dd184A0c850F9A1D713")
USDC     = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

ERC20_ABI = [
    {"name": "balanceOf", "type": "function",
     "inputs": [{"name": "a", "type": "address"}],
     "outputs": [{"type": "uint256"}], "stateMutability": "view"},
    {"name": "transfer", "type": "function",
     "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable"},
]

w3 = None
for rpc in RPCS:
    try:
        _w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
        _w3.eth.block_number
        w3 = _w3
        print(f"Connected via {rpc}")
        break
    except Exception as e:
        print(f"  Failed {rpc}: {e}")

if not w3:
    print("All RPCs failed. Try ProtonVPN.")
    sys.exit(1)

usdc = w3.eth.contract(address=USDC, abi=ERC20_ABI)
balance = usdc.functions.balanceOf(OLD_ADDR).call()
matic   = w3.eth.get_balance(OLD_ADDR) / 1e18

print(f"\nOld wallet USDC:  ${balance / 1e6:.4f}")
print(f"Old wallet MATIC: {matic:.6f}")

if balance == 0:
    print("\nNothing to sweep.")
    sys.exit(0)

if matic < 0.001:
    print(f"\nNeed MATIC for gas — send ~0.01 MATIC to {OLD_ADDR} first.")
    sys.exit(1)

print(f"\nSweeping ${balance / 1e6:.4f} USDC → {NEW_ADDR}")

nonce = w3.eth.get_transaction_count(OLD_ADDR)
tx = usdc.functions.transfer(NEW_ADDR, balance).build_transaction({
    "from":     OLD_ADDR,
    "nonce":    nonce,
    "gasPrice": w3.eth.gas_price,
    "gas":      100_000,
    "chainId":  137,
})
signed  = w3.eth.account.sign_transaction(tx, OLD_KEY)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"TX sent: {tx_hash.hex()}")
receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
print(f"Status: {'OK' if receipt['status'] == 1 else 'FAILED'} — block {receipt['blockNumber']}")
