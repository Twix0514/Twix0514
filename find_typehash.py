"""
Brute-force the EIP-712 Order type string for the new Polymarket CTF Exchange.

Real tx: 0xfde5a7215fac86ef005cbb67a45f6c0abcc4e8810a769d31701da26ec65c0a95
Known signer: 0xdf328efc1c64605cbdbd2fa50592bce0688f8f62
"""
from eth_utils import keccak
from eth_abi import encode

SIGNER     = "0xdf328efc1c64605cbdbd2fa50592bce0688f8f62"
SIGNATURE  = bytes.fromhex(
    "02b5c2ed3fbdaa83b18ecf9f8f5a4429b3b4235d1ae2159ab27d487968ad538d"
    "6514737c572d43418983422d5828941d456fa1e2b19543ca6ff393fe25009a701b"
)

SALT       = 335113571253
MAKER      = "0xdf328efc1c64605cbdbd2fa50592bce0688f8f62"
TOKEN_ID   = 83795385488674421748086320696200611459990224764287022571066140510571460611340
MAKER_AMT  = 3900000
TAKER_AMT  = 5000000
SIDE       = 0
SIG_TYPE   = 0
EXPIRATION = 1779229869823

EXCHANGE   = "0xE111180000d2663C0091e4f400237545B87B996B"
CHAIN_ID   = 137

ZERO_ADDR  = "0x0000000000000000000000000000000000000000"


def make_domain_sep(name, version, chain_id, contract):
    type_hash = keccak(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
    return keccak(encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [type_hash, keccak(name.encode()), keccak(version.encode()), chain_id, contract]
    ))


def make_struct_hash(type_string, field2_addr):
    typehash = keccak(type_string.encode())
    encoded = encode(
        ["bytes32","uint256","address","address","uint256","uint256","uint256","uint8","uint8","uint256"],
        [typehash, SALT, MAKER, field2_addr, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
    )
    return keccak(encoded)


def recover_signer(domain_sep, struct_hash):
    msg_hash = keccak(b"\x19\x01" + domain_sep + struct_hash)
    sig = SIGNATURE
    r = int.from_bytes(sig[0:32], "big")
    s = int.from_bytes(sig[32:64], "big")
    v = sig[64]
    from eth_keys import keys
    from eth_keys.exceptions import BadSignature
    try:
        recovery_id = v - 27 if v >= 27 else v
        sig_obj = keys.Signature(vrs=(recovery_id, r, s))
        pk = sig_obj.recover_public_key_from_msg_hash(msg_hash)
        return pk.to_checksum_address()
    except Exception:
        return "INVALID"


field0_names = ["salt", "nonce", "id", "orderId"]
versions     = ["1", "2"]
field2_names = ["signer", "taker", "filler"]
taker_addrs  = [MAKER, ZERO_ADDR]

print("Target signer:", SIGNER)
print()

found = False
for version in versions:
    ds = make_domain_sep("Polymarket CTF Exchange", version, CHAIN_ID, EXCHANGE)
    print(f"Domain sep v{version}: 0x{ds.hex()}")
    for f0 in field0_names:
        for f2name in field2_names:
            for f2addr in taker_addrs:
                ts = (
                    f"Order(uint256 {f0},address maker,address {f2name},"
                    f"uint256 tokenId,uint256 makerAmount,uint256 takerAmount,"
                    f"uint8 side,uint8 signatureType,uint256 expiration)"
                )
                sh = make_struct_hash(ts, f2addr)
                rec = recover_signer(ds, sh)
                addr_tag = "EOA" if f2addr == MAKER else "0x0"
                if rec.lower() == SIGNER.lower():
                    print("="*60)
                    print("MATCH FOUND!")
                    print(f"  version:     {version}")
                    print(f"  field2_addr: {addr_tag}")
                    print(f"  type_string: {ts}")
                    print(f"  typehash:    0x{keccak(ts.encode()).hex()}")
                    print(f"  struct_hash: 0x{sh.hex()}")
                    print(f"  recovered:   {rec}")
                    print("="*60)
                    found = True
                else:
                    print(f"  v={version} f0={f0:7} f2={f2name:6} addr={addr_tag:3} -> {rec[:22]}...")

if not found:
    print()
    print("No match. Trying additional field name variants...")

    # Maybe expiration field has a different name but same value,
    # or maybe there are extra/missing fields
    extra_types = [
        # Without signer/taker field (8 fields)
        "Order(uint256 salt,address maker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 expiration)",
        # With 'nonce' as expiration
        "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 nonce)",
    ]
    for version in versions:
        ds = make_domain_sep("Polymarket CTF Exchange", version, CHAIN_ID, EXCHANGE)
        for ts in extra_types:
            # Build struct hash for 8-field version
            if "address signer" not in ts and "address taker" not in ts and "address filler" not in ts:
                typehash = keccak(ts.encode())
                encoded = encode(
                    ["bytes32","uint256","address","uint256","uint256","uint256","uint8","uint8","uint256"],
                    [typehash, SALT, MAKER, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
                )
                sh = keccak(encoded)
            else:
                sh = make_struct_hash(ts, MAKER)
            rec = recover_signer(ds, sh)
            if rec.lower() == SIGNER.lower():
                print("MATCH FOUND!")
                print(f"  version:     {version}")
                print(f"  type_string: {ts}")
                print(f"  typehash:    0x{keccak(ts.encode()).hex()}")
                print(f"  recovered:   {rec}")
                found = True
            else:
                print(f"  v={version} {ts[:50]}... -> {rec[:22]}...")

if not found:
    print()
    print("Still no match. Dumping domain separators for reference:")
    for v in ["1", "2"]:
        ds = make_domain_sep("Polymarket CTF Exchange", v, CHAIN_ID, EXCHANGE)
        print(f"  v={v}: 0x{ds.hex()}")
    print()
    print("Previously confirmed domain sep (v=2): 0x3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2")
