"""
Use known PUSH32 constants from bytecode as typehash candidates.
Try each against the real transaction signature to find the ORDER_TYPEHASH.
"""
from eth_utils import keccak
from eth_abi import encode
from eth_keys import keys
from eth_keys.exceptions import BadSignature

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
ZERO_ADDR  = "0x0000000000000000000000000000000000000000"

DOMAIN_SEP = bytes.fromhex("3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2")

# Typehash candidates from PUSH32 scan (excluding obvious non-hashes)
CANDIDATES = [
    "787a2e12f4a55b658b8f573c32432ee11a5e8b51677d1e1e937aaf6a0bb5776e",
    "f7262ed0443cc211121ceb1a80d69004f319245615a7488f951f1437fd91642c",
    "bb86318a2138f5fa8ae32fbe8e659f8fcf13cc6ae4014a707893055433818589",
    "1419d4111b5c8636aecff843bf618525f4f8e1aa6898a14357021d68dde8af12",
    "f9ffabca9c8276e99321725bcb43fb076a6c66a54b7f21c4e8146d8519b417dc",
    "2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf",
    "203c4bd3e526634f661575359ff30de3b0edaba6c2cb1eac60f730b6d2d9d536",
    "8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39400f",
    "f30041e9aac4c4d3a1481d2941dfb0a844a72040e9bbc79a810d1ec5b5d6c7af",
    "ad7c5bef027816a800da1736444fb58a807ef4c9603b7848673f7e3a68eb14a5",
    "b766aa470f20b094f26a9a14ea5bf63a60af51703c15776e2e739b6a0428adf6",
    "a1e8a54850dbd7f520bcc09f47bff152294b77b2081da545a7adf531b7ea283b",
    "f1e04d73c4304b5ff164f9d10c7473e2a1593b740674a6107975e2a7001c1e5c",
    "a3e76126f19eb25001b29726d2a9502b6377938633d2d6a955107dd442e7a14a",
    "e380d7c3967dd06cc7c01db8b17332a1d806fd18f63206dcbd12aaef455c7ff2",
    "8c8acf678b7cd311e3b5768c92794d63943684862fdea390856e14d9e2a9ef88",
    "e92c22722d9c284034b6c9f5aaec018edb3e593c0e084900b6b9d390a1182a0b",
    "27aae5db36d94179909d019ae0b1ac7c16d96d953148f63c0f6a0a9c8ead79ee",
    "174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c",
    "55bb3cade9d43b798a4fe5ffdd05024b2d7870df53920673bfc7e68047cd0ab1",
    "d543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",
]

def recover(domain_sep, struct_hash):
    msg_hash = keccak(b"\x19\x01" + domain_sep + struct_hash)
    sig = SIGNATURE
    r = int.from_bytes(sig[0:32], "big")
    s = int.from_bytes(sig[32:64], "big")
    v = sig[64]
    try:
        rid = v - 27 if v >= 27 else v
        sig_obj = keys.Signature(vrs=(rid, r, s))
        pk = sig_obj.recover_public_key_from_msg_hash(msg_hash)
        return pk.to_checksum_address()
    except Exception:
        return "INVALID"

# Field encoding variants to try with each typehash:
# Variant A: 9 fields (no taker): salt,maker,signer,tokenId,makerAmt,takerAmt,side,sigType,expiration
# Variant B: 10 fields (with taker=0x0): salt,maker,signer,taker,tokenId,makerAmt,takerAmt,side,sigType,expiration
# Variant C: 10 fields (with taker=maker): salt,maker,signer,taker=maker,tokenId,makerAmt,takerAmt,side,sigType,expiration
# Variant D: 12 fields (old format): salt,maker,signer,taker,tokenId,makerAmt,takerAmt,expiration,nonce,feeRateBps,side,sigType

VARIANTS = {
    "A_9f_notaker": lambda th: keccak(encode(
        ["bytes32","uint256","address","address","uint256","uint256","uint256","uint8","uint8","uint256"],
        [th, SALT, MAKER, MAKER, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
    )),
    "B_10f_taker0": lambda th: keccak(encode(
        ["bytes32","uint256","address","address","address","uint256","uint256","uint256","uint8","uint8","uint256"],
        [th, SALT, MAKER, MAKER, ZERO_ADDR, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
    )),
    "C_10f_takerM": lambda th: keccak(encode(
        ["bytes32","uint256","address","address","address","uint256","uint256","uint256","uint8","uint8","uint256"],
        [th, SALT, MAKER, MAKER, MAKER, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
    )),
    "D_12f_old": lambda th: keccak(encode(
        ["bytes32","uint256","address","address","address","uint256","uint256","uint256","uint256","uint256","uint256","uint8","uint8"],
        [th, SALT, MAKER, MAKER, ZERO_ADDR, TOKEN_ID, MAKER_AMT, TAKER_AMT, EXPIRATION, 0, 0, SIDE, SIG_TYPE]
    )),
    "E_9f_side256": lambda th: keccak(encode(
        ["bytes32","uint256","address","address","uint256","uint256","uint256","uint256","uint256","uint256"],
        [th, SALT, MAKER, MAKER, TOKEN_ID, MAKER_AMT, TAKER_AMT, SIDE, SIG_TYPE, EXPIRATION]
    )),
}

print(f"Testing {len(CANDIDATES)} typehash candidates x {len(VARIANTS)} field variants")
print(f"Target signer: {SIGNER}")
print()

found = False
for cand_hex in CANDIDATES:
    th = bytes.fromhex(cand_hex)
    for vname, struct_fn in VARIANTS.items():
        sh = struct_fn(th)
        rec = recover(DOMAIN_SEP, sh)
        if rec.lower() == SIGNER.lower():
            print("="*60)
            print("MATCH FOUND!")
            print(f"  typehash:  0x{cand_hex}")
            print(f"  variant:   {vname}")
            print(f"  recovered: {rec}")
            print("="*60)
            found = True
        # Also try with v1 domain sep just in case
        ds_v1 = bytes.fromhex("7a8baa91aed7bcbb077f7a55bb6f3daf9780be630ea763458e9ed10d72686f41")
        rec1 = recover(ds_v1, sh)
        if rec1.lower() == SIGNER.lower():
            print("="*60)
            print("MATCH (v1 domain)!")
            print(f"  typehash:  0x{cand_hex}")
            print(f"  variant:   {vname}")
            print(f"  recovered: {rec1}")
            print("="*60)
            found = True

if not found:
    print("No direct match. Trying with additional field orderings...")
    # Maybe the field order in the struct hash is different
    # Try: salt, maker, signer, tokenId, makerAmt, takerAmt, expiration, side, sigType (expiration before side)
    for cand_hex in CANDIDATES:
        th = bytes.fromhex(cand_hex)
        # Variant: expiration BEFORE side/sigType
        sh = keccak(encode(
            ["bytes32","uint256","address","address","uint256","uint256","uint256","uint256","uint8","uint8"],
            [th, SALT, MAKER, MAKER, TOKEN_ID, MAKER_AMT, TAKER_AMT, EXPIRATION, SIDE, SIG_TYPE]
        ))
        rec = recover(DOMAIN_SEP, sh)
        if rec.lower() == SIGNER.lower():
            print("MATCH! Expiration before side/sigType")
            print(f"  typehash: 0x{cand_hex}")
            print(f"  recovered: {rec}")
            found = True
        # Also try with taker=0x0 AND expiration before side
        sh2 = keccak(encode(
            ["bytes32","uint256","address","address","address","uint256","uint256","uint256","uint256","uint8","uint8"],
            [th, SALT, MAKER, MAKER, ZERO_ADDR, TOKEN_ID, MAKER_AMT, TAKER_AMT, EXPIRATION, SIDE, SIG_TYPE]
        ))
        rec2 = recover(DOMAIN_SEP, sh2)
        if rec2.lower() == SIGNER.lower():
            print("MATCH! taker=0 + expiration before side/sigType")
            print(f"  typehash: 0x{cand_hex}")
            print(f"  recovered: {rec2}")
            found = True

if not found:
    print("Still no match.")
    print()
    print("Checking known EIP712Domain typehash:")
    known_domain_th = keccak(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
    print(f"  EIP712Domain typehash: 0x{known_domain_th.hex()}")
    print(f"  In bytecode? {'8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39400f' == known_domain_th.hex()}")
