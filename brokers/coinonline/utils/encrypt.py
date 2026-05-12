import re

def rc4_encrypt(key: bytes, data: bytes) -> bytes:
    s = list(range(256))
    j = 0
    key_len = len(key)
    for i in range(256):
        j = (j + s[i] + key[i % key_len]) % 256
        s[i], s[j] = s[j], s[i]
    i = j = 0
    out = bytearray()
    for byte in data:
        i = (i + 1) % 256
        j = (j + s[i]) % 256
        s[i], s[j] = s[j], s[i]
        k = s[(s[i] + s[j]) % 256]
        out.append(byte ^ k)
    return bytes(out)


def rc4_decrypt(key: bytes, data: bytes) -> bytes:
    return rc4_encrypt(key, data)


def hex_encode(data: bytes) -> str:
    return "".join(f"{b:02x} " for b in data)


def hex_decode(text: str):
    if not re.fullmatch(r"[a-f0-9]*", text, re.I):
        return False
    if len(text) % 2 == 1:
        text = "0" + text
    return bytes.fromhex(text)

def encrypt_password(hash_code: str, password: str | None = None, verify_code: str| None = None):
    key = hash_code.encode("latin1")
    pwd_enc = hex_encode(rc4_encrypt(key, password.encode("latin1"))) if password else None
    ver_enc = hex_encode(rc4_encrypt(key, verify_code.encode("latin1"))) if verify_code else None
    return {"pwd_enc": pwd_enc, "ver_enc": ver_enc}
