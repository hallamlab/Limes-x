from typing import Any

HEADER_SIZE = 2
BUFFER_SIZE = 256**HEADER_SIZE

class Transaction:
    def __init__(self, payload: Any, key: str) -> None:
        self.payload = payload
        self.key = key

def Pad(payload: bytes):
    lp = len(payload)-1
    payload = lp.to_bytes(HEADER_SIZE) + payload
    return payload.ljust(BUFFER_SIZE, b'\0')

def Unpad(raw: bytes):
    end = int.from_bytes(raw[:HEADER_SIZE])+1 + HEADER_SIZE
    return raw[HEADER_SIZE:end]