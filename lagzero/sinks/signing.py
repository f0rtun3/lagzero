from __future__ import annotations

import hashlib
import hmac


def build_signature(secret: str, *, timestamp: str, raw_body: str) -> str:
    payload = f"{timestamp}.{raw_body}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return f"sha256={digest}"
