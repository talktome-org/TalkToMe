import base64
import hashlib
import hmac
import os
import time
import uuid
from typing import Optional


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def get_file_signing_secret() -> bytes:
    raw: Optional[str] = os.getenv("FILE_SIGNING_SECRET")
    if not raw:
        raise RuntimeError("FILE_SIGNING_SECRET must be set.")
    return raw.encode("utf-8")


def sign_upload_id(*, upload_id: uuid.UUID, exp: int) -> str:
    secret = get_file_signing_secret()
    msg = f"{upload_id}:{exp}".encode("utf-8")
    digest = hmac.new(secret, msg, hashlib.sha256).digest()
    return _b64url(digest)


def verify_upload_sig(*, upload_id: uuid.UUID, exp: int, sig: str) -> bool:
    if exp < int(time.time()):
        return False
    expected = sign_upload_id(upload_id=upload_id, exp=exp)
    return hmac.compare_digest(expected, sig or "")

