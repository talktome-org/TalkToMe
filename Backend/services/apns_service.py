import json
import os
import time
import uuid
from typing import Any, Dict, Optional

import httpx
import jwt

from ..crud.apns.device_tokens_crud import disable_token_by_value, list_tokens_for_user


_cached_jwt_token: Optional[str] = None
_cached_jwt_exp: float = 0.0


def _load_apns_auth_key_pem() -> str:
    key_b64 = os.getenv("APNS_AUTH_KEY_BASE64")
    if key_b64:
        import base64

        pem_bytes = base64.b64decode(key_b64)
        return pem_bytes.decode("utf-8")
    raise RuntimeError("Missing APNS_AUTH_KEY_BASE64 env var for APNs auth key")


def _get_apns_jwt_token() -> str:
    global _cached_jwt_token, _cached_jwt_exp

    now = time.time()
    if _cached_jwt_token and now < (_cached_jwt_exp - 300):
        return _cached_jwt_token

    team_id = os.getenv("APPLE_TEAM_ID")
    key_id = os.getenv("APNS_KEY_ID")
    if not team_id or not key_id:
        raise RuntimeError("Missing APPLE_TEAM_ID or APNS_KEY_ID")

    private_key_pem = _load_apns_auth_key_pem()

    headers = {"alg": "ES256", "kid": key_id}
    payload = {"iss": team_id, "iat": int(now), "exp": int(now + 3600)}
    token = jwt.encode(payload, private_key_pem, algorithm="ES256", headers=headers)  # type: ignore[no-untyped-call]

    _cached_jwt_token = token
    _cached_jwt_exp = now + 55 * 60
    return token


async def _post_apns(device_token: str, payload: Dict[str, Any]) -> tuple[int, str]:
    prefer_sandbox = os.getenv("APNS_USE_SANDBOX", "true").lower() == "true"
    hosts_order = (
        ["api.sandbox.push.apple.com", "api.push.apple.com"]
        if prefer_sandbox
        else ["api.push.apple.com", "api.sandbox.push.apple.com"]
    )

    bundle_id = os.getenv("BUNDLE_ID")
    if not bundle_id:
        raise RuntimeError("Missing BUNDLE_ID")

    auth_token = _get_apns_jwt_token()

    def _headers() -> Dict[str, str]:
        return {
            "authorization": f"bearer {auth_token}",
            "apns-topic": bundle_id,
            "apns-push-type": "alert",
            "apns-priority": "10",
            "content-type": "application/json",
        }

    last_status = 0
    last_text = ""

    for idx, host in enumerate(hosts_order):
        url = f"https://{host}/3/device/{device_token}"
        async with httpx.AsyncClient(http2=True, timeout=10.0) as client:
            resp = await client.post(url, headers=_headers(), content=json.dumps(payload))
            last_status = resp.status_code
            last_text = resp.text

        if last_status == 200:
            return last_status, last_text
        if idx == 0 and last_status in (400, 410) and ("BadDeviceToken" in (last_text or "")):
            continue
        break

    return last_status, last_text


async def send_friend_added_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    sender_name: Optional[str] = None,
) -> None:
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    if not tokens:
        return

    name = (sender_name or "").strip() or "Someone"
    aps = {
        "alert": {"title": "New Friend", "body": f"{name} added you as a friend!"},
        "sound": "default",
        "category": "FRIEND_ADDED",
    }
    payload = {"aps": aps, "type": "friend_added"}

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = t.get("enabled", True) if isinstance(t, dict) else True
        if not token_val or not enabled:
            continue
        try:
            status, resp_text = await _post_apns(device_token=token_val, payload=payload)
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
        except Exception:
            continue


async def send_partner_message_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    session_id: uuid.UUID,
    preview: str,
    sender_name: Optional[str] = None,
) -> None:
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    if not tokens:
        return

    title = sender_name or "Partner Message"
    body = preview or "Sent you a message."
    aps = {
        "alert": {"title": title, "body": body},
        "sound": "default",
        "category": "PARTNER_MESSAGE",
    }
    payload = {"aps": aps, "session_id": str(session_id)}

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = t.get("enabled", True) if isinstance(t, dict) else True
        if not token_val or not enabled:
            continue
        try:
            status, resp_text = await _post_apns(device_token=token_val, payload=payload)
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
        except Exception:
            continue