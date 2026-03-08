import json
import logging
import os
import random
import time
import uuid
from typing import Any, Dict, Optional

import httpx
import jwt

from ..crud.apns.device_tokens_crud import disable_token_by_value, list_tokens_for_user

DAILY_CHECKIN_MESSAGES = [
    "You've been on my mind. How are you really doing today?",
    "Just checking in — you matter, and I'm here whenever you need to talk.",
    "Hey, I hope today treated you well. Want to share what's on your mind?",
    "Remember, it's okay to not be okay. I'm here if you need someone to listen.",
    "You're doing better than you think. Want to reflect on your day together?",
]

logger = logging.getLogger(__name__)

_cached_jwt_token: Optional[str] = None
_cached_jwt_exp: float = 0.0

# Persistent HTTP/2 client reused across all APNS requests.
_apns_client: Optional[httpx.AsyncClient] = None


def _get_apns_client() -> httpx.AsyncClient:
    global _apns_client
    if _apns_client is None or _apns_client.is_closed:
        _apns_client = httpx.AsyncClient(http2=True, timeout=15.0)
    return _apns_client


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


async def _post_apns(
    device_token: str,
    payload: Dict[str, Any],
    *,
    push_type: str = "alert",
    priority: str = "10",
) -> tuple[int, str]:
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
            "apns-push-type": push_type,
            "apns-priority": priority,
            "content-type": "application/json",
        }

    last_status = 0
    last_text = ""
    client = _get_apns_client()

    for idx, host in enumerate(hosts_order):
        url = f"https://{host}/3/device/{device_token}"
        resp = await client.post(url, headers=_headers(), content=json.dumps(payload))
        last_status = resp.status_code
        last_text = resp.text

        if last_status == 200:
            return last_status, last_text
        if idx == 0 and last_status in (400, 410) and ("BadDeviceToken" in (last_text or "")):
            logger.info("[APNS] BadDeviceToken on %s, falling back to next host", host)
            continue
        break

    return last_status, last_text


async def send_friend_added_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    sender_name: Optional[str] = None,
) -> None:
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    logger.info("[APNS] friend_added -> user %s, %d token(s)", recipient_user_id, len(tokens))
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
            logger.info("[APNS] friend_added status=%d resp=%s", status, resp_text[:200] if resp_text else "")
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
                logger.warning("[APNS] Disabled stale token: %s…", token_val[:12])
        except Exception as exc:
            logger.exception("[APNS] Failed to send friend_added push: %s", exc)
            continue


async def send_partner_message_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    session_id: uuid.UUID,
    preview: str,
    sender_name: Optional[str] = None,
) -> None:
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    logger.info("[APNS] partner_message -> user %s, %d token(s)", recipient_user_id, len(tokens))
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
            logger.info("[APNS] partner_message status=%d resp=%s", status, resp_text[:200] if resp_text else "")
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
                logger.warning("[APNS] Disabled stale token: %s…", token_val[:12])
        except Exception as exc:
            logger.exception("[APNS] Failed to send partner_message push: %s", exc)
            continue


async def send_unsend_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Send a silent content-available push so the recipient's app can
    retract the delivered notification and refresh its message list."""
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    logger.info("[APNS] partner_unsend -> user %s, %d token(s)", recipient_user_id, len(tokens))
    if not tokens:
        return

    payload = {
        "aps": {"content-available": 1},
        "type": "partner_unsend",
        "session_id": str(session_id),
    }

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = t.get("enabled", True) if isinstance(t, dict) else True
        if not token_val or not enabled:
            continue
        try:
            status, resp_text = await _post_apns(
                device_token=token_val,
                payload=payload,
                push_type="background",
                priority="5",
            )
            logger.info("[APNS] partner_unsend status=%d resp=%s", status, resp_text[:200] if resp_text else "")
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
                logger.warning("[APNS] Disabled stale token: %s…", token_val[:12])
        except Exception as exc:
            logger.exception("[APNS] Failed to send partner_unsend push: %s", exc)
            continue


async def send_daily_checkin_notification(*, user_id: uuid.UUID) -> None:
    """Send a randomized supportive check-in push notification to a user."""
    tokens = await list_tokens_for_user(user_id=user_id)
    logger.info("[APNS] daily_checkin -> user %s, %d token(s)", user_id, len(tokens))
    if not tokens:
        return

    message = random.choice(DAILY_CHECKIN_MESSAGES)
    aps = {
        "alert": {"title": "TalkToMe", "body": message},
        "sound": "default",
        "category": "DAILY_CHECKIN",
    }
    payload = {"aps": aps, "type": "daily_checkin"}

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = t.get("enabled", True) if isinstance(t, dict) else True
        if not token_val or not enabled:
            continue
        try:
            status, resp_text = await _post_apns(device_token=token_val, payload=payload)
            logger.info("[APNS] daily_checkin status=%d resp=%s", status, resp_text[:200] if resp_text else "")
            if status != 200 and status in (400, 410) and (
                "BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")
            ):
                await disable_token_by_value(token=token_val)
                logger.warning("[APNS] Disabled stale token: %s…", token_val[:12])
        except Exception as exc:
            logger.exception("[APNS] Failed to send daily_checkin push: %s", exc)
            continue
