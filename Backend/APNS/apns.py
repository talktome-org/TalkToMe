import os
import time
import json
import uuid
from typing import Any, Dict, Optional

import httpx
import jwt

from ..Database.device_tokens_repo import list_tokens_for_user, disable_token_by_value


_cached_jwt_token: Optional[str] = None
_cached_jwt_exp: float = 0.0
DAILY_CHECKIN_TITLE = "Check-In"


def _load_apns_auth_key_pem() -> str:
    key_b64 = os.getenv("APNS_AUTH_KEY_BASE64")
    key_path = os.getenv("APNS_KEY_PATH")
    if key_b64:
        try:
            import base64
            pem_bytes = base64.b64decode(key_b64)
            return pem_bytes.decode("utf-8")
        except Exception as e:
            raise RuntimeError(f"Failed to decode APNS_AUTH_KEY_BASE64: {e}")
    if key_path:
        try:
            with open(key_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            raise RuntimeError(f"Failed to read APNS_KEY_PATH: {e}")
    raise RuntimeError("Missing APNS_AUTH_KEY_BASE64 or APNS_KEY_PATH env var for APNs auth key")


def _get_apns_jwt_token() -> str:
    global _cached_jwt_token, _cached_jwt_exp

    now = time.time()
    # Refresh if less than 5 minutes left or missing
    if _cached_jwt_token and now < (_cached_jwt_exp - 300):
        return _cached_jwt_token

    team_id = os.getenv("APNS_TEAM_ID")
    key_id = os.getenv("APNS_KEY_ID")
    if not team_id or not key_id:
        raise RuntimeError("Missing APNS_TEAM_ID or APNS_KEY_ID")

    private_key_pem = _load_apns_auth_key_pem()

    # Debug logging
    print(f"[APNs JWT] Creating token with team_id={team_id} key_id={key_id}")

    headers = {"alg": "ES256", "kid": key_id}
    payload = {
        "iss": team_id,
        "iat": int(now),
        "exp": int(now + 3600)  # Add explicit expiration
    }

    try:
        token = jwt.encode(payload, private_key_pem, algorithm="ES256", headers=headers)  # type: ignore[no-untyped-call]
    except Exception as e:
        print(f"[APNs JWT] Failed to encode JWT: {e}")
        raise

    # APNs allows up to 1 hour; cache for 55 minutes
    _cached_jwt_token = token
    _cached_jwt_exp = now + 55 * 60
    return token


async def _post_apns(device_token: str, payload: Dict[str, Any]) -> tuple[int, str]:
    """Send to APNs with automatic environment fallback.

    Primary host is chosen via APNS_USE_SANDBOX; on 400 BadDeviceToken we retry the other host.
    This allows a single backend to serve both developer (sandbox) and TestFlight (production) devices.
    """
    prefer_sandbox = (os.getenv("APNS_USE_SANDBOX", "true").lower() == "true")
    hosts_order = [
        "api.sandbox.push.apple.com",
        "api.push.apple.com",
    ] if prefer_sandbox else [
        "api.push.apple.com",
        "api.sandbox.push.apple.com",
    ]

    bundle_id = os.getenv("APNS_BUNDLE_ID") or os.getenv("AASA_BUNDLE_ID")
    if not bundle_id:
        raise RuntimeError("Missing APNS_BUNDLE_ID (and AASA_BUNDLE_ID fallback)")

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

    # Try primary, then conditionally fall back on BadDeviceToken
    for idx, host in enumerate(hosts_order):
        url = f"https://{host}/3/device/{device_token}"
        async with httpx.AsyncClient(http2=True, timeout=10.0) as client:
            try:
                host_label = "sandbox" if host.startswith("api.sandbox") else "prod"
                print(f"[APNs] POST host={host_label} topic={bundle_id} token={device_token[:10]}… payload_keys={list(payload.keys())}")
                auth_header = _headers().get("authorization", "")
                print(f"[APNs] Auth header length: {len(auth_header)} chars")
            except Exception:
                pass
            resp = await client.post(url, headers=_headers(), content=json.dumps(payload))
            last_status = resp.status_code
            last_text = resp.text

        # Success: stop here
        if last_status == 200:
            return last_status, last_text

        # If first attempt returned BadDeviceToken, try the other environment
        if idx == 0 and last_status in (400, 410) and ("BadDeviceToken" in (last_text or "")):
            try:
                print("[APNs] BadDeviceToken on primary host; attempting fallback host…")
            except Exception:
                pass
            continue

        # Otherwise, don't fall back (either already tried fallback, or error type not suitable)
        break

    return last_status, last_text


async def send_partner_request_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    request_id: uuid.UUID,
    relationship_id: uuid.UUID,
    preview: str,
    sender_name: Optional[str] = None,
) -> None:
    """Send an APNs alert to all active tokens for the recipient user.

    Keeps payload tiny; app routes using request_id.
    """
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    if not tokens:
        return

    title = "Partner Request"

    aps = {
        "alert": {"title": title, "body": "Your partner has sent you a request. Tap to open."},
        "sound": "default",
        "category": "PARTNER_REQUEST",
    }
    payload = {
        "aps": aps,
        "request_id": str(request_id),
        "relationship_id": str(relationship_id),
    }

    # Summary log before fanout
    try:
        print(f"[Push] PartnerRequest notify recipient={recipient_user_id} tokens={len(tokens)} request_id={request_id}")
        # Log first token shape for debugging
        sample = tokens[0] if tokens else None
        if isinstance(sample, dict):
            print(f"[Push] sample token keys={list(sample.keys())}")
        else:
            print(f"[Push] sample token type={type(sample)}")
    except Exception:
        pass

    # Fire-and-forget to each device; do not raise if one fails.
    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = (t.get("enabled", True) if isinstance(t, dict) else True)
        if not token_val or not enabled:
            try:
                print(f"[APNs] skip token entry enabled={enabled} keys={list(t.keys()) if isinstance(t, dict) else 'n/a'}")
            except Exception:
                pass
            continue
        try:
            status, resp_text = await _post_apns(device_token=token_val, payload=payload)
            # Basic diagnostics
            if status != 200:
                try:
                    print(f"[APNs] send token={token_val[:10]}… status={status} body={resp_text}")
                except Exception:
                    pass
                # Token is invalid or app uninstalled
                if status in (400, 410) and ("BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")):
                    try:
                        await disable_token_by_value(token=token_val)
                    except Exception:
                        pass
            else:
                try:
                    print(f"[APNs] send OK token={token_val[:10]}… status=200")
                except Exception:
                    pass
        except Exception as e:
            # Avoid crashing the request handler on APNs failure, but log details
            try:
                print(f"[APNs] send exception token={token_val[:10]}… err={e}")
            except Exception:
                pass
            continue


async def send_partner_message_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    session_id: uuid.UUID,
    preview: str,
    sender_name: Optional[str] = None,
) -> None:
    """Send an APNs alert when a new partner message is delivered directly to a linked personal session.

    Payload carries only the session_id to route the user directly into that chat.
    """
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    if not tokens:
        return

    title = "Partner Message"
    body = "Your partner has sent a new message. Tap to open."

    aps = {
        "alert": {"title": title, "body": body},
        "sound": "default",
        "category": "PARTNER_MESSAGE",
    }
    payload = {
        "aps": aps,
        "session_id": str(session_id),
    }

    try:
        print(f"[Push] PartnerMessage notify recipient={recipient_user_id} tokens={len(tokens)} session_id={session_id}")
    except Exception:
        pass

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = (t.get("enabled", True) if isinstance(t, dict) else True)
        if not token_val or not enabled:
            try:
                print(f"[APNs] skip token entry enabled={enabled} keys={list(t.keys()) if isinstance(t, dict) else 'n/a'}")
            except Exception:
                pass
            continue


async def send_daily_checkin_notification_to_user(
    *,
    recipient_user_id: uuid.UUID,
    body: str,
) -> None:
    """Send a friendly daily check-in APNs alert to all active tokens for the recipient user."""
    tokens = await list_tokens_for_user(user_id=recipient_user_id)
    if not tokens:
        return

    aps = {
        "alert": {"title": DAILY_CHECKIN_TITLE, "body": body},
        "sound": "default",
        "category": "DAILY_CHECKIN",
    }
    payload = {
        "aps": aps,
        "kind": "daily_checkin",
    }

    for t in tokens:
        token_val = t.get("token") if isinstance(t, dict) else None
        enabled = (t.get("enabled", True) if isinstance(t, dict) else True)
        if not token_val or not enabled:
            continue
        try:
            status, resp_text = await _post_apns(device_token=token_val, payload=payload)
            if status != 200:
                try:
                    print(f"[APNs] send token={token_val[:10]}… status={status} body={resp_text}")
                except Exception:
                    pass
                if status in (400, 410) and ("BadDeviceToken" in (resp_text or "") or "Unregistered" in (resp_text or "")):
                    try:
                        await disable_token_by_value(token=token_val)
                    except Exception:
                        pass
            else:
                try:
                    print(f"[APNs] send OK token={token_val[:10]}… status=200")
                except Exception:
                    pass
        except Exception as e:
            try:
                print(f"[APNs] send exception token={token_val[:10]}… err={e}")
            except Exception:
                pass
            continue





