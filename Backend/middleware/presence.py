"""Lightweight presence middleware -- updates profiles.last_seen_at on authenticated requests.

Throttled: at most one DB write per user per 60 seconds (in-memory LRU).
Fire-and-forget: the update runs in a background thread; it never blocks the response.
"""

from __future__ import annotations

import base64
import json
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from Backend.database import SessionLocal

_THROTTLE_SECONDS = 60
_MAX_CACHE_SIZE = 4096
_last_touch: OrderedDict[str, float] = OrderedDict()
_lock = threading.Lock()


def _should_touch(user_id_str: str) -> bool:
    now = time.monotonic()
    with _lock:
        prev = _last_touch.get(user_id_str)
        if prev is not None and (now - prev) < _THROTTLE_SECONDS:
            return False
        _last_touch[user_id_str] = now
        _last_touch.move_to_end(user_id_str)
        while len(_last_touch) > _MAX_CACHE_SIZE:
            _last_touch.popitem(last=False)
    return True


def _touch_last_seen(user_id: uuid.UUID) -> None:
    db = SessionLocal()
    try:
        from Backend.models.profile.profile_model import Profile

        row = db.get(Profile, user_id)
        if row is not None:
            row.last_seen_at = datetime.now(timezone.utc).replace(tzinfo=None)
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


class PresenceMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        # Skip presence endpoint — the explicit online/offline signal manages
        # its own last_seen_at; touching it here would defeat staleness checks.
        if request.url.path.rstrip("/").endswith("/presence"):
            return response

        if response.status_code < 200 or response.status_code >= 400:
            return response

        auth_header = request.headers.get("authorization", "")
        if not auth_header.lower().startswith("bearer "):
            return response

        token = auth_header.split(" ", 1)[1]
        try:
            payload_segment = token.split(".")[1]
            payload_segment += "=" * (-len(payload_segment) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_segment))
            user_id_str = payload.get("sub", "")
            user_id = uuid.UUID(user_id_str)
        except Exception:
            return response

        if _should_touch(user_id_str):
            threading.Thread(target=_touch_last_seen, args=(user_id,), daemon=True).start()

        return response
