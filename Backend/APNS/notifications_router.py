import uuid
from fastapi import APIRouter, Depends, HTTPException

from ..auth import get_current_user
from ..Database.device_tokens_repo import upsert_token, disable_token_by_value
from ..Database.preferences_repo import (
    get_daily_checkins_preference,
    set_daily_checkins_preference,
    list_users_with_daily_checkins_enabled,
)
from ..Database.supabase_client import supabase
from .apns import send_daily_checkin_notification_to_user
import random
from datetime import datetime, date
from zoneinfo import ZoneInfo  # Python 3.9+


router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.post("/register")
async def register_token(payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    token = payload.get("token")
    platform = payload.get("platform") or "ios"
    bundle_id = payload.get("bundle_id")
    if not token or not isinstance(token, str) or len(token) < 10:
        raise HTTPException(status_code=400, detail="Missing or invalid device token")

    try:
        await upsert_token(user_id=user_uuid, token=token, platform=platform, bundle_id=bundle_id)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unregister")
async def unregister_token(payload: dict, current_user: dict = Depends(get_current_user)):
    # We only require token; ownership is implied by client choice, but no leak occurs.
    token = payload.get("token")
    if not token or not isinstance(token, str):
        raise HTTPException(status_code=400, detail="Missing token")
    try:
        await disable_token_by_value(token=token)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/daily-checkins")
async def get_daily_checkins(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")
    try:
        pref = await get_daily_checkins_preference(user_id=user_uuid)
        return {"enabled": pref.get("enabled", False), "hour": pref.get("hour"), "minute": pref.get("minute")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/daily-checkins")
async def set_daily_checkins(payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")
    enabled = bool(payload.get("enabled"))
    hour = payload.get("hour")
    minute = payload.get("minute")
    timezone = payload.get("timezone")
    # Validate optional time if provided
    if hour is not None:
        try:
            hour = int(hour)
            if hour < 0 or hour > 23:
                raise ValueError()
        except Exception:
            raise HTTPException(status_code=400, detail="hour must be 0-23")
    if minute is not None:
        try:
            minute = int(minute)
            if minute < 0 or minute > 59:
                raise ValueError()
        except Exception:
            raise HTTPException(status_code=400, detail="minute must be 0-59")
    # Validate timezone if provided
    if timezone is not None:
        try:
            _ = ZoneInfo(str(timezone))
        except Exception:
            raise HTTPException(status_code=400, detail="invalid timezone")
    try:
        await set_daily_checkins_preference(user_id=user_uuid, enabled=enabled, hour=hour, minute=minute, timezone=timezone)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Shared fanout worker (callable by cron endpoint and in-app scheduler)
async def send_daily_checkins_for_now() -> int:
    messages = [
        "Hi [person_name], how’s your day going?",
        "Hey [person_name], how are you feeling today?",
        "Hi [person_name], I hope everything’s okay. I’m here if you want to talk.",
        "Hey [person_name], anything hard on your mind today?",
        "Hi [person_name], want to share anything with me today?",
    ]
    try:
        enabled_rows = await list_users_with_daily_checkins_enabled()
    except Exception:
        return 0

    # Default time if user-specific hour/minute not set
    default_hour = 10
    default_minute = 0

    def _get_full_name_for_user(user_id: str) -> str:
        try:
            res = supabase.table("profiles").select("full_name").eq("user_id", user_id).limit(1).execute()
            if not getattr(res, "error", None) and res.data:
                name = (res.data[0].get("full_name") or "").strip()
                if name:
                    return name
        except Exception:
            pass
        try:
            admin = supabase.auth.admin.get_user_by_id(user_id)  # type: ignore[attr-defined]
            user = getattr(admin, "user", None) or getattr(admin, "data", None)
            meta = (user.get("user_metadata") if isinstance(user, dict) else getattr(user, "user_metadata", None)) or {}
            if isinstance(meta, dict):
                raw = (meta.get("full_name") or meta.get("name") or meta.get("display_name") or "").strip()
                if raw:
                    # Use only first token from auth metadata (no last name)
                    first = raw.split()[0].strip()
                    if first:
                        return first
        except Exception:
            pass
        return "there"

    sent = 0
    for row in enabled_rows:
        try:
            uid_str = row.get("user_id")
            if not uid_str:
                continue
            tz_name = row.get("timezone") or "America/Los_Angeles"
            try:
                local_now = datetime.now(ZoneInfo(tz_name))
            except Exception:
                local_now = datetime.now(ZoneInfo("America/Los_Angeles"))
            # Use per-user preferred time if present; else default to 10:00
            user_hour = row.get("daily_checkin_hour")
            user_minute = row.get("daily_checkin_minute")
            target_hour = int(user_hour) if isinstance(user_hour, (int, float)) else default_hour
            target_minute = int(user_minute) if isinstance(user_minute, (int, float)) else default_minute
            if not (local_now.hour == target_hour and local_now.minute == target_minute):
                continue
            # Idempotency (relaxed): if already sent today, skip; otherwise send then update best-effort
            today_str = date.today().isoformat()
            try:
                sel = (
                    supabase
                    .table("user_preferences")
                    .select("last_checkin_sent_date")
                    .eq("user_id", uid_str)
                    .limit(1)
                    .execute()
                )
                last = None
                if getattr(sel, "data", None):
                    last = sel.data[0].get("last_checkin_sent_date")
                if last == today_str:
                    continue
            except Exception:
                # If we can't read, proceed to send to avoid silent drops
                pass
            user_uuid = uuid.UUID(uid_str)
            name = _get_full_name_for_user(uid_str)
            template = random.choice(messages)
            body = template.replace("[person_name]", name)
            await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)
            # Best-effort: mark as sent today (do not block send outcome)
            try:
                _ = (
                    supabase
                    .table("user_preferences")
                    .update({"last_checkin_sent_date": today_str})
                    .eq("user_id", uid_str)
                    .execute()
                )
            except Exception:
                pass
            sent += 1
        except Exception:
            continue
    return sent


# External trigger removed: daily check-ins are scheduled and sent automatically by the app on startup.



