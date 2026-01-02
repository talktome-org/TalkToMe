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
from datetime import datetime, date, timezone
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
        return {"enabled": pref.get("enabled", False), "hour": pref.get("hour"), "minute": pref.get("minute"), "timezone": pref.get("timezone")}
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
        "Hi [person_name], how's your day going?",
        "Hey [person_name], how are you feeling today?",
        "Hi [person_name], I hope everything's okay. I'm here if you want to talk.",
        "Hey [person_name], anything hard on your mind today?",
        "Hi [person_name], want to share anything with me today?",
    ]
    try:
        enabled_rows = await list_users_with_daily_checkins_enabled()
        print(f"[DailyCheckin] Found {len(enabled_rows)} users with checkins enabled")
    except Exception as e:
        print(f"[DailyCheckin] Error fetching enabled users: {e}")
        return 0

    # Default time if user-specific hour/minute not set
    # Note: Daily check-ins are enabled by default for ALL users (NULL = enabled)
    default_hour = 17
    default_minute = 0

    def _get_first_name_for_user(user_id: str) -> str:
        """Get only the first name for more personal, casual notifications."""
        try:
            res = supabase.table("profiles").select("full_name").eq("user_id", user_id).limit(1).execute()
            if not getattr(res, "error", None) and res.data:
                name = (res.data[0].get("full_name") or "").strip()
                if name:
                    # Use only first name for notifications
                    first = name.split()[0].strip()
                    if first:
                        return first
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
    # TESTING: Uncomment to limit to specific user
    # enabled_rows = [r for r in enabled_rows if r.get("user_id") == "YOUR_USER_ID_HERE"]
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
            # Use per-user preferred time if present; else default to 17:00
            user_hour = row.get("daily_checkin_hour")
            user_minute = row.get("daily_checkin_minute")
            target_hour = int(user_hour) if isinstance(user_hour, (int, float)) else default_hour
            target_minute = int(user_minute) if isinstance(user_minute, (int, float)) else default_minute
            # Debug: log time comparison for each user
            print(f"[DailyCheckin] user={uid_str[:8]}… tz={tz_name} local={local_now.hour}:{local_now.minute:02d} target={target_hour}:{target_minute:02d}")
            if not (local_now.hour == target_hour and local_now.minute == target_minute):
                continue
            # Idempotency (relaxed): if already sent today, skip; otherwise send then update best-effort
            today_str = local_now.date().isoformat()
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
            name = _get_first_name_for_user(uid_str)
            template = random.choice(messages)
            body = template.replace("[person_name]", name)
            print(f"[DailyCheckin] SENDING to user={uid_str[:8]}… name={name} body={body[:40]}…")
            await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)
            # Best-effort: mark as sent today (do not block send outcome)
            try:
                upsert_data = {
                    "user_id": uid_str,
                    "last_checkin_sent_date": today_str,
                    "daily_checkins_enabled": True,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                # Preserve user's time preferences if they were used
                if isinstance(user_hour, (int, float)):
                    upsert_data["daily_checkin_hour"] = int(user_hour)
                if isinstance(user_minute, (int, float)):
                    upsert_data["daily_checkin_minute"] = int(user_minute)
                if tz_name and tz_name != "America/Los_Angeles":
                    upsert_data["timezone"] = tz_name

                _ = (
                    supabase
                    .table("user_preferences")
                    .upsert(upsert_data, on_conflict="user_id")
                    .execute()
                )
            except Exception:
                pass
            sent += 1
        except Exception:
            continue
    return sent


# External trigger removed: daily check-ins are scheduled and sent automatically by the app on startup.


# Debug endpoint to manually trigger daily check-in for testing
@router.post("/daily-checkins/test")
async def test_daily_checkin(current_user: dict = Depends(get_current_user)):
    """Send a test daily check-in notification to the current user immediately."""
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    messages = [
        "Hi [person_name], how's your day going?",
        "Hey [person_name], how are you feeling today?",
        "Hi [person_name], I hope everything's okay. I'm here if you want to talk.",
        "Hey [person_name], anything hard on your mind today?",
        "Hi [person_name], want to share anything with me today?",
    ]

    # Get user's first name
    name = "there"
    try:
        res = supabase.table("profiles").select("full_name").eq("user_id", str(user_uuid)).limit(1).execute()
        if not getattr(res, "error", None) and res.data:
            full = (res.data[0].get("full_name") or "").strip()
            if full:
                name = full.split()[0].strip() or "there"
    except Exception:
        pass

    template = random.choice(messages)
    body = template.replace("[person_name]", name)

    print(f"[DailyCheckin][TEST] Sending test notification to user={user_uuid} name={name}")
    await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)

    return {"success": True, "message": f"Test notification sent: {body}"}



