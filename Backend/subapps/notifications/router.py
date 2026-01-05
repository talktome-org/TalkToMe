import random
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException

from ...auth import get_current_user
from ...crud.device_tokens_repo import disable_token_by_value, upsert_token
from ...crud.preferences_repo import (
    get_daily_checkins_preference,
    list_users_with_daily_checkins_enabled,
    set_daily_checkins_preference,
)
from ...db.supabase_client import supabase
from ...services.push.apns import send_daily_checkin_notification_to_user

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
    tz = payload.get("timezone")

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
    if tz is not None:
        try:
            _ = ZoneInfo(str(tz))
        except Exception:
            raise HTTPException(status_code=400, detail="invalid timezone")
    try:
        await set_daily_checkins_preference(user_id=user_uuid, enabled=enabled, hour=hour, minute=minute, timezone=tz)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    except Exception:
        return 0

    default_hour = 17
    default_minute = 0

    def _get_first_name_for_user(user_id: str) -> str:
        try:
            res = supabase.table("profiles").select("full_name").eq("user_id", user_id).limit(1).execute()
            if not getattr(res, "error", None) and res.data:
                name = (res.data[0].get("full_name") or "").strip()
                if name:
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

            user_hour = row.get("daily_checkin_hour")
            user_minute = row.get("daily_checkin_minute")
            target_hour = int(user_hour) if isinstance(user_hour, (int, float)) else default_hour
            target_minute = int(user_minute) if isinstance(user_minute, (int, float)) else default_minute
            if not (local_now.hour == target_hour and local_now.minute == target_minute):
                continue

            today_str = local_now.date().isoformat()
            try:
                sel = (
                    supabase.table("user_preferences")
                    .select("last_checkin_sent_date")
                    .eq("user_id", uid_str)
                    .limit(1)
                    .execute()
                )
                last = sel.data[0].get("last_checkin_sent_date") if getattr(sel, "data", None) else None
                if last == today_str:
                    continue
            except Exception:
                pass

            user_uuid = uuid.UUID(uid_str)
            name = _get_first_name_for_user(uid_str)
            template = random.choice(messages)
            body = template.replace("[person_name]", name)
            await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)

            try:
                upsert_data = {
                    "user_id": uid_str,
                    "last_checkin_sent_date": today_str,
                    "daily_checkins_enabled": True,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                if isinstance(user_hour, (int, float)):
                    upsert_data["daily_checkin_hour"] = int(user_hour)
                if isinstance(user_minute, (int, float)):
                    upsert_data["daily_checkin_minute"] = int(user_minute)
                if tz_name and tz_name != "America/Los_Angeles":
                    upsert_data["timezone"] = tz_name

                _ = supabase.table("user_preferences").upsert(upsert_data, on_conflict="user_id").execute()
            except Exception:
                pass

            sent += 1
        except Exception:
            continue
    return sent


@router.post("/daily-checkins/run")
async def run_daily_checkins_for_now():
    sent = await send_daily_checkins_for_now()
    return {"success": True, "sent": sent}


@router.post("/daily-checkins/test")
async def test_daily_checkin(current_user: dict = Depends(get_current_user)):
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

    await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)

    return {"success": True, "message": f"Test notification sent: {body}"}


