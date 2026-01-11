import random
import uuid
from datetime import date
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException

from Backend.auth import get_current_user
from Backend.crud.apns.device_tokens_crud import disable_token_by_value, upsert_token
from Backend.crud.apns.preferences_crud import (
    get_daily_checkins_preference,
    list_users_with_daily_checkins_enabled,
    set_daily_checkins_preference,
)
from Backend.database import SessionLocal
from Backend.models.apns.user_preferences_model import UserPreference
from Backend.models.profile.profile_model import Profile
from Backend.services.apns_service import send_daily_checkin_notification_to_user
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

router = APIRouter(prefix="/notifications", tags=["notifications"])

def _now_ts():
    # Store as naive UTC to match Supabase `timestamp` (no tz) UI.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _auth_user_metadata(user_id: str) -> dict:
    db = SessionLocal()
    try:
        row = db.execute(text("select raw_user_meta_data from auth.users where id = :id"), {"id": user_id}).first()
        if not row or row[0] is None:
            return {}
        return row[0] if isinstance(row[0], dict) else {}
    finally:
        db.close()


def _get_first_name_for_user(user_id: str) -> str:
    try:
        db = SessionLocal()
        try:
            u = uuid.UUID(user_id)
            row = db.execute(select(Profile.full_name).where(Profile.user_id == u).limit(1)).first()
            if row and isinstance(row[0], str):
                full = row[0].strip()
                if full:
                    first = full.split()[0].strip()
                    if first:
                        return first
        finally:
            db.close()
    except Exception:
        pass

    try:
        meta = _auth_user_metadata(user_id)
        raw = (meta.get("full_name") or meta.get("name") or meta.get("display_name") or "").strip()
        if raw:
            first = raw.split()[0].strip()
            if first:
                return first
    except Exception:
        pass
    return "there"


def _get_last_checkin_sent_date(user_id: uuid.UUID) -> date | None:
    db = SessionLocal()
    try:
        row = db.execute(select(UserPreference.last_checkin_sent_date).where(UserPreference.user_id == user_id)).first()
        if not row:
            return None
        return row[0]
    finally:
        db.close()


def _upsert_last_checkin_sent(
    *,
    user_id: uuid.UUID,
    sent_date: date,
    hour: int | None,
    minute: int | None,
    timezone_name: str | None,
) -> None:
    db = SessionLocal()
    try:
        payload = {
            "user_id": user_id,
            "last_checkin_sent_date": sent_date,
            "daily_checkins_enabled": True,
            "updated_at": _now_ts(),
        }
        if hour is not None:
            payload["daily_checkin_hour"] = int(hour)
        if minute is not None:
            payload["daily_checkin_minute"] = int(minute)
        if timezone_name is not None:
            payload["timezone"] = str(timezone_name)

        stmt = (
            insert(UserPreference)
            .values(**payload)
            .on_conflict_do_update(index_elements=[UserPreference.user_id], set_=payload)
        )
        db.execute(stmt)
        db.commit()
    finally:
        db.close()


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

            today = local_now.date()
            try:
                last = _get_last_checkin_sent_date(uuid.UUID(uid_str))
                if last == today:
                    continue
            except Exception:
                pass

            user_uuid = uuid.UUID(uid_str)
            name = _get_first_name_for_user(uid_str)
            template = random.choice(messages)
            body = template.replace("[person_name]", name)
            await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)

            try:
                _upsert_last_checkin_sent(
                    user_id=user_uuid,
                    sent_date=today,
                    hour=(int(user_hour) if isinstance(user_hour, (int, float)) else None),
                    minute=(int(user_minute) if isinstance(user_minute, (int, float)) else None),
                    timezone_name=(tz_name if tz_name and tz_name != "America/Los_Angeles" else None),
                )
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
        name = _get_first_name_for_user(str(user_uuid))
    except Exception:
        pass

    template = random.choice(messages)
    body = template.replace("[person_name]", name)

    await send_daily_checkin_notification_to_user(recipient_user_id=user_uuid, body=body)

    return {"success": True, "message": f"Test notification sent: {body}"}


