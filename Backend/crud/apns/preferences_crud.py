import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from Backend.database import SessionLocal
from Backend.models.apns.device_token_model import DeviceToken
from Backend.models.apns.user_preferences_model import UserPreference

TABLE = "user_preferences"


def _now_ts():
    # Supabase column is `timestamp` (no tz) in UI; store as naive UTC.
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def get_daily_checkins_preference(*, user_id: uuid.UUID) -> Dict[str, Any]:
    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(
                        UserPreference.daily_checkins_enabled,
                        UserPreference.daily_checkin_hour,
                        UserPreference.daily_checkin_minute,
                        UserPreference.timezone,
                    ).where(UserPreference.user_id == user_id)
                )
                .first()
            )
            if not row:
                return {"enabled": False, "hour": None, "minute": None, "timezone": None}
            enabled_val, hour, minute, tz = row[0], row[1], row[2], row[3]
            enabled = bool(enabled_val) if enabled_val is not None else False
            return {"enabled": enabled, "hour": hour, "minute": minute, "timezone": tz}
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def set_daily_checkins_preference(
    *,
    user_id: uuid.UUID,
    enabled: bool,
    hour: Optional[int] = None,
    minute: Optional[int] = None,
    timezone: Optional[str] = None,
) -> None:
    def _upsert():
        db = SessionLocal()
        try:
            payload: Dict[str, Any] = {
                "user_id": user_id,
                "daily_checkins_enabled": bool(enabled),
                "updated_at": _now_ts(),
            }
            if hour is not None:
                payload["daily_checkin_hour"] = int(hour)
            if minute is not None:
                payload["daily_checkin_minute"] = int(minute)
            if timezone is not None:
                payload["timezone"] = str(timezone)

            stmt = (
                insert(UserPreference)
                .values(**payload)
                .on_conflict_do_update(
                    index_elements=[UserPreference.user_id],
                    set_=payload,
                )
            )
            db.execute(stmt)
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_upsert)


async def list_users_with_daily_checkins_enabled() -> List[Dict[str, Any]]:
    def _select():
        db = SessionLocal()
        try:
            user_ids = [
                r[0]
                for r in db.execute(
                    select(DeviceToken.user_id)
                    .where(DeviceToken.enabled.is_(True))
                    .where(DeviceToken.user_id.is_not(None))
                ).all()
                if r and r[0]
            ]
            unique_user_ids = list(set(user_ids))
            if not unique_user_ids:
                return []

            prefs = (
                db.execute(
                    select(
                        UserPreference.user_id,
                        UserPreference.daily_checkin_hour,
                        UserPreference.daily_checkin_minute,
                        UserPreference.daily_checkins_enabled,
                        UserPreference.timezone,
                    ).where(UserPreference.user_id.in_(unique_user_ids))
                )
                .all()
            )
            prefs_by_user = {str(p[0]): p for p in prefs if p and p[0]}

            out: List[Dict[str, Any]] = []
            for uid in unique_user_ids:
                key = str(uid)
                pref = prefs_by_user.get(key)
                if not pref:
                    continue
                _, hour, minute, enabled_val, tz = pref
                if enabled_val is not True:
                    continue
                out.append(
                    {
                        "user_id": key,
                        "daily_checkin_hour": hour,
                        "daily_checkin_minute": minute,
                        "daily_checkins_enabled": True,
                        "timezone": tz,
                    }
                )
            return out
        finally:
            db.close()

    return await run_in_threadpool(_select)


