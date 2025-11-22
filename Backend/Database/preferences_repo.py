import uuid
from typing import List, Optional, Tuple, Dict, Any
from starlette.concurrency import run_in_threadpool
from datetime import datetime, timezone

from .supabase_client import supabase

TABLE = "user_preferences"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def get_daily_checkins_preference(*, user_id: uuid.UUID) -> Dict[str, Any]:
    def _select():
        return (
            supabase
            .table(TABLE)
            .select("daily_checkins_enabled, daily_checkin_hour, daily_checkin_minute, timezone")
            .eq("user_id", str(user_id))
            .limit(1)
            .execute()
        )
    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select user_preferences failed: {res.error}")
    row = res.data[0] if res.data else {}
    enabled = bool(row.get("daily_checkins_enabled") or False)
    hour = row.get("daily_checkin_hour")
    minute = row.get("daily_checkin_minute")
    timezone = row.get("timezone")
    return {"enabled": enabled, "hour": hour, "minute": minute, "timezone": timezone}


async def set_daily_checkins_preference(*, user_id: uuid.UUID, enabled: bool, hour: Optional[int] = None, minute: Optional[int] = None, timezone: Optional[str] = None) -> None:
    payload = {
        "user_id": str(user_id),
        "daily_checkins_enabled": bool(enabled),
        "updated_at": _now_iso(),
    }
    if hour is not None:
        payload["daily_checkin_hour"] = int(hour)
    if minute is not None:
        payload["daily_checkin_minute"] = int(minute)
    if timezone:
        payload["timezone"] = str(timezone)

    def _upsert():
        return supabase.table(TABLE).upsert(payload, on_conflict="user_id").execute()
    res = await run_in_threadpool(_upsert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase upsert user_preferences failed: {res.error}")


async def list_users_with_daily_checkins_enabled() -> List[Dict[str, Any]]:
    def _select():
        return (
            supabase
            .table(TABLE)
            .select("user_id, daily_checkin_hour, daily_checkin_minute, daily_checkins_enabled, timezone")
            .eq("daily_checkins_enabled", True)
            .execute()
        )
    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select enabled user_preferences failed: {res.error}")
    return res.data or []

