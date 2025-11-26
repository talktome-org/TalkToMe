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
    # Default to enabled=True if daily_checkins_enabled is NULL or missing
    daily_checkins_value = row.get("daily_checkins_enabled")
    enabled = True if daily_checkins_value is None else bool(daily_checkins_value)
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
    # First get all users who have device tokens
    def _get_device_token_users():
        return (
            supabase
            .table("device_tokens")
            .select("user_id")
            .eq("enabled", True)
            .execute()
        )

    token_res = await run_in_threadpool(_get_device_token_users)
    if getattr(token_res, "error", None) or not token_res.data:
        return []

    # Get unique user IDs
    unique_user_ids = list(set(row["user_id"] for row in token_res.data if row.get("user_id")))

    # Get preferences for these users
    def _get_prefs():
        return (
            supabase
            .table(TABLE)
            .select("user_id, daily_checkin_hour, daily_checkin_minute, daily_checkins_enabled, timezone")
            .in_("user_id", unique_user_ids)
            .execute()
        )

    prefs_res = await run_in_threadpool(_get_prefs)
    prefs_by_user = {}
    if not getattr(prefs_res, "error", None) and prefs_res.data:
        prefs_by_user = {p["user_id"]: p for p in prefs_res.data}

    # Combine: users with tokens get notifications unless explicitly disabled
    users = []
    for user_id in unique_user_ids:
        pref = prefs_by_user.get(user_id, {})

        # Only skip if explicitly set to false
        if pref.get("daily_checkins_enabled") is False:
            continue

        users.append({
            "user_id": user_id,
            "daily_checkin_hour": pref.get("daily_checkin_hour"),
            "daily_checkin_minute": pref.get("daily_checkin_minute"),
            "daily_checkins_enabled": pref.get("daily_checkins_enabled", True),  # Default to True
            "timezone": pref.get("timezone")
        })

    return users

