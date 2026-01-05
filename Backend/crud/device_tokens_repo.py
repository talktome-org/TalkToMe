import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from ..db.supabase_client import supabase

TABLE = "device_tokens"


async def upsert_token(*, user_id: uuid.UUID, token: str, platform: str, bundle_id: Optional[str]) -> None:
    payload = {
        "user_id": str(user_id),
        "token": token,
        "platform": platform,
        "bundle_id": bundle_id,
        "enabled": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    def _upsert():
        return supabase.table(TABLE).upsert(payload, on_conflict="user_id,token").execute()

    res = await run_in_threadpool(_upsert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase upsert device_token failed: {res.error}")


async def disable_token_by_value(*, token: str) -> None:
    def _update():
        return (
            supabase.table(TABLE)
            .update({"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()})
            .eq("token", token)
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase disable device_token failed: {res.error}")


async def list_tokens_for_user(*, user_id: uuid.UUID) -> List[dict]:
    def _select():
        return (
            supabase.table(TABLE)
            .select("token, enabled")
            .eq("user_id", str(user_id))
            .eq("enabled", True)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select device_tokens failed: {res.error}")
    return res.data


