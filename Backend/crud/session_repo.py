import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from ..db.supabase_client import supabase

SESSIONS_TABLE = "user_chat_sessions"


async def create_session(*, user_id: uuid.UUID, title: Optional[str] = None) -> dict:
    payload = {
        "user_id": str(user_id),
        "title": title,
        "last_message_at": datetime.now(timezone.utc).isoformat(),
    }

    def _insert():
        return supabase.table(SESSIONS_TABLE).insert(payload).execute()

    res = await run_in_threadpool(_insert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase insert session failed: {res.error}")
    if not hasattr(res, "data") or not res.data:
        raise RuntimeError("Supabase insert session returned no data")
    return res.data[0]


async def list_sessions_for_user(*, user_id: uuid.UUID, limit: int = 100, offset: int = 0) -> List[dict]:
    def _select():
        return (
            supabase.table(SESSIONS_TABLE)
            .select("*")
            .eq("user_id", str(user_id))
            .order("last_message_at", desc=True)
            .order("created_at", desc=True)
            .range(offset, offset + max(limit - 1, 0))
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select sessions failed: {res.error}")
    if not hasattr(res, "data"):
        raise RuntimeError("Supabase select sessions returned invalid response")

    for session in res.data or []:
        session["last_message_content"] = session.get("last_message_content")

    return res.data or []


async def get_session_by_id(*, user_id: uuid.UUID, session_id: uuid.UUID) -> Optional[dict]:
    def _select():
        return (
            supabase.table(SESSIONS_TABLE)
            .select("*")
            .eq("id", str(session_id))
            .eq("user_id", str(user_id))
            .limit(1)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select session failed: {res.error}")
    return (res.data or [None])[0]


async def touch_session(*, session_id: uuid.UUID) -> None:
    def _update():
        return (
            supabase.table(SESSIONS_TABLE)
            .update({"last_message_at": datetime.now(timezone.utc).isoformat()})
            .eq("id", str(session_id))
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update session failed: {res.error}")


async def assert_session_owned_by_user(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    def _select():
        return (
            supabase.table(SESSIONS_TABLE)
            .select("id")
            .eq("id", str(session_id))
            .eq("user_id", str(user_id))
            .limit(1)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase verify session failed: {res.error}")
    if not res.data:
        raise PermissionError("Session not found or not owned by user")


async def update_session_title(*, user_id: uuid.UUID, session_id: uuid.UUID, title: Optional[str]) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _update():
        return (
            supabase.table(SESSIONS_TABLE)
            .update({"title": title})
            .eq("id", str(session_id))
            .eq("user_id", str(user_id))
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update session title failed: {res.error}")


async def delete_session(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _delete():
        return (
            supabase.table(SESSIONS_TABLE)
            .delete()
            .eq("id", str(session_id))
            .eq("user_id", str(user_id))
            .execute()
        )

    res = await run_in_threadpool(_delete)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase delete session failed: {res.error}")


