import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from ..db.supabase_client import supabase

TABLE = "partner_requests"


async def create_partner_request(
    *,
    relationship_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    recipient_user_id: uuid.UUID,
    sender_session_id: uuid.UUID,
    content: str,
) -> dict:
    payload = {
        "relationship_id": str(relationship_id),
        "sender_user_id": str(sender_user_id),
        "recipient_user_id": str(recipient_user_id),
        "sender_session_id": str(sender_session_id),
        "content": content,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    def _insert():
        return supabase.table(TABLE).insert(payload).execute()

    res = await run_in_threadpool(_insert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase insert partner_request failed: {res.error}")
    return res.data[0]


async def get_latest_pending_for_context(
    *,
    relationship_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    recipient_user_id: uuid.UUID,
    sender_session_id: uuid.UUID,
) -> Optional[dict]:
    def _select():
        return (
            supabase.table(TABLE)
            .select("*")
            .eq("relationship_id", str(relationship_id))
            .eq("sender_user_id", str(sender_user_id))
            .eq("recipient_user_id", str(recipient_user_id))
            .eq("sender_session_id", str(sender_session_id))
            .in_("status", ["pending", "delivered"])
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select partner_request (latest pending) failed: {res.error}")
    rows = getattr(res, "data", []) or []
    return rows[0] if rows else None


async def list_pending_for_user(*, user_id: uuid.UUID, limit: int = 50) -> List[dict]:
    def _select():
        return (
            supabase.table(TABLE)
            .select("*")
            .eq("recipient_user_id", str(user_id))
            .in_("status", ["pending", "delivered"])
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select pending partner_requests failed: {res.error}")
    return res.data


async def mark_delivered(*, request_id: uuid.UUID) -> None:
    def _update():
        return (
            supabase.table(TABLE)
            .update({"status": "delivered", "delivered_at": datetime.now(timezone.utc).isoformat()})
            .eq("id", str(request_id))
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update partner_request delivered failed: {res.error}")


async def update_content(*, request_id: uuid.UUID, content: str) -> None:
    def _update():
        return (
            supabase.table(TABLE)
            .update({"content": content, "updated_at": datetime.now(timezone.utc).isoformat()})
            .eq("id", str(request_id))
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update partner_request content failed: {res.error}")


async def mark_accepted_and_attach(
    *, request_id: uuid.UUID, recipient_session_id: uuid.UUID, created_message_id: uuid.UUID
) -> None:
    def _update():
        return (
            supabase.table(TABLE)
            .update(
                {
                    "status": "accepted",
                    "accepted_at": datetime.now(timezone.utc).isoformat(),
                    "recipient_session_id": str(recipient_session_id),
                    "created_message_id": str(created_message_id),
                }
            )
            .eq("id", str(request_id))
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update partner_request accepted failed: {res.error}")


async def attach_session_and_message_on_pending(
    *, request_id: uuid.UUID, recipient_session_id: uuid.UUID, created_message_id: uuid.UUID
) -> None:
    def _update():
        return (
            supabase.table(TABLE)
            .update(
                {
                    "recipient_session_id": str(recipient_session_id),
                    "created_message_id": str(created_message_id),
                }
            )
            .eq("id", str(request_id))
            .eq("status", "pending")
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase update partner_request attach pending failed: {res.error}")


async def get_request_by_id(*, request_id: uuid.UUID) -> Optional[dict]:
    def _select():
        return supabase.table(TABLE).select("*").eq("id", str(request_id)).limit(1).execute()

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase select partner_request failed: {res.error}")
    return res.data[0] if res.data else None


