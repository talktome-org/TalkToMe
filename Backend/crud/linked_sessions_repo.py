import uuid
from datetime import datetime, timezone
from typing import Optional

from starlette.concurrency import run_in_threadpool

from ..db.supabase_client import supabase

LINKED_SESSIONS_TABLE = "linked_sessions"


async def create_linked_session(
    *,
    relationship_id: uuid.UUID,
    user_a_id: uuid.UUID,
    user_b_id: uuid.UUID,
    user_a_personal_session_id: uuid.UUID,
    user_b_personal_session_id: Optional[uuid.UUID],
) -> dict:
    payload = {
        "relationship_id": str(relationship_id),
        "user_a_id": str(user_a_id),
        "user_b_id": str(user_b_id),
        "user_a_personal_session_id": str(user_a_personal_session_id),
        "user_b_personal_session_id": str(user_b_personal_session_id) if user_b_personal_session_id else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    def _upsert():
        return supabase.table(LINKED_SESSIONS_TABLE).upsert(
            payload,
            on_conflict="relationship_id,user_a_personal_session_id",
        ).execute()

    res = await run_in_threadpool(_upsert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase upsert linked session failed: {res.error}")
    return res.data[0]


async def get_linked_session_by_relationship_and_source_session(
    *, relationship_id: uuid.UUID, source_session_id: uuid.UUID
) -> Optional[dict]:
    relationship_id_str = str(relationship_id)
    source_session_id_str = str(source_session_id)

    def _select():
        return (
            supabase.table(LINKED_SESSIONS_TABLE)
            .select("*")
            .eq("relationship_id", relationship_id_str)
            .or_(
                f"user_a_personal_session_id.eq.{source_session_id_str},user_b_personal_session_id.eq.{source_session_id_str}"
            )
            .limit(1)
            .execute()
        )

    res = await run_in_threadpool(_select)
    if getattr(res, "error", None):
        raise RuntimeError(
            f"Supabase select linked session by relationship and session failed: {res.error}"
        )
    return res.data[0] if res.data else None


async def update_linked_session_partner_session_for_source(
    *, relationship_id: uuid.UUID, source_session_id: uuid.UUID, partner_session_id: uuid.UUID
) -> None:
    relationship_id_str = str(relationship_id)
    source_session_id_str = str(source_session_id)
    partner_session_id_str = str(partner_session_id)

    existing = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=source_session_id
    )
    if not existing:
        raise RuntimeError(f"No linked session found for relationship {relationship_id} and source {source_session_id}")

    if existing.get("user_a_personal_session_id") == source_session_id_str:
        field_to_update = "user_b_personal_session_id"
    elif existing.get("user_b_personal_session_id") == source_session_id_str:
        field_to_update = "user_a_personal_session_id"
    else:
        raise RuntimeError(f"Source session {source_session_id} not found in linked session record")

    def _update():
        return (
            supabase.table(LINKED_SESSIONS_TABLE)
            .update({field_to_update: partner_session_id_str})
            .eq("relationship_id", relationship_id_str)
            .or_(
                f"user_a_personal_session_id.eq.{source_session_id_str},user_b_personal_session_id.eq.{source_session_id_str}"
            )
            .execute()
        )

    res = await run_in_threadpool(_update)
    if getattr(res, "error", None):
        raise RuntimeError(
            f"Supabase update linked session partner session by source failed: {res.error}"
        )


async def count_accepted_linked_pairs(*, relationship_id: uuid.UUID) -> int:
    relationship_id_str = str(relationship_id)

    def _count():
        return (
            supabase.table(LINKED_SESSIONS_TABLE)
            .select("*", count="exact")
            .eq("relationship_id", relationship_id_str)
            .not_.is_("user_b_personal_session_id", "null")
            .execute()
        )

    res = await run_in_threadpool(_count)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase count linked accepted pairs failed: {res.error}")
    return res.count if hasattr(res, "count") else 0


