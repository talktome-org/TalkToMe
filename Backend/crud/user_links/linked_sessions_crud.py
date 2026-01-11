import uuid
from datetime import datetime, timezone
from typing import Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from ...database import SessionLocal
from ...models.links.linked_sessions_model import LinkedSession


async def create_linked_session(
    *,
    relationship_id: uuid.UUID,
    user_a_id: uuid.UUID,
    user_b_id: uuid.UUID,
    user_a_personal_session_id: uuid.UUID,
    user_b_personal_session_id: Optional[uuid.UUID],
) -> dict:
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    def _upsert():
        db = SessionLocal()
        try:
            values = {
                "relationship_id": relationship_id,
                "user_a_id": user_a_id,
                "user_b_id": user_b_id,
                "user_a_personal_session_id": user_a_personal_session_id,
                "user_b_personal_session_id": user_b_personal_session_id,
                "created_at": created_at,
            }

            stmt = (
                insert(LinkedSession)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[LinkedSession.relationship_id, LinkedSession.user_a_personal_session_id],
                    set_={
                        "user_a_id": user_a_id,
                        "user_b_id": user_b_id,
                        "user_b_personal_session_id": user_b_personal_session_id,
                    },
                )
                .returning(
                    LinkedSession.id,
                    LinkedSession.relationship_id,
                    LinkedSession.user_a_id,
                    LinkedSession.user_b_id,
                    LinkedSession.user_a_personal_session_id,
                    LinkedSession.user_b_personal_session_id,
                    LinkedSession.created_at,
                )
            )
            row = db.execute(stmt).first()
            db.commit()
            if not row:
                raise RuntimeError("Failed to upsert linked session")
            return {
                "id": str(row[0]),
                "relationship_id": str(row[1]),
                "user_a_id": str(row[2]),
                "user_b_id": str(row[3]),
                "user_a_personal_session_id": str(row[4]),
                "user_b_personal_session_id": (str(row[5]) if row[5] else None),
                "created_at": (row[6].isoformat() if row[6] else None),
            }
        finally:
            db.close()

    return await run_in_threadpool(_upsert)


async def get_linked_session_by_relationship_and_source_session(
    *, relationship_id: uuid.UUID, source_session_id: uuid.UUID
) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(LinkedSession).where(
                        and_(
                            LinkedSession.relationship_id == relationship_id,
                            or_(
                                LinkedSession.user_a_personal_session_id == source_session_id,
                                LinkedSession.user_b_personal_session_id == source_session_id,
                            ),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not row:
                return None
            return {
                "id": str(row.id),
                "relationship_id": str(row.relationship_id),
                "user_a_id": str(row.user_a_id),
                "user_b_id": str(row.user_b_id),
                "user_a_personal_session_id": str(row.user_a_personal_session_id),
                "user_b_personal_session_id": (str(row.user_b_personal_session_id) if row.user_b_personal_session_id else None),
                "created_at": (row.created_at.isoformat() if row.created_at else None),
            }
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def update_linked_session_partner_session_for_source(*, relationship_id: uuid.UUID, source_session_id: uuid.UUID, partner_session_id: uuid.UUID) -> None:
    existing = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=source_session_id
    )
    if not existing:
        raise RuntimeError(f"No linked session found for relationship {relationship_id} and source {source_session_id}")

    if existing.get("user_a_personal_session_id") == str(source_session_id):
        field_to_update = "user_b_personal_session_id"
    elif existing.get("user_b_personal_session_id") == str(source_session_id):
        field_to_update = "user_a_personal_session_id"
    else:
        raise RuntimeError(f"Source session {source_session_id} not found in linked session record")

    def _update():
        db = SessionLocal()
        try:
            stmt = (
                update(LinkedSession)
                .where(LinkedSession.relationship_id == relationship_id)
                .where(
                    or_(
                        LinkedSession.user_a_personal_session_id == source_session_id,
                        LinkedSession.user_b_personal_session_id == source_session_id,
                    )
                )
                .values(**{field_to_update: partner_session_id})
            )
            db.execute(stmt)
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def count_accepted_linked_pairs(*, relationship_id: uuid.UUID) -> int:
    def _count():
        db = SessionLocal()
        try:
            n = db.execute(
                select(func.count())
                .select_from(LinkedSession)
                .where(LinkedSession.relationship_id == relationship_id)
                .where(LinkedSession.user_b_personal_session_id.is_not(None))
            ).scalar_one()
            return int(n or 0)
        finally:
            db.close()

    return await run_in_threadpool(_count)


