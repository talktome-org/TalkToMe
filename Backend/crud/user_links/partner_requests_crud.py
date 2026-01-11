import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import Text, cast, select, text, update

from ...database import SessionLocal
from ...models.links.partner_requests_model import PartnerRequest

TABLE = "partner_requests"

def _to_dict(obj) -> dict:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}


async def create_partner_request(
    *,
    relationship_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    recipient_user_id: uuid.UUID,
    sender_session_id: uuid.UUID,
    content: str,
) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def _insert():
        db = SessionLocal()
        try:
            row = PartnerRequest(
                relationship_id=relationship_id,
                sender_user_id=sender_user_id,
                recipient_user_id=recipient_user_id,
                sender_session_id=sender_session_id,
                content=content,
                # Let the DB enum default handle status ('pending'::request_status)
                created_at=now,
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return _to_dict(row)
        finally:
            db.close()

    return await run_in_threadpool(_insert)


async def get_latest_pending_for_context(
    *,
    relationship_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    recipient_user_id: uuid.UUID,
    sender_session_id: uuid.UUID,
) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(PartnerRequest)
                    .where(
                        PartnerRequest.relationship_id == relationship_id,
                        PartnerRequest.sender_user_id == sender_user_id,
                        PartnerRequest.recipient_user_id == recipient_user_id,
                        PartnerRequest.sender_session_id == sender_session_id,
                        cast(PartnerRequest.status, Text).in_(["pending", "delivered"]),
                    )
                    .order_by(PartnerRequest.created_at.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            return _to_dict(row) if row else None
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def list_pending_for_user(*, user_id: uuid.UUID, limit: int = 50) -> List[dict]:
    def _select():
        db = SessionLocal()
        try:
            rows = (
                db.execute(
                    select(PartnerRequest)
                    .where(
                        PartnerRequest.recipient_user_id == user_id,
                        cast(PartnerRequest.status, Text).in_(["pending", "delivered"]),
                    )
                    .order_by(PartnerRequest.created_at.desc())
                    .limit(int(limit))
                )
                .scalars()
                .all()
            )
            return [_to_dict(r) for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def mark_delivered(*, request_id: uuid.UUID) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(PartnerRequest)
                .where(PartnerRequest.id == request_id)
                .values(
                    status=text("'delivered'::request_status"),
                    delivered_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def update_content(*, request_id: uuid.UUID, content: str) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(PartnerRequest)
                .where(PartnerRequest.id == request_id)
                .values(content=content, updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def mark_accepted_and_attach(
    *, request_id: uuid.UUID, recipient_session_id: uuid.UUID, created_message_id: uuid.UUID
) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(PartnerRequest)
                .where(PartnerRequest.id == request_id)
                .values(
                    status=text("'accepted'::request_status"),
                    accepted_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    recipient_session_id=recipient_session_id,
                    created_message_id=created_message_id,
                )
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def attach_session_and_message_on_pending(
    *, request_id: uuid.UUID, recipient_session_id: uuid.UUID, created_message_id: uuid.UUID
) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(PartnerRequest)
                .where(PartnerRequest.id == request_id, cast(PartnerRequest.status, Text) == "pending")
                .values(
                    recipient_session_id=recipient_session_id,
                    created_message_id=created_message_id,
                )
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def get_request_by_id(*, request_id: uuid.UUID) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(select(PartnerRequest).where(PartnerRequest.id == request_id).limit(1))
                .scalars()
                .first()
            )
            return _to_dict(row) if row else None
        finally:
            db.close()

    return await run_in_threadpool(_select)


