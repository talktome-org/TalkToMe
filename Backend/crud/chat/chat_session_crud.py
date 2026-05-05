import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import delete, select, update

from ...database import SessionLocal
from ...models.chat.chat_models import SessionReport, UserChatMessage, UserChatSession

SESSIONS_TABLE = "user_chat_sessions"


def _to_dict(obj) -> dict:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}


async def create_session(*, user_id: uuid.UUID, title: Optional[str] = None) -> dict:
    def _insert():
        db = SessionLocal()
        try:
            sess = UserChatSession(
                user_id=user_id,
                title=title,
                last_message_at=datetime.now(timezone.utc),
            )
            db.add(sess)
            db.commit()
            db.refresh(sess)
            return _to_dict(sess)
        finally:
            db.close()

    return await run_in_threadpool(_insert)


async def attach_friendship_to_session(*, user_id: uuid.UUID, session_id: uuid.UUID, friendship_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _update():
        db = SessionLocal()
        try:
            row = db.execute(
                select(UserChatSession.friendship_id)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .limit(1)
            ).first()
            existing = row[0] if row else None
            if existing is not None and existing != friendship_id:
                raise PermissionError("Session is already connected to a different friend")

            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(friendship_id=friendship_id)
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    await run_in_threadpool(_update)


async def ensure_linked_session_for_friendship(
    *,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
    recipient_user_id: uuid.UUID,
    friendship_id: uuid.UUID,
    title: Optional[str] = None,
) -> uuid.UUID:
    """
    Ensure `session_id` (owned by `user_id`) is linked 1:1 to a counterpart session
    owned by `recipient_user_id` for the given `friendship_id`.

    Returns the recipient session id.
    """

    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _ensure() -> uuid.UUID:
        db = SessionLocal()
        try:
            sender = (
                db.execute(
                    select(UserChatSession)
                    .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                    .with_for_update()
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if not sender:
                raise PermissionError("Session not found or not owned by user")

            # Enforce "one chat connects to one friend".
            if sender.friendship_id is not None and sender.friendship_id != friendship_id:
                raise PermissionError("Session is already connected to a different friend")

            # If another session already links TO this one, adopt it (repairs partial links).
            incoming = (
                db.execute(select(UserChatSession).where(UserChatSession.linked_session_id == sender.id).limit(1))
                .scalars()
                .first()
            )

            if sender.linked_session_id is not None:
                linked = (
                    db.execute(select(UserChatSession).where(UserChatSession.id == sender.linked_session_id).limit(1))
                    .scalars()
                    .first()
                )
                if not linked and incoming:
                    linked = incoming
                    sender.linked_session_id = linked.id
                if not linked:
                    raise PermissionError("Linked session not found")
                if linked.user_id != recipient_user_id:
                    raise PermissionError("Session is already linked to a different user")
                if linked.linked_session_id is not None and linked.linked_session_id != sender.id:
                    raise PermissionError("Linked session is already linked to another session")

                # Repair missing reciprocity.
                if linked.linked_session_id is None:
                    linked.linked_session_id = sender.id
                if sender.friendship_id is None:
                    sender.friendship_id = friendship_id
                if linked.friendship_id is None:
                    linked.friendship_id = friendship_id
                db.commit()
                return uuid.UUID(str(linked.id))

            if incoming:
                if incoming.user_id != recipient_user_id:
                    raise PermissionError("Session is already linked to a different user")
                if incoming.friendship_id is not None and incoming.friendship_id != friendship_id:
                    raise PermissionError("Session is already connected to a different friend")

                sender.linked_session_id = incoming.id
                if incoming.linked_session_id is None:
                    incoming.linked_session_id = sender.id
                if sender.friendship_id is None:
                    sender.friendship_id = friendship_id
                if incoming.friendship_id is None:
                    incoming.friendship_id = friendship_id
                db.commit()
                return uuid.UUID(str(incoming.id))

            # Create a brand new counterpart session (per-chat thread).
            recipient = UserChatSession(
                user_id=recipient_user_id,
                title=title,
                friendship_id=friendship_id,
                linked_session_id=sender.id,
                last_message_at=datetime.now(timezone.utc),
            )
            db.add(recipient)
            db.flush()  # assign recipient.id

            if sender.friendship_id is None:
                sender.friendship_id = friendship_id
            sender.linked_session_id = recipient.id

            db.commit()
            return uuid.UUID(str(recipient.id))
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    return await run_in_threadpool(_ensure)


async def list_sessions_for_user(*, user_id: uuid.UUID, limit: int = 100, offset: int = 0) -> List[dict]:
    def _select():
        db = SessionLocal()
        try:
            q = (
                select(UserChatSession)
                .where(UserChatSession.user_id == user_id)
                .order_by(UserChatSession.last_message_at.desc().nullslast(), UserChatSession.created_at.desc())
                .limit(int(limit))
                .offset(int(offset))
            )
            rows = db.execute(q).scalars().all()
            return [_to_dict(r) for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def get_session_by_id(*, user_id: uuid.UUID, session_id: uuid.UUID) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(UserChatSession).where(UserChatSession.id == session_id, UserChatSession.user_id == user_id).limit(1)
                )
                .scalars()
                .first()
            )
            return _to_dict(row) if row else None
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def touch_session(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(last_message_at=datetime.now(timezone.utc))
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def assert_session_owned_by_user(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    def _select():
        db = SessionLocal()
        try:
            row = db.execute(
                select(UserChatSession.id).where(UserChatSession.id == session_id, UserChatSession.user_id == user_id).limit(1)
            ).first()
            if not row:
                raise PermissionError("Session not found or not owned by user")
        finally:
            db.close()

    await run_in_threadpool(_select)


async def update_session_title(*, user_id: uuid.UUID, session_id: uuid.UUID, title: Optional[str]) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(title=title)
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def increment_unread_count(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(unread_count=UserChatSession.unread_count + 1)
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def mark_session_read(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(unread_count=0)
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def delete_session(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _delete():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(UserChatSession.id, UserChatSession.linked_session_id)
                    .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                    .limit(1)
                )
                .first()
            )
            linked_id: Optional[uuid.UUID] = None
            if row:
                linked_id = row[1]

            # Delete this session, and also delete its linked counterpart ONLY if it points back.
            session_ids_to_delete: list[uuid.UUID] = [session_id]
            if linked_id:
                other = db.execute(
                    select(UserChatSession.id, UserChatSession.linked_session_id).where(UserChatSession.id == linked_id).limit(1)
                ).first()
                if other and other[1] == session_id:
                    session_ids_to_delete.append(linked_id)
            else:
                # Also support the inverse case: another session points to this one, but this row has no outgoing link.
                incoming = db.execute(
                    select(UserChatSession.id).where(UserChatSession.linked_session_id == session_id).limit(1)
                ).first()
                if incoming and incoming[0] is not None:
                    session_ids_to_delete.append(incoming[0])

            # Delete dependent rows first (FKs aren't ON DELETE CASCADE).
            if session_ids_to_delete:
                db.execute(delete(SessionReport).where(SessionReport.session_id.in_(session_ids_to_delete)))
                db.execute(delete(UserChatMessage).where(UserChatMessage.session_id.in_(session_ids_to_delete)))
                db.execute(delete(UserChatSession).where(UserChatSession.id.in_(session_ids_to_delete)))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    await run_in_threadpool(_delete)


async def create_session_report(
    *,
    session_id: uuid.UUID,
    reporter_user_id: uuid.UUID,
    category: str,
    reason: str,
    details: Optional[str] = None,
) -> dict:
    def _insert():
        db = SessionLocal()
        try:
            report = SessionReport(
                session_id=session_id,
                reporter_user_id=reporter_user_id,
                category=category,
                reason=reason,
                details=details,
            )
            db.add(report)
            db.commit()
            db.refresh(report)
            return _to_dict(report)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    return await run_in_threadpool(_insert)