import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import delete, select, update

from ...database import SessionLocal
from ...models.chat.chat_models import UserChatMessage, UserChatSession

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


async def get_or_create_session_for_friendship(*, user_id: uuid.UUID, friendship_id: uuid.UUID, title: Optional[str] = None) -> dict:
    def _get_or_create():
        db = SessionLocal()
        try:
            existing = (
                db.execute(
                    select(UserChatSession)
                    .where(UserChatSession.user_id == user_id, UserChatSession.friendship_id == friendship_id)
                    .order_by(UserChatSession.last_message_at.desc().nullslast(), UserChatSession.created_at.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing:
                return _to_dict(existing)

            sess = UserChatSession(
                user_id=user_id,
                title=title,
                friendship_id=friendship_id,
                last_message_at=datetime.now(timezone.utc),
            )
            db.add(sess)
            db.commit()
            db.refresh(sess)
            return _to_dict(sess)
        finally:
            db.close()

    return await run_in_threadpool(_get_or_create)


async def attach_friendship_to_session(*, user_id: uuid.UUID, session_id: uuid.UUID, friendship_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                .values(friendship_id=friendship_id)
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


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


async def touch_session(*, session_id: uuid.UUID) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id)
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


async def delete_session(*, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
    await assert_session_owned_by_user(user_id=user_id, session_id=session_id)

    def _delete():
        db = SessionLocal()
        try:
            # If this session is linked to a friendship thread, delete BOTH users' sessions for that friendship.
            # Otherwise, delete just the one session row.
            row = (
                db.execute(
                    select(UserChatSession.id, UserChatSession.friendship_id)
                    .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
                    .limit(1)
                )
                .first()
            )
            friendship_id: Optional[uuid.UUID] = None
            if row:
                friendship_id = row[1]

            session_ids_to_delete: list[uuid.UUID]
            if friendship_id:
                session_ids_to_delete = [
                    r[0]
                    for r in db.execute(
                        select(UserChatSession.id).where(UserChatSession.friendship_id == friendship_id)
                    ).all()
                    if r and r[0] is not None
                ]
            else:
                session_ids_to_delete = [session_id]

            # Delete messages first (backend FK may not be ON DELETE CASCADE everywhere).
            if session_ids_to_delete:
                db.execute(delete(UserChatMessage).where(UserChatMessage.session_id.in_(session_ids_to_delete)))
                db.execute(delete(UserChatSession).where(UserChatSession.id.in_(session_ids_to_delete)))
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_delete)