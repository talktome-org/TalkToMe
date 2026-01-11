import uuid
from typing import List

from starlette.concurrency import run_in_threadpool

from sqlalchemy import delete, select, update

from ...database import SessionLocal
from ...models.chat.chat_models import UserChatMessage, UserChatSession

TABLE_NAME = "user_chat_messages"
SESSIONS_TABLE = "user_chat_sessions"


def _to_dict(obj) -> dict:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}


async def save_message(*, user_id: uuid.UUID, session_id: uuid.UUID, role: str, content: str) -> dict:
    try:
        preview = (content or "")[:120].replace("\n", " ")
        print(f"[DB] save_message insert role={role} session_id={session_id} user_id={user_id} preview={preview!r}")
    except Exception:
        pass

    def _insert():
        db = SessionLocal()
        try:
            msg = UserChatMessage(user_id=user_id, session_id=session_id, role=role, content=content)
            db.add(msg)
            db.commit()
            db.refresh(msg)
            return _to_dict(msg)
        finally:
            db.close()

    inserted = await run_in_threadpool(_insert)
    try:
        print(f"[DB] save_message ok id={inserted.get('id')}")
    except Exception:
        pass
    return inserted


async def list_messages_for_session(
    *, user_id: uuid.UUID, session_id: uuid.UUID, limit: int = 100, offset: int = 0
) -> List[dict]:
    def _select():
        db = SessionLocal()
        try:
            q = (
                select(UserChatMessage)
                .where(UserChatMessage.user_id == user_id, UserChatMessage.session_id == session_id)
                .order_by(UserChatMessage.created_at.asc())
                .limit(int(limit))
                .offset(int(offset))
            )
            rows = db.execute(q).scalars().all()
            return [_to_dict(r) for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def update_session_last_message(*, session_id: uuid.UUID, content: str) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id)
                .values(last_message_content=content)
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def delete_messages_for_session(*, user_id: uuid.UUID, session_id: uuid.UUID) -> int:
    def _delete():
        db = SessionLocal()
        try:
            # First count then delete (keeps behavior similar without relying on RETURNING)
            count = db.execute(
                select(UserChatMessage.id).where(
                    UserChatMessage.user_id == user_id, UserChatMessage.session_id == session_id
                )
            ).all()
            db.execute(
                delete(UserChatMessage).where(
                    UserChatMessage.user_id == user_id, UserChatMessage.session_id == session_id
                )
            )
            db.commit()
            return len(count or [])
        finally:
            db.close()

    return await run_in_threadpool(_delete)


async def is_session_empty(*, session_id: uuid.UUID) -> bool:
    def _count():
        db = SessionLocal()
        try:
            row = db.execute(
                select(UserChatMessage.id).where(UserChatMessage.session_id == session_id).limit(1)
            ).first()
            return row is None
        finally:
            db.close()

    return await run_in_threadpool(_count)


async def count_user_messages(*, session_id: uuid.UUID) -> int:
    def _count():
        db = SessionLocal()
        try:
            rows = db.execute(
                select(UserChatMessage.id).where(
                    UserChatMessage.session_id == session_id, UserChatMessage.role == "user"
                )
            ).all()
            return len(rows or [])
        finally:
            db.close()

    return await run_in_threadpool(_count)


async def get_recent_user_messages(*, session_id: uuid.UUID, limit: int = 2) -> List[str]:
    def _select():
        db = SessionLocal()
        try:
            rows = (
                db.execute(
                    select(UserChatMessage.content)
                    .where(UserChatMessage.session_id == session_id, UserChatMessage.role == "user")
                    .order_by(UserChatMessage.created_at.asc())
                    .limit(int(limit))
                )
                .all()
            )
            return [r[0] for r in rows if r and r[0] is not None]
        finally:
            db.close()

    return await run_in_threadpool(_select)