import uuid
from typing import Any, Dict, List, Optional

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


async def save_message_with_id(
    *,
    user_id: uuid.UUID,
    session_id: uuid.UUID,
    role: str,
    content: str,
    message_id: Optional[uuid.UUID],
) -> dict:
    """
    Idempotent insert for client-supplied message ids.
    - If `message_id` is None, behaves like `save_message`.
    - If a row with `message_id` already exists:
        - Return it if it belongs to the same user/session/role
        - Otherwise raise PermissionError (prevents cross-user id collisions)
    """
    if message_id is None:
        return await save_message(user_id=user_id, session_id=session_id, role=role, content=content)

    try:
        preview = (content or "")[:120].replace("\n", " ")
        print(
            f"[DB] save_message_with_id insert role={role} session_id={session_id} user_id={user_id} id={message_id} preview={preview!r}"
        )
    except Exception:
        pass

    def _insert():
        db = SessionLocal()
        try:
            existing = db.get(UserChatMessage, message_id)
            if existing is not None:
                if (
                    existing.user_id == user_id
                    and existing.session_id == session_id
                    and existing.role == role
                ):
                    return _to_dict(existing)
                raise PermissionError("Message ID already exists for a different user/session")

            msg = UserChatMessage(id=message_id, user_id=user_id, session_id=session_id, role=role, content=content)
            db.add(msg)
            db.commit()
            db.refresh(msg)
            return _to_dict(msg)
        finally:
            db.close()

    inserted = await run_in_threadpool(_insert)
    try:
        print(f"[DB] save_message_with_id ok id={inserted.get('id')}")
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


async def update_session_last_message(*, user_id: uuid.UUID, session_id: uuid.UUID, content: str) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(UserChatSession)
                .where(UserChatSession.id == session_id, UserChatSession.user_id == user_id)
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


async def delete_messages_after(*, user_id: uuid.UUID, session_id: uuid.UUID, after_message_id: uuid.UUID, include_anchor: bool = False) -> int:
    """Delete all messages in a session that were created after the given message.
    When *include_anchor* is True the anchor message itself is also deleted
    (useful for regeneration where the user message is re-created)."""

    def _delete():
        db = SessionLocal()
        try:
            anchor = db.get(UserChatMessage, after_message_id)
            if anchor is None:
                return 0
            if anchor.session_id != session_id:
                return 0

            anchor_ts = anchor.created_at

            from sqlalchemy import and_, or_

            if include_anchor:
                condition = and_(
                    UserChatMessage.session_id == session_id,
                    or_(
                        UserChatMessage.created_at > anchor_ts,
                        UserChatMessage.created_at == anchor_ts,
                    ),
                )
            else:
                condition = and_(
                    UserChatMessage.session_id == session_id,
                    or_(
                        UserChatMessage.created_at > anchor_ts,
                        and_(
                            UserChatMessage.created_at == anchor_ts,
                            UserChatMessage.id != after_message_id,
                        ),
                    ),
                )
            count = len(db.execute(select(UserChatMessage.id).where(condition)).all())
            db.execute(delete(UserChatMessage).where(condition))
            db.commit()
            return count
        finally:
            db.close()

    return await run_in_threadpool(_delete)


async def delete_partner_message(
    *,
    sender_user_id: uuid.UUID,
    sender_session_id: uuid.UUID,
    message_text: str,
) -> Optional[Dict[str, Any]]:
    """Delete a partner message from the recipient's linked session.
    Identifies the message by matching the JSON payload content.
    Returns recipient info dict on success, None on failure."""
    import json

    def _delete():
        db = SessionLocal()
        try:
            sender_session = (
                db.execute(
                    select(UserChatSession).where(
                        UserChatSession.id == sender_session_id,
                        UserChatSession.user_id == sender_user_id,
                    )
                )
                .scalars()
                .first()
            )
            if not sender_session or not sender_session.linked_session_id:
                return None

            recipient_session_id = sender_session.linked_session_id

            # Resolve recipient user_id from their session
            recipient_session = (
                db.execute(
                    select(UserChatSession).where(
                        UserChatSession.id == recipient_session_id,
                    )
                )
                .scalars()
                .first()
            )
            recipient_user_id = recipient_session.user_id if recipient_session else None

            expected_payload = json.dumps(
                {
                    "_talktome": {
                        "type": "partner_received",
                        "text": message_text,
                        "sender_user_id": str(sender_user_id),
                    }
                }
            )

            msg = (
                db.execute(
                    select(UserChatMessage).where(
                        UserChatMessage.session_id == recipient_session_id,
                        UserChatMessage.role == "assistant",
                        UserChatMessage.content == expected_payload,
                    )
                    .order_by(UserChatMessage.created_at.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if not msg:
                return None

            db.delete(msg)
            db.commit()
            return {
                "deleted": True,
                "recipient_user_id": recipient_user_id,
                "recipient_session_id": recipient_session_id,
            }
        finally:
            db.close()

    return await run_in_threadpool(_delete)


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