from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from Backend.auth import get_current_user
from Backend.crud.chat.chat_crud import delete_partner_message, save_message, update_session_last_message
from Backend.crud.chat.chat_session_crud import (
    ensure_linked_session_for_friendship,
    get_session_by_id,
    increment_unread_count,
    touch_session,
    update_session_title,
)
from Backend.crud.friends.friends_crud import get_friendship_id_for_pair
from Backend.database import SessionLocal
from Backend.models.friends.friendship_model import Friendship
from Backend.models.profile.profile_model import Profile
from Backend.services.apns_service import send_partner_message_notification_to_user, send_unsend_notification_to_user
from sqlalchemy import text as sa_text
from starlette.concurrency import run_in_threadpool


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/partner", tags=["partner"])


class SendPartnerMessageRequest(BaseModel):
    message: str
    session_id: Optional[uuid.UUID] = None
    friend_user_id: Optional[uuid.UUID] = None


class SendPartnerMessageResponse(BaseModel):
    success: bool
    recipient_session_id: uuid.UUID


async def _get_friendship_id_for_pair(*, user_id: uuid.UUID, friend_user_id: uuid.UUID) -> Optional[uuid.UUID]:
    return await get_friendship_id_for_pair(user_id=user_id, friend_user_id=friend_user_id)


async def _resolve_display_name(*, user_id: uuid.UUID) -> Optional[str]:
    def _select():
        db = SessionLocal()
        try:
            prof = db.get(Profile, user_id)
            if prof is not None:
                n = (getattr(prof, "full_name", None) or "").strip()
                if n:
                    return n
            row = db.execute(sa_text("select raw_user_meta_data from auth.users where id = :id"), {"id": str(user_id)}).first()
            meta = row[0] if row and isinstance(row[0], dict) else {}
            if isinstance(meta, dict):
                n = (meta.get("full_name") or meta.get("name") or "").strip()
                if n:
                    return n
            return None
        finally:
            db.close()

    return await run_in_threadpool(_select)


@router.post("/send-message", response_model=SendPartnerMessageResponse)
async def send_message(request: SendPartnerMessageRequest, current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    message = (request.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    if not request.session_id:
        raise HTTPException(status_code=400, detail="session_id required")

    sender_session = await get_session_by_id(user_id=user_id, session_id=request.session_id)
    if not sender_session:
        raise HTTPException(status_code=404, detail="Session not found")

    friendship_id: Optional[uuid.UUID] = None

    # Determine friendship_id (code-based connection).
    if request.friend_user_id:
        friendship_id = await _get_friendship_id_for_pair(user_id=user_id, friend_user_id=request.friend_user_id)
        if not friendship_id:
            raise HTTPException(status_code=400, detail="You are not connected to that friend")
    else:
        # If session already has a friendship, use it. Otherwise require friend_user_id.
        fid = sender_session.get("friendship_id")
        if fid:
            try:
                friendship_id = uuid.UUID(str(fid))
            except Exception:
                friendship_id = None

        if not friendship_id:
            raise HTTPException(status_code=400, detail="Friend selection required")

    # Enforce "one chat connects to one friend" (prevent switching a session's friend).
    existing_fid = sender_session.get("friendship_id")
    if existing_fid and friendship_id:
        try:
            existing_uuid = uuid.UUID(str(existing_fid))
            if existing_uuid != friendship_id:
                raise HTTPException(status_code=400, detail="Session is already connected to a different friend")
        except HTTPException:
            raise
        except Exception:
            # If we can't parse, allow backend to handle as a normal "friend selection required" flow.
            pass

    # Resolve recipient user_id from friendship.
    def _get_other_user():
        db = SessionLocal()
        try:
            row = db.query(Friendship).filter(Friendship.id == friendship_id).limit(1).first()
            if not row:
                return None
            low = uuid.UUID(str(row.user_low_id))
            high = uuid.UUID(str(row.user_high_id))
            return high if low == user_id else low
        finally:
            db.close()

    recipient_user_id = await run_in_threadpool(_get_other_user)
    if not recipient_user_id or recipient_user_id == user_id:
        raise HTTPException(status_code=400, detail="Invalid friendship")

    # Ensure a 1:1 linked session exists for this sender session.
    try:
        recipient_session_id = await ensure_linked_session_for_friendship(
            user_id=user_id,
            session_id=request.session_id,
            recipient_user_id=recipient_user_id,
            friendship_id=friendship_id,
            title=None,
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))

    # Make the title match the sender's AI-generated title (first conversation title).
    try:
        sender_title = (sender_session or {}).get("title") if sender_session else None
        if isinstance(sender_title, str):
            sender_title = sender_title.strip()
        if sender_title and sender_title != "New Chat":
            await update_session_title(user_id=recipient_user_id, session_id=recipient_session_id, title=sender_title)
    except Exception:
        pass

    # Store message as an assistant message with TalkToMe metadata.
    # IMPORTANT: `user_id` on chat message rows is the *recipient/session owner* (RLS),
    # so we include the real sender id in metadata for the client UI.
    payload = json.dumps({"_talktome": {"type": "partner_received", "text": message, "sender_user_id": str(user_id)}})
    await save_message(user_id=recipient_user_id, session_id=recipient_session_id, role="assistant", content=payload)
    # Keep the sidebar preview human-readable.
    await update_session_last_message(session_id=recipient_session_id, content=message[:120])
    await touch_session(session_id=recipient_session_id)
    await increment_unread_count(session_id=recipient_session_id)

    # Resolve sender display name for push notification.
    sender_name: Optional[str] = None
    try:
        sender_name = await _resolve_display_name(user_id=user_id)
    except Exception:
        pass

    # Push notify recipient.
    try:
        await send_partner_message_notification_to_user(
            recipient_user_id=recipient_user_id,
            session_id=recipient_session_id,
            preview=message[:120],
            sender_name=sender_name,
        )
    except Exception as exc:
        # Push failures should not block sending.
        logger.exception("[PARTNER] Push notification failed for recipient %s: %s", recipient_user_id, exc)

    return SendPartnerMessageResponse(success=True, recipient_session_id=recipient_session_id)


class UnsendPartnerMessageRequest(BaseModel):
    session_id: uuid.UUID
    message_text: str


class UnsendPartnerMessageResponse(BaseModel):
    success: bool


@router.post("/unsend-message", response_model=UnsendPartnerMessageResponse)
async def unsend_message(request: UnsendPartnerMessageRequest, current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    message_text = (request.message_text or "").strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="message_text required")

    result = await delete_partner_message(
        sender_user_id=user_id,
        sender_session_id=request.session_id,
        message_text=message_text,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Message not found or already deleted")

    # Send silent push so recipient's app retracts the notification.
    recipient_user_id = result.get("recipient_user_id")
    recipient_session_id = result.get("recipient_session_id")
    if recipient_user_id and recipient_session_id:
        try:
            await send_unsend_notification_to_user(
                recipient_user_id=recipient_user_id,
                session_id=recipient_session_id,
            )
        except Exception as exc:
            logger.exception("[PARTNER] Unsend push failed for recipient %s: %s", recipient_user_id, exc)

    return UnsendPartnerMessageResponse(success=True)

