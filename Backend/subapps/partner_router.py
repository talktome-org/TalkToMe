from __future__ import annotations

import json
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from Backend.auth import get_current_user
from Backend.crud.chat.chat_crud import save_message, update_session_last_message
from Backend.crud.chat.chat_session_crud import (
    attach_friendship_to_session,
    get_or_create_session_for_friendship,
    get_session_by_id,
    touch_session,
    update_session_title,
)
from Backend.crud.friends.friends_crud import get_friendship_id_for_pair
from Backend.database import SessionLocal
from Backend.models.friends.friendship_model import Friendship
from Backend.services.apns_service import send_partner_message_notification_to_user


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


@router.post("/send-message", response_model=SendPartnerMessageResponse)
async def send_message(request: SendPartnerMessageRequest, current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    message = (request.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    friendship_id: Optional[uuid.UUID] = None

    # Determine friendship_id (code-based connection).
    if request.friend_user_id:
        friendship_id = await _get_friendship_id_for_pair(user_id=user_id, friend_user_id=request.friend_user_id)
        if not friendship_id:
            raise HTTPException(status_code=400, detail="You are not connected to that friend")
    else:
        # If session already has a friendship, use it. Otherwise require friend_user_id.
        if request.session_id:
            sess = await get_session_by_id(user_id=user_id, session_id=request.session_id)
            if not sess:
                raise HTTPException(status_code=404, detail="Session not found")
            fid = sess.get("friendship_id")
            if fid:
                try:
                    friendship_id = uuid.UUID(str(fid))
                except Exception:
                    friendship_id = None

        if not friendship_id:
            raise HTTPException(status_code=400, detail="Friend selection required")

    # Ensure the sender session is connected (auto-connect on first send).
    if request.session_id:
        try:
            await attach_friendship_to_session(user_id=user_id, session_id=request.session_id, friendship_id=friendship_id)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e))

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

    from starlette.concurrency import run_in_threadpool

    recipient_user_id = await run_in_threadpool(_get_other_user)
    if not recipient_user_id or recipient_user_id == user_id:
        raise HTTPException(status_code=400, detail="Invalid friendship")

    # Create/get recipient session bound to this friendship.
    recipient_session = await get_or_create_session_for_friendship(
        user_id=recipient_user_id,
        friendship_id=friendship_id,
        title=None,
    )
    recipient_session_id = uuid.UUID(str(recipient_session["id"]))

    # Make the title match the sender's AI-generated title (first conversation title).
    try:
        if request.session_id:
            sender_sess = await get_session_by_id(user_id=user_id, session_id=request.session_id)
            sender_title = (sender_sess or {}).get("title") if sender_sess else None
            if isinstance(sender_title, str):
                sender_title = sender_title.strip()
            if sender_title and sender_title != "New Chat":
                await update_session_title(user_id=recipient_user_id, session_id=recipient_session_id, title=sender_title)
    except Exception:
        pass

    # Store message as an assistant message with TalkToMe metadata.
    payload = json.dumps({"_talktome": {"type": "partner_received", "text": message}})
    await save_message(user_id=recipient_user_id, session_id=recipient_session_id, role="assistant", content=payload)
    # Keep the sidebar preview human-readable.
    await update_session_last_message(session_id=recipient_session_id, content=message[:120])
    await touch_session(session_id=recipient_session_id)

    # Push notify recipient.
    try:
        await send_partner_message_notification_to_user(
            recipient_user_id=recipient_user_id,
            session_id=recipient_session_id,
            preview=message[:120],
            sender_name=None,
        )
    except Exception:
        # Push failures should not block sending.
        pass

    return SendPartnerMessageResponse(success=True, recipient_session_id=recipient_session_id)

