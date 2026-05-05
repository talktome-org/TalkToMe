"""
LiveKit endpoint.

POST /livekit/token — mints a room token tied to the caller's Supabase identity.
The token also embeds an agent dispatch request with per-session metadata
(including the chat session_id), so the worker reads context from
ctx.job.metadata and persists voice turns into that chat session.
"""
from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from Backend.auth import get_current_user
from Backend.crud.chat.chat_session_crud import assert_session_owned_by_user
from Backend.voice_agent.token import mint_token


router = APIRouter(tags=["livekit"])


class TokenRequest(BaseModel):
    voice_agent: str
    ghost_name: Optional[str] = None
    custom_prompt: Optional[str] = None
    session_id: Optional[uuid.UUID] = None


class TokenResponse(BaseModel):
    url: str
    token: str
    room: str


@router.post("/livekit/token", response_model=TokenResponse)
async def create_token(
    body: TokenRequest,
    current_user: dict = Depends(get_current_user),
) -> TokenResponse:
    user_id = current_user.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing subject claim")

    voice_agent = (body.voice_agent or "").strip()
    if not voice_agent:
        raise HTTPException(status_code=400, detail="voice_agent is required")

    # Match the iOS client cap so a tampered payload can't force unbounded
    # text into the worker's system prompt.
    custom_prompt = body.custom_prompt
    if custom_prompt is not None and len(custom_prompt) > 8000:
        custom_prompt = custom_prompt[:8000]

    if body.session_id is not None:
        try:
            await assert_session_owned_by_user(
                user_id=uuid.UUID(user_id), session_id=body.session_id
            )
        except PermissionError:
            raise HTTPException(status_code=403, detail="Session not found or not owned by user")

    try:
        url, jwt_token, room = mint_token(
            user_id=user_id,
            voice_agent=voice_agent,
            ghost_name=body.ghost_name,
            custom_prompt=custom_prompt,
            chat_session_id=str(body.session_id) if body.session_id else None,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return TokenResponse(url=url, token=jwt_token, room=room)
