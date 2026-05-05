"""
LiveKit endpoint.

POST /livekit/token — mints a room token tied to the caller's Supabase identity.
The token also embeds an agent dispatch request with per-session metadata, so
the worker reads context from ctx.job.metadata — no separate lookup endpoint.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from Backend.auth import get_current_user
from Backend.voice_agent.token import mint_token


router = APIRouter(tags=["livekit"])


class TokenRequest(BaseModel):
    voice_agent: str
    ghost_name: Optional[str] = None
    custom_prompt: Optional[str] = None


class TokenResponse(BaseModel):
    url: str
    token: str
    room: str


@router.post("/livekit/token", response_model=TokenResponse)
def create_token(
    body: TokenRequest,
    current_user: dict = Depends(get_current_user),
) -> TokenResponse:
    user_id = current_user.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing subject claim")

    voice_agent = (body.voice_agent or "").strip()
    if not voice_agent:
        raise HTTPException(status_code=400, detail="voice_agent is required")

    try:
        url, jwt_token, room = mint_token(
            user_id=user_id,
            voice_agent=voice_agent,
            ghost_name=body.ghost_name,
            custom_prompt=body.custom_prompt,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return TokenResponse(url=url, token=jwt_token, room=room)
