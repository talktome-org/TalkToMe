"""
LiveKit access-token issuance.

The token embeds an explicit agent-dispatch request with per-session metadata
(voice agent, ghost name, custom prompt, chat session id) via RoomConfiguration.
The worker reads this from ctx.job.metadata directly — no out-of-band lookup,
no shared state between the app and worker processes.
"""
from __future__ import annotations

import json
import os
import secrets
from datetime import timedelta
from typing import Optional

from livekit import api


_TOKEN_TTL_SECONDS = 15 * 60
_AGENT_NAME = "bobo-voice"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def mint_token(
    user_id: str,
    voice_agent: str,
    ghost_name: Optional[str] = None,
    custom_prompt: Optional[str] = None,
    chat_session_id: Optional[str] = None,
) -> tuple[str, str, str]:
    """
    Mint a LiveKit access token that also dispatches the bobo voice agent
    with per-session metadata.

    Returns (livekit_url, jwt, room_name).
    """
    livekit_url = _require_env("LIVEKIT_URL")
    api_key = _require_env("LIVEKIT_API_KEY")
    api_secret = _require_env("LIVEKIT_API_SECRET")

    room = f"voice_{user_id}_{secrets.token_hex(6)}"
    metadata = json.dumps(
        {
            "user_id": user_id,
            "voice_agent": voice_agent,
            "ghost_name": ghost_name,
            "custom_prompt": custom_prompt,
            "chat_session_id": chat_session_id,
        }
    )

    token = (
        api.AccessToken(api_key, api_secret)
        .with_identity(user_id)
        .with_name(user_id)
        .with_ttl(timedelta(seconds=_TOKEN_TTL_SECONDS))
        .with_grants(
            api.VideoGrants(
                room_join=True,
                room=room,
                can_publish=True,
                can_subscribe=True,
                can_publish_data=True,
            )
        )
        .with_room_config(
            api.RoomConfiguration(
                agents=[api.RoomAgentDispatch(agent_name=_AGENT_NAME, metadata=metadata)],
            )
        )
        .to_jwt()
    )

    return livekit_url, token, room
