"""
LiveKit Agents worker for Bobo voice mode.

Architecture:
  user audio  ─WebRTC─▶  OpenAI Realtime  ─text─▶  ElevenLabs TTS  ─WebRTC─▶  user

The Realtime model handles ASR + LLM + semantic-VAD + tool calling but emits
text only (modalities=["text"]); ElevenLabs renders the voice so we keep our
6 branded characters (Mira, Pax, Luma, Snow, Jay, Hex).

Per-session config (buddy, custom prompt, user id, chat session id) arrives
via ctx.job.metadata, embedded by the backend in the access token via
RoomAgentDispatch. When chat_session_id is present, every committed
conversation turn is persisted into the user's chat thread (source='voice')
and a `bobo.message_committed` text-stream event is published to the room
so the iOS client can insert the message instantly.

Entry: `python -m Backend.voice_agent.worker dev` for local, or as a Fly
process per fly.toml.
"""
from __future__ import annotations

from Backend._env import load as _load_env

_load_env()

import asyncio
import json
import logging
import os
import uuid
from typing import Optional

from livekit.agents import (
    Agent,
    AgentSession,
    ConversationItemAddedEvent,
    JobContext,
    WorkerOptions,
    cli,
)
from livekit.agents.llm import ChatMessage
from livekit.plugins import elevenlabs, openai
from openai.types.realtime.realtime_audio_input_turn_detection import SemanticVad

from Backend.crud.chat.chat_crud import save_message, update_session_last_message
from Backend.crud.chat.chat_session_crud import touch_session
from Backend.voice_agent.prompts import VoiceAgentPromptLibrary
from Backend.voice_agent.tools import build_tools


logger = logging.getLogger("bobo.voice_agent")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

_AGENT_NAME = "bobo-voice"
_DEFAULT_VOICE_AGENT = "mira"
_REALTIME_MODEL = "gpt-realtime"
_ELEVENLABS_MODEL = "eleven_flash_v2_5"
_COMMIT_TOPIC = "bobo.message_committed"


_prompts = VoiceAgentPromptLibrary()


def _resolve_voice_id(voice_agent: str) -> str:
    """Resolve buddy name to ElevenLabs voice id; fall back to Mira if missing."""
    key = f"VOICE_ID_{voice_agent.strip().upper()}"
    voice_id = os.getenv(key)
    if voice_id:
        return voice_id

    fallback_key = f"VOICE_ID_{_DEFAULT_VOICE_AGENT.upper()}"
    fallback = os.getenv(fallback_key)
    if fallback:
        logger.warning("%s not set; falling back to %s", key, fallback_key)
        return fallback

    raise RuntimeError(
        f"Neither {key} nor {fallback_key} is set; cannot resolve any ElevenLabs voice"
    )


def _parse_metadata(raw: Optional[str]) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        logger.warning("Job metadata is not valid JSON; using defaults")
        return {}


def _build_instructions(voice_agent: str, custom_prompt: Optional[str]) -> str:
    base = _prompts.get_prompt(voice_agent) or ""
    if custom_prompt:
        suffix = custom_prompt.strip()
        if suffix:
            base = f"{base}\n\n{suffix}".strip()
    return base or "You are a warm, conversational companion. Keep replies brief and natural for voice."


class BoboAgent(Agent):
    def __init__(self, instructions: str, tools: list) -> None:
        super().__init__(instructions=instructions, tools=tools)


async def _persist_and_announce_turn(
    *,
    ctx: JobContext,
    user_uuid: uuid.UUID,
    session_uuid: uuid.UUID,
    role: str,
    content: str,
) -> None:
    """Save a committed voice turn to the chat DB and notify the iOS client."""
    try:
        saved = await save_message(
            user_id=user_uuid,
            session_id=session_uuid,
            role=role,
            content=content,
            source="voice",
        )
    except Exception as exc:
        logger.exception("Failed to persist voice turn: %s", exc)
        return

    # Keep the chat-list preview in sync; tolerate any single failure.
    try:
        await update_session_last_message(
            user_id=user_uuid, session_id=session_uuid, content=content[:120]
        )
        await touch_session(user_id=user_uuid, session_id=session_uuid)
    except Exception:
        logger.exception("Failed to update session preview after voice turn")

    created_at = saved.get("created_at")
    payload = json.dumps(
        {
            "id": str(saved.get("id")) if saved.get("id") is not None else None,
            "session_id": str(session_uuid),
            "role": role,
            "content": content,
            "source": "voice",
            "created_at": created_at.isoformat() if created_at is not None else None,
        }
    )
    try:
        await ctx.room.local_participant.send_text(payload, topic=_COMMIT_TOPIC)
    except Exception:
        logger.exception("Failed to publish %s event for turn", _COMMIT_TOPIC)


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    metadata = _parse_metadata(ctx.job.metadata)
    voice_agent = (metadata.get("voice_agent") or _DEFAULT_VOICE_AGENT).strip().lower()
    voice_id = _resolve_voice_id(voice_agent)
    instructions = _build_instructions(voice_agent, metadata.get("custom_prompt"))
    user_id = metadata.get("user_id")
    chat_session_id = metadata.get("chat_session_id")

    user_uuid: Optional[uuid.UUID] = None
    session_uuid: Optional[uuid.UUID] = None
    if user_id and chat_session_id:
        try:
            user_uuid = uuid.UUID(user_id)
            session_uuid = uuid.UUID(chat_session_id)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid user_id or chat_session_id in metadata; voice turns will not be persisted"
            )

    logger.info(
        "voice agent dispatched: room=%s voice_agent=%s voice_id=%s prompt_chars=%d user_id=%s session=%s",
        ctx.room.name,
        voice_agent,
        voice_id,
        len(instructions),
        user_id,
        chat_session_id,
    )

    session = AgentSession(
        # No local VAD: OpenAI Realtime handles VAD + turn detection + interruptions
        # server-side via SemanticVad. Same pattern ChatGPT voice mode uses.
        llm=openai.realtime.RealtimeModel(
            model=_REALTIME_MODEL,
            modalities=["text"],
            turn_detection=SemanticVad(type="semantic_vad", eagerness="medium"),
        ),
        tts=elevenlabs.TTS(
            voice_id=voice_id,
            model=_ELEVENLABS_MODEL,
        ),
    )

    if user_uuid is not None and session_uuid is not None:
        @session.on("conversation_item_added")
        def _on_item(event: ConversationItemAddedEvent) -> None:
            item = event.item
            if not isinstance(item, ChatMessage):
                return
            text_content = (item.text_content or "").strip()
            if not text_content:
                return
            if item.role not in ("user", "assistant"):
                return
            asyncio.create_task(
                _persist_and_announce_turn(
                    ctx=ctx,
                    user_uuid=user_uuid,
                    session_uuid=session_uuid,
                    role=item.role,
                    content=text_content,
                )
            )

    tools = build_tools(user_id=user_id) if user_id else []

    await session.start(
        agent=BoboAgent(instructions=instructions, tools=tools),
        room=ctx.room,
    )


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, agent_name=_AGENT_NAME))
