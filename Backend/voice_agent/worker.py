"""
LiveKit Agents worker for Bobo voice mode.

Architecture:
  user audio  ─WebRTC─▶  OpenAI Realtime  ─text─▶  ElevenLabs TTS  ─WebRTC─▶  user

The Realtime model handles ASR + LLM + semantic-VAD + tool calling but emits
text only (modalities=["text"]); ElevenLabs renders the voice so we keep our
6 branded characters (Mira, Pax, Luma, Snow, Jay, Hex).

Per-session config (buddy, custom prompt, user id) arrives via ctx.job.metadata,
embedded by the backend in the access token via RoomAgentDispatch.

Entry: `python -m Backend.voice_agent.worker dev` for local, or as a Fly
process per fly.toml.
"""
from __future__ import annotations

from Backend._env import load as _load_env

_load_env()

import json
import logging
import os
from typing import Optional

from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli
from livekit.plugins import elevenlabs, openai, silero

from Backend.voice_agent.prompts import VoiceAgentPromptLibrary
from Backend.voice_agent.tools import build_tools


logger = logging.getLogger("bobo.voice_agent")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

_AGENT_NAME = "bobo-voice"
_DEFAULT_VOICE_AGENT = "mira"
_REALTIME_MODEL = "gpt-realtime"
_ELEVENLABS_MODEL = "eleven_flash_v2_5"


_prompts = VoiceAgentPromptLibrary()


def _resolve_voice_id(voice_agent: str) -> str:
    """
    Resolve a buddy name to its ElevenLabs voice id. Falls back to the default
    buddy's voice if the requested env var is missing — losing the per-buddy
    voice is better than killing the whole voice session.
    """
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


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    metadata = _parse_metadata(ctx.job.metadata)
    voice_agent = (metadata.get("voice_agent") or _DEFAULT_VOICE_AGENT).strip().lower()
    voice_id = _resolve_voice_id(voice_agent)
    instructions = _build_instructions(voice_agent, metadata.get("custom_prompt"))
    user_id = metadata.get("user_id")

    logger.info(
        "voice agent dispatched: room=%s voice_agent=%s voice_id=%s prompt_chars=%d user_id=%s",
        ctx.room.name,
        voice_agent,
        voice_id,
        len(instructions),
        user_id,
    )

    session = AgentSession(
        vad=silero.VAD.load(),
        llm=openai.realtime.RealtimeModel(
            model=_REALTIME_MODEL,
            modalities=["text"],
            turn_detection={"type": "semantic_vad", "eagerness": "medium"},
        ),
        tts=elevenlabs.TTS(
            voice_id=voice_id,
            model=_ELEVENLABS_MODEL,
        ),
    )

    tools = build_tools(user_id=user_id) if user_id else []

    await session.start(
        agent=BoboAgent(instructions=instructions, tools=tools),
        room=ctx.room,
    )


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, agent_name=_AGENT_NAME))
