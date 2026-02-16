import json
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, List, Optional

from google import genai
from google.genai import types as genai_types
from openai import AsyncOpenAI


def _backend_root() -> Path:
    # Backend/
    return Path(__file__).resolve().parents[1]


class ChatService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5.2"
        self.vision_model = "gpt-5.2"

        prompt_path = _backend_root() / "resources" / "general_prompts" / "chat_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.system_prompt = f.read().strip()

    def build_messages(
        self,
        *,
        last_user_message: str,
        context_text: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
        file_urls: Optional[List[str]] = None,
        system_prompt_override: Optional[str] = None,
    ) -> List[dict]:
        input_text = f"last user message: {last_user_message}"
        if file_urls:
            joined = "\n".join([u for u in file_urls if u])
            if joined:
                input_text += f"\n\nUser attached file(s):\n{joined}"

        user_content: list = [{"type": "input_text", "text": input_text}]
        for url in image_urls or []:
            if url:
                user_content.append({"type": "input_image", "image_url": url})

        input_messages: List[dict] = []

        system_prompt = system_prompt_override if system_prompt_override is not None else self.system_prompt
        if system_prompt:
            input_messages.append({"role": "system", "content": system_prompt})

        input_messages.append({"role": "user", "content": user_content})

        if context_text:
            input_messages.append({"role": "system", "content": context_text})

        return input_messages

    async def create_response(self, *, messages: List[dict], previous_response_id: Optional[str] = None):
        return await self.client.responses.create(
            model=self.vision_model if any(isinstance(m.get("content"), list) for m in messages) else self.model,
            input=messages,
            # Ensure `previous_response_id` remains usable across subsequent calls.
            # If responses aren't stored, OpenAI may reject future requests that reference an id.
            store=True,
            text={"verbosity": "medium"},
            reasoning={"effort": "minimal"},
            previous_response_id=previous_response_id,
        )

    def stream_response(self, *, messages: List[dict], previous_response_id: Optional[str] = None, reasoning_effort: str = "medium"):
        return self.client.responses.stream(
            model=self.vision_model if any(isinstance(m.get("content"), list) for m in messages) else self.model,
            input=messages,
            store=True,
            text={"verbosity": "medium"},
            reasoning={"effort": reasoning_effort, "summary": "auto"},
            previous_response_id=previous_response_id,
        )


class ChatTitleService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5.2"

        prompt_path = _backend_root() / "resources" / "general_prompts" / "chat_title_generation_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.title_generation_prompt = f.read().strip()

    async def generate_chat_title(self, user_messages: list[str]) -> str:
        try:
            def _extract_text(msg: str) -> str:
                try:
                    obj = json.loads(msg or "")
                    if not isinstance(obj, dict):
                        return (msg or "").strip()
                    talktome = obj.get("_talktome")
                    if isinstance(talktome, str):
                        try:
                            talktome = json.loads(talktome)
                        except Exception:
                            talktome = None
                    if isinstance(talktome, dict) and talktome.get("type") == "segments":
                        segs = talktome.get("segments") or []
                        texts = []
                        for seg in segs:
                            if isinstance(seg, dict) and seg.get("type") == "text":
                                t = (seg.get("content") or "").strip()
                                if t:
                                    texts.append(t)
                        return " ".join(texts).strip()
                    return (msg or "").strip()
                except Exception:
                    return (msg or "").strip()

            combined = " ... ".join(_extract_text(msg) for msg in user_messages if msg and _extract_text(msg))
            if not combined:
                return "New chat"

            input_messages = [
                {"role": "system", "content": self.title_generation_prompt},
                {"role": "user", "content": combined},
            ]

            resp = await self.client.responses.create(
                model=self.model,
                input=input_messages,
            )

            text = getattr(resp, "output_text", None) or "".join(
                block.text
                for block in getattr(resp, "output", [])
                if getattr(block, "type", None) == "output_text" and getattr(block, "text", None)
            )

            title = (text or "").strip().strip("\"'")
            return title
        except Exception as e:
            print(f"OpenAI API error in chat title generation: {e}")
            return "New chat"


@dataclass
class GeminiStreamEvent:
    """Event object mimicking OpenAI streaming events for compatibility."""
    type: str
    delta: str = ""
    response: Optional[object] = None
    error: Optional[str] = None


class VoiceChatService:
    """
    Gemini-based chat service for voice-first mode.
    Uses gemini-2.5-flash for fast, low-latency responses.
    """

    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        self.client = genai.Client(api_key=api_key) if api_key else None
        self.model_name = "gemini-2.5-flash"

    def build_messages(
        self,
        *,
        last_user_message: str,
        chat_history: Optional[List[dict]] = None,
        context_text: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
        file_urls: Optional[List[str]] = None,
        system_prompt_override: Optional[str] = None,
    ) -> tuple[Optional[str], List[genai_types.Content]]:
        """
        Build messages for Gemini. Returns (system_instruction, contents).
        Gemini uses a different format: system_instruction is separate from contents.
        Contents is a list of Content objects for multi-turn conversation.
        """
        system_instruction = system_prompt_override if system_prompt_override is not None else None

        # Build multi-turn conversation contents
        contents: List[genai_types.Content] = []

        # Add chat history as previous turns (if any)
        if chat_history:
            for msg in chat_history:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if not content:
                    continue
                # Gemini uses "user" and "model" roles (not "assistant")
                gemini_role = "model" if role == "assistant" else "user"
                contents.append(genai_types.Content(
                    role=gemini_role,
                    parts=[genai_types.Part.from_text(text=content)]
                ))

        # Build current user message
        input_text = last_user_message
        if file_urls:
            joined = "\n".join([u for u in file_urls if u])
            if joined:
                input_text += f"\n\nUser attached file(s):\n{joined}"

        if context_text:
            input_text = f"Context:\n{context_text}\n\nUser message: {input_text}"

        # Add current user message
        contents.append(genai_types.Content(
            role="user",
            parts=[genai_types.Part.from_text(text=input_text)]
        ))

        return system_instruction, contents

    @asynccontextmanager
    async def stream_response(
        self,
        *,
        messages: tuple[Optional[str], List[genai_types.Content]],
        previous_response_id: Optional[str] = None,
    ) -> AsyncIterator[AsyncIterator[GeminiStreamEvent]]:
        """
        Stream response from Gemini, yielding events compatible with the existing consumer.
        Uses the new google-genai SDK with native async support.
        """
        system_instruction, contents = messages

        async def event_generator() -> AsyncIterator[GeminiStreamEvent]:
            if not self.client:
                yield GeminiStreamEvent(type="response.error", error="GEMINI_API_KEY not configured")
                return

            try:
                # Emit response.created event
                yield GeminiStreamEvent(type="response.created")

                # Build config with system instruction
                config = genai_types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    temperature=0.7,
                    max_output_tokens=2048,
                )

                # Use async streaming with multi-turn contents
                chunk_count = 0
                async for chunk in await self.client.aio.models.generate_content_stream(
                    model=self.model_name,
                    contents=contents,
                    config=config,
                ):
                    chunk_count += 1
                    if chunk.text:
                        yield GeminiStreamEvent(
                            type="response.output_text.delta",
                            delta=chunk.text,
                        )
                    else:
                        # Log why this chunk had no text (safety filter, empty candidates, etc.)
                        finish_reason = None
                        safety_ratings = None
                        with suppress(Exception):
                            if chunk.candidates:
                                finish_reason = chunk.candidates[0].finish_reason
                                safety_ratings = chunk.candidates[0].safety_ratings
                        print(f"[Gemini] Empty chunk #{chunk_count}: finish_reason={finish_reason} safety={safety_ratings}")

                if chunk_count == 0:
                    print("[Gemini] WARNING: Stream returned zero chunks")

                # Emit response.completed event
                yield GeminiStreamEvent(type="response.completed")

            except Exception as e:
                print(f"[Gemini] Stream error: {e}")
                yield GeminiStreamEvent(type="response.error", error=str(e))

        yield event_generator()
