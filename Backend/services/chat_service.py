import json
import os
from pathlib import Path
from typing import List, Optional

from openai import AsyncOpenAI


def _backend_root() -> Path:
    # Backend/
    return Path(__file__).resolve().parents[1]


class ChatService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5.2"
        self.vision_model = "gpt-5.2"

        prompt_path = _backend_root() / "resources" / "chat_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.system_prompt = f.read().strip()

    def build_messages(
        self,
        *,
        session_partner_letter: str,
        last_user_message: str,
        partner_ab_context_text: Optional[str] = None,
        image_urls: Optional[List[str]] = None,
        file_urls: Optional[List[str]] = None,
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

        input_messages: List[dict] = [
            {"role": "system", "content": f"I'm Partner {session_partner_letter}"},
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content},
        ]

        if partner_ab_context_text:
            input_messages.append({"role": "system", "content": partner_ab_context_text})

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

    def stream_response(self, *, messages: List[dict], previous_response_id: Optional[str] = None):
        return self.client.responses.stream(
            model=self.vision_model if any(isinstance(m.get("content"), list) for m in messages) else self.model,
            input=messages,
            store=True,
            text={"verbosity": "medium"},
            reasoning={"effort": "medium"},
            previous_response_id=previous_response_id,
        )


class ChatTitleService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5.2"

        prompt_path = _backend_root() / "resources" / "chat_title_generation_prompt.txt"
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

