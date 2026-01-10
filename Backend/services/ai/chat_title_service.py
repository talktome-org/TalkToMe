import json
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


def _backend_root() -> Path:
    return Path(__file__).resolve().parents[2]


load_dotenv(dotenv_path=_backend_root() / ".env")


class ChatTitleService:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5-mini"

        prompt_path = _backend_root() / "resources" / "chat_title_generation_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.title_generation_prompt = f.read().strip()

    def generate_chat_title(self, user_messages: list[str]) -> str:
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

            resp = self.client.responses.create(
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

