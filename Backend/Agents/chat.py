import os
import sys
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from Services.rag_service import RAGService

load_dotenv(dotenv_path = Path(__file__).resolve().parent.parent / ".env")


class ChatAgent:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        # Adjust model if needed
        self.model = "gpt-5-mini"
        self.rag_service = RAGService()

        prompt_path = Path(__file__).resolve().parent.parent / "Prompts" / "chat_prompt.txt"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.base_system_prompt = f.read().strip()

    async def build_messages(
        self,
        *,
        session_partner_letter: str,
        last_user_message: str,
        partner_ab_context_text: Optional[str] = None,
    ) -> List[dict]:
        # Retrieve relevant context from therapy books
        context = await self.rag_service.retrieve_context(query=last_user_message, limit=5)

        # Enhance system prompt with RAG context
        enhanced_prompt = self.rag_service.enhance_prompt_with_context(
            base_prompt=self.base_system_prompt,
            context=context,
        )

        input_messages: List[dict] = [
            {"role": "system", "content": f"I'm Partner {session_partner_letter}"},
            {"role": "system", "content": enhanced_prompt},
            {"role": "user", "content": f"last user message: {last_user_message}"},
        ]

        if partner_ab_context_text:
            input_messages.append({"role": "system", "content": partner_ab_context_text})

        return input_messages

    def create_response(self, *, messages: List[dict], previous_response_id: Optional[str] = None):
        return self.client.responses.create(
            model=self.model,
            input=messages,
            text={"verbosity": "medium"},
            reasoning={"effort": "minimal"},
            previous_response_id=previous_response_id,
        )

    def stream_response(self, *, messages: List[dict], previous_response_id: Optional[str] = None):
        return self.client.responses.stream(
            model=self.model,
            input=messages,
            text={"verbosity": "medium"},
            reasoning={"effort": "minimal"},
            previous_response_id=previous_response_id,
        )

