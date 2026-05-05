import os
from typing import List

from openai import AsyncOpenAI


class EmbeddingService:
    """
    Wraps OpenAI embeddings.
    Default: text-embedding-3-small (1536 dims). Switch model if needed.
    """

    def __init__(self, model: str = "text-embedding-3-small", *, timeout_seconds: float = 60.0, max_retries: int = 2):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set (check Backend/.env or environment variables).")

        try:
            self.client = AsyncOpenAI(api_key=api_key, timeout=timeout_seconds, max_retries=max_retries)
        except TypeError:
            self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self.dimension_map = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }

    async def get_embedding(self, text: str) -> List[float]:
        resp = await self.client.embeddings.create(model=self.model, input=text)
        return resp.data[0].embedding

    async def get_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        embeddings: List[List[float]] = []
        total = len(texts)
        for i in range(0, total, batch_size):
            batch = texts[i : i + batch_size]
            resp = await self.client.embeddings.create(model=self.model, input=batch)
            embeddings.extend([item.embedding for item in resp.data])
        return embeddings

    def get_dimension(self) -> int:
        return self.dimension_map.get(self.model, 1536)

