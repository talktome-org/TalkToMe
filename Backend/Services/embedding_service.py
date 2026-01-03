import os
from typing import List
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")


class EmbeddingService:
    """
    Wraps OpenAI embeddings.
    Default: text-embedding-3-small (1536 dims). Switch model if needed.
    """

    def __init__(self, model: str = "text-embedding-3-small"):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.dimension_map = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }

    def get_embedding(self, text: str) -> List[float]:
        resp = self.client.embeddings.create(model=self.model, input=text)
        return resp.data[0].embedding

    def get_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        embeddings: List[List[float]] = []
        total = len(texts)
        for i in range(0, total, batch_size):
            batch = texts[i : i + batch_size]
            resp = self.client.embeddings.create(model=self.model, input=batch)
            embeddings.extend([item.embedding for item in resp.data])
        return embeddings

    def get_dimension(self) -> int:
        return self.dimension_map.get(self.model, 1536)



