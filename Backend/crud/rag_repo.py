import hashlib
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from starlette.concurrency import run_in_threadpool

from ..db.supabase_client import supabase

CHUNKS_TABLE = "therapy_book_chunks"
DOCUMENTS_TABLE = "documents"


async def upsert_document(
    *, source_type: str, source: str, title: Optional[str] = None, metadata: Optional[dict] = None
) -> Tuple[uuid.UUID, bool]:
    payload = {
        "source_type": source_type,
        "source": source,
        "title": title,
        "metadata": metadata or {},
    }

    def _upsert():
        return supabase.table(DOCUMENTS_TABLE).upsert(payload, on_conflict="source_type,source").execute()

    res = await run_in_threadpool(_upsert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase upsert document failed: {res.error}")
    if not hasattr(res, "data") or not res.data:
        raise RuntimeError("Supabase upsert document returned no data")
    row = res.data[0]
    return uuid.UUID(row["id"]), row.get("created_at") == row.get("updated_at") if "updated_at" in row else True


def _hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def insert_chunks_batch(
    *,
    document_id: uuid.UUID,
    chunks: List[dict],
    embeddings: List[List[float]],
    embedding_model: str,
) -> int:
    if len(chunks) != len(embeddings):
        raise ValueError("chunks and embeddings must have the same length")

    now_iso = datetime.now(timezone.utc).isoformat()
    payloads = []
    for chunk, embedding in zip(chunks, embeddings):
        chunk_text = chunk["text"]
        meta = chunk.get("metadata", {}) or {}
        payloads.append(
            {
                "id": str(uuid.uuid4()),
                "document_id": str(document_id),
                "chunk_text": chunk_text,
                "embedding": embedding,
                "source": meta.get("source", "unknown"),
                "source_type": meta.get("source_type", "unknown"),
                "chunk_index": meta.get("chunk_index", 0),
                "metadata": meta,
                "content_hash": _hash_content(chunk_text),
                "token_count": meta.get("token_count"),
                "embedding_model": embedding_model,
                "created_at": now_iso,
            }
        )

    def _insert():
        return supabase.table(CHUNKS_TABLE).insert(payloads).execute()

    res = await run_in_threadpool(_insert)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase batch insert failed: {res.error}")
    return len(res.data or [])


async def delete_chunks_by_source(*, source: str) -> int:
    def _delete():
        return supabase.table(CHUNKS_TABLE).delete().eq("source", source).execute()

    res = await run_in_threadpool(_delete)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase delete chunks failed: {res.error}")
    return len(res.data or [])


async def search_similar_chunks(
    *,
    query_embedding: List[float],
    limit: int = 5,
    source: Optional[str] = None,
    document_id: Optional[uuid.UUID] = None,
) -> List[dict]:
    query = supabase.table(CHUNKS_TABLE).select("*")
    if source:
        query = query.eq("source", source)
    if document_id:
        query = query.eq("document_id", str(document_id))

    query = query.match({"embedding": query_embedding}).limit(limit)

    def _search():
        return query.execute()

    res = await run_in_threadpool(_search)
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase vector search failed: {res.error}")
    return res.data or []


