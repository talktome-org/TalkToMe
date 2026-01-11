import hashlib
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple
import sys
from pathlib import Path
import json

from starlette.concurrency import run_in_threadpool
from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert

# Support execution both as package and as script runner
try:
    from ..database import SessionLocal  # type: ignore
    from ..models.books.document_model import Document  # type: ignore
    from ..models.books.book_chunks_model import BookChunks  # type: ignore
except ImportError:
    # Fallback when relative import fails (script execution)
    ROOT = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(ROOT))
    from database import SessionLocal  # type: ignore
    from models.books.document_model import Document  # type: ignore
    from models.books.book_chunks_model import BookChunks  # type: ignore

CHUNKS_TABLE = "book_chunks"
DOCUMENTS_TABLE = "documents"


async def upsert_document(
    *, source_type: str, source: str, title: Optional[str] = None, metadata: Optional[dict] = None
) -> Tuple[uuid.UUID, bool]:
    meta = metadata or {}

    def _upsert():
        db = SessionLocal()
        try:
            existing = (
                db.execute(
                    select(Document.id).where(
                        Document.source_type == source_type,
                        Document.source == source,
                    )
                )
                .scalars()
                .first()
            )
            created = existing is None

            stmt = (
                insert(Document)
                .values(source_type=source_type, source=source, title=title, metadata=meta)
                .on_conflict_do_update(
                    index_elements=[Document.source_type, Document.source],
                    set_={"title": title, "metadata": meta},
                )
                .returning(Document.id)
            )
            doc_id = db.execute(stmt).scalar_one()
            db.commit()
            return uuid.UUID(str(doc_id)), created
        finally:
            db.close()

    return await run_in_threadpool(_upsert)


def _hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _vector_literal(vec: List[float]) -> str:
    # pgvector accepts literals like: [0.1,0.2,0.3]
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


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
        db = SessionLocal()
        try:
            # Use explicit cast to vector so we don't need a pgvector Python adapter.
            stmt = text(
                f"""
                insert into {CHUNKS_TABLE}
                (id, document_id, chunk_text, embedding, source, source_type, chunk_index, metadata, content_hash, token_count, embedding_model, created_at)
                values
                (:id, :document_id, :chunk_text, (:embedding)::vector, :source, :source_type, :chunk_index, (:metadata)::jsonb, :content_hash, :token_count, :embedding_model, :created_at)
                """
            )

            exec_payloads = []
            for p in payloads:
                exec_payloads.append(
                    {
                        "id": p["id"],
                        "document_id": p["document_id"],
                        "chunk_text": p["chunk_text"],
                        "embedding": _vector_literal(p["embedding"]),
                        "source": p["source"],
                        "source_type": p["source_type"],
                        "chunk_index": p["chunk_index"],
                        "metadata": json.dumps(p["metadata"] or {}),
                        "content_hash": p["content_hash"],
                        "token_count": p.get("token_count"),
                        "embedding_model": p.get("embedding_model"),
                        "created_at": p["created_at"],
                    }
                )

            db.execute(stmt, exec_payloads)
            db.commit()
            return len(exec_payloads)
        finally:
            db.close()

    return await run_in_threadpool(_insert)


async def delete_chunks_by_source(*, source: str) -> int:
    def _delete():
        db = SessionLocal()
        try:
            count = db.execute(select(BookChunks.id).where(BookChunks.source == source)).all()
            db.execute(delete(BookChunks).where(BookChunks.source == source))
            db.commit()
            return len(count or [])
        finally:
            db.close()

    return await run_in_threadpool(_delete)


async def search_similar_chunks(
    *,
    query_embedding: List[float],
    limit: int = 5,
    source: Optional[str] = None,
    document_id: Optional[uuid.UUID] = None,
) -> List[dict]:
    def _search():
        db = SessionLocal()
        try:
            where_sql = []
            params = {"q": _vector_literal(query_embedding), "limit": int(limit)}
            if source:
                where_sql.append("source = :source")
                params["source"] = source
            if document_id:
                where_sql.append("document_id = :document_id")
                params["document_id"] = str(document_id)
            where_clause = ("where " + " and ".join(where_sql)) if where_sql else ""

            stmt = text(
                f"""
                select *
                from {CHUNKS_TABLE}
                {where_clause}
                order by embedding <-> (:q)::vector asc
                limit :limit
                """
            )
            rows = db.execute(stmt, params).mappings().all()
            return [dict(r) for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_search)


