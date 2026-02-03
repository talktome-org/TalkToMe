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

# Use absolute imports so this module works when invoked from:
# - FastAPI app (as a package)
# - background scripts that add repo root to sys.path (see background_tasks/ingest_books.py)
from Backend.database import SessionLocal, engine
from Backend.models.books.document_model import Document
from Backend.models.books.book_chunks_model import BookChunks

CHUNKS_TABLE = "book_chunks"
DOCUMENTS_TABLE = "documents"


async def upsert_document(
    *, source_type: str, source: str, title: Optional[str] = None, metadata: Optional[dict] = None
) -> Tuple[uuid.UUID, bool]:
    meta = metadata or {}

    def _upsert():
        # Use an Engine connection instead of ORM Session for maximum compatibility
        # across SQLAlchemy versions/drivers in "executemany" paths.
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    f"""
                    select id
                    from {DOCUMENTS_TABLE}
                    where source_type = :source_type and source = :source
                    limit 1
                    """
                ),
                {"source_type": source_type, "source": source},
            ).scalar_one_or_none()
            created = existing is None

            stmt = text(
                f"""
                insert into {DOCUMENTS_TABLE} (source_type, source, title, metadata)
                values (:source_type, :source, :title, (:metadata)::jsonb)
                on conflict (source_type, source)
                do update set title = excluded.title, metadata = excluded.metadata
                returning id
                """
            )
            doc_id = conn.execute(
                stmt,
                {
                    "source_type": source_type,
                    "source": source,
                    "title": title,
                    "metadata": json.dumps(meta or {}),
                },
            ).scalar_one()
            return uuid.UUID(str(doc_id)), created

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

        # Prefer a single executemany for speed.
        try:
            with engine.begin() as conn:
                conn.execute(stmt, exec_payloads)
            return len(exec_payloads)
        except Exception as bulk_exc:
            # IMPORTANT: once a statement fails, the transaction is aborted and cannot be reused.
            # So we restart in a new transaction and try row-by-row.
            inserted = 0
            first_row_exc: Exception | None = None
            with engine.begin() as conn:
                for row in exec_payloads:
                    try:
                        conn.execute(stmt, row)
                        inserted += 1
                    except Exception as row_exc:
                        if first_row_exc is None:
                            first_row_exc = row_exc
                        # Skip bad rows; keep inserting the rest.
                        continue
            if inserted == 0 and first_row_exc is not None:
                # If nothing inserted, surface the most helpful error.
                raise first_row_exc from bulk_exc
            return inserted

    return await run_in_threadpool(_insert)


async def delete_chunks_by_source(*, source: str) -> int:
    def _delete():
        with engine.begin() as conn:
            count = conn.execute(
                text(f"select count(1) from {CHUNKS_TABLE} where source = :source"),
                {"source": source},
            ).scalar_one()
            conn.execute(text(f"delete from {CHUNKS_TABLE} where source = :source"), {"source": source})
            return int(count or 0)

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


