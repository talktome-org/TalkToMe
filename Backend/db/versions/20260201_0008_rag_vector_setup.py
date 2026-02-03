"""RAG DB setup: pgvector + documents/book_chunks + indexes.

This migration makes the global "books RAG" storage production-ready:
- Ensures required extensions exist (pgcrypto for gen_random_uuid, vector for pgvector)
- Creates `documents` and `book_chunks` tables if missing
- Adds the unique constraint required by `upsert_document` (source_type + source)
- Adds a pgvector similarity index on `book_chunks.embedding` (HNSW)

All operations are written to be safe-ish in Supabase environments that may already
have some of these objects created.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "20260201_0008"
down_revision = "20260124_0007"
branch_labels = None
depends_on = None


class Vector(sa.types.UserDefinedType):
    def get_col_spec(self, **kw):  # type: ignore[override]
        # pgvector type
        return "vector"


def _ensure_extension(name: str) -> None:
    # `CREATE EXTENSION` is idempotent with IF NOT EXISTS.
    op.execute(sa.text(f'CREATE EXTENSION IF NOT EXISTS "{name}";'))


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    # Needed for server_default=gen_random_uuid() used by multiple tables.
    _ensure_extension("pgcrypto")

    # Needed for the `vector` column type and similarity operators.
    _ensure_extension("vector")

    public_tables = set(insp.get_table_names(schema="public"))

    if "documents" not in public_tables:
        op.create_table(
            "documents",
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                nullable=False,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("source_type", sa.Text(), nullable=True),
            sa.Column("source", sa.Text(), nullable=True),
            sa.Column("title", sa.Text(), nullable=True),
            sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=True, server_default=sa.text("now()")),
            schema="public",
        )
        public_tables.add("documents")

    if "book_chunks" not in public_tables:
        op.create_table(
            "book_chunks",
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                nullable=False,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("chunk_text", sa.Text(), nullable=True),
            sa.Column("embedding", Vector(), nullable=True),
            sa.Column("source", sa.Text(), nullable=True),
            sa.Column("source_type", sa.Text(), nullable=True),
            sa.Column("chunk_index", sa.Integer(), nullable=True),
            sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default=sa.text("'{}'::jsonb")),
            sa.Column("content_hash", sa.Text(), nullable=True),
            sa.Column("token_count", sa.Integer(), nullable=True),
            sa.Column("embedding_model", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=False), nullable=True, server_default=sa.text("now()")),
            sa.ForeignKeyConstraint(["document_id"], ["public.documents.id"], name="therapy_book_chunks_document_fk"),
            schema="public",
        )
        op.create_index("ix_book_chunks_document_id", "book_chunks", ["document_id"], unique=False, schema="public")
        public_tables.add("book_chunks")

    # Ensure documents(source_type, source) uniqueness for upsert_document(... on_conflict_do_update(index_elements=[...])).
    if "documents" in public_tables:
        uniques = insp.get_unique_constraints("documents", schema="public")
        has_unique = any(
            set((u.get("column_names") or [])) == {"source_type", "source"}
            for u in uniques
        )
        if not has_unique:
            op.create_unique_constraint(
                "documents_source_type_source_key",
                "documents",
                ["source_type", "source"],
                schema="public",
            )

    # Ensure an ANN index exists for fast similarity search.
    #
    # We prefer HNSW. Supabase generally supports it on pgvector >= 0.5.
    # Name is explicit so we can check by name across environments.
    if "book_chunks" in public_tables:
        indexes = {ix.get("name") for ix in insp.get_indexes("book_chunks", schema="public")}
        if "book_chunks_embedding_hnsw_idx" not in indexes:
            op.execute(
                sa.text(
                    """
                    CREATE INDEX IF NOT EXISTS book_chunks_embedding_hnsw_idx
                    ON public.book_chunks
                    USING hnsw (embedding vector_l2_ops);
                    """
                )
            )


def downgrade() -> None:
    # Testing reset: no downgrade.
    pass

