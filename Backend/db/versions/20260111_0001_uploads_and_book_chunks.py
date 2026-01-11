"""Create uploads table and align book_chunks table name.

This project uses Supabase Postgres. We manage schema changes via Alembic going forward.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


# Revision identifiers, used by Alembic.
revision = "20260111_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    # 1) Rename legacy table if present: therapy_book_chunks -> book_chunks
    public_tables = set(insp.get_table_names(schema="public"))
    if "therapy_book_chunks" in public_tables and "book_chunks" not in public_tables:
        op.rename_table("therapy_book_chunks", "book_chunks", schema="public")

    # 2) Create uploads table (DB-backed file storage) if missing
    public_tables = set(insp.get_table_names(schema="public"))
    if "uploads" not in public_tables:
        op.create_table(
            "uploads",
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "user_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("auth.users.id", name="uploads_user_id_fkey"),
                nullable=False,
            ),
            sa.Column("kind", sa.Text(), nullable=False),
            sa.Column("content_type", sa.Text(), nullable=False),
            sa.Column("filename", sa.Text(), nullable=True),
            sa.Column("data", sa.LargeBinary(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=False), server_default=sa.func.now(), nullable=True),
            schema="public",
        )

    # Keep index name aligned with SQLAlchemy's default for index=True.
    public_indexes = {ix["name"] for ix in insp.get_indexes("uploads", schema="public")} if "uploads" in public_tables else set()
    if "ix_uploads_user_id" not in public_indexes:
        op.create_index("ix_uploads_user_id", "uploads", ["user_id"], unique=False, schema="public")


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    public_tables = set(insp.get_table_names(schema="public"))

    # Drop uploads table if present
    if "uploads" in public_tables:
        op.drop_index("ix_uploads_user_id", table_name="uploads", schema="public")
        op.drop_table("uploads", schema="public")

    # Rename back if needed
    public_tables = set(insp.get_table_names(schema="public"))
    if "book_chunks" in public_tables and "therapy_book_chunks" not in public_tables:
        op.rename_table("book_chunks", "therapy_book_chunks", schema="public")

