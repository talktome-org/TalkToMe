"""Create session_reports table for chat report flow."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import UUID


revision = "20260311_0022"
down_revision = "20260308_0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "session_reports" in public_tables:
        return

    op.create_table(
        "session_reports",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("session_id", UUID(as_uuid=True), sa.ForeignKey("user_chat_sessions.id", name="fk_session_reports_session_id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("reporter_user_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=False), server_default=sa.func.now()),
        schema="public",
    )


def downgrade() -> None:
    op.drop_table("session_reports", schema="public")
