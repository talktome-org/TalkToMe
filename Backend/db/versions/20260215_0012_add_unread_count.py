"""Add unread_count to user_chat_sessions for partner message badges."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260215_0012"
down_revision = "20260215_0011b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "user_chat_sessions" not in public_tables:
        return

    cols = {c["name"] for c in insp.get_columns("user_chat_sessions", schema="public")}
    if "unread_count" in cols:
        return

    with op.batch_alter_table("user_chat_sessions", schema="public") as batch:
        batch.add_column(sa.Column("unread_count", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    pass
