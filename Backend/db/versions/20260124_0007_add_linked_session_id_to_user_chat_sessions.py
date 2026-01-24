"""Add linked_session_id to user_chat_sessions (per-chat partner threads).

We previously routed partner messages by `friendship_id`, which caused multiple
independent chats to deliver into the same partner thread. This migration
reintroduces a self-referential link so each chat session can be paired 1:1
with exactly one counterpart session.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "20260124_0007"
down_revision = "20260123_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "user_chat_sessions" not in public_tables:
        return

    cols = {c["name"] for c in insp.get_columns("user_chat_sessions", schema="public")}
    if "linked_session_id" in cols:
        return

    with op.batch_alter_table("user_chat_sessions", schema="public") as batch:
        batch.add_column(sa.Column("linked_session_id", postgresql.UUID(as_uuid=True), nullable=True))
        # Enforce 1:1 pairing (multiple NULLs allowed).
        batch.create_index("user_chat_sessions_linked_session_id_key", ["linked_session_id"], unique=True)
        batch.create_foreign_key(
            "user_chat_sessions_linked_session_id_fkey",
            "user_chat_sessions",
            ["linked_session_id"],
            ["id"],
            referent_schema="public",
        )


def downgrade() -> None:
    # Testing reset: no downgrade.
    pass

