"""Add friendship_id to user_chat_sessions (friends-based partner messaging).

Testing reset: safe-ish migration that checks for existing column.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "20260119_0005"
down_revision = "20260119_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "user_chat_sessions" not in public_tables:
        return

    cols = {c["name"] for c in insp.get_columns("user_chat_sessions", schema="public")}
    if "friendship_id" in cols:
        return

    with op.batch_alter_table("user_chat_sessions", schema="public") as batch:
        batch.add_column(sa.Column("friendship_id", postgresql.UUID(as_uuid=True), nullable=True))
        batch.create_index("user_chat_sessions_friendship_id_idx", ["friendship_id"], unique=False)
        batch.create_foreign_key(
            "user_chat_sessions_friendship_id_fkey",
            "friendships",
            ["friendship_id"],
            ["id"],
            referent_schema="public",
        )


def downgrade() -> None:
    # Testing reset: no downgrade.
    pass

