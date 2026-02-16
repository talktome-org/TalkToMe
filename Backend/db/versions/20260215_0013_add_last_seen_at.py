"""Add last_seen_at to profiles for user presence tracking."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260215_0013"
down_revision = "20260215_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "profiles" not in public_tables:
        return

    cols = {c["name"] for c in insp.get_columns("profiles", schema="public")}
    if "last_seen_at" in cols:
        return

    with op.batch_alter_table("profiles", schema="public") as batch:
        batch.add_column(sa.Column("last_seen_at", sa.DateTime(timezone=False), nullable=True))


def downgrade() -> None:
    pass
