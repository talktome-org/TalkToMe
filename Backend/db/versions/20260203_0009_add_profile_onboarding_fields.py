"""Add profile onboarding fields (gender, DOB, topics).

Adds nullable columns to `public.profiles` so onboarding can collect:
- gender (text)
- date_of_birth (date)
- relationship_topics (jsonb array)
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "20260203_0009"
down_revision = "20260201_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "profiles" not in public_tables:
        return

    cols = {c["name"] for c in insp.get_columns("profiles", schema="public")}

    with op.batch_alter_table("profiles", schema="public") as batch:
        if "gender" not in cols:
            batch.add_column(sa.Column("gender", sa.Text(), nullable=True))
        if "date_of_birth" not in cols:
            batch.add_column(sa.Column("date_of_birth", sa.Date(), nullable=True))
        if "relationship_topics" not in cols:
            batch.add_column(
                sa.Column(
                    "relationship_topics",
                    postgresql.JSONB(astext_type=sa.Text()),
                    nullable=True,
                )
            )


def downgrade() -> None:
    # Testing reset: no downgrade.
    pass

