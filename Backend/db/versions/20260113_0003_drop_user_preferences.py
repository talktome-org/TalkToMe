"""Drop user_preferences table.

We are removing the legacy daily check-in notifications feature and will re-add it with a
new schema later.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect


# Revision identifiers, used by Alembic.
revision = "20260113_0003"
down_revision = "20260111_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)

    public_tables = set(insp.get_table_names(schema="public"))
    if "user_preferences" in public_tables:
        op.drop_table("user_preferences", schema="public")


def downgrade() -> None:
    # Intentionally not recreating: we will reintroduce preferences with a new schema.
    pass

