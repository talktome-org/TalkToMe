"""Placeholder for deleted revision 20260203_0010.

This revision exists on the deployed database, but the migration file is missing
from the repo. Alembic requires the revision script to be present in order to
traverse history (even if the revision is already applied).

We keep this as a no-op so deployments can run. If the original migration made
schema changes, author a new follow-up migration to reproduce/adjust them.
"""

from __future__ import annotations


# Alembic identifiers.
revision = "20260203_0010"
down_revision = "20260203_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Intentionally empty: the DB already has whatever schema changes were applied
    # under this revision in production.
    pass


def downgrade() -> None:
    # Intentionally empty.
    pass
