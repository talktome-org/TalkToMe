"""Placeholder for deleted revision 20260123_0006.

This revision existed on the deployed database at some point, but the migration
file was deleted from the repo. Alembic requires the revision script to be
present in order to traverse history (even if the revision is already applied).

We keep this as a no-op so deployments can run and we can author a follow-up
migration to undo the schema changes that were applied at that time.
"""

from __future__ import annotations


# Alembic identifiers.
revision = "20260123_0006"
down_revision = "20260119_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Intentionally empty: the DB already has whatever schema changes were applied
    # under this revision in production.
    pass


def downgrade() -> None:
    # Intentionally empty.
    pass

