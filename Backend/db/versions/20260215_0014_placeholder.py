"""Placeholder for revisions 0012-0014 that were applied directly to the database.

The production database was already upgraded to this revision. This file
exists so Alembic can locate the revision and continue the chain.
"""
from __future__ import annotations


revision = "20260215_0014"
down_revision = "20260215_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
