"""Add timezone column to device_tokens.

Stores the IANA timezone identifier (e.g. 'America/New_York') so the
backend can schedule daily check-in notifications at 5 PM local time.
"""

from __future__ import annotations

from alembic import op


revision = "20260308_0021"
down_revision = "20260307_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE public.device_tokens
        ADD COLUMN IF NOT EXISTS timezone TEXT;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE public.device_tokens DROP COLUMN IF EXISTS timezone;")
