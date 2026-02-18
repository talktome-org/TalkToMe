"""Track the existing friend_invites table in alembic.

The table already exists in the database (created outside alembic).
This migration uses IF NOT EXISTS so it is safe to run on both fresh
and existing databases.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260218_0019"
down_revision = "20260218_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.friend_invites (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            inviter_user_id UUID NOT NULL REFERENCES auth.users(id),
            token TEXT,
            expires_at TIMESTAMP WITHOUT TIME ZONE,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            used_by_user_id UUID REFERENCES auth.users(id),
            used_at TIMESTAMP WITHOUT TIME ZONE
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_friend_invites_inviter_user_id
        ON public.friend_invites (inviter_user_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_friend_invites_used_by_user_id
        ON public.friend_invites (used_by_user_id);
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_friend_invites_used_by_user_id;")
    op.execute("DROP INDEX IF EXISTS ix_friend_invites_inviter_user_id;")
    op.execute("DROP TABLE IF EXISTS public.friend_invites;")
