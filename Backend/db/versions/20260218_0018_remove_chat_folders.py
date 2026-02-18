"""Remove chat folders feature.

Drops the folder_id column from user_chat_sessions and the
user_chat_folders table that were added in revision 0017.
"""

from __future__ import annotations

from alembic import op


revision = "20260218_0018"
down_revision = "20260217_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.user_chat_sessions DROP COLUMN IF EXISTS folder_id;")
    op.execute("DROP INDEX IF EXISTS ix_user_chat_sessions_folder_id;")
    op.execute("DROP INDEX IF EXISTS ix_user_chat_folders_user_id;")
    op.execute("DROP TABLE IF EXISTS public.user_chat_folders;")


def downgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.user_chat_folders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL,
            name TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        );
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_user_chat_folders_user_id
        ON public.user_chat_folders (user_id);
    """)
    op.execute("""
        ALTER TABLE public.user_chat_sessions
        ADD COLUMN IF NOT EXISTS folder_id UUID
        REFERENCES public.user_chat_folders(id) ON DELETE SET NULL;
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_user_chat_sessions_folder_id
        ON public.user_chat_sessions (folder_id);
    """)
