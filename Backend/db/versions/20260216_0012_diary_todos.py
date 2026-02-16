"""Add diary_todos table for per-user todo items (Diary ToDo tab).

Stores id, user_id, title, is_completed, created_at with RLS so each user
only sees and mutates their own todos.
"""

from __future__ import annotations

from alembic import op


revision = "20260216_0012"
down_revision = "20260215_0011b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.diary_todos (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
            title text NOT NULL,
            is_completed boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_diary_todos_user_created
        ON public.diary_todos (user_id, created_at DESC);
        """
    )
    op.execute("ALTER TABLE public.diary_todos ENABLE ROW LEVEL SECURITY;")

    op.execute('DROP POLICY IF EXISTS "Users can read own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can delete own diary todos" ON public.diary_todos;')

    op.execute(
        """
        CREATE POLICY "Users can read own diary todos"
        ON public.diary_todos FOR SELECT
        TO authenticated
        USING (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can insert own diary todos"
        ON public.diary_todos FOR INSERT
        TO authenticated
        WITH CHECK (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can update own diary todos"
        ON public.diary_todos FOR UPDATE
        TO authenticated
        USING (auth.uid() = user_id)
        WITH CHECK (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can delete own diary todos"
        ON public.diary_todos FOR DELETE
        TO authenticated
        USING (auth.uid() = user_id);
        """
    )


def downgrade() -> None:
    op.execute('DROP POLICY IF EXISTS "Users can read own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary todos" ON public.diary_todos;')
    op.execute('DROP POLICY IF EXISTS "Users can delete own diary todos" ON public.diary_todos;')
    op.execute("DROP TABLE IF EXISTS public.diary_todos;")
