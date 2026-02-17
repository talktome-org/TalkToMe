"""Fix 'Database error saving new user' on email/password sign-up.

Root causes:
1. A duplicate trigger ``on_auth_user_created_profiles`` (calling
   ``ensure_profile_row``) was created via the Supabase Dashboard.  Both it
   and our ``on_auth_user_created`` trigger fired on the same INSERT into
   ``auth.users``, causing a duplicate-key conflict on ``profiles.user_id``.
2. RLS INSERT policies for ``postgres`` and ``supabase_auth_admin`` were
   dropped when the Dashboard GUI was used to edit policies.

This migration:
- Drops the duplicate trigger and function.
- Re-adds the two RLS policies idempotently.
"""

from __future__ import annotations

from alembic import op


revision = "20260217_0015"
down_revision = "20260216_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove duplicate trigger/function created via Dashboard
    op.execute(
        "DROP TRIGGER IF EXISTS on_auth_user_created_profiles ON auth.users;"
    )
    op.execute("DROP FUNCTION IF EXISTS public.ensure_profile_row();")

    # Policy for the trigger function (runs as postgres via SECURITY DEFINER)
    op.execute(
        'DROP POLICY IF EXISTS "Allow insert for new user trigger" ON public.profiles;'
    )
    op.execute(
        """
        CREATE POLICY "Allow insert for new user trigger"
            ON public.profiles FOR INSERT
            TO postgres
            WITH CHECK (true);
        """
    )

    # Policy for supabase_auth_admin (the role GoTrue uses internally)
    op.execute(
        'DROP POLICY IF EXISTS "Allow auth admin to insert profile" ON public.profiles;'
    )
    op.execute(
        """
        CREATE POLICY "Allow auth admin to insert profile"
            ON public.profiles FOR INSERT
            TO supabase_auth_admin
            WITH CHECK (true);
        """
    )


def downgrade() -> None:
    op.execute(
        'DROP POLICY IF EXISTS "Allow insert for new user trigger" ON public.profiles;'
    )
    op.execute(
        'DROP POLICY IF EXISTS "Allow auth admin to insert profile" ON public.profiles;'
    )
