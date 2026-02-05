"""Fix 'database error saving new user' by ensuring profiles exists and trigger works.

When a new user signs up, Supabase Auth inserts into auth.users. A trigger
runs to insert a row into public.profiles. The error happens when:
- public.profiles table does not exist
- onboarding_step enum is missing
- RLS on profiles blocks the trigger insert
- Trigger/function is missing or broken

This migration:
1. Creates enum public.onboarding_step if not exists.
2. Creates public.profiles if not exists (full schema matching Profile model).
3. Adds RLS policy so the trigger (running as definer) can insert.
4. Creates/replaces handle_new_user() with SECURITY DEFINER and the trigger.
"""
from __future__ import annotations

from alembic import op


revision = "20260203_0010"
down_revision = "20260203_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Enum type for onboarding_step (required by profiles.onboarding_step default)
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE public.onboarding_step AS ENUM ('none', 'asked_name', 'completed');
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
    """)

    # 2. Create profiles table if not exists (so trigger INSERT has a target)
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.profiles (
            user_id uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
            avatar_path text,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now(),
            bio text,
            full_name text,
            partner_display_name text,
            onboarding_step public.onboarding_step DEFAULT 'none'::public.onboarding_step,
            gender text,
            date_of_birth date,
            relationship_topics jsonb
        );
    """)

    # 3. RLS: enable RLS and allow trigger (SECURITY DEFINER runs as postgres; Auth may use supabase_auth_admin)
    op.execute("""
        ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
    """)
    op.execute("""
        DROP POLICY IF EXISTS "Allow insert for new user trigger" ON public.profiles;
    """)
    op.execute("""
        CREATE POLICY "Allow insert for new user trigger"
            ON public.profiles FOR INSERT
            TO postgres
            WITH CHECK (true);
    """)
    op.execute("""
        DROP POLICY IF EXISTS "Allow auth admin to insert profile" ON public.profiles;
    """)
    op.execute("""
        CREATE POLICY "Allow auth admin to insert profile"
            ON public.profiles FOR INSERT
            TO supabase_auth_admin
            WITH CHECK (true);
    """)
    op.execute("""
        DROP POLICY IF EXISTS "Users can read own profile" ON public.profiles;
    """)
    op.execute("""
        CREATE POLICY "Users can read own profile"
            ON public.profiles FOR SELECT
            USING (auth.uid() = user_id);
    """)
    op.execute("""
        DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;
    """)
    op.execute("""
        CREATE POLICY "Users can update own profile"
            ON public.profiles FOR UPDATE
            USING (auth.uid() = user_id);
    """)

    # 4. Trigger function: only insert user_id; run as definer to bypass RLS for this insert
    op.execute("""
        CREATE OR REPLACE FUNCTION public.handle_new_user()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public
        AS $$
        BEGIN
            INSERT INTO public.profiles (user_id)
            VALUES (new.id);
            RETURN new;
        END;
        $$;
    """)

    op.execute("""
        DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
    """)
    op.execute("""
        CREATE TRIGGER on_auth_user_created
            AFTER INSERT ON auth.users
            FOR EACH ROW
            EXECUTE FUNCTION public.handle_new_user();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;")
    op.execute("DROP FUNCTION IF EXISTS public.handle_new_user();")
