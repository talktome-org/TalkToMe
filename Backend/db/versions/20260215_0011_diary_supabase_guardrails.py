"""Ensure diary tables/policies and storage bucket guardrails are present.

Why:
- Diary notes with photos are saved directly from iOS to Supabase.
- If table/bucket policies drift, writes fail with generic DB/storage errors.
- The diary photos bucket previously had a strict 5MB object limit; modern phone
  photos often exceed that size.

This migration is intentionally idempotent and safe to run repeatedly.
"""

from __future__ import annotations

from alembic import op


revision = "20260215_0011b"
down_revision = "20260203_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
        EXCEPTION
            WHEN insufficient_privilege THEN NULL;
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.diary_settings (
            user_id uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
            name text NOT NULL DEFAULT 'My Diary',
            description text NOT NULL DEFAULT '',
            header_color_hex text NOT NULL DEFAULT '#B8DEFF',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.diary_entries (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
            date date NOT NULL,
            title text NOT NULL DEFAULT 'Untitled',
            body_blocks jsonb NOT NULL DEFAULT '[]'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            timezone_abbreviation text NOT NULL DEFAULT 'UTC'
        );
        """
    )

    # Backfill/fix nullable drift from manual schema edits.
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS name text;")
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS description text;")
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS header_color_hex text;")
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();")
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();")
    op.execute("UPDATE public.diary_settings SET name = COALESCE(name, 'My Diary');")
    op.execute("UPDATE public.diary_settings SET description = COALESCE(description, '');")
    op.execute("UPDATE public.diary_settings SET header_color_hex = COALESCE(header_color_hex, '#B8DEFF');")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN name SET DEFAULT 'My Diary';")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN description SET DEFAULT '';")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN header_color_hex SET DEFAULT '#B8DEFF';")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN name SET NOT NULL;")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN description SET NOT NULL;")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN header_color_hex SET NOT NULL;")

    op.execute("ALTER TABLE public.diary_entries ADD COLUMN IF NOT EXISTS title text;")
    op.execute("ALTER TABLE public.diary_entries ADD COLUMN IF NOT EXISTS body_blocks jsonb;")
    op.execute("ALTER TABLE public.diary_entries ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();")
    op.execute("ALTER TABLE public.diary_entries ADD COLUMN IF NOT EXISTS timezone_abbreviation text;")
    op.execute("UPDATE public.diary_entries SET title = COALESCE(title, 'Untitled');")
    op.execute("UPDATE public.diary_entries SET body_blocks = COALESCE(body_blocks, '[]'::jsonb);")
    op.execute("UPDATE public.diary_entries SET timezone_abbreviation = COALESCE(timezone_abbreviation, 'UTC');")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN title SET DEFAULT 'Untitled';")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN body_blocks SET DEFAULT '[]'::jsonb;")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN timezone_abbreviation SET DEFAULT 'UTC';")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN title SET NOT NULL;")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN body_blocks SET NOT NULL;")
    op.execute("ALTER TABLE public.diary_entries ALTER COLUMN timezone_abbreviation SET NOT NULL;")

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_diary_entries_user_date_created
        ON public.diary_entries (user_id, date DESC, created_at DESC);
        """
    )

    # Table RLS + per-user policies.
    op.execute("ALTER TABLE public.diary_settings ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE public.diary_entries ENABLE ROW LEVEL SECURITY;")

    op.execute('DROP POLICY IF EXISTS "Users can read own diary settings" ON public.diary_settings;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary settings" ON public.diary_settings;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary settings" ON public.diary_settings;')
    op.execute(
        """
        CREATE POLICY "Users can read own diary settings"
        ON public.diary_settings FOR SELECT
        TO authenticated
        USING (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can insert own diary settings"
        ON public.diary_settings FOR INSERT
        TO authenticated
        WITH CHECK (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can update own diary settings"
        ON public.diary_settings FOR UPDATE
        TO authenticated
        USING (auth.uid() = user_id)
        WITH CHECK (auth.uid() = user_id);
        """
    )

    op.execute('DROP POLICY IF EXISTS "Users can read own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can delete own diary entries" ON public.diary_entries;')
    op.execute(
        """
        CREATE POLICY "Users can read own diary entries"
        ON public.diary_entries FOR SELECT
        TO authenticated
        USING (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can insert own diary entries"
        ON public.diary_entries FOR INSERT
        TO authenticated
        WITH CHECK (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can update own diary entries"
        ON public.diary_entries FOR UPDATE
        TO authenticated
        USING (auth.uid() = user_id)
        WITH CHECK (auth.uid() = user_id);
        """
    )
    op.execute(
        """
        CREATE POLICY "Users can delete own diary entries"
        ON public.diary_entries FOR DELETE
        TO authenticated
        USING (auth.uid() = user_id);
        """
    )

    # Ensure diary photo bucket exists and can hold modern photos.
    # Wrapped in DO block: if storage schema does not exist (e.g. non-Supabase Postgres),
    # migration still succeeds and diary_entries is created; otherwise 42P01 would roll back the whole migration.
    op.execute(
        """
        DO $$
        BEGIN
            INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
            VALUES (
                'diary-photos',
                'diary-photos',
                true,
                20971520,
                ARRAY['image/jpeg','image/png','image/heic','image/heif','image/webp']::text[]
            )
            ON CONFLICT (id) DO UPDATE
            SET
                public = EXCLUDED.public,
                file_size_limit = EXCLUDED.file_size_limit,
                allowed_mime_types = EXCLUDED.allowed_mime_types;
        EXCEPTION
            WHEN undefined_table THEN NULL;
            WHEN undefined_function THEN NULL;
            WHEN insufficient_privilege THEN NULL;
        END $$;
        """
    )

    # storage.objects ownership can differ per Supabase setup; keep migration non-fatal.
    op.execute(
        """
        DO $$
        BEGIN
            DROP POLICY IF EXISTS "diary_photos_select" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_insert" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_update" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_delete" ON storage.objects;

            CREATE POLICY "diary_photos_select"
            ON storage.objects FOR SELECT
            TO authenticated
            USING (
                bucket_id = 'diary-photos'
                AND (string_to_array(name, '/'))[1] = auth.uid()::text
            );

            CREATE POLICY "diary_photos_insert"
            ON storage.objects FOR INSERT
            TO authenticated
            WITH CHECK (
                bucket_id = 'diary-photos'
                AND (string_to_array(name, '/'))[1] = auth.uid()::text
            );

            CREATE POLICY "diary_photos_update"
            ON storage.objects FOR UPDATE
            TO authenticated
            USING (
                bucket_id = 'diary-photos'
                AND (string_to_array(name, '/'))[1] = auth.uid()::text
            );

            CREATE POLICY "diary_photos_delete"
            ON storage.objects FOR DELETE
            TO authenticated
            USING (
                bucket_id = 'diary-photos'
                AND (string_to_array(name, '/'))[1] = auth.uid()::text
            );
        EXCEPTION
            WHEN undefined_table THEN NULL;
            WHEN undefined_function THEN NULL;
            WHEN insufficient_privilege THEN NULL;
        END $$;
        """
    )


def downgrade() -> None:
    # Keep data tables/bucket intact on downgrade; only remove policies introduced here.
    op.execute(
        """
        DO $$
        BEGIN
            DROP POLICY IF EXISTS "diary_photos_select" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_insert" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_update" ON storage.objects;
            DROP POLICY IF EXISTS "diary_photos_delete" ON storage.objects;
        EXCEPTION
            WHEN undefined_table THEN NULL;
            WHEN undefined_function THEN NULL;
            WHEN insufficient_privilege THEN NULL;
        END $$;
        """
    )

    op.execute('DROP POLICY IF EXISTS "Users can read own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary entries" ON public.diary_entries;')
    op.execute('DROP POLICY IF EXISTS "Users can delete own diary entries" ON public.diary_entries;')

    op.execute('DROP POLICY IF EXISTS "Users can read own diary settings" ON public.diary_settings;')
    op.execute('DROP POLICY IF EXISTS "Users can insert own diary settings" ON public.diary_settings;')
    op.execute('DROP POLICY IF EXISTS "Users can update own diary settings" ON public.diary_settings;')
