"""Add unique constraint on (user_id, token) to device_tokens.

The device_tokens table already exists in Supabase but is missing the
unique constraint that the upsert_token CRUD operation requires for
ON CONFLICT DO UPDATE.  Without it every token registration fails.
"""

from __future__ import annotations

from alembic import op


revision = "20260307_0020"
down_revision = "20260218_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure the table exists (safe no-op if it already does).
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.device_tokens (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID REFERENCES auth.users(id),
            token TEXT,
            platform TEXT DEFAULT 'ios',
            bundle_id TEXT,
            enabled BOOLEAN DEFAULT true,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        );
    """)

    # Add the unique constraint required by the upsert CRUD.
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'device_tokens_user_id_token_key'
            ) THEN
                ALTER TABLE public.device_tokens
                    ADD CONSTRAINT device_tokens_user_id_token_key
                    UNIQUE (user_id, token);
            END IF;
        END$$;
    """)

    # Index for fast lookups by user_id (used by list_tokens_for_user).
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_device_tokens_user_id
        ON public.device_tokens (user_id);
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE public.device_tokens DROP CONSTRAINT IF EXISTS device_tokens_user_id_token_key;")
    op.execute("DROP INDEX IF EXISTS ix_device_tokens_user_id;")
