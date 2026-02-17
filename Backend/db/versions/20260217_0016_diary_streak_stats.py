"""Persist diary streak stats on diary_settings.

Adds:
- current_streak_days: consecutive note days ending today.
- max_streak_days: best streak across all recorded diary days.
"""

from __future__ import annotations

from alembic import op


revision = "20260217_0016"
down_revision = "20260217_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS current_streak_days integer;")
    op.execute("ALTER TABLE public.diary_settings ADD COLUMN IF NOT EXISTS max_streak_days integer;")
    op.execute("UPDATE public.diary_settings SET current_streak_days = COALESCE(current_streak_days, 0);")
    op.execute("UPDATE public.diary_settings SET max_streak_days = COALESCE(max_streak_days, 0);")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN current_streak_days SET DEFAULT 0;")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN max_streak_days SET DEFAULT 0;")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN current_streak_days SET NOT NULL;")
    op.execute("ALTER TABLE public.diary_settings ALTER COLUMN max_streak_days SET NOT NULL;")


def downgrade() -> None:
    op.execute("ALTER TABLE public.diary_settings DROP COLUMN IF EXISTS max_streak_days;")
    op.execute("ALTER TABLE public.diary_settings DROP COLUMN IF EXISTS current_streak_days;")
