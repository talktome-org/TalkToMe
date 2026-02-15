"""SQLAlchemy models for diary_settings and diary_entries.

These tables are created by the migration and used directly from the iOS app
via Supabase. The models here provide schema metadata for Alembic and optional
Backend API use. Persistence of diary data happens from iOS → Supabase, not
through these models unless you add Backend endpoints that use them.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import Date, DateTime, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class DiarySettings(Base):
    """Per-user diary customization (name, description, header color)."""

    __tablename__ = "diary_settings"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'My Diary'::text"))
    description: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("''::text"))
    header_color_hex: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'#B8DEFF'::text"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DiaryEntry(Base):
    """Diary/journal entry with body blocks (text + image storage paths)."""

    __tablename__ = "diary_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'Untitled'::text"))
    body_blocks: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    timezone_abbreviation: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'UTC'::text"))
