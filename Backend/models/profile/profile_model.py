from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Text, func, text
from sqlalchemy.dialects.postgresql import ENUM, JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class Profile(Base):
    __tablename__ = "profiles"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="profiles_user_id_fkey"),
        primary_key=True,
    )
    avatar_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True, server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True, server_default=func.now())
    bio: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    full_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    partner_display_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Postgres enum type already exists in DB as `onboarding_step`.
    onboarding_step: Mapped[Optional[str]] = mapped_column(
        ENUM("none", "asked_name", "completed", name="onboarding_step", create_type=False),
        nullable=True,
        server_default=text("'none'::onboarding_step"),
    )

    # Onboarding fields
    gender: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    # JSON array of strings (e.g. ["Work","Family"])
    relationship_topics: Mapped[Optional[list[str]]] = mapped_column(JSONB, nullable=True)