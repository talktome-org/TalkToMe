from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, SmallInteger, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class UserPreference(Base):
    __tablename__ = "user_preferences"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="user_preferences_user_id_fkey"),
        primary_key=True,
    )
    daily_checkins_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True, server_default=text("false"))
    daily_checkin_hour: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    daily_checkin_minute: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True, server_default=func.now())
    timezone: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_checkin_sent_date: Mapped[Optional[date]] = mapped_column("last_checkin_sent_date", Date, nullable=True)