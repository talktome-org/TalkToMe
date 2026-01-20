from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class Friendship(Base):
    __tablename__ = "friendships"

    __table_args__ = (
        UniqueConstraint("user_low_id", "user_high_id", name="friendships_user_low_id_user_high_id_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    # Canonical ordering: store the smaller UUID as low, larger as high.
    user_low_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="friendships_user_low_id_fkey"),
        index=True,
        nullable=False,
    )
    user_high_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="friendships_user_high_id_fkey"),
        index=True,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'accepted'::text"))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), server_default=func.now())

