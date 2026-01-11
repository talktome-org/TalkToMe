from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class LinkedSession(Base):
    __tablename__ = "linked_sessions"

    __table_args__ = (
        UniqueConstraint(
            "relationship_id",
            "user_a_personal_session_id",
            name="linked_sessions_relationship_id_user_a_personal_session_id_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    relationship_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("paired_accounts.id", name="linked_sessions_relationship_id_fkey"),
        nullable=False,
        index=True,
    )
    user_a_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="linked_sessions_user_a_id_fkey"),
        nullable=False,
    )
    user_b_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="linked_sessions_user_b_id_fkey"),
        nullable=False,
    )
    user_a_personal_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_sessions.id", name="linked_sessions_user_a_personal_session_id_fkey"),
        nullable=False,
    )
    user_b_personal_session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_sessions.id", name="linked_sessions_user_b_personal_session_id_fkey"),
        nullable=True,
    )
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), server_default=func.now())