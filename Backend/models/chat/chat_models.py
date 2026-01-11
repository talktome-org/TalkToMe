from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class UserChatSession(Base):
    __tablename__ = "user_chat_sessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), server_default=func.now())
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    linked_session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("linked_sessions.id", name="user_chat_sessions_linked_session_id_fkey"),
        nullable=True,
        index=True,
    )
    last_message_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class UserChatMessage(Base):
    __tablename__ = "user_chat_messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="fk_user_chat_messages_user_id"),
        index=True,
        nullable=True,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("''::text"))
    content: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("''::text"))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), server_default=func.now())
    session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_sessions.id", name="fk_user_chat_messages_session_id"),
        index=True,
        nullable=True,
    )