from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Text, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.schema import ForeignKey

from ...database import Base


class PartnerRequest(Base):
    __tablename__ = "partner_requests"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    relationship_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("paired_accounts.id", name="partner_requests_relationship_id_fkey"),
        nullable=False,
        index=True,
    )
    sender_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="partner_requests_sender_user_id_fkey"),
        nullable=False,
        index=True,
    )
    recipient_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("auth.users.id", name="partner_requests_recipient_user_id_fkey"),
        nullable=False,
        index=True,
    )
    sender_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_sessions.id", name="partner_requests_sender_session_id_fkey"),
        nullable=False,
        index=True,
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pending'::request_status"))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), server_default=func.now())
    linked_session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("linked_sessions.id", name="partner_requests_linked_session_id_fkey"),
        nullable=True,
        index=True,
    )
    recipient_session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_sessions.id", name="partner_requests_recipient_session_id_fkey"),
        nullable=True,
        index=True,
    )
    created_message_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("user_chat_messages.id", name="partner_requests_created_message_id_fkey"),
        nullable=True,
        index=True,
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    accepted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)