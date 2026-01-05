from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel


class PartnerRequestBody(BaseModel):
    message: str
    session_id: UUID


class PartnerRequestResponse(BaseModel):
    success: bool
    request_id: UUID


class PartnerPendingRequestDTO(BaseModel):
    id: UUID
    sender_user_id: UUID
    sender_session_id: UUID
    content: str
    created_at: str
    status: str
    recipient_session_id: UUID | None = None
    created_message_id: UUID | None = None


class PartnerPendingRequestsResponse(BaseModel):
    requests: list[PartnerPendingRequestDTO]


