from uuid import UUID
from typing import Optional

from pydantic import BaseModel


# Chat
class ChatHistoryMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[UUID] = None
    chat_history: Optional[list[ChatHistoryMessage]] = None
    previous_response_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    success: bool
    session_id: Optional[UUID] = None


class MessageDTO(BaseModel):
    id: UUID
    user_id: UUID
    session_id: UUID
    role: str
    content: str


class MessagesResponse(BaseModel):
    messages: list[MessageDTO]


class SessionDTO(BaseModel):
    id: UUID
    user_id: UUID
    title: Optional[str] = None
    last_message_at: Optional[str] = None
    last_message_content: Optional[str] = None


class SessionsResponse(BaseModel):
    sessions: list[SessionDTO]


# Link
class CreateLinkInviteResponse(BaseModel):
    invite_token: str
    share_url: str


class AcceptLinkInviteRequest(BaseModel):
    invite_token: str


class AcceptLinkInviteResponse(BaseModel):
    success: bool
    relationship_id: UUID | None = None


class UnlinkResponse(BaseModel):
    success: bool
    unlinked: bool


class LinkStatusResponse(BaseModel):
    success: bool
    linked: bool
    relationship_id: UUID | None = None
    linked_at: str | None = None


# Partner
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


