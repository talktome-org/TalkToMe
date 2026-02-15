from __future__ import annotations

from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ChatHistoryMessage(BaseModel):
    role: str
    content: str


class ChatAttachment(BaseModel):
    type: str
    path: str
    filename: Optional[str] = None
    content_type: Optional[str] = None


# Input payload accepted by the chat endpoint.
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[UUID] = None
    message_id: Optional[UUID] = None
    chat_history: Optional[list[ChatHistoryMessage]] = None
    previous_response_id: Optional[str] = None
    attachments: Optional[list[ChatAttachment]] = None
    friend_user_id: Optional[UUID] = None
    ephemeral: bool = False
    voice_agent: Optional[str] = None
    ghost_name: Optional[str] = None
    delete_before: Optional[UUID] = None


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
    created_at: Optional[str] = None


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


