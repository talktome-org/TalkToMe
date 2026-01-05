from __future__ import annotations

from typing import Optional
from uuid import UUID

from pydantic import BaseModel


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


