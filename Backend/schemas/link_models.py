from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel


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


