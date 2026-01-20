from __future__ import annotations

from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field


class MyCodeResponse(BaseModel):
    code: str
    expires_at: datetime


class AddFriendByCodeRequest(BaseModel):
    code: str = Field(..., min_length=4, max_length=4)


class AddFriendByCodeResponse(BaseModel):
    success: bool
    friend_user_id: UUID


class FriendsListResponse(BaseModel):
    friends: List[UUID]

