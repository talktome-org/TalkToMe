import uuid

from fastapi import APIRouter, Depends, HTTPException

from Backend.auth import get_current_user
from Backend.crud.friends.friends_crud import (
    add_friend_by_code,
    get_or_create_my_code,
    list_friends_for_user,
)
from Backend.schemas.friend_models import AddFriendByCodeRequest, AddFriendByCodeResponse, FriendsListResponse, MyCodeResponse


router = APIRouter(prefix="/friends", tags=["friends"])


@router.get("/my-code", response_model=MyCodeResponse)
async def my_code(current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        row = await get_or_create_my_code(user_id=user_id, expires_in_minutes=10)
        return MyCodeResponse(code=row["code"], expires_at=row["expires_at"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-by-code", response_model=AddFriendByCodeResponse)
async def add_by_code(request: AddFriendByCodeRequest, current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        friend_id = await add_friend_by_code(user_id=user_id, code=request.code)
        return AddFriendByCodeResponse(success=True, friend_user_id=friend_id)
    except PermissionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=FriendsListResponse)
async def list_friends(current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        ids = await list_friends_for_user(user_id=user_id)
        return FriendsListResponse(friends=ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

