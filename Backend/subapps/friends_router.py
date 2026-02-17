import uuid

from fastapi import APIRouter, Depends, HTTPException, Request

from Backend.auth import get_current_user
from Backend.crud.friends.friends_crud import (
    add_friend_by_code,
    get_or_create_my_code,
    list_friends_for_user,
)
from Backend.database import SessionLocal
from Backend.models.profile.profile_model import Profile
from Backend.schemas.friend_models import (
    AddFriendByCodeRequest,
    AddFriendByCodeResponse,
    FriendSummary,
    FriendsListResponse,
    MyCodeResponse,
)
from Backend.services.apns_service import send_friend_added_notification_to_user
from Backend.services.file_signing_service import sign_upload_id
import time
from sqlalchemy import text


router = APIRouter(prefix="/friends", tags=["friends"])

def _resolve_public_base_url(request) -> str:
    try:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
        if not host:
            return ""
        return f"{proto}://{host}".rstrip("/")
    except Exception:
        return ""


def _signed_url_for_upload_path(*, base_url: str, path_value: str, expires_seconds: int = 60 * 60 * 24) -> str | None:
    try:
        if not isinstance(path_value, str) or not path_value.startswith("uploads/"):
            return None
        upload_id = uuid.UUID(path_value.split("/", 1)[1])
        exp = int(time.time()) + int(expires_seconds)
        sig = sign_upload_id(upload_id=upload_id, exp=exp)
        if not base_url:
            return None
        return f"{base_url}/files/{upload_id}?exp={exp}&sig={sig}"
    except Exception:
        return None


def _auth_meta_for_user(user_id: uuid.UUID) -> tuple[dict, dict]:
    db = SessionLocal()
    try:
        row = db.execute(
            text("select raw_user_meta_data, raw_app_meta_data from auth.users where id = :id"),
            {"id": str(user_id)},
        ).first()
        if not row:
            return ({}, {})
        user_meta = row[0] if isinstance(row[0], dict) else {}
        app_meta = row[1] if isinstance(row[1], dict) else {}
        return (user_meta, app_meta)
    finally:
        db.close()


def _profile_fields_for_user(user_id: uuid.UUID) -> dict:
    db = SessionLocal()
    try:
        row = db.get(Profile, user_id)
        if row is None:
            return {}
        return {
            "full_name": getattr(row, "full_name", None),
            "avatar_path": getattr(row, "avatar_path", None),
            "last_seen_at": getattr(row, "last_seen_at", None),
            "is_online": getattr(row, "is_online", False),
        }
    finally:
        db.close()


def _resolve_display_name(user_id: uuid.UUID) -> str | None:
    profile = _profile_fields_for_user(user_id)
    name = (profile.get("full_name") or "").strip()
    if name:
        return name
    user_meta, _ = _auth_meta_for_user(user_id)
    return (user_meta.get("full_name") or user_meta.get("name") or "").strip() or None


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
        friend_id, is_new = await add_friend_by_code(user_id=user_id, code=request.code)
    except PermissionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Notify the friend that they were added (only for new friendships).
    if is_new:
        try:
            sender_name = _resolve_display_name(user_id)
            await send_friend_added_notification_to_user(
                recipient_user_id=friend_id,
                sender_name=sender_name,
            )
        except Exception:
            pass  # Push failures should not block the response.

    return AddFriendByCodeResponse(success=True, friend_user_id=friend_id)


@router.get("", response_model=FriendsListResponse)
async def list_friends(http_request: Request, current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        ids = await list_friends_for_user(user_id=user_id)
        base_url = _resolve_public_base_url(http_request)
        out: list[FriendSummary] = []
        for fid in ids:
            profile = _profile_fields_for_user(fid)
            user_meta, app_meta = _auth_meta_for_user(fid)

            fallback_full = (user_meta.get("full_name") or user_meta.get("name") or "").strip()
            full_name = (profile.get("full_name") or fallback_full or "Friend").strip()

            provider_raw = (app_meta.get("provider") or "")
            auth_provider = (str(provider_raw).strip() or "Unknown").title()

            avatar_url = None
            avatar_path = profile.get("avatar_path")
            if avatar_path:
                avatar_url = _signed_url_for_upload_path(base_url=base_url, path_value=str(avatar_path))
            if not avatar_url:
                avatar_url = (user_meta.get("avatar_url") or user_meta.get("picture")) if isinstance(user_meta, dict) else None

            last_seen_dt = profile.get("last_seen_at")
            last_seen_str = last_seen_dt.isoformat() + "Z" if last_seen_dt else None

            out.append(
                FriendSummary(
                    user_id=fid,
                    full_name=full_name,
                    auth_provider=auth_provider,
                    avatar_url=avatar_url,
                    last_seen_at=last_seen_str,
                    is_online=bool(profile.get("is_online", False)),
                )
            )
        return FriendsListResponse(friends=out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

