import urllib.parse
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select, text

from Backend.auth import get_current_user
from Backend.crud.user_links.link_crud import (
    accept_link_invite,
    get_link_status_for_user,
    get_or_create_link_invite,
    unlink_relationship_for_user,
)
from Backend.database import SessionLocal
from Backend.models.profile.profile_model import Profile
from Backend.schemas.link_models import (
    AcceptLinkInviteRequest,
    AcceptLinkInviteResponse,
    CreateLinkInviteResponse,
    LinkStatusResponse,
    UnlinkResponse,
)

router = APIRouter(prefix="/link")


def _resolve_public_base_url(request: Request | None) -> str:
    if request is None:
        return ""
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    if not host:
        return ""
    return f"{proto}://{host}".rstrip("/")

def _auth_user_metadata(user_id: uuid.UUID) -> dict:
    db = SessionLocal()
    try:
        row = db.execute(text("select raw_user_meta_data from auth.users where id = :id"), {"id": str(user_id)}).first()
        if not row or row[0] is None:
            return {}
        return row[0] if isinstance(row[0], dict) else {}
    finally:
        db.close()


def _inviter_display_name(user_id: uuid.UUID) -> str:
    db = SessionLocal()
    try:
        row = db.execute(select(Profile.full_name).where(Profile.user_id == user_id).limit(1)).first()
        if row and isinstance(row[0], str):
            saved_full = row[0].strip()
            if saved_full:
                return saved_full
    finally:
        db.close()

    meta = _auth_user_metadata(user_id)
    if isinstance(meta, dict):
        return (meta.get("full_name") or meta.get("name") or meta.get("display_name") or "").strip()
    return ""


@router.post("/send-invite", response_model=CreateLinkInviteResponse)
async def create_invite(http_request: Request, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        row = await get_or_create_link_invite(inviter_user_id=user_uuid, expires_in_hours=24)
        base = _resolve_public_base_url(http_request)
        if not base:
            raise HTTPException(status_code=500, detail="Cannot determine public base URL for share links")

        inviter_name: str = ""
        try:
            inviter_name = _inviter_display_name(user_uuid)
        except Exception:
            inviter_name = ""

        qp = f"code={row['invite_token']}"
        if inviter_name:
            qp += f"&name={urllib.parse.quote(inviter_name)}"
        share_url = f"{base.rstrip('/')}/link?{qp}"

        return CreateLinkInviteResponse(invite_token=row["invite_token"], share_url=share_url)
    except PermissionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating invite: {str(e)}")


@router.post("/accept-invite", response_model=AcceptLinkInviteResponse)
async def accept_invite(request: AcceptLinkInviteRequest, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        relationship_id = await accept_link_invite(invite_token=request.invite_token, invitee_user_id=user_uuid)
        return AcceptLinkInviteResponse(success=True, relationship_id=relationship_id)
    except PermissionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accepting invite: {str(e)}")


@router.post("/unlink-pair", response_model=UnlinkResponse)
async def unlink(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        deleted = await unlink_relationship_for_user(user_id=user_uuid)
        try:
            await get_or_create_link_invite(inviter_user_id=user_uuid, expires_in_hours=24)
        except Exception:
            pass
        return UnlinkResponse(success=True, unlinked=deleted)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error unlinking: {str(e)}")


@router.get("/status", response_model=LinkStatusResponse)
async def link_status(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        linked, rel_id, linked_at = await get_link_status_for_user(user_id=user_uuid)
        return LinkStatusResponse(success=True, linked=linked, relationship_id=rel_id, linked_at=linked_at)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching link status: {str(e)}")


