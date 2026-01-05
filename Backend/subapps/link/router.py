import os
import urllib.parse
import uuid

from fastapi import APIRouter, Depends, HTTPException

from ...auth import get_current_user
from ...crud.link_repo import (
    accept_link_invite,
    get_link_status_for_user,
    get_or_create_link_invite,
    unlink_relationship_for_user,
)
from ...db.supabase_client import supabase
from ...schemas.requests import (
    AcceptLinkInviteRequest,
    AcceptLinkInviteResponse,
    CreateLinkInviteResponse,
    LinkStatusResponse,
    UnlinkResponse,
)

router = APIRouter(prefix="/link")


@router.post("/send-invite", response_model=CreateLinkInviteResponse)
async def create_invite(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        row = await get_or_create_link_invite(inviter_user_id=user_uuid, expires_in_hours=24)
        base = os.getenv("SHARE_LINK_BASE_URL", "https://example.com")

        inviter_name: str = ""
        try:
            prof = supabase.table("profiles").select("full_name").eq("user_id", str(user_uuid)).limit(1).execute()
            if not getattr(prof, "error", None) and prof.data:
                saved_full = (prof.data[0].get("full_name") or "").strip()
                inviter_name = saved_full or ""
            if not inviter_name:
                res = supabase.auth.admin.get_user_by_id(str(user_uuid))  # type: ignore[attr-defined]
                user = getattr(res, "user", None) or getattr(res, "data", None)

                def extract_name(meta_dict):
                    if not isinstance(meta_dict, dict):
                        return ""
                    return (meta_dict.get("full_name") or meta_dict.get("name") or meta_dict.get("display_name") or "").strip()

                if isinstance(user, dict):
                    inviter_name = extract_name(user.get("user_metadata"))
                else:
                    meta = getattr(user, "user_metadata", None)
                    inviter_name = extract_name(meta if isinstance(meta, dict) else None)
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


