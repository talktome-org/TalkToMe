import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from Backend.auth import get_current_user
from Backend.crud.apns.device_tokens_crud import disable_token_by_value, upsert_token
from sqlalchemy import text
from Backend.database import SessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/notifications", tags=["notifications"])

def _now_ts():
    # Store as naive UTC to match Supabase `timestamp` (no tz) UI.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _auth_user_metadata(user_id: str) -> dict:
    db = SessionLocal()
    try:
        row = db.execute(text("select raw_user_meta_data from auth.users where id = :id"), {"id": user_id}).first()
        if not row or row[0] is None:
            return {}
        return row[0] if isinstance(row[0], dict) else {}
    finally:
        db.close()


@router.post("/register")
async def register_token(payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    token = payload.get("token")
    platform = payload.get("platform") or "ios"
    bundle_id = payload.get("bundle_id")
    tz = payload.get("timezone")
    if not token or not isinstance(token, str) or len(token) < 10:
        raise HTTPException(status_code=400, detail="Missing or invalid device token")

    try:
        await upsert_token(user_id=user_uuid, token=token, platform=platform, bundle_id=bundle_id, tz=tz)
        logger.info("[APNS] Registered token for user %s (token=%s…)", user_uuid, token[:12])
        return {"success": True}
    except Exception as e:
        logger.exception("[APNS] Failed to register token for user %s: %s", user_uuid, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unregister")
async def unregister_token(payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    token = payload.get("token")
    if not token or not isinstance(token, str):
        raise HTTPException(status_code=400, detail="Missing token")
    try:
        await disable_token_by_value(token=token, user_id=user_uuid)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
