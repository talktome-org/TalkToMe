import uuid

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Request, UploadFile

from Backend.auth import get_current_user
from Backend.crud.user_links.link_crud import get_link_status_for_user, get_partner_user_id
from Backend.database import SessionLocal
from Backend.models.profile.profile_model import Profile
from Backend.crud.client_uploads.uploads_crud import create_upload
from Backend.services.file_signing_service import sign_upload_id
import time
from sqlalchemy import text

router = APIRouter(prefix="/profile", tags=["profile"])

def _resolve_public_base_url(request: Request | None) -> str:
    if request is None:
        return ""
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    if not host:
        return ""
    return f"{proto}://{host}".rstrip("/")

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

def _auth_user_metadata(user_id: uuid.UUID) -> dict:
    db = SessionLocal()
    try:
        row = db.execute(text("select raw_user_meta_data from auth.users where id = :id"), {"id": str(user_id)}).first()
        if not row or row[0] is None:
            return {}
        return row[0] if isinstance(row[0], dict) else {}
    finally:
        db.close()

def _get_profile_row(user_id: uuid.UUID) -> Profile | None:
    db = SessionLocal()
    try:
        return db.get(Profile, user_id)
    finally:
        db.close()

def _get_profile_fields(user_id: uuid.UUID, *field_names: str) -> dict:
    db = SessionLocal()
    try:
        row = db.get(Profile, user_id)
        if row is None:
            return {}
        return {name: getattr(row, name, None) for name in field_names}
    finally:
        db.close()

def _upsert_profile(user_id: uuid.UUID, update_data: dict) -> None:
    db = SessionLocal()
    try:
        row = db.get(Profile, user_id)
        if row is None:
            row = Profile(user_id=user_id)
            db.add(row)
        for k, v in update_data.items():
            setattr(row, k, v)
        # Keep updated_at aligned with typical Supabase patterns.
        try:
            import datetime as _dt

            if hasattr(row, "updated_at"):
                setattr(row, "updated_at", _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None))
        except Exception:
            pass
        db.commit()
    finally:
        db.close()


@router.post("/avatar")
async def upload_avatar(http_request: Request, file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        data = await file.read()
        content_type = file.content_type or "application/octet-stream"
        if not data:
            raise HTTPException(status_code=400, detail="Empty upload")

        upload_id = await create_upload(
            user_id=user_id,
            kind="avatar",
            content_type=content_type,
            filename=file.filename,
            data=data,
        )
        path_value = f"uploads/{upload_id}"
        _upsert_profile(user_id, {"avatar_path": path_value})

        base = _resolve_public_base_url(http_request)
        url_value = _signed_url_for_upload_path(base_url=base, path_value=path_value, expires_seconds=60 * 60 * 24)
        return {"path": path_value, "url": url_value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update")
async def update_profile(
    full_name: str = Form(None),
    bio: str = Form(None),
    partner_display_name: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        update_data = {}
        if full_name is not None:
            if len(full_name) > 22:
                raise HTTPException(status_code=400, detail="Full name must be 22 characters or fewer")
            update_data["full_name"] = full_name
        if bio is not None:
            update_data["bio"] = bio
        if partner_display_name is not None:
            if len(partner_display_name) > 22:
                raise HTTPException(status_code=400, detail="Partner name must be 22 characters or fewer")
            update_data["partner_display_name"] = partner_display_name

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields provided for update")

        _upsert_profile(user_id, update_data)

        return {"success": True, "message": "Profile updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update")
async def update_profile_post(
    full_name: str = Form(None),
    bio: str = Form(None),
    partner_display_name: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        update_data = {}
        if full_name is not None:
            if len(full_name) > 22:
                raise HTTPException(status_code=400, detail="Full name must be 22 characters or fewer")
            update_data["full_name"] = full_name
        if bio is not None:
            update_data["bio"] = bio
        if partner_display_name is not None:
            if len(partner_display_name) > 22:
                raise HTTPException(status_code=400, detail="Partner name must be 22 characters or fewer")
            update_data["partner_display_name"] = partner_display_name

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields provided for update")

        _upsert_profile(user_id, update_data)

        return {"success": True, "message": "Profile updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/info")
async def get_profile_info(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        profile_data = _get_profile_fields(user_id, "full_name", "bio")
        auth_metadata = current_user.get("user_metadata", {})
        fallback_full = (auth_metadata.get("full_name") or auth_metadata.get("name") or "").strip()

        return {"full_name": profile_data.get("full_name") or fallback_full, "bio": profile_data.get("bio") or ""}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/avatars")
async def get_self_and_partner_avatars(http_request: Request, current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        base = _resolve_public_base_url(http_request)
        me_path = _get_profile_fields(user_id, "avatar_path").get("avatar_path")

        if me_path:
            me_url = _signed_url_from_path(base_url=base, path_value=me_path)
        else:
            try:
                meta = current_user.get("user_metadata") or {}
                me_url = meta.get("avatar_url") or meta.get("picture") or current_user.get("picture")
            except Exception:
                me_url = None

        me_source = "storage" if me_path and me_url else ("provider" if me_url else "default")

        try:
            partner_id = await get_partner_user_id(user_id=user_id)
        except Exception:
            partner_id = None

        partner_url = None
        partner_source = "default"
        if partner_id:
            p_path = _get_profile_fields(partner_id, "avatar_path").get("avatar_path")
            partner_url = _signed_url_from_path(base_url=base, path_value=p_path) if p_path else _provider_avatar_from_admin(partner_id)
            partner_source = "storage" if p_path and partner_url else ("provider" if partner_url else "default")

        return {
            "me": {"url": me_url, "source": me_source},
            "partner": {"url": partner_url, "source": partner_source} if partner_id else {"url": None, "source": "default"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _signed_url_from_path(*, base_url: str, path_value: str) -> str | None:
    return _signed_url_for_upload_path(base_url=base_url, path_value=path_value, expires_seconds=60 * 60 * 24)


def _provider_avatar_from_admin(user_id: uuid.UUID) -> str | None:
    try:
        meta = _auth_user_metadata(user_id)
        return (meta.get("avatar_url") or meta.get("picture")) if isinstance(meta, dict) else None
    except Exception:
        return None


@router.get("/partner-info")
async def get_partner_info(http_request: Request, current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        base = _resolve_public_base_url(http_request)
        linked, _, _ = await get_link_status_for_user(user_id=user_id)
        if not linked:
            return {"linked": False, "partner": None}

        partner_id = await get_partner_user_id(user_id=user_id)
        if not partner_id:
            return {"linked": False, "partner": None}

        try:
            def extract_name_from_meta(meta_dict):
                if not isinstance(meta_dict, dict):
                    return "Unknown"
                return meta_dict.get("full_name") or meta_dict.get("name") or meta_dict.get("display_name") or "Unknown"

            def extract_avatar_from_meta(meta_dict):
                if not isinstance(meta_dict, dict):
                    return None
                return meta_dict.get("avatar_url") or meta_dict.get("picture")

            meta = _auth_user_metadata(partner_id)
            name = extract_name_from_meta(meta if isinstance(meta, dict) else None)
            avatar_url = extract_avatar_from_meta(meta if isinstance(meta, dict) else None)

            try:
                saved_full = (_get_profile_fields(partner_id, "full_name").get("full_name") or "").strip()
                if saved_full:
                    name = saved_full
            except Exception:
                pass

            p_path = _get_profile_fields(partner_id, "avatar_path").get("avatar_path")
            custom_avatar_url = _signed_url_from_path(base_url=base, path_value=p_path) if p_path else None

            return {"linked": True, "partner": {"name": name, "avatar_url": custom_avatar_url or avatar_url}}
        except Exception as e:
            print(f"[Partner Info] Error fetching partner info for {partner_id}: {e}")
            return {"linked": True, "partner": {"name": "Unknown", "avatar_url": None}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/onboarding")
async def get_onboarding(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        row = _get_profile_fields(user_id, "full_name", "partner_display_name", "onboarding_step")
        linked, _, _ = await get_link_status_for_user(user_id=user_id)

        return {
            "full_name": row.get("full_name") or "",
            "partner_display_name": row.get("partner_display_name"),
            "onboarding_step": row.get("onboarding_step") or "none",
            "linked": linked,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/onboarding")
async def update_onboarding(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        allowed_keys = {"partner_display_name", "onboarding_step"}
        update_data = {k: v for k, v in (payload or {}).items() if k in allowed_keys}

        if not update_data:
            raise HTTPException(status_code=400, detail="No allowed fields provided")

        if "partner_display_name" in update_data and update_data["partner_display_name"] is not None:
            name_val = str(update_data["partner_display_name"]).strip()
            if len(name_val) > 22:
                raise HTTPException(status_code=400, detail="Partner name must be 22 characters or fewer")
            update_data["partner_display_name"] = name_val

        if "onboarding_step" in update_data:
            new_step = update_data["onboarding_step"]
            if new_step not in ("none", "asked_name", "asked_partner", "suggested_link", "completed"):
                raise HTTPException(status_code=400, detail="Invalid onboarding_step")
            cur_step = (_get_profile_fields(user_id, "onboarding_step").get("onboarding_step") or "none") or "none"
            order = {"none": 0, "asked_name": 1, "asked_partner": 2, "suggested_link": 3, "completed": 4}
            if order.get(new_step, -1) < order.get(cur_step, 0) and new_step != "completed":
                raise HTTPException(status_code=400, detail="Onboarding step cannot regress")

        _upsert_profile(user_id, update_data)

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


