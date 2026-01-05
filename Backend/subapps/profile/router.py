import uuid

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile

from ...auth import get_current_user
from ...crud.link_repo import get_link_status_for_user, get_partner_user_id
from ...db.supabase_client import supabase

router = APIRouter(prefix="/profile", tags=["profile"])


@router.post("/avatar")
async def upload_avatar(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        data = await file.read()
        content_type = file.content_type or "application/octet-stream"

        if content_type == "image/jpeg":
            key = f"{user_id}.jpg"
        elif content_type == "image/png":
            key = f"{user_id}.png"
        elif content_type == "image/webp":
            key = f"{user_id}.webp"
        else:
            key = f"{user_id}"

        res = supabase.storage.from_("avatar").upload(
            path=key,
            file=data,
            file_options={"contentType": content_type, "upsert": "true"},
        )

        if getattr(res, "error", None):
            raise HTTPException(status_code=500, detail=f"Storage upload failed: {res.error}")

        path_value = f"avatar/{key}"
        up = supabase.table("profiles").upsert({"user_id": str(user_id), "avatar_path": path_value}).execute()
        if getattr(up, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to update profile: {up.error}")

        signed = supabase.storage.from_("avatar").create_signed_url(key, 60 * 60 * 24)
        url_value = signed.get("signedURL") if isinstance(signed, dict) else None

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

        result = supabase.table("profiles").upsert({"user_id": str(user_id), **update_data}).execute()
        if getattr(result, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to update profile: {result.error}")

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

        result = supabase.table("profiles").upsert({"user_id": str(user_id), **update_data}).execute()
        if getattr(result, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to update profile: {result.error}")

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

        result = supabase.table("profiles").select("full_name, bio").eq("user_id", str(user_id)).limit(1).execute()
        if getattr(result, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to get profile: {result.error}")

        profile_data = result.data[0] if result.data else {}
        auth_metadata = current_user.get("user_metadata", {})
        fallback_full = (auth_metadata.get("full_name") or auth_metadata.get("name") or "").strip()

        return {"full_name": profile_data.get("full_name") or fallback_full, "bio": profile_data.get("bio") or ""}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/avatars")
async def get_self_and_partner_avatars(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        me_sel = supabase.table("profiles").select("avatar_path").eq("user_id", str(user_id)).limit(1).execute()
        me_path = (me_sel.data[0].get("avatar_path") if me_sel.data else None) if not getattr(me_sel, "error", None) else None

        if me_path:
            me_url = _signed_url_from_path(me_path)
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
            ps = supabase.table("profiles").select("avatar_path").eq("user_id", str(partner_id)).limit(1).execute()
            p_path = (ps.data[0].get("avatar_path") if ps.data else None) if not getattr(ps, "error", None) else None
            partner_url = _signed_url_from_path(p_path) if p_path else _provider_avatar_from_admin(partner_id)
            partner_source = "storage" if p_path and partner_url else ("provider" if partner_url else "default")

        return {
            "me": {"url": me_url, "source": me_source},
            "partner": {"url": partner_url, "source": partner_source} if partner_id else {"url": None, "source": "default"},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _signed_url_from_path(path_value: str) -> str | None:
    try:
        if "/" in path_value:
            bucket, key = path_value.split("/", 1)
        else:
            bucket, key = "avatar", path_value
        signed = supabase.storage.from_(bucket).create_signed_url(key, 60 * 60 * 24)
        return signed.get("signedURL") if isinstance(signed, dict) else None
    except Exception:
        return None


def _provider_avatar_from_admin(user_id: uuid.UUID) -> str | None:
    try:
        res = supabase.auth.admin.get_user_by_id(str(user_id))  # type: ignore[attr-defined]
        user = getattr(res, "user", None) or getattr(res, "data", None)
        if not user:
            return None

        def extract_from_meta(meta_dict):
            if not isinstance(meta_dict, dict):
                return None
            return meta_dict.get("avatar_url") or meta_dict.get("picture")

        if isinstance(user, dict):
            meta = user.get("user_metadata")
            url = extract_from_meta(meta)
            if url:
                return url
        else:
            meta = getattr(user, "user_metadata", None)
            url = extract_from_meta(meta if isinstance(meta, dict) else None)
            if url:
                return url
        return None
    except Exception as e:
        print(f"[Avatar] Error fetching partner avatar for {user_id}: {e}")
        return None


@router.get("/partner-info")
async def get_partner_info(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        linked, _, _ = await get_link_status_for_user(user_id=user_id)
        if not linked:
            return {"linked": False, "partner": None}

        partner_id = await get_partner_user_id(user_id=user_id)
        if not partner_id:
            return {"linked": False, "partner": None}

        try:
            res = supabase.auth.admin.get_user_by_id(str(partner_id))  # type: ignore[attr-defined]
            user = getattr(res, "user", None) or getattr(res, "data", None)

            if not user:
                return {"linked": True, "partner": {"name": "Unknown", "avatar_url": None}}

            def extract_name_from_meta(meta_dict):
                if not isinstance(meta_dict, dict):
                    return "Unknown"
                return meta_dict.get("full_name") or meta_dict.get("name") or meta_dict.get("display_name") or "Unknown"

            def extract_avatar_from_meta(meta_dict):
                if not isinstance(meta_dict, dict):
                    return None
                return meta_dict.get("avatar_url") or meta_dict.get("picture")

            if isinstance(user, dict):
                meta = user.get("user_metadata")
                name = extract_name_from_meta(meta)
                avatar_url = extract_avatar_from_meta(meta)
            else:
                meta = getattr(user, "user_metadata", None)
                name = extract_name_from_meta(meta if isinstance(meta, dict) else None)
                avatar_url = extract_avatar_from_meta(meta if isinstance(meta, dict) else None)

            try:
                prof = supabase.table("profiles").select("full_name").eq("user_id", str(partner_id)).limit(1).execute()
                if not getattr(prof, "error", None) and prof.data:
                    saved_full = (prof.data[0].get("full_name") or "").strip()
                    if saved_full:
                        name = saved_full
            except Exception:
                pass

            ps = supabase.table("profiles").select("avatar_path").eq("user_id", str(partner_id)).limit(1).execute()
            p_path = (ps.data[0].get("avatar_path") if ps.data else None) if not getattr(ps, "error", None) else None
            custom_avatar_url = _signed_url_from_path(p_path) if p_path else None

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

        sel = (
            supabase.table("profiles")
            .select("full_name, partner_display_name, onboarding_step")
            .eq("user_id", str(user_id))
            .limit(1)
            .execute()
        )

        if getattr(sel, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to load onboarding: {sel.error}")

        row = sel.data[0] if sel.data else {}
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
            cur = supabase.table("profiles").select("onboarding_step").eq("user_id", str(user_id)).limit(1).execute()
            if getattr(cur, "error", None):
                raise HTTPException(status_code=500, detail=f"Failed to load current step: {cur.error}")
            cur_step = (cur.data[0].get("onboarding_step") if cur.data else "none") or "none"
            order = {"none": 0, "asked_name": 1, "asked_partner": 2, "suggested_link": 3, "completed": 4}
            if order.get(new_step, -1) < order.get(cur_step, 0) and new_step != "completed":
                raise HTTPException(status_code=400, detail="Onboarding step cannot regress")

        res = supabase.table("profiles").upsert({"user_id": str(user_id), **update_data}).execute()
        if getattr(res, "error", None):
            raise HTTPException(status_code=500, detail=f"Failed to update onboarding: {res.error}")

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


