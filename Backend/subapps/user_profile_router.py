import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Request, UploadFile

from Backend.auth import get_current_user
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

        return {
            "me": {"url": me_url, "source": me_source},
            # Partner linking has been removed (friends-only work-in-progress).
            "partner": {"url": None, "source": "default"},
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


@router.get("/onboarding")
async def get_onboarding(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        row = _get_profile_fields(user_id, "full_name", "onboarding_step", "gender", "date_of_birth", "relationship_topics")
        dob = row.get("date_of_birth")

        return {
            "full_name": row.get("full_name") or "",
            "onboarding_step": row.get("onboarding_step") or "none",
            "gender": row.get("gender") or "",
            "date_of_birth": dob.isoformat() if dob else None,
            "relationship_topics": row.get("relationship_topics") or [],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/onboarding")
async def update_onboarding(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    update_data: dict = {}
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        allowed_keys = {"onboarding_step", "full_name", "gender", "date_of_birth", "relationship_topics"}
        update_data = {k: v for k, v in (payload or {}).items() if k in allowed_keys}

        if not update_data:
            raise HTTPException(status_code=400, detail="No allowed fields provided")

        if "onboarding_step" in update_data:
            new_step = update_data["onboarding_step"]
            if new_step not in ("none", "asked_name", "completed"):
                raise HTTPException(status_code=400, detail="Invalid onboarding_step")
            cur_step = (_get_profile_fields(user_id, "onboarding_step").get("onboarding_step") or "none") or "none"
            order = {"none": 0, "asked_name": 1, "completed": 2}
            if order.get(new_step, -1) < order.get(cur_step, 0) and new_step != "completed":
                raise HTTPException(status_code=400, detail="Onboarding step cannot regress")

        if "full_name" in update_data and update_data["full_name"] is not None:
            full_name = str(update_data["full_name"]).strip()
            if len(full_name) > 22:
                raise HTTPException(status_code=400, detail="Full name must be 22 characters or fewer")
            update_data["full_name"] = full_name

        if "gender" in update_data and update_data["gender"] is not None:
            gender = str(update_data["gender"]).strip()
            if len(gender) > 32:
                raise HTTPException(status_code=400, detail="Gender must be 32 characters or fewer")
            update_data["gender"] = gender

        if "date_of_birth" in update_data:
            dob = update_data.get("date_of_birth")
            if dob in (None, ""):
                update_data["date_of_birth"] = None
            else:
                try:
                    import datetime as _dt

                    update_data["date_of_birth"] = _dt.date.fromisoformat(str(dob))
                except Exception:
                    raise HTTPException(status_code=400, detail="date_of_birth must be ISO format YYYY-MM-DD")

        if "relationship_topics" in update_data:
            topics = update_data.get("relationship_topics")
            if topics is None:
                update_data["relationship_topics"] = []
            elif not isinstance(topics, list):
                raise HTTPException(status_code=400, detail="relationship_topics must be an array")
            else:
                normalized: list[str] = []
                for t in topics:
                    if t is None:
                        continue
                    s = str(t).strip()
                    if not s:
                        continue
                    if len(s) > 40:
                        raise HTTPException(status_code=400, detail="Each relationship topic must be 40 characters or fewer")
                    normalized.append(s)
                # de-dupe preserving order
                seen: set[str] = set()
                deduped: list[str] = []
                for t in normalized:
                    if t in seen:
                        continue
                    seen.add(t)
                    deduped.append(t)
                update_data["relationship_topics"] = deduped

        _upsert_profile(user_id, update_data)

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        # Log full traceback to Fly logs for debugging.
        try:
            import traceback

            print("[onboarding] update_onboarding failed")
            print(f"[onboarding] user_id={current_user.get('sub')}")
            print(f"[onboarding] payload_keys={list((payload or {}).keys())}")
            print(f"[onboarding] update_keys={list((update_data or {}).keys())}")
            print(traceback.format_exc())
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


@router.delete("/account")
async def delete_account(current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        db = SessionLocal()
        try:
            from Backend.models.chat.chat_models import UserChatMessage, UserChatSession
            from Backend.models.diary.diary_models import DiaryEntry, DiarySettings
            from Backend.models.apns.device_token_model import DeviceToken
            from Backend.models.client_uploads.upload_model import Upload
            from Backend.models.friends.friend_code_model import FriendCode
            from Backend.models.friends.friendship_model import Friendship
            from Backend.models.friends.friend_invite_model import FriendInvite
            from sqlalchemy import delete, or_

            # Collect the user's session IDs
            user_session_ids = db.execute(
                UserChatSession.__table__.select()
                .where(UserChatSession.user_id == user_id)
                .with_only_columns(UserChatSession.id)
            ).scalars().all()

            # Collect friendship IDs and the friend's linked sessions
            friendship_ids = db.execute(
                Friendship.__table__.select()
                .where(or_(Friendship.user_low_id == user_id, Friendship.user_high_id == user_id))
                .with_only_columns(Friendship.id)
            ).scalars().all()

            friend_session_ids = []
            if friendship_ids:
                friend_session_ids = db.execute(
                    UserChatSession.__table__.select()
                    .where(
                        UserChatSession.friendship_id.in_(friendship_ids),
                        UserChatSession.user_id != user_id,
                    )
                    .with_only_columns(UserChatSession.id)
                ).scalars().all()

            all_session_ids = list(user_session_ids) + list(friend_session_ids)

            # Clear linked_session_id references before deleting sessions
            if all_session_ids:
                db.execute(
                    UserChatSession.__table__.update()
                    .where(UserChatSession.linked_session_id.in_(all_session_ids))
                    .values(linked_session_id=None)
                )
            db.execute(
                UserChatSession.__table__.update()
                .where(UserChatSession.user_id == user_id)
                .values(linked_session_id=None)
            )

            # Delete all messages in affected sessions (user's + friend's linked sessions)
            if all_session_ids:
                db.execute(delete(UserChatMessage).where(
                    or_(UserChatMessage.user_id == user_id, UserChatMessage.session_id.in_(all_session_ids))
                ))
            else:
                db.execute(delete(UserChatMessage).where(UserChatMessage.user_id == user_id))

            # Delete all affected sessions (user's + friend's linked sessions)
            if friend_session_ids:
                db.execute(delete(UserChatSession).where(UserChatSession.id.in_(friend_session_ids)))
            db.execute(delete(UserChatSession).where(UserChatSession.user_id == user_id))

            db.execute(delete(DiaryEntry).where(DiaryEntry.user_id == user_id))
            db.execute(delete(DiarySettings).where(DiarySettings.user_id == user_id))
            db.execute(delete(DeviceToken).where(DeviceToken.user_id == user_id))
            db.execute(delete(Upload).where(Upload.user_id == user_id))
            db.execute(delete(FriendCode).where(FriendCode.user_id == user_id))
            db.execute(delete(Friendship).where(
                or_(Friendship.user_low_id == user_id, Friendship.user_high_id == user_id)
            ))
            db.execute(delete(Profile).where(Profile.user_id == user_id))

            db.execute(delete(FriendInvite).where(
                or_(FriendInvite.inviter_user_id == user_id, FriendInvite.used_by_user_id == user_id)
            ))

            # Remove the Supabase auth record
            db.execute(text("DELETE FROM auth.users WHERE id = :id"), {"id": str(user_id)})

            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/presence")
async def update_presence(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    try:
        user_id = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    online = bool(payload.get("online", False))

    update_data: dict = {"is_online": online}
    # Only touch last_seen_at when coming online; when going offline we
    # preserve the existing timestamp so staleness checks work immediately.
    if online:
        update_data["last_seen_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    _upsert_profile(user_id, update_data)

    return {"success": True}
