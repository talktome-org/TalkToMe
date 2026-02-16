"""Diary API: settings, entries, and photo uploads (stored in the uploads table)."""

import time
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile

from Backend.auth import get_current_user
from Backend.crud.client_uploads.uploads_crud import create_upload
from Backend.crud.diary.diary_crud import (
    delete_entry as crud_delete_entry,
    get_entry as crud_get_entry,
    get_settings as crud_get_settings,
    list_entries as crud_list_entries,
    save_entry as crud_save_entry,
    upsert_settings as crud_upsert_settings,
)
from Backend.services.file_signing_service import sign_upload_id

router = APIRouter(prefix="/diary", tags=["diary"])

_SIGNED_URL_TTL = 60 * 60 * 24 * 7  # 7 days


def _user_id(current_user: dict) -> uuid.UUID:
    try:
        return uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")


def _is_undefined_table(exc: BaseException) -> bool:
    """True if this is PostgreSQL 42P01 (undefined_table)."""
    if "42P01" in str(exc):
        return True
    code = getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)
    if code == "42P01":
        return True
    cause = getattr(exc, "__cause__", None)
    return cause is not None and _is_undefined_table(cause)


def _resolve_public_base_url(request: Request | None) -> str:
    if request is None:
        return ""
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    if not host:
        return ""
    return f"{proto}://{host}".rstrip("/")


def _inject_image_urls(entries: list[dict], request: Request) -> None:
    """Add signed ``url`` to image blocks that reference the uploads table."""
    base = _resolve_public_base_url(request)
    if not base:
        return
    for entry in entries:
        for block in entry.get("body_blocks") or []:
            path = block.get("path") or ""
            if block.get("type") == "image" and path.startswith("uploads/"):
                try:
                    upload_id = uuid.UUID(path.split("/", 1)[1])
                except Exception:
                    continue
                exp = int(time.time()) + _SIGNED_URL_TTL
                sig = sign_upload_id(upload_id=upload_id, exp=exp)
                block["url"] = f"{base}/files/{upload_id}?exp={exp}&sig={sig}"


# --- Attachments ---


@router.post("/attachments")
async def upload_diary_photo(
    request: Request, file: UploadFile = File(...), current_user: dict = Depends(get_current_user),
):
    uid = _user_id(current_user)
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty upload")

    content_type = file.content_type or "image/jpeg"
    filename = (file.filename or "photo").strip() or None

    upload_id = await create_upload(
        user_id=uid,
        kind="diary_photo",
        content_type=content_type,
        filename=filename,
        data=data,
    )
    path_value = f"uploads/{upload_id}"
    exp = int(time.time()) + _SIGNED_URL_TTL
    sig = sign_upload_id(upload_id=upload_id, exp=exp)
    base = _resolve_public_base_url(request)
    url_value = f"{base}/files/{upload_id}?exp={exp}&sig={sig}" if base else None
    return {"path": path_value, "url": url_value}


# --- Settings ---


@router.get("/settings")
async def get_settings(current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    name, description, header_color_hex = await crud_get_settings(uid)
    return {"name": name, "description": description, "header_color_hex": header_color_hex}


@router.put("/settings")
async def put_settings(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    uid = _user_id(current_user)
    name = payload.get("name", "My Diary")
    description = payload.get("description", "")
    header_color_hex = payload.get("header_color_hex", "#B8DEFF")
    await crud_upsert_settings(uid, name, description, header_color_hex)
    return {"success": True}


# --- Entries ---


@router.get("/entries")
async def list_entries(request: Request, current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    rows = await crud_list_entries(uid)
    _inject_image_urls(rows, request)
    return {"entries": rows}


@router.get("/entries/{entry_id}")
async def get_entry(entry_id: uuid.UUID, request: Request, current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    row = await crud_get_entry(uid, entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Entry not found")
    _inject_image_urls([row], request)
    return row


@router.post("/entries")
async def save_entry(payload: dict, current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    eid = payload.get("id")
    if eid is None:
        raise HTTPException(status_code=400, detail="Missing entry id")
    try:
        eid_uuid = uuid.UUID(str(eid)) if not isinstance(eid, uuid.UUID) else eid
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid entry id")

    date_str = payload.get("date")
    title = payload.get("title", "Untitled")
    body_blocks = payload.get("body_blocks", [])
    timezone_abbreviation = payload.get("timezone_abbreviation", "UTC")

    if not date_str:
        raise HTTPException(status_code=400, detail="Missing date")

    try:
        await crud_save_entry(
            user_id=uid,
            entry_id=eid_uuid,
            date_str=date_str,
            title=title,
            body_blocks=body_blocks,
            timezone_abbreviation=timezone_abbreviation,
        )
        return {"id": str(eid_uuid), "success": True}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        if _is_undefined_table(e):
            raise HTTPException(
                status_code=503,
                detail=(
                    "Diary table missing (database error 42P01). "
                    "Ensure DATABASE_URL points to your Supabase project and run migrations there: "
                    "cd Backend && PYTHONPATH=.. ./venv/bin/python -m alembic upgrade head"
                ),
            )
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/entries/{entry_id}")
async def delete_entry(entry_id: uuid.UUID, current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    try:
        await crud_delete_entry(uid, entry_id)
        return {"success": True}
    except PermissionError:
        raise HTTPException(status_code=404, detail="Entry not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
