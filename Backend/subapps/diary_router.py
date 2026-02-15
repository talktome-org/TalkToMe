"""Diary API: settings and entries. Photos are uploaded by iOS directly to Supabase Storage."""

import uuid

from fastapi import APIRouter, Depends, HTTPException

from Backend.auth import get_current_user
from Backend.crud.diary.diary_crud import (
    delete_entry as crud_delete_entry,
    get_entry as crud_get_entry,
    get_settings as crud_get_settings,
    list_entries as crud_list_entries,
    save_entry as crud_save_entry,
    upsert_settings as crud_upsert_settings,
)

router = APIRouter(prefix="/diary", tags=["diary"])


def _user_id(current_user: dict) -> uuid.UUID:
    try:
        return uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")


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
async def list_entries(current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    rows = await crud_list_entries(uid)
    return {"entries": rows}


@router.get("/entries/{entry_id}")
async def get_entry(entry_id: uuid.UUID, current_user: dict = Depends(get_current_user)):
    uid = _user_id(current_user)
    row = await crud_get_entry(uid, entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Entry not found")
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
