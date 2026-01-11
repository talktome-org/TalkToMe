import os
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from Backend.crud.client_uploads.uploads_crud import get_upload_bytes
from Backend.services.file_signing_service import verify_upload_sig


router = APIRouter(prefix="/files", tags=["files"])


@router.get("/{upload_id}")
async def get_file(
    upload_id: uuid.UUID,
    exp: int = Query(..., description="Unix timestamp expiry"),
    sig: str = Query(..., description="HMAC signature"),
):
    if not verify_upload_sig(upload_id=upload_id, exp=exp, sig=sig):
        raise HTTPException(status_code=403, detail="Invalid or expired signature")

    row = await get_upload_bytes(upload_id=upload_id)
    if not row:
        raise HTTPException(status_code=404, detail="File not found")

    data: bytes = row["data"]
    content_type: str = row.get("content_type") or "application/octet-stream"

    headers = {}
    filename: Optional[str] = row.get("filename")
    if filename:
        headers["Content-Disposition"] = f'inline; filename="{filename}"'

    return Response(content=data, media_type=content_type, headers=headers)


