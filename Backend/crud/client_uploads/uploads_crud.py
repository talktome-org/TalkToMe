import uuid
from typing import Optional

from starlette.concurrency import run_in_threadpool

from ...database import SessionLocal
from ...models.client_uploads.upload_model import Upload


async def create_upload(
    *,
    user_id: uuid.UUID,
    kind: str,
    content_type: str,
    filename: Optional[str],
    data: bytes,
) -> uuid.UUID:
    def _insert() -> uuid.UUID:
        db = SessionLocal()
        try:
            row = Upload(
                user_id=user_id,
                kind=kind,
                content_type=content_type,
                filename=filename,
                data=data,
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return row.id
        finally:
            db.close()

    return await run_in_threadpool(_insert)


async def get_upload_meta(*, upload_id: uuid.UUID) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = db.get(Upload, upload_id)
            if not row:
                return None
            return {
                "id": row.id,
                "user_id": row.user_id,
                "kind": row.kind,
                "content_type": row.content_type,
                "filename": row.filename,
                "created_at": row.created_at,
            }
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def get_upload_bytes(*, upload_id: uuid.UUID) -> Optional[dict]:
    def _select():
        db = SessionLocal()
        try:
            row = db.get(Upload, upload_id)
            if not row:
                return None
            return {
                "content_type": row.content_type,
                "filename": row.filename,
                "data": row.data,
            }
        finally:
            db.close()

    return await run_in_threadpool(_select)


