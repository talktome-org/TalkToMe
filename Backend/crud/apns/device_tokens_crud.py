import uuid
from datetime import datetime, timezone
from typing import List, Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, update

from Backend.database import SessionLocal
from Backend.models.apns.device_token_model import DeviceToken

TABLE = "device_tokens"


async def upsert_token(*, user_id: uuid.UUID, token: str, platform: str, bundle_id: Optional[str]) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def _upsert():
        db = SessionLocal()
        try:
            stmt = (
                insert(DeviceToken)
                .values(
                    user_id=user_id,
                    token=token,
                    platform=platform,
                    bundle_id=bundle_id,
                    enabled=True,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[DeviceToken.user_id, DeviceToken.token],
                    set_={
                        "platform": platform,
                        "bundle_id": bundle_id,
                        "enabled": True,
                        "updated_at": now,
                    },
                )
            )
            db.execute(stmt)
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_upsert)


async def disable_token_by_value(*, token: str) -> None:
    def _update():
        db = SessionLocal()
        try:
            db.execute(
                update(DeviceToken)
                .where(DeviceToken.token == token)
                .values(enabled=False, updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
            )
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_update)


async def list_tokens_for_user(*, user_id: uuid.UUID) -> List[dict]:
    def _select():
        db = SessionLocal()
        try:
            rows = (
                db.execute(
                    select(DeviceToken.token, DeviceToken.enabled)
                    .where(DeviceToken.user_id == user_id, DeviceToken.enabled.is_(True))
                )
                .all()
            )
            return [{"token": r[0], "enabled": r[1]} for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_select)


