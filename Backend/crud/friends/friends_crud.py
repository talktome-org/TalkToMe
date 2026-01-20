from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from starlette.concurrency import run_in_threadpool

from Backend.database import SessionLocal
from Backend.models.friends.friend_code_model import FriendCode
from Backend.models.friends.friendship_model import Friendship


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _format_4_digit(n: int) -> str:
    return f"{int(n) % 10000:04d}"


async def get_or_create_my_code(*, user_id: uuid.UUID, expires_in_minutes: int = 10) -> dict:
    now = _utc_now_naive()

    def _select_existing():
        db = SessionLocal()
        try:
            row = db.get(FriendCode, user_id)
            if not row:
                return None
            if row.expires_at and row.expires_at > now and row.code:
                return {"user_id": str(row.user_id), "code": row.code, "expires_at": row.expires_at}
            return None
        finally:
            db.close()

    existing = await run_in_threadpool(_select_existing)
    if existing:
        return existing

    expires_at = now + timedelta(minutes=int(expires_in_minutes))

    def _upsert_new():
        db = SessionLocal()
        try:
            # Try a few times to avoid collisions with other active codes.
            for _ in range(25):
                code = _format_4_digit(random.randint(0, 9999))
                row = FriendCode(user_id=user_id, code=code, expires_at=expires_at, updated_at=now)
                try:
                    db.merge(row)
                    db.commit()
                    db.refresh(row)
                    return {"user_id": str(row.user_id), "code": row.code, "expires_at": row.expires_at}
                except IntegrityError:
                    db.rollback()
                    continue
            raise RuntimeError("Failed to generate a unique friend code. Try again.")
        finally:
            db.close()

    return await run_in_threadpool(_upsert_new)


async def add_friend_by_code(*, user_id: uuid.UUID, code: str) -> uuid.UUID:
    cleaned = (code or "").strip()
    if len(cleaned) != 4 or not cleaned.isdigit():
        raise PermissionError("Code must be exactly 4 digits")

    now = _utc_now_naive()

    def _resolve_owner():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(FriendCode.user_id, FriendCode.expires_at).where(FriendCode.code == cleaned).limit(1)
                )
                .first()
            )
            if not row:
                return None
            owner_id, expires_at = row[0], row[1]
            if expires_at is None or expires_at <= now:
                return None
            return uuid.UUID(str(owner_id))
        finally:
            db.close()

    owner_id = await run_in_threadpool(_resolve_owner)
    if not owner_id:
        raise PermissionError("Invalid or expired code")
    if owner_id == user_id:
        raise PermissionError("You cannot add yourself")

    low = min(user_id, owner_id)
    high = max(user_id, owner_id)

    def _insert_friendship():
        db = SessionLocal()
        try:
            # Best-effort: if exists, treat as success.
            existing = (
                db.execute(
                    select(Friendship.id).where(
                        Friendship.user_low_id == low,
                        Friendship.user_high_id == high,
                    )
                    .limit(1)
                )
                .first()
            )
            if existing:
                return
            fr = Friendship(user_low_id=low, user_high_id=high, status="accepted")
            db.add(fr)
            db.commit()
        finally:
            db.close()

    await run_in_threadpool(_insert_friendship)
    return owner_id


async def list_friends_for_user(*, user_id: uuid.UUID) -> list[uuid.UUID]:
    def _select():
        db = SessionLocal()
        try:
            rows = (
                db.execute(
                    select(Friendship.user_low_id, Friendship.user_high_id).where(
                        (Friendship.user_low_id == user_id) | (Friendship.user_high_id == user_id)
                    )
                )
                .all()
            )
            out: list[uuid.UUID] = []
            for a, b in rows or []:
                other = b if uuid.UUID(str(a)) == user_id else a
                out.append(uuid.UUID(str(other)))
            return out
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def get_friendship_id_for_pair(*, user_id: uuid.UUID, friend_user_id: uuid.UUID) -> uuid.UUID | None:
    low = min(user_id, friend_user_id)
    high = max(user_id, friend_user_id)

    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(Friendship.id).where(
                        Friendship.user_low_id == low,
                        Friendship.user_high_id == high,
                    ).limit(1)
                )
                .first()
            )
            if not row:
                return None
            return uuid.UUID(str(row[0]))
        finally:
            db.close()

    return await run_in_threadpool(_select)

