"""CRUD for diary_settings and diary_entries."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from starlette.concurrency import run_in_threadpool
from sqlalchemy import select

from ...database import SessionLocal
from ...models.diary.diary_models import DiaryEntry, DiarySettings


def _row_to_dict(obj) -> dict:
    d = {}
    for c in obj.__table__.columns:
        v = getattr(obj, c.name)
        if v is None:
            d[c.name] = None
        elif isinstance(v, uuid.UUID):
            d[c.name] = str(v)
        elif isinstance(v, (date, datetime)):
            d[c.name] = v.isoformat()
        else:
            d[c.name] = v
    return d


async def get_settings(user_id: uuid.UUID) -> tuple[str, str, str]:
    """Return (name, description, header_color_hex)."""

    def _get():
        db = SessionLocal()
        try:
            row = db.get(DiarySettings, user_id)
            if row is None:
                return ("My Diary", "", "#B8DEFF")
            return (row.name, row.description, row.header_color_hex)
        finally:
            db.close()

    return await run_in_threadpool(_get)


async def upsert_settings(
    user_id: uuid.UUID,
    name: str,
    description: str,
    header_color_hex: str,
) -> None:
    def _upsert():
        db = SessionLocal()
        try:
            row = db.get(DiarySettings, user_id)
            if row is None:
                row = DiarySettings(user_id=user_id, name=name, description=description, header_color_hex=header_color_hex)
                db.add(row)
            else:
                row.name = name
                row.description = description
                row.header_color_hex = header_color_hex
                row.updated_at = datetime.now(timezone.utc)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    await run_in_threadpool(_upsert)


async def list_entries(user_id: uuid.UUID) -> list[dict[str, Any]]:
    def _list():
        db = SessionLocal()
        try:
            result = db.execute(
                select(DiaryEntry)
                .where(DiaryEntry.user_id == user_id)
                .order_by(DiaryEntry.date.desc(), DiaryEntry.created_at.desc())
            )
            rows = result.scalars().all()
            return [_row_to_dict(r) for r in rows]
        finally:
            db.close()

    return await run_in_threadpool(_list)


async def get_entry(user_id: uuid.UUID, entry_id: uuid.UUID) -> Optional[dict[str, Any]]:
    def _get():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(DiaryEntry)
                    .where(DiaryEntry.id == entry_id, DiaryEntry.user_id == user_id)
                    .limit(1)
                )
            ).scalars().first()
            return _row_to_dict(row) if row else None
        finally:
            db.close()

    return await run_in_threadpool(_get)


async def save_entry(
    user_id: uuid.UUID,
    entry_id: uuid.UUID,
    date_str: str,
    title: str,
    body_blocks: list[dict[str, str]],
    timezone_abbreviation: str,
) -> uuid.UUID:
    def _save():
        db = SessionLocal()
        try:
            row = db.get(DiaryEntry, entry_id)
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if row is None:
                row = DiaryEntry(
                    id=entry_id,
                    user_id=user_id,
                    date=parsed_date,
                    title=title,
                    body_blocks=body_blocks,
                    timezone_abbreviation=timezone_abbreviation,
                )
                db.add(row)
            else:
                if row.user_id != user_id:
                    raise PermissionError("Entry belongs to another user")
                row.date = parsed_date
                row.title = title
                row.body_blocks = body_blocks
                row.timezone_abbreviation = timezone_abbreviation
            db.commit()
            return entry_id
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    return await run_in_threadpool(_save)


async def delete_entry(user_id: uuid.UUID, entry_id: uuid.UUID) -> None:
    def _delete():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(DiaryEntry)
                    .where(DiaryEntry.id == entry_id, DiaryEntry.user_id == user_id)
                    .limit(1)
                )
            ).scalars().first()
            if row is None:
                raise PermissionError("Entry not found or not owned by user")
            db.delete(row)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    await run_in_threadpool(_delete)
