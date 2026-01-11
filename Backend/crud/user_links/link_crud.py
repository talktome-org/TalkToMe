import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from starlette.concurrency import run_in_threadpool

from sqlalchemy import delete, select
from sqlalchemy import text

from ...database import SessionLocal
from ...models.links.link_invites_model import LinkInvite
from ...models.links.paired_accounts_model import PairedAccount

RELATIONSHIP_LINKS_TABLE = "link_invites"
RELATIONSHIPS_TABLE = "paired_accounts"


def _utc_in_hours_iso(hours: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def is_user_linked(*, user_id: uuid.UUID) -> bool:
    user_id_str = str(user_id)

    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(PairedAccount.id).where(
                        (PairedAccount.partner_a_user_id == user_id) | (PairedAccount.partner_b_user_id == user_id)
                    ).limit(1)
                )
                .first()
            )
            return row is not None
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def _create_link_invite(*, inviter_user_id: uuid.UUID, expires_in_hours: int = 24) -> dict:
    if await is_user_linked(user_id=inviter_user_id):
        raise PermissionError("You are already linked to a partner. Please unlink first.")
    invite_token = uuid.uuid4().hex

    expires_at = (datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)).replace(tzinfo=None)

    def _insert():
        db = SessionLocal()
        try:
            inv = LinkInvite(
                invite_user_id=inviter_user_id,
                invite_token=invite_token,
                expires_at=expires_at,
                used_at=None,
                invitee_user_id=None,
                paired_account_id=None,
            )
            db.add(inv)
            db.commit()
            db.refresh(inv)
            return {
                "id": str(inv.id),
                "invite_user_id": str(inv.invite_user_id),
                "invite_token": inv.invite_token,
                "expires_at": inv.expires_at.isoformat() if inv.expires_at else None,
                "used_at": inv.used_at.isoformat() if inv.used_at else None,
                "invitee_user_id": str(inv.invitee_user_id) if inv.invitee_user_id else None,
                "paired_account_id": str(inv.paired_account_id) if inv.paired_account_id else None,
            }
        finally:
            db.close()

    return await run_in_threadpool(_insert)


async def get_unexpired_invite_for_user(*, inviter_user_id: uuid.UUID) -> dict | None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def _select():
        db = SessionLocal()
        try:
            row = (
                db.execute(
                    select(LinkInvite)
                    .where(
                        LinkInvite.invite_user_id == inviter_user_id,
                        LinkInvite.used_at.is_(None),
                        LinkInvite.expires_at > now,
                    )
                    .order_by(LinkInvite.expires_at.asc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if not row:
                return None
            return {
                "id": str(row.id),
                "invite_user_id": str(row.invite_user_id),
                "invite_token": row.invite_token,
                "expires_at": row.expires_at.isoformat() if row.expires_at else None,
                "used_at": row.used_at.isoformat() if row.used_at else None,
                "invitee_user_id": str(row.invitee_user_id) if row.invitee_user_id else None,
                "paired_account_id": str(row.paired_account_id) if row.paired_account_id else None,
            }
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def get_or_create_link_invite(*, inviter_user_id: uuid.UUID, expires_in_hours: int = 24) -> dict:
    if await is_user_linked(user_id=inviter_user_id):
        raise PermissionError("You are already linked to a partner. Please unlink first.")
    existing = await get_unexpired_invite_for_user(inviter_user_id=inviter_user_id)
    if existing:
        return existing
    return await _create_link_invite(inviter_user_id=inviter_user_id, expires_in_hours=expires_in_hours)


async def accept_link_invite(*, invite_token: str, invitee_user_id: uuid.UUID) -> uuid.UUID:
    def _call():
        db = SessionLocal()
        try:
            row = db.execute(
                text("select accept_link_invite_tx(:invite_token, :invitee_user_id) as relationship_id"),
                {"invite_token": invite_token, "invitee_user_id": str(invitee_user_id)},
            ).first()
            if not row:
                raise RuntimeError("accept_link_invite_tx returned no data")
            rel_id = row[0]
            return uuid.UUID(str(rel_id))
        finally:
            db.close()

    try:
        return await run_in_threadpool(_call)
    except Exception as e:
        msg = str(e)
        if any(
            key in msg
            for key in [
                "Invalid invite token",
                "Invite token already used",
                "Invite token expired",
                "Cannot link to the same user",
                "Inviter already linked to another partner",
                "Invitee already linked to another partner",
            ]
        ):
            raise PermissionError(msg)
        raise


async def unlink_relationship_for_user(*, user_id: uuid.UUID) -> bool:
    def _delete():
        db = SessionLocal()
        try:
            rel = (
                db.execute(
                    select(PairedAccount).where(
                        (PairedAccount.partner_a_user_id == user_id) | (PairedAccount.partner_b_user_id == user_id)
                    ).limit(1)
                )
                .scalars()
                .first()
            )
            if not rel:
                return False
            db.execute(delete(PairedAccount).where(PairedAccount.id == rel.id))
            db.commit()
            return True
        finally:
            db.close()

    return await run_in_threadpool(_delete)


async def get_link_status_for_user(*, user_id: uuid.UUID) -> tuple[bool, Optional[uuid.UUID], Optional[str]]:
    def _select():
        db = SessionLocal()
        try:
            rel = (
                db.execute(
                    select(PairedAccount).where(
                        (PairedAccount.partner_a_user_id == user_id) | (PairedAccount.partner_b_user_id == user_id)
                    ).limit(1)
                )
                .scalars()
                .first()
            )
            if not rel:
                return False, None, None
            linked_at_iso = rel.created_at.isoformat() if rel.created_at else None
            return True, rel.id, linked_at_iso
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def get_partner_user_id(*, user_id: uuid.UUID) -> Optional[uuid.UUID]:
    def _select():
        db = SessionLocal()
        try:
            rel = (
                db.execute(
                    select(PairedAccount.partner_a_user_id, PairedAccount.partner_b_user_id).where(
                        (PairedAccount.partner_a_user_id == user_id) | (PairedAccount.partner_b_user_id == user_id)
                    ).limit(1)
                )
                .first()
            )
            if not rel:
                return None
            a_id, b_id = rel[0], rel[1]
            if a_id == user_id:
                return b_id
            if b_id == user_id:
                return a_id
            return None
        finally:
            db.close()

    return await run_in_threadpool(_select)


