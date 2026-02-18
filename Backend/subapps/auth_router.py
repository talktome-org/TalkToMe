import re

from fastapi import APIRouter, Body, HTTPException
from sqlalchemy import text

from Backend.database import SessionLocal

router = APIRouter(prefix="/auth", tags=["auth"])

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@router.post("/email-exists")
async def email_exists(payload: dict = Body(...)):
    raw_email = str((payload or {}).get("email", "")).strip().lower()
    if not raw_email or _EMAIL_RE.match(raw_email) is None:
        raise HTTPException(status_code=400, detail="Invalid email")

    db = SessionLocal()
    try:
        row = db.execute(
            text("select 1 from auth.users where lower(email) = :email limit 1"),
            {"email": raw_email},
        ).first()
        return {"exists": row is not None}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Unable to check email right now")
    finally:
        db.close()
