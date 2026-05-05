"""Scheduler that sends daily 10 AM check-in notifications per timezone.

Runs a job every hour on the hour.  For each run it computes which IANA
timezones currently have local hour == 10 and sends a supportive push
notification to every user whose device token is registered in one of
those timezones.
"""

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, available_timezones

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..crud.apns.device_tokens_crud import list_users_by_timezone
from .apns_service import send_daily_checkin_notification

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _timezones_at_hour(hour: int = 10) -> list[str]:
    """Return IANA timezone names whose current local hour equals *hour*."""
    now_utc = datetime.now(timezone.utc)
    result = []
    for tz_name in available_timezones():
        try:
            local_time = now_utc.astimezone(ZoneInfo(tz_name))
            if local_time.hour == hour:
                result.append(tz_name)
        except Exception:
            continue
    return result


async def _send_checkins_for_timezone(tz: str) -> None:
    user_ids = await list_users_by_timezone(tz=tz)
    if not user_ids:
        return
    logger.info("[DailyCheckin] Sending to %d user(s) in %s", len(user_ids), tz)
    for uid in user_ids:
        try:
            await send_daily_checkin_notification(user_id=uid)
        except Exception:
            logger.exception("[DailyCheckin] Failed for user %s", uid)


async def _daily_checkin_job() -> None:
    """Hourly job: find timezones at 10 AM and send check-ins."""
    matching = _timezones_at_hour(10)
    logger.info("[DailyCheckin] Timezones at 10 AM: %d", len(matching))
    for tz in matching:
        await _send_checkins_for_timezone(tz)


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(_daily_checkin_job, "cron", minute=0, id="daily_checkin")
    _scheduler.start()
    logger.info("[DailyCheckin] Scheduler started (runs every hour at :00)")


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=True)
        _scheduler = None
        logger.info("[DailyCheckin] Scheduler stopped")
