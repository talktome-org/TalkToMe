from fastapi import FastAPI
import asyncio
from .APNS.notifications_router import send_daily_checkins_for_now

from .Apple.aasa_router import router as aasa_router
from .Routers.link_router import router as link_router
from .Routers.partner_router import router as partner_router
from .Routers.profile_router import router as profile_router
from .APNS.notifications_router import router as notifications_router
from .Routers.chat_router import router as chat_router

app = FastAPI()

app.include_router(aasa_router)
app.include_router(link_router)
app.include_router(partner_router)
app.include_router(profile_router)
app.include_router(notifications_router)
app.include_router(chat_router)


# In-app scheduler to run daily check-ins automatically
@app.on_event("startup")
async def _start_daily_checkins_scheduler():
    async def _runner():
        # Run every minute, aligned to minute boundaries
        while True:
            try:
                await send_daily_checkins_for_now()
            except Exception:
                pass
            # Sleep until next minute boundary
            try:
                loop = asyncio.get_event_loop()
                now = loop.time()
                # 60-second ticks based on monotonic time
                sleep_for = 60 - (now % 60)
                await asyncio.sleep(sleep_for)
            except Exception:
                await asyncio.sleep(60)

    asyncio.create_task(_runner())