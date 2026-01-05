from fastapi import FastAPI

from .subapps.apple.router import router as aasa_router
from .subapps.chat.router import router as chat_router
from .subapps.link.router import router as link_router
from .subapps.notifications.router import router as notifications_router
from .subapps.partner.router import router as partner_router
from .subapps.profile.router import router as profile_router

app = FastAPI()

app.include_router(aasa_router)
app.include_router(link_router)
app.include_router(partner_router)
app.include_router(profile_router)
app.include_router(notifications_router)
app.include_router(chat_router)