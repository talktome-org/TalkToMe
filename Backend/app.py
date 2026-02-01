from fastapi import FastAPI

from .subapps.apple_router import router as aasa_router
from .subapps.apns_router import router as notifications_router
from .subapps.chat_router import router as chat_router
from .subapps.speech_router import router as speech_router
from .subapps.client_upload_router import router as files_router
from .subapps.friends_router import router as friends_router
from .subapps.partner_router import router as partner_router
from .subapps.user_profile_router import router as profile_router

app = FastAPI()

app.include_router(aasa_router)
app.include_router(files_router)
app.include_router(friends_router)
app.include_router(partner_router)
app.include_router(profile_router)
app.include_router(notifications_router)
app.include_router(chat_router)
app.include_router(speech_router)