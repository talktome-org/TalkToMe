import os

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/.well-known/apple-app-site-association", include_in_schema=False)
async def apple_app_site_association():
    try:
        team_id = os.getenv("APPLE_TEAM_ID")
        bundle_id = os.getenv("BUNDLE_ID")
        if not team_id or not bundle_id:
            raise RuntimeError("APPLE_TEAM_ID and BUNDLE_ID must be set.")
        app_id = f"{team_id}.{bundle_id}"

        paths_env = os.getenv("AASA_PATHS", "/link*,/link")
        raw_paths = paths_env.split(",")
        trimmed_paths = [path.strip() for path in raw_paths]
        paths = [path for path in trimmed_paths if path]
        payload = {"applinks": {"apps": [], "details": [{"appID": app_id, "paths": paths}]}}
        return JSONResponse(content=payload, media_type="application/json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AASA not available: {str(e)}")


