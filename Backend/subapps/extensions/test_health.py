import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health() -> dict:
    return {"ok": True}


@router.get("/stream")
async def health_stream(request: Request) -> StreamingResponse:
    async def gen():
        # Send an initial event so clients can treat "200 + first bytes received" as connected.
        yield "event: ready\ndata: {}\n\n"
        while True:
            if await request.is_disconnected():
                break
            # SSE comment heartbeat (ignored by most parsers, but keeps the connection active).
            yield ": ping\n\n"
            await asyncio.sleep(10)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # Helpful when running behind some proxies (e.g. nginx) to avoid buffering SSE.
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)

