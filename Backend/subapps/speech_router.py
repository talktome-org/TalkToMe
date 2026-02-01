import asyncio
import base64
import inspect
import json
import os
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, WebSocket
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

from Backend.auth import get_current_user
from Backend.auth import SupabaseAuth


router = APIRouter(prefix="/speech", tags=["speech"])


ELEVEN_BASE = "https://api.elevenlabs.io"
ELEVEN_WS_BASE = "wss://api.elevenlabs.io"

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None


def _ws_connect_with_headers(
    uri: str,
    headers: dict[str, str],
    max_size: int,
    *,
    open_timeout: float = 25.0,
):
    """
    Compat layer for `websockets.connect` header kwargs across versions.
    (Kept here to be reused across multiple WS proxies.)
    """
    connect = websockets.connect  # type: ignore[attr-defined]
    header_items = list(headers.items())

    param_names: set[str] = set()
    try:
        sig = inspect.signature(connect)
        param_names = set(sig.parameters.keys())
    except Exception:
        param_names = set()

    header_kw_candidates: list[str] = []
    for k in ("additional_headers", "extra_headers", "headers"):
        if not param_names or k in param_names:
            header_kw_candidates.append(k)
    if not header_kw_candidates:
        header_kw_candidates = ["additional_headers", "extra_headers", "headers"]

    last_type_error: Optional[TypeError] = None
    for header_kw in header_kw_candidates:
        for include_max_size in (True, False):
            kwargs: dict[str, Any] = {header_kw: header_items}
            if include_max_size:
                kwargs["max_size"] = max_size
            if (not param_names) or ("open_timeout" in param_names):
                kwargs["open_timeout"] = open_timeout
            try:
                return connect(uri, **kwargs)
            except TypeError as e:
                last_type_error = e
                if "unexpected keyword argument" in str(e):
                    continue
                raise

    if last_type_error:
        raise last_type_error
    return connect(uri)


def _extract_bearer_token(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parts = value.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() != "bearer":
        return None
    token = (parts[1] or "").strip()
    return token or None


class ElevenVoice(BaseModel):
    voice_id: str
    name: str
    preview_url: Optional[str] = None
    category: Optional[str] = None
    labels: dict[str, Any] = {}


class ListVoicesResponse(BaseModel):
    voices: list[ElevenVoice]


@router.get("/voices", response_model=ListVoicesResponse)
async def list_elevenlabs_voices(current_user: dict = Depends(get_current_user)):
    """
    Lists ElevenLabs voices for the authenticated user.
    Uses server-side ELEVENLABS_API_KEY so the key is never exposed to the client.
    """
    _ = current_user  # auth already validated
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing ELEVENLABS_API_KEY")

    voices: list[ElevenVoice] = []
    next_page_token: Optional[str] = None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            for _page in range(1, 6):  # cap to avoid runaway
                params: dict[str, Any] = {
                    "page_size": 100,
                    "include_total_count": False,
                }
                if next_page_token:
                    params["next_page_token"] = next_page_token

                r = await client.get(
                    f"{ELEVEN_BASE}/v2/voices",
                    params=params,
                    headers={"xi-api-key": api_key},
                )
                if r.status_code >= 400:
                    raise HTTPException(status_code=502, detail=f"ElevenLabs voices error: HTTP {r.status_code}")
                payload = r.json()
                items = payload.get("voices") if isinstance(payload, dict) else None
                if not isinstance(items, list):
                    break
                for v in items:
                    if not isinstance(v, dict):
                        continue
                    vid = v.get("voice_id")
                    name = v.get("name")
                    if not isinstance(vid, str) or not isinstance(name, str):
                        continue
                    voices.append(
                        ElevenVoice(
                            voice_id=vid,
                            name=name,
                            preview_url=v.get("preview_url") if isinstance(v.get("preview_url"), str) else None,
                            category=v.get("category") if isinstance(v.get("category"), str) else None,
                            labels=v.get("labels") if isinstance(v.get("labels"), dict) else {},
                        )
                    )

                has_more = bool(payload.get("has_more")) if isinstance(payload, dict) else False
                next_page_token = payload.get("next_page_token") if isinstance(payload, dict) else None
                if not has_more or not isinstance(next_page_token, str) or not next_page_token:
                    break
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Deterministic order for UI.
    voices.sort(key=lambda x: x.name.lower())
    return ListVoicesResponse(voices=voices)


class TTSRequest(BaseModel):
    text: str
    voice_id: str
    model_id: str = "eleven_multilingual_v2"
    output_format: str = "mp3_44100_128"
    # Optional overrides:
    voice_settings: Optional[dict[str, Any]] = None


@router.post("/tts")
async def elevenlabs_tts(req: TTSRequest, current_user: dict = Depends(get_current_user)):
    """
    Streams ElevenLabs TTS audio back to the client.

    Default output_format is MP3; iOS can play it with AVAudioPlayer.
    """
    _ = current_user
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing ELEVENLABS_API_KEY")

    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    voice_id = (req.voice_id or "").strip()
    if not voice_id:
        raise HTTPException(status_code=400, detail="voice_id is required")

    out_fmt = (req.output_format or "mp3_44100_128").strip()
    media_type = "audio/mpeg" if out_fmt.startswith("mp3") else "application/octet-stream"

    url = f"{ELEVEN_BASE}/v1/text-to-speech/{voice_id}/stream"
    params = {"output_format": out_fmt}
    body: dict[str, Any] = {
        "text": text,
        "model_id": req.model_id,
    }
    if req.voice_settings:
        body["voice_settings"] = req.voice_settings

    async def iter_bytes() -> AsyncIterator[bytes]:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                url,
                params=params,
                headers={
                    "xi-api-key": api_key,
                    "Content-Type": "application/json",
                },
                json=body,
            ) as r:
                if r.status_code >= 400:
                    # Consume small error body for logs/debugging.
                    try:
                        detail = (await r.aread())[:4000].decode("utf-8", errors="ignore")
                    except Exception:
                        detail = f"HTTP {r.status_code}"
                    raise HTTPException(status_code=502, detail=f"ElevenLabs TTS error: {detail}")
                async for chunk in r.aiter_bytes():
                    if chunk:
                        yield chunk

    return StreamingResponse(iter_bytes(), media_type=media_type)


@router.websocket("/tts/stream")
async def elevenlabs_tts_stream(websocket: WebSocket):
    """
    WebSocket proxy to ElevenLabs TTS stream-input.

    Client sends JSON events:
    - {"type":"text","text":"..."}  (repeatable)
    - {"type":"end"}               (finalize)
    - {"type":"cancel"}            (abort)

    Server forwards ElevenLabs JSON messages (audio base64 chunks, isFinal, etc) as-is.
    Use `output_format=pcm_24000` for easy realtime playback on iOS.
    """
    if websockets is None:
        await websocket.close(code=1011, reason="Server missing websockets dependency")
        return

    token = _extract_bearer_token(websocket.headers.get("authorization"))
    if not token:
        await websocket.close(code=1008, reason="Missing Authorization bearer token")
        return
    try:
        auth = SupabaseAuth()
        _ = auth.verify_jwt(token)
    except Exception:
        await websocket.close(code=1008, reason="Invalid or expired token")
        return

    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        await websocket.close(code=1011, reason="Server missing ELEVENLABS_API_KEY")
        return

    voice_id = (websocket.query_params.get("voice_id") or "").strip()
    if not voice_id:
        await websocket.close(code=1008, reason="Missing voice_id")
        return

    model_id = (websocket.query_params.get("model_id") or "eleven_multilingual_v2").strip() or "eleven_multilingual_v2"
    output_format = (websocket.query_params.get("output_format") or "pcm_24000").strip() or "pcm_24000"

    await websocket.accept()

    eleven_url = (
        f"{ELEVEN_WS_BASE}/v1/text-to-speech/{voice_id}/stream-input"
        f"?model_id={model_id}"
        f"&output_format={output_format}"
        f"&auto_mode=true"
        f"&inactivity_timeout=180"
    )

    # Conservative defaults; can be tuned later or made client-configurable.
    init_payload: dict[str, Any] = {
        "text": " ",
        "voice_settings": {
            "stability": 0.45,
            "similarity_boost": 0.85,
            "style": 0.0,
            "use_speaker_boost": True,
            "speed": 1.0,
        },
    }

    try:
        async with _ws_connect_with_headers(
            eleven_url,
            headers={"xi-api-key": api_key},
            max_size=16 * 1024 * 1024,
        ) as eleven_ws:
            await eleven_ws.send(json.dumps(init_payload))

            async def client_to_eleven():
                while True:
                    raw = await websocket.receive_text()
                    try:
                        evt = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(evt, dict):
                        continue
                    t = evt.get("type")
                    if t == "text":
                        txt = evt.get("text")
                        if not isinstance(txt, str) or not txt:
                            continue
                        await eleven_ws.send(json.dumps({"text": txt, "try_trigger_generation": True}))
                    elif t == "end":
                        await eleven_ws.send(json.dumps({"text": ""}))
                    elif t == "cancel":
                        break

            async def eleven_to_client():
                async for raw in eleven_ws:
                    if isinstance(raw, str):
                        await websocket.send_text(raw)
                    elif isinstance(raw, (bytes, bytearray)):
                        try:
                            await websocket.send_text(bytes(raw).decode("utf-8"))
                        except Exception:
                            continue

            forward_task = asyncio.create_task(client_to_eleven())
            back_task = asyncio.create_task(eleven_to_client())
            done, pending = await asyncio.wait({forward_task, back_task}, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    raise exc
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await websocket.close(code=1011, reason=f"TTS stream error: {str(e)[:120]}")
        except Exception:
            pass


@router.websocket("/stt/stream")
async def deepgram_stt_stream(websocket: WebSocket):
    """
    WebSocket proxy to Deepgram live streaming STT.

    Client sends:
    - binary frames: raw PCM16 mono (recommended)
    - OR JSON text: {"type":"audio","audio":"<base64>"} (fallback)
    - OR {"type":"finalize"} to request finalization (best-effort)

    Server forwards Deepgram JSON transcript messages as-is to the client.
    """
    if websockets is None:
        await websocket.close(code=1011, reason="Server missing websockets dependency")
        return

    token = _extract_bearer_token(websocket.headers.get("authorization"))
    if not token:
        await websocket.close(code=1008, reason="Missing Authorization bearer token")
        return
    try:
        auth = SupabaseAuth()
        _ = auth.verify_jwt(token)
    except Exception:
        await websocket.close(code=1008, reason="Invalid or expired token")
        return

    api_key = os.getenv("DEEPGRAM_API_KEY")
    if not api_key:
        await websocket.close(code=1011, reason="Server missing DEEPGRAM_API_KEY")
        return

    # Safe tuning via query params (dictation defaults).
    model = (websocket.query_params.get("model") or "nova-3").strip() or "nova-3"
    language = (websocket.query_params.get("language") or "en-US").strip() or "en-US"
    endpointing = (websocket.query_params.get("endpointing") or "400").strip() or "400"
    sample_rate = (websocket.query_params.get("sample_rate") or "24000").strip() or "24000"

    # Deepgram accepts raw audio params via query string.
    deepgram_url = (
        "wss://api.deepgram.com/v1/listen"
        f"?model={model}"
        f"&language={language}"
        "&interim_results=true"
        "&smart_format=true"
        "&punctuate=true"
        f"&endpointing={endpointing}"
        "&encoding=linear16"
        "&channels=1"
        f"&sample_rate={sample_rate}"
    )

    await websocket.accept()
    # Confirm readiness to the client immediately (Deepgram may not send any message until audio arrives).
    try:
        await websocket.send_text(json.dumps({"type": "talktome.stt.ready"}))
    except Exception:
        await websocket.close(code=1011, reason="Failed to initialize STT stream")
        return

    try:
        async with _ws_connect_with_headers(
            deepgram_url,
            headers={"Authorization": f"Token {api_key}"},
            max_size=16 * 1024 * 1024,
        ) as dg_ws:

            async def client_to_deepgram():
                while True:
                    msg = await websocket.receive()
                    if msg.get("type") == "websocket.disconnect":
                        break

                    raw_bytes = msg.get("bytes")
                    if isinstance(raw_bytes, (bytes, bytearray)) and raw_bytes:
                        await dg_ws.send(bytes(raw_bytes))
                        continue

                    raw_text = msg.get("text")
                    if not isinstance(raw_text, str) or not raw_text:
                        continue
                    try:
                        evt = json.loads(raw_text)
                    except Exception:
                        continue
                    if not isinstance(evt, dict):
                        continue
                    t = evt.get("type")
                    if t == "audio":
                        b64 = evt.get("audio")
                        if not isinstance(b64, str) or not b64:
                            continue
                        try:
                            data = base64.b64decode(b64, validate=False)
                        except Exception:
                            continue
                        if data:
                            await dg_ws.send(data)
                    elif t == "finalize":
                        # Deepgram supports a control message to finalize buffered audio.
                        # Best-effort: ignore if unsupported.
                        try:
                            await dg_ws.send(json.dumps({"type": "Finalize"}))
                        except Exception:
                            pass
                    elif t == "close":
                        break

            async def deepgram_to_client():
                async for raw in dg_ws:
                    if isinstance(raw, str):
                        await websocket.send_text(raw)
                    elif isinstance(raw, (bytes, bytearray)):
                        try:
                            await websocket.send_text(bytes(raw).decode("utf-8"))
                        except Exception:
                            continue

            forward_task = asyncio.create_task(client_to_deepgram())
            back_task = asyncio.create_task(deepgram_to_client())
            done, pending = await asyncio.wait({forward_task, back_task}, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    raise exc
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await websocket.close(code=1011, reason=f"STT stream error: {str(e)[:120]}")
        except Exception:
            pass

