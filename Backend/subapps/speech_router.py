"""
Speech endpoints.

Voice mode (the speak feature) now flows through `Backend.voice_agent.worker`
over LiveKit. This module retains:
  - GET  /speech/app-voices       — voice picker UI
  - GET  /speech/voices           — voice picker UI (full ElevenLabs list)
  - WS   /speech/stt/stream       — Deepgram proxy for in-chat dictation
                                    (push-to-talk on iOS; not voice mode)
"""
import asyncio
import base64
import inspect
import json
import logging
import os
import time as _time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, WebSocket
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

from Backend.auth import SupabaseAuth, get_current_user

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None


router = APIRouter(prefix="/speech", tags=["speech"])

ELEVEN_BASE = "https://api.elevenlabs.io"


def _ws_connect_with_headers(
    uri: str,
    headers: dict[str, str],
    max_size: int,
    *,
    open_timeout: float = 25.0,
):
    """
    Compat layer for `websockets.connect` header kwargs across versions.
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
    if len(parts) != 2 or parts[0].lower() != "bearer":
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


class AppVoice(BaseModel):
    voice_id: str
    name: str
    description: str


class AppVoicesResponse(BaseModel):
    voices: list[AppVoice]
    default_voice_id: str


# Voice metadata (descriptions are not sensitive, keep in code)
_VOICE_METADATA: dict[str, dict[str, str]] = {
    "Mira": {"description": "Cheerful, affectionate with light, bubbly warmth. Soft breathiness, smiling delivery."},
    "Pax": {"description": "Clear, articulate warmth with a patient teacher vibe. Steady cadence, gentle humor."},
    "Luma": {"description": "Warm, compassionate. Soft, airy timbre with gentle breathiness and a reassuring smile."},
    "Snow": {"description": "Regal, poised, and grand. Elegant diction with a calm, stately presence."},
    "Jay": {"description": "Bold, quick, and driven. Confident energy with sharp, upbeat cadence."},
    "Hex": {"description": "Bright, playful, magical energy with a light airy tone. Expressive inflection."},
}


def _load_app_voices() -> tuple[list[AppVoice], str]:
    voices: list[AppVoice] = []
    voice_names = ["Mira", "Pax", "Luma", "Snow", "Jay", "Hex"]
    for name in voice_names:
        voice_id = os.getenv(f"VOICE_ID_{name.upper()}", "").strip()
        if voice_id:
            meta = _VOICE_METADATA.get(name, {})
            voices.append(AppVoice(
                voice_id=voice_id,
                name=name,
                description=meta.get("description", ""),
            ))

    default_voice_id = os.getenv("VOICE_ID_DEFAULT", "").strip()
    if not default_voice_id and voices:
        default_voice_id = voices[0].voice_id

    return voices, default_voice_id


@router.get("/app-voices", response_model=AppVoicesResponse)
async def get_app_voices(current_user: dict = Depends(get_current_user)):
    _ = current_user
    voices, default_voice_id = _load_app_voices()
    if not voices:
        raise HTTPException(status_code=500, detail="No voices configured")
    return AppVoicesResponse(voices=voices, default_voice_id=default_voice_id)


@router.get("/voices", response_model=ListVoicesResponse)
async def list_elevenlabs_voices(current_user: dict = Depends(get_current_user)):
    """
    Server-side proxy of ElevenLabs voice list — keeps the API key off the client.
    """
    _ = current_user
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing ELEVENLABS_API_KEY")

    voices: list[ElevenVoice] = []
    next_page_token: Optional[str] = None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            for _page in range(1, 6):
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

    voices.sort(key=lambda x: x.name.lower())
    return ListVoicesResponse(voices=voices)


@router.websocket("/stt/stream")
async def deepgram_stt_stream(websocket: WebSocket):
    """
    WebSocket proxy to Deepgram live STT. Used by in-chat dictation
    (push-to-talk to fill the text input). Voice mode does NOT use this —
    speak mode flows through LiveKit + the voice agent worker.
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

    model = (websocket.query_params.get("model") or "nova-3").strip() or "nova-3"
    language = (websocket.query_params.get("language") or "multi").strip() or "multi"
    endpointing = (websocket.query_params.get("endpointing") or "400").strip() or "400"
    sample_rate = (websocket.query_params.get("sample_rate") or "24000").strip() or "24000"

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

    stt_logger = logging.getLogger("speech.stt")
    _stt_start = _time.time()
    stt_logger.info(f"[STT] WebSocket accepted — model={model} lang={language} endpointing={endpointing} sample_rate={sample_rate}")

    await websocket.accept()
    try:
        await websocket.send_text(json.dumps({"type": "bobo.stt.ready"}))
    except Exception:
        stt_logger.error("[STT] Failed to send ready message — closing")
        await websocket.close(code=1011, reason="Failed to initialize STT stream")
        return

    try:
        async with _ws_connect_with_headers(
            deepgram_url,
            headers={"Authorization": f"Token {api_key}"},
            max_size=16 * 1024 * 1024,
        ) as dg_ws:
            stt_logger.info(f"[STT] Deepgram upstream connected — elapsed={_time.time() - _stt_start:.2f}s")

            _audio_frames = 0
            _transcript_count = 0

            async def client_to_deepgram():
                nonlocal _audio_frames
                while True:
                    msg = await websocket.receive()
                    if msg.get("type") == "websocket.disconnect":
                        stt_logger.info(f"[STT] Client disconnected — audio_frames={_audio_frames} transcripts={_transcript_count} elapsed={_time.time() - _stt_start:.1f}s")
                        break

                    raw_bytes = msg.get("bytes")
                    if isinstance(raw_bytes, (bytes, bytearray)) and raw_bytes:
                        _audio_frames += 1
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
                            _audio_frames += 1
                            await dg_ws.send(data)
                    elif t == "KeepAlive":
                        try:
                            await dg_ws.send(json.dumps({"type": "KeepAlive"}))
                        except Exception:
                            pass
                    elif t == "finalize":
                        stt_logger.info(f"[STT] Client sent finalize — audio_frames={_audio_frames}")
                        try:
                            await dg_ws.send(json.dumps({"type": "Finalize"}))
                        except Exception:
                            pass
                    elif t == "close":
                        stt_logger.info("[STT] Client sent close")
                        break

            async def deepgram_to_client():
                nonlocal _transcript_count
                async for raw in dg_ws:
                    if isinstance(raw, str):
                        _transcript_count += 1
                        try:
                            await websocket.send_text(raw)
                        except Exception:
                            stt_logger.info(f"[STT] Client gone while forwarding transcript — transcripts={_transcript_count}")
                            return
                    elif isinstance(raw, (bytes, bytearray)):
                        try:
                            _transcript_count += 1
                            await websocket.send_text(bytes(raw).decode("utf-8"))
                        except Exception:
                            continue
                stt_logger.info(f"[STT] Deepgram stream ended — transcripts={_transcript_count} elapsed={_time.time() - _stt_start:.1f}s")

            forward_task = asyncio.create_task(client_to_deepgram())
            back_task = asyncio.create_task(deepgram_to_client())
            done, pending = await asyncio.wait({forward_task, back_task}, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    stt_logger.error(f"[STT] Task error: {type(exc).__name__}: {exc}")
                    raise exc
            stt_logger.info(f"[STT] Session ended normally — audio_frames={_audio_frames} transcripts={_transcript_count} elapsed={_time.time() - _stt_start:.1f}s")
    except WebSocketDisconnect:
        stt_logger.info(f"[STT] Client disconnected — elapsed={_time.time() - _stt_start:.1f}s")
        return
    except Exception as e:
        stt_logger.error(f"[STT] Stream error: {type(e).__name__}: {e} elapsed={_time.time() - _stt_start:.1f}s")
        try:
            await websocket.close(code=1011, reason=f"STT stream error: {str(e)[:120]}")
        except Exception:
            pass
