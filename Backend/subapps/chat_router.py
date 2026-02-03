from __future__ import annotations

import json
import asyncio
import os
import time
import traceback
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from Backend.auth import get_current_user
from Backend.crud.chat.chat_crud import (
    count_user_messages,
    get_recent_user_messages,
    list_messages_for_session,
    save_message,
    save_message_with_id,
    update_session_last_message,
)
from Backend.crud.chat.chat_session_crud import (
    assert_session_owned_by_user,
    create_session,
    delete_session,
    get_session_by_id,
    list_sessions_for_user,
    attach_friendship_to_session,
    update_session_title,
)
from Backend.schemas.chat_models import ChatRequest, MessageDTO, MessagesResponse, SessionDTO, SessionsResponse
from Backend.crud.client_uploads.uploads_crud import create_upload
from Backend.database import SessionLocal
from Backend.models.client_uploads.upload_model import Upload
from Backend.services.chat_service import ChatService, ChatTitleService
from Backend.services.embedding_service import EmbeddingService
from Backend.services.file_signing_service import sign_upload_id
from Backend.crud.books.rag_crud import search_similar_chunks
from Backend.crud.friends.friends_crud import get_friendship_id_for_pair, list_friends_for_user
from Backend.models.profile.profile_model import Profile
from Backend.models.friends.friendship_model import Friendship
from sqlalchemy import select, text

router = APIRouter(prefix="/chat", tags=["chat"])

chat_service = ChatService()
chat_title_service = ChatTitleService()
embedding_service = EmbeddingService()

RAG_TOP_K = int(os.getenv("RAG_TOP_K", "5") or "5")
RAG_MAX_CHARS_PER_CHUNK = int(os.getenv("RAG_MAX_CHARS_PER_CHUNK", "1200") or "1200")


def _format_rag_context(rows: list[dict]) -> Optional[str]:
    """
    Convert retrieved DB chunks to a compact, citation-friendly context block.
    """
    if not rows:
        return None

    parts: list[str] = []
    for i, r in enumerate(rows, 1):
        source = (r.get("source") or "unknown").strip()
        chunk_index = r.get("chunk_index")
        chunk_text = (r.get("chunk_text") or "").strip()
        if not chunk_text:
            continue
        if len(chunk_text) > RAG_MAX_CHARS_PER_CHUNK:
            chunk_text = chunk_text[:RAG_MAX_CHARS_PER_CHUNK].rstrip() + "…"
        parts.append(f"[{i}] source={source} chunk_index={chunk_index}\n{chunk_text}")

    if not parts:
        return None

    return (
        "Retrieved context (use only if relevant; do not invent facts):\n"
        + "\n\n".join(parts)
    )


async def _resolve_friend_display_name(*, user_id: uuid.UUID) -> Optional[str]:
    """
    Best-effort display name for a user:
    - profiles.full_name
    - auth.users.raw_user_meta_data full_name/name
    """

    def _select():
        db = SessionLocal()
        try:
            prof = db.get(Profile, user_id)
            if prof is not None:
                n = (getattr(prof, "full_name", None) or "").strip()
                if n:
                    return n
            row = db.execute(text("select raw_user_meta_data from auth.users where id = :id"), {"id": str(user_id)}).first()
            meta = row[0] if row and isinstance(row[0], dict) else {}
            if isinstance(meta, dict):
                n = (meta.get("full_name") or meta.get("name") or "").strip()
                if n:
                    return n
            return None
        finally:
            db.close()

    return await run_in_threadpool(_select)


async def _resolve_other_user_id_from_friendship(*, friendship_id: uuid.UUID, current_user_id: uuid.UUID) -> Optional[uuid.UUID]:
    def _select():
        db = SessionLocal()
        try:
            row = db.execute(
                select(Friendship.user_low_id, Friendship.user_high_id).where(Friendship.id == friendship_id).limit(1)
            ).first()
            if not row:
                return None
            low, high = uuid.UUID(str(row[0])), uuid.UUID(str(row[1]))
            return high if low == current_user_id else low
        finally:
            db.close()

    return await run_in_threadpool(_select)

def _as_uuid(value) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))

def _as_iso8601(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _resolve_public_base_url(request: Request | None) -> str:
    if request is None:
        return ""
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    if not host:
        return ""
    return f"{proto}://{host}".rstrip("/")


async def _signed_url_for_upload_path(
    *, base_url: str, user_id: uuid.UUID, path_value: str, expires_seconds: int
) -> str:
    if not isinstance(path_value, str) or not path_value.startswith("uploads/"):
        raise HTTPException(status_code=400, detail="Invalid attachment path")
    try:
        upload_id = uuid.UUID(path_value.split("/", 1)[1])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid attachment id")

    def _verify_ownership():
        db = SessionLocal()
        try:
            row = db.get(Upload, upload_id)
            if not row or row.user_id != user_id:
                raise HTTPException(status_code=403, detail="Forbidden attachment")
        finally:
            db.close()

    await run_in_threadpool(_verify_ownership)

    exp = int(time.time()) + int(expires_seconds)
    sig = sign_upload_id(upload_id=upload_id, exp=exp)
    if not base_url:
        raise HTTPException(status_code=500, detail="Cannot determine public base URL for signed files")
    return f"{base_url}/files/{upload_id}?exp={exp}&sig={sig}"


def _inject_signed_urls_into_content(*, base_url: str, content: str) -> str:
    try:
        obj = json.loads(content or "")
        if not isinstance(obj, dict):
            return content
        talktome = obj.get("_talktome")
        if isinstance(talktome, str):
            try:
                talktome = json.loads(talktome)
            except Exception:
                talktome = None
        if not isinstance(talktome, dict):
            return content
        if talktome.get("type") != "segments":
            return content
        segments = talktome.get("segments")
        if not isinstance(segments, list):
            return content
        changed = False
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            if seg.get("type") not in ("image", "file"):
                continue
            path_value = seg.get("path")
            if not isinstance(path_value, str) or "/" not in path_value:
                continue
            try:
                # For stored messages, we don't re-check ownership here; messages are already scoped by session/user.
                if path_value.startswith("uploads/"):
                    upload_id = uuid.UUID(path_value.split("/", 1)[1])
                    exp = int(time.time()) + 60 * 60 * 24 * 7
                    sig = sign_upload_id(upload_id=upload_id, exp=exp)
                    if base_url:
                        seg["url"] = f"{base_url}/files/{upload_id}?exp={exp}&sig={sig}"
                        changed = True
            except Exception:
                continue
        if not changed:
            return content
        obj["_talktome"] = talktome
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return content


@router.post("/attachments")
async def upload_chat_attachment(
    request: Request, file: UploadFile = File(...), current_user: dict = Depends(get_current_user)
):
    try:
        try:
            user_id = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="Empty upload")

        content_type = file.content_type or "application/octet-stream"
        filename = (file.filename or "upload").strip() or None

        upload_id = await create_upload(
            user_id=user_id,
            kind="chat_attachment",
            content_type=content_type,
            filename=filename,
            data=data,
        )
        path_value = f"uploads/{upload_id}"
        exp = int(time.time()) + 60 * 60 * 24 * 7
        sig = sign_upload_id(upload_id=upload_id, exp=exp)
        base = _resolve_public_base_url(request)
        url_value = f"{base}/files/{upload_id}?exp={exp}&sig={sig}" if base else None
        return {"path": path_value, "url": url_value}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sessions/message/stream")
async def chat_message_stream(http_request: Request, chat_request: ChatRequest, current_user: dict = Depends(get_current_user)):
    try:
        try:
            user_uuid = uuid.UUID(current_user.get("sub"))
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")

        public_base = _resolve_public_base_url(http_request)

        if chat_request.session_id is not None:
            try:
                await assert_session_owned_by_user(user_id=user_uuid, session_id=chat_request.session_id)
                session_uuid = chat_request.session_id
            except PermissionError:
                raise HTTPException(status_code=403, detail="Forbidden: invalid session")
        else:
            session_row = await create_session(user_id=user_uuid, title=None)
            session_uuid = _as_uuid(session_row["id"])

        # Friend selection is OPTIONAL for AI chat.
        # If provided, we attach the friendship to the session so partner messaging can reuse it later.
        # (Partner messaging itself is handled via /partner/send-message, and may still require selection.)
        if chat_request.friend_user_id is not None:
            fid = await get_friendship_id_for_pair(user_id=user_uuid, friend_user_id=chat_request.friend_user_id)
            if not fid:
                raise HTTPException(status_code=400, detail="Not friends with selected user")
            await attach_friendship_to_session(user_id=user_uuid, session_id=session_uuid, friendship_id=fid)

        store_as_segments = bool(chat_request.attachments)
        if store_as_segments:
            segs = []
            msg = (chat_request.message or "").strip()
            if msg:
                segs.append({"type": "text", "content": msg})
            for a in chat_request.attachments or []:
                try:
                    t = getattr(a, "type", None) or (a.get("type") if isinstance(a, dict) else None)
                    p = getattr(a, "path", None) or (a.get("path") if isinstance(a, dict) else None)
                    fn = getattr(a, "filename", None) or (a.get("filename") if isinstance(a, dict) else None)
                    ct = getattr(a, "content_type", None) or (a.get("content_type") if isinstance(a, dict) else None)
                    if not t or not p:
                        continue
                    seg_obj = {"type": t, "path": p}
                    if fn:
                        seg_obj["filename"] = fn
                    if ct:
                        seg_obj["content_type"] = ct
                    segs.append(seg_obj)
                except Exception:
                    continue
            content_to_store = json.dumps({"_talktome": {"type": "segments", "segments": segs}}, ensure_ascii=False)
        else:
            content_to_store = chat_request.message

        try:
            await save_message_with_id(
                user_id=user_uuid,
                session_id=session_uuid,
                role="user",
                content=content_to_store,
                message_id=chat_request.message_id,
            )
        except PermissionError:
            # If the client reuses a message id that already exists for a different user/session, treat it as a bad request.
            raise HTTPException(status_code=400, detail="Invalid message_id")
        last_preview = (chat_request.message or "").strip()
        if not last_preview and chat_request.attachments:
            has_image = False
            try:
                for a in chat_request.attachments or []:
                    t = getattr(a, "type", None) or (a.get("type") if isinstance(a, dict) else None)
                    if t == "image":
                        has_image = True
                        break
            except Exception:
                has_image = False
            last_preview = "Sent a photo." if has_image else "Sent an attachment."
        await update_session_last_message(session_id=session_uuid, content=last_preview)
        user_message_count = await count_user_messages(session_id=session_uuid)

        if user_message_count in (1, 2):
            try:
                recent_user_messages = await get_recent_user_messages(session_id=session_uuid, limit=2)
                chat_title = await chat_title_service.generate_chat_title(recent_user_messages)
                if chat_title:
                    await update_session_title(user_id=user_uuid, session_id=session_uuid, title=chat_title)
            except Exception:
                pass

        # Provide minimal context about who the "other person" is (if selected / attached).
        partner_ab_context_text: Optional[str] = None
        try:
            other_user_id: Optional[uuid.UUID] = None
            if chat_request.friend_user_id is not None:
                other_user_id = chat_request.friend_user_id
            else:
                sess = await get_session_by_id(user_id=user_uuid, session_id=session_uuid)
                fid = sess.get("friendship_id") if isinstance(sess, dict) else None
                if fid:
                    try:
                        fid_uuid = uuid.UUID(str(fid))
                        other_user_id = await _resolve_other_user_id_from_friendship(
                            friendship_id=fid_uuid, current_user_id=user_uuid
                        )
                    except Exception:
                        other_user_id = None

            if other_user_id:
                other_name = await _resolve_friend_display_name(user_id=other_user_id)
                if other_name:
                    partner_ab_context_text = (
                        "Context:\n"
                        f"- The other person in this chat is named {other_name}.\n"
                    )
        except Exception:
            partner_ab_context_text = None
        partner_letter = "A"

        state = {"final_text": "", "partner_texts": [], "segments": []}

        async def persist_stream_results():
            try:
                final_text = (state.get("final_text") or "").strip()
                segments = state.get("segments") or []
                if segments:
                    try:
                        annotation_obj = {"_talktome": {"type": "segments", "segments": segments}}
                        annotation = json.dumps(annotation_obj, ensure_ascii=False)
                        await save_message(user_id=user_uuid, session_id=session_uuid, role="assistant", content=annotation)
                        return
                    except Exception:
                        pass
                if final_text:
                    try:
                        annotation_obj = {
                            "_talktome": {"type": "segments", "segments": [{"type": "text", "content": final_text}]}
                        }
                        annotation = json.dumps(annotation_obj, ensure_ascii=False)
                        await save_message(user_id=user_uuid, session_id=session_uuid, role="assistant", content=annotation)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[SSE] persist task fatal: {e}")

        async def iter_sse():
            yield (":" + " " * 2048 + "\n\n").encode()

            sess_payload = json.dumps({"session_id": str(session_uuid)})
            yield f"event: session\ndata: {sess_payload}\n\n".encode()

            full_text_parts = []
            segments_list = []
            current_text_segment = ""
            try:
                image_urls = []
                file_urls = []
                if chat_request.attachments:
                    for a in chat_request.attachments or []:
                        try:
                            t = getattr(a, "type", None) or (a.get("type") if isinstance(a, dict) else None)
                            p = getattr(a, "path", None) or (a.get("path") if isinstance(a, dict) else None)
                            if not t or not p:
                                continue
                            url_value = await _signed_url_for_upload_path(
                                base_url=public_base,
                                user_id=user_uuid,
                                path_value=p,
                                expires_seconds=60 * 60 * 24 * 7,
                            )
                            if t == "image":
                                image_urls.append(url_value)
                            else:
                                file_urls.append(url_value)
                        except HTTPException:
                            raise
                        except Exception:
                            continue

                # RAG: retrieve relevant book chunks from Supabase.
                rag_context_text: Optional[str] = None
                try:
                    q_text = (chat_request.message or "").strip()
                    if q_text and RAG_TOP_K > 0:
                        # Embeddings call is synchronous; run it off the event loop.
                        q_embed = await run_in_threadpool(embedding_service.get_embedding, q_text)
                        rows = await search_similar_chunks(query_embedding=q_embed, limit=RAG_TOP_K)
                        rag_context_text = _format_rag_context(rows)
                except Exception as e:
                    # Never fail chat if RAG fails.
                    print(f"[RAG] retrieval failed: {e}")
                    rag_context_text = None

                merged_partner_context = partner_ab_context_text
                if rag_context_text:
                    # Append so partner context remains small and first.
                    merged_partner_context = (partner_ab_context_text or "").rstrip()
                    if merged_partner_context:
                        merged_partner_context += "\n\n"
                    merged_partner_context += rag_context_text

                input_messages = chat_service.build_messages(
                    session_partner_letter=partner_letter,
                    last_user_message=chat_request.message,
                    partner_ab_context_text=merged_partner_context,
                    image_urls=image_urls or None,
                    file_urls=file_urls or None,
                )

                import re
                from contextlib import suppress

                open_pat = re.compile(r"<partner_message(?:\s+[^>]*)?>")
                end_marker = "</partner_message>"
                tag_start = "<partner_message"

                buffer = ""
                in_partner = False

                # Keep SSE connection alive without burning CPU per connection.
                # For high concurrency, frequent heartbeats add up quickly.
                heartbeat_interval = 5.0

                async def _consume_stream(stream):
                    nonlocal buffer, in_partner, current_text_segment
                    aiter = stream.__aiter__()
                    while True:
                        try:
                            event = await asyncio.wait_for(aiter.__anext__(), timeout=heartbeat_interval)
                        except asyncio.TimeoutError:
                            if not in_partner and buffer:
                                max_k = min(len(buffer), len(tag_start))
                                overlap = 0
                                for k in range(max_k, -1, -1):
                                    if buffer.endswith(tag_start[:k]):
                                        overlap = k
                                        break
                                flush_len = len(buffer) - overlap
                                if flush_len > 0:
                                    flushable = buffer[:flush_len]
                                    full_text_parts.append(flushable)
                                    yield f"event: token\ndata: {json.dumps(flushable)}\n\n".encode()
                                    current_text_segment += flushable
                                    buffer = buffer[flush_len:]
                            yield b":\n\n"
                            continue
                        except StopAsyncIteration:
                            break
                        except Exception as e:
                            yield f"event: error\ndata: {json.dumps(str(e))}\n\n".encode()
                            break

                        etype = getattr(event, "type", "")

                        if etype == "response.created":
                            rid = None
                            with suppress(Exception):
                                rid = getattr(getattr(event, "response", None), "id", None)
                            if rid:
                                payload = json.dumps({"response_id": rid})
                                yield f"event: response_id\ndata: {payload}\n\n".encode()
                            continue

                        if etype == "response.error":
                            err_msg = "Streaming error"
                            with suppress(Exception):
                                err_obj = getattr(event, "error", None)
                                if err_obj is not None:
                                    err_msg = str(err_obj)
                            yield f"event: error\ndata: {json.dumps(err_msg)}\n\n".encode()
                            break

                        if etype == "response.completed":
                            break

                        if etype != "response.output_text.delta":
                            continue

                        delta = getattr(event, "delta", "") or ""
                        if not isinstance(delta, str):
                            with suppress(Exception):
                                delta = str(delta)
                        if not delta:
                            continue

                        buffer += delta

                        while True:
                            if not in_partner:
                                m = open_pat.search(buffer)
                                if m:
                                    before = buffer[: m.start()]
                                    if before:
                                        full_text_parts.append(before)
                                        yield f"event: token\ndata: {json.dumps(before)}\n\n".encode()
                                        current_text_segment += before
                                    buffer = buffer[m.end() :]
                                    in_partner = True
                                    continue
                                if buffer:
                                    max_k = min(len(buffer), len(tag_start))
                                    overlap = 0
                                    for k in range(max_k, -1, -1):
                                        if buffer.endswith(tag_start[:k]):
                                            overlap = k
                                            break
                                    flush_len = len(buffer) - overlap
                                    if flush_len > 0:
                                        flushable = buffer[:flush_len]
                                        full_text_parts.append(flushable)
                                        yield f"event: token\ndata: {json.dumps(flushable)}\n\n".encode()
                                        current_text_segment += flushable
                                        buffer = buffer[flush_len:]
                                break
                            else:
                                close_idx = buffer.find(end_marker)
                                if close_idx == -1:
                                    break
                                content = buffer[:close_idx]
                                yield f"event: tool_start\ndata: {json.dumps({'name': 'emit_partner_message'})}\n\n".encode()
                                yield f"event: partner_message\ndata: {json.dumps(content)}\n\n".encode()
                                yield b"event: tool_done\ndata: {}\n\n"

                                if current_text_segment:
                                    segments_list.append({"type": "text", "content": current_text_segment})
                                    current_text_segment = ""
                                segments_list.append({"type": "partner_draft", "text": content})

                                buffer = buffer[close_idx + len(end_marker) :]
                                in_partner = False
                                continue

                try:
                    async with chat_service.stream_response(
                        messages=input_messages,
                        previous_response_id=chat_request.previous_response_id,
                    ) as stream:
                        async for chunk in _consume_stream(stream):
                            yield chunk
                except Exception as e:
                    # If OpenAI rejects `previous_response_id`, retry once without it.
                    msg = str(e)
                    if chat_request.previous_response_id and (
                        "previous_response_not_found" in msg
                        or ("previous response with id" in msg and "not found" in msg)
                    ):
                        buffer = ""
                        in_partner = False
                        current_text_segment = ""
                        async with chat_service.stream_response(messages=input_messages, previous_response_id=None) as stream:
                            async for chunk in _consume_stream(stream):
                                yield chunk
                    else:
                        raise

                if buffer:
                    full_text_parts.append(buffer)
                    yield f"event: token\ndata: {json.dumps(buffer)}\n\n".encode()
                    current_text_segment += buffer
                    buffer = ""

                if current_text_segment:
                    segments_list.append({"type": "text", "content": current_text_segment})
                    current_text_segment = ""

                final_text = "".join(full_text_parts)
                state["final_text"] = final_text or ""
                if segments_list:
                    state["segments"] = segments_list

                yield b"event: done\ndata: {}\n\n"
            except Exception as e:
                print(f"[SSE] /chat stream error: {e}\n" + traceback.format_exc())
                yield f"event: error\ndata: {json.dumps(str(e))}\n\n".encode()

        return StreamingResponse(
            iter_sse(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "Content-Encoding": "identity",
                "Content-Type": "text/event-stream; charset=utf-8",
            },
            background=BackgroundTask(persist_stream_results),
        )
    except HTTPException:
        raise
    except Exception as e:
        try:
            print(f"[chat_message_stream] fatal: {e}\n{traceback.format_exc()}")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error processing stream: {str(e)}")


@router.get("/sessions/{session_id}/messages", response_model=MessagesResponse)
async def get_messages(http_request: Request, session_id: uuid.UUID, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    base = _resolve_public_base_url(http_request)

    try:
        await assert_session_owned_by_user(user_id=user_uuid, session_id=session_id)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Forbidden: invalid session")

    rows = await list_messages_for_session(user_id=user_uuid, session_id=session_id, limit=200, offset=0)
    return MessagesResponse(
        messages=[
            MessageDTO(
                id=_as_uuid(r["id"]),
                user_id=_as_uuid(r["user_id"]),
                session_id=_as_uuid(r["session_id"]),
                role=r["role"],
                content=_inject_signed_urls_into_content(base_url=base, content=r["content"]),
                created_at=_as_iso8601(r.get("created_at")),
            )
            for r in rows
        ]
    )


@router.get("/sessions", response_model=SessionsResponse)
async def get_sessions(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    rows = await list_sessions_for_user(user_id=user_uuid, limit=100, offset=0)
    return SessionsResponse(
        sessions=[
            SessionDTO(
                id=_as_uuid(r["id"]),
                user_id=_as_uuid(r["user_id"]),
                title=r.get("title"),
                last_message_at=_as_iso8601(r.get("last_message_at")),
                last_message_content=r.get("last_message_content"),
            )
            for r in rows
        ]
    )


@router.post("/sessions", response_model=SessionDTO)
async def create_empty_session(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    row = await create_session(user_id=user_uuid, title=None)
    return SessionDTO(id=_as_uuid(row["id"]), user_id=user_uuid, title=row.get("title"))


@router.patch("/sessions/{session_id}")
async def rename_session(session_id: uuid.UUID, payload: dict, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    title = payload.get("title")
    if title is not None and not isinstance(title, str):
        raise HTTPException(status_code=400, detail="title must be a string or null")
    try:
        await update_session_title(user_id=user_uuid, session_id=session_id, title=title)
        return {"success": True}
    except PermissionError:
        raise HTTPException(status_code=403, detail="Forbidden: invalid session")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/sessions/{session_id}")
async def delete_session_route(session_id: uuid.UUID, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    try:
        await assert_session_owned_by_user(user_id=user_uuid, session_id=session_id)
        await delete_session(user_id=user_uuid, session_id=session_id)
        return {"success": True}
    except PermissionError:
        raise HTTPException(status_code=403, detail="Forbidden: invalid session")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


