import json
import time
import traceback
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.concurrency import iterate_in_threadpool

from Backend.auth import get_current_user
from Backend.crud.chat.chat_crud import (
    count_user_messages,
    get_recent_user_messages,
    list_messages_for_session,
    save_message,
    update_session_last_message,
)
from Backend.crud.user_links.link_crud import get_link_status_for_user, get_partner_user_id
from Backend.crud.user_links.linked_sessions_crud import get_linked_session_by_relationship_and_source_session
from Backend.crud.chat.chat_session_crud import (
    assert_session_owned_by_user,
    create_session,
    delete_session,
    list_sessions_for_user,
    update_session_title,
)
from Backend.schemas.chat_models import ChatRequest, MessageDTO, MessagesResponse, SessionDTO, SessionsResponse
from Backend.crud.client_uploads.uploads_crud import create_upload
from Backend.database import SessionLocal
from Backend.models.client_uploads.upload_model import Upload
from Backend.services.chat_service import ChatService, ChatTitleService
from Backend.services.file_signing_service import sign_upload_id

router = APIRouter(prefix="/chat", tags=["chat"])

chat_service = ChatService()
chat_title_service = ChatTitleService()

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


def _signed_url_for_upload_path(
    *, base_url: str, user_id: uuid.UUID, path_value: str, expires_seconds: int
) -> str:
    if not isinstance(path_value, str) or not path_value.startswith("uploads/"):
        raise HTTPException(status_code=400, detail="Invalid attachment path")
    try:
        upload_id = uuid.UUID(path_value.split("/", 1)[1])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid attachment id")

    # Verify ownership (iter_sse runs in a threadpool, so do a sync DB read here)
    db = SessionLocal()
    try:
        row = db.get(Upload, upload_id)
        if not row or row.user_id != user_id:
            raise HTTPException(status_code=403, detail="Forbidden attachment")
    finally:
        db.close()

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

        await save_message(user_id=user_uuid, session_id=session_uuid, role="user", content=content_to_store)
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
                chat_title = chat_title_service.generate_chat_title(recent_user_messages)
                if chat_title:
                    await update_session_title(user_id=user_uuid, session_id=session_uuid, title=chat_title)
            except Exception:
                pass

        partner_ab_context_text: Optional[str] = None
        linked_session = None
        try:
            linked, relationship_id, _ = await get_link_status_for_user(user_id=user_uuid)
            if linked and relationship_id:
                linked_session = await get_linked_session_by_relationship_and_source_session(
                    relationship_id=relationship_id, source_session_id=session_uuid
                )
                partner_session_id_str = None
                if linked_session:
                    cur_id = str(user_uuid)
                    if linked_session.get("user_a_id") == cur_id:
                        partner_session_id_str = linked_session.get("user_b_personal_session_id")
                    elif linked_session.get("user_b_id") == cur_id:
                        partner_session_id_str = linked_session.get("user_a_personal_session_id")
                if partner_session_id_str:
                    partner_user_id = await get_partner_user_id(user_id=user_uuid)
                    if partner_user_id:
                        partner_messages = await list_messages_for_session(
                            user_id=partner_user_id,
                            session_id=uuid.UUID(partner_session_id_str),
                            limit=500,
                        )

                        try:
                            current_messages = await list_messages_for_session(
                                user_id=user_uuid,
                                session_id=session_uuid,
                                limit=500,
                            )

                            def _extract_partner_received(rows, sender_label):
                                items = []
                                for r in rows or []:
                                    try:
                                        if r.get("role") != "assistant":
                                            continue
                                        raw = r.get("content") or ""
                                        obj = json.loads(raw)
                                        meta = (obj or {}).get("_talktome") if isinstance(obj, dict) else None
                                        if not meta or meta.get("type") != "partner_received":
                                            continue
                                        text = meta.get("text") or ""
                                        created = r.get("created_at")
                                        if created is None:
                                            continue
                                        items.append({"created_at": created, "sender": sender_label, "text": text})
                                    except Exception:
                                        continue
                                return items

                            if linked_session:
                                cur_id = str(user_uuid)
                                if linked_session.get("user_a_id") == cur_id:
                                    me_label = "Partner A"
                                    partner_label = "Partner B"
                                else:
                                    me_label = "Partner B"
                                    partner_label = "Partner A"
                            else:
                                me_label = "Partner A"
                                partner_label = "Partner B"

                            sent_by_me = _extract_partner_received(partner_messages, me_label)
                            sent_by_partner = _extract_partner_received(current_messages, partner_label)
                            merged = sent_by_me + sent_by_partner
                            merged.sort(key=lambda x: x["created_at"])

                            if merged:
                                lines = ["Messages:"]
                                for m in merged:
                                    try:
                                        text = (m.get("text") or "").strip()
                                        if text:
                                            lines.append(f"{m['sender']}: {text}")
                                    except Exception:
                                        continue
                                partner_ab_context_text = "\n".join(lines)
                        except Exception:
                            partner_ab_context_text = None
        except Exception as e:
            print(f"Context retrieval warning (stream): {e}")

        try:
            partner_letter = "A"
            if linked_session:
                cur_id = str(user_uuid)
                if linked_session.get("user_a_id") == cur_id:
                    partner_letter = "A"
                elif linked_session.get("user_b_id") == cur_id:
                    partner_letter = "B"
        except Exception:
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

        def iter_sse():
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
                            url_value = _signed_url_for_upload_path(
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

                input_messages = chat_service.build_messages(
                    session_partner_letter=partner_letter,
                    last_user_message=chat_request.message,
                    partner_ab_context_text=partner_ab_context_text,
                    image_urls=image_urls or None,
                    file_urls=file_urls or None,
                )

                import re
                import threading
                from contextlib import suppress
                from queue import Empty, Queue

                open_pat = re.compile(r"<partner_message(?:\s+[^>]*)?>")
                end_marker = "</partner_message>"
                tag_start = "<partner_message"

                buffer = ""
                in_partner = False

                q: Queue = Queue()
                done = {"flag": False}

                def producer():
                    try:
                        with chat_service.stream_response(
                            messages=input_messages,
                            previous_response_id=chat_request.previous_response_id,
                        ) as stream:
                            for event in stream:
                                etype = getattr(event, "type", "")
                                if etype == "response.created":
                                    rid = None
                                    with suppress(Exception):
                                        rid = getattr(getattr(event, "response", None), "id", None)
                                    if rid:
                                        q.put(("response_id", json.dumps({"response_id": rid})))
                                    continue
                                if etype == "response.output_text.delta":
                                    delta = getattr(event, "delta", "") or ""
                                    if not isinstance(delta, str):
                                        with suppress(Exception):
                                            delta = str(delta)
                                    if delta:
                                        q.put(("delta", delta))
                                    continue
                                if etype == "response.error":
                                    err_msg = "Streaming error"
                                    with suppress(Exception):
                                        err_obj = getattr(event, "error", None)
                                        if err_obj is not None:
                                            err_msg = str(err_obj)
                                    q.put(("error", err_msg))
                                    return
                                if etype == "response.completed":
                                    break
                    finally:
                        done["flag"] = True

                t = threading.Thread(target=producer, daemon=True)
                t.start()

                heartbeat_interval = 0.1

                while True:
                    try:
                        kind, payload = q.get(timeout=heartbeat_interval)
                    except Empty:
                        if done["flag"] and q.empty():
                            break
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

                    if kind == "response_id":
                        yield f"event: response_id\ndata: {payload}\n\n".encode()
                        continue

                    if kind == "error":
                        err_msg = payload
                        yield f"event: error\ndata: {json.dumps(err_msg)}\n\n".encode()
                        break

                    if kind == "delta":
                        delta = payload
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
            iterate_in_threadpool(iter_sse()),
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


