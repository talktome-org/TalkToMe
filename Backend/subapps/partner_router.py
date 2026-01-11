import asyncio
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from Backend.auth import get_current_user
from Backend.crud.chat.chat_crud import list_messages_for_session, save_message, update_session_last_message
from Backend.crud.user_links.link_crud import get_link_status_for_user, get_partner_user_id
from Backend.crud.user_links.linked_sessions_crud import (
    create_linked_session,
    get_linked_session_by_relationship_and_source_session,
    update_linked_session_partner_session_for_source,
)
from Backend.crud.user_links.partner_requests_crud import (
    attach_session_and_message_on_pending,
    create_partner_request,
    get_latest_pending_for_context,
    get_request_by_id,
    list_pending_for_user,
    mark_accepted_and_attach,
    mark_delivered,
    update_content,
)
from Backend.crud.chat.chat_session_crud import (
    assert_session_owned_by_user,
    create_session,
    delete_session,
    get_session_by_id,
    touch_session,
)
from Backend.schemas.partner_models import (
    PartnerPendingRequestDTO,
    PartnerPendingRequestsResponse,
    PartnerRequestBody,
    PartnerRequestResponse,
)
from Backend.database import SessionLocal
from Backend.models.links.partner_requests_model import PartnerRequest
from sqlalchemy import Text, cast, text, update
from Backend.services.apns_service import (
    send_partner_message_notification_to_user,
    send_partner_request_notification_to_user,
)

router = APIRouter(prefix="/partner", tags=["partner"])

def _as_uuid(value) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))

def _as_uuid_opt(value) -> uuid.UUID | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _as_uuid(value)


@router.post("/request", response_model=PartnerRequestResponse)
async def create_partner_request_endpoint(body: PartnerRequestBody, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    await assert_session_owned_by_user(user_id=user_uuid, session_id=body.session_id)

    linked, relationship_id, _ = await get_link_status_for_user(user_id=user_uuid)
    if not linked or not relationship_id:
        raise HTTPException(status_code=400, detail="User is not linked to a partner")
    partner_user_id = await get_partner_user_id(user_id=user_uuid)
    if not partner_user_id:
        raise HTTPException(status_code=400, detail="Could not find partner for the linked relationship")

    linked_row = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=body.session_id
    )
    if not linked_row:
        await create_linked_session(
            relationship_id=relationship_id,
            user_a_id=user_uuid,
            user_b_id=partner_user_id,
            user_a_personal_session_id=body.session_id,
            user_b_personal_session_id=None,
        )

    row = await create_partner_request(
        relationship_id=relationship_id,
        sender_user_id=user_uuid,
        recipient_user_id=partner_user_id,
        sender_session_id=body.session_id,
        content=body.message.strip(),
    )

    try:
        meta = current_user.get("user_metadata") or {}
        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
        await send_partner_request_notification_to_user(
            recipient_user_id=partner_user_id,
            request_id=_as_uuid(row["id"]),
            relationship_id=relationship_id,
            preview=body.message.strip(),
            sender_name=sender_name,
        )
    except Exception:
        pass

    return PartnerRequestResponse(success=True, request_id=_as_uuid(row["id"]))


@router.get("/pending", response_model=PartnerPendingRequestsResponse)
async def get_pending_partner_requests(current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    rows = await list_pending_for_user(user_id=user_uuid, limit=50)
    return PartnerPendingRequestsResponse(
        requests=[
            PartnerPendingRequestDTO(
                id=_as_uuid(r["id"]),
                sender_user_id=_as_uuid(r["sender_user_id"]),
                sender_session_id=_as_uuid(r["sender_session_id"]),
                content=r["content"],
                created_at=r["created_at"],
                status=r["status"],
                recipient_session_id=_as_uuid_opt(r.get("recipient_session_id")),
                created_message_id=_as_uuid_opt(r.get("created_message_id")),
            )
            for r in rows
        ]
    )


@router.post("/requests/{request_id}/delivered")
async def mark_delivered_endpoint(request_id: uuid.UUID, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    req = await get_request_by_id(request_id=request_id)
    if not req or req.get("recipient_user_id") != str(user_uuid):
        raise HTTPException(status_code=404, detail="Request not found")

    await mark_delivered(request_id=request_id)
    return {"success": True}


@router.post("/requests/{request_id}/accept")
async def accept_request_endpoint(
    request_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)
):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    req = await get_request_by_id(request_id=request_id)
    if not req or req.get("recipient_user_id") != str(user_uuid):
        raise HTTPException(status_code=404, detail="Request not found")

    if (req.get("status") == "accepted") and req.get("recipient_session_id"):
        try:
            existing_session_id = _as_uuid(req["recipient_session_id"])  # type: ignore[index]
        except Exception:
            existing_session_id = None  # type: ignore[assignment]
        if existing_session_id:
            return {"success": True, "recipient_session_id": str(existing_session_id)}

    relationship_id = _as_uuid(req["relationship_id"])  # type: ignore[arg-type]
    sender_session_id = _as_uuid(req["sender_session_id"])  # type: ignore[arg-type]

    linked_row = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=sender_session_id
    )
    recipient_session_id: uuid.UUID

    if linked_row:
        sender_user_id = _as_uuid(req["sender_user_id"])  # type: ignore[arg-type]
        if linked_row.get("user_a_id") == str(sender_user_id):
            recipient_session_str = linked_row.get("user_b_personal_session_id")
        else:
            recipient_session_str = linked_row.get("user_a_personal_session_id")

        if recipient_session_str:
            recipient_session_id = _as_uuid(recipient_session_str)
        else:
            linked_row = None

    if not linked_row or not recipient_session_str:
        sender_user_id = _as_uuid(req["sender_user_id"])  # type: ignore[arg-type]
        try:
            sender_session_row = await get_session_by_id(user_id=sender_user_id, session_id=sender_session_id)
            mirrored_title = (sender_session_row or {}).get("title")
        except Exception:
            mirrored_title = None

        new_session = await create_session(user_id=user_uuid, title=mirrored_title or "New Chat")
        candidate_session_id = _as_uuid(new_session["id"])  # type: ignore[index]
        await update_linked_session_partner_session_for_source(
            relationship_id=relationship_id,
            source_session_id=sender_session_id,
            partner_session_id=candidate_session_id,
        )
        refreshed = await get_linked_session_by_relationship_and_source_session(
            relationship_id=relationship_id, source_session_id=sender_session_id
        )
        final_id_str = (refreshed or {}).get("user_b_personal_session_id")
        if final_id_str and final_id_str != str(candidate_session_id):
            try:
                await delete_session(user_id=user_uuid, session_id=candidate_session_id)
            except Exception:
                pass
            recipient_session_id = _as_uuid(final_id_str)
        else:
            recipient_session_id = candidate_session_id

    def _claim_accept():
        db = SessionLocal()
        try:
            stmt = (
                update(PartnerRequest)
                .where(PartnerRequest.id == request_id)
                .where(cast(PartnerRequest.status, Text).in_(["pending", "delivered"]))
                .values(
                    status=text("'accepted'::request_status"),
                    accepted_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    recipient_session_id=recipient_session_id,
                )
            )
            res = db.execute(stmt)
            db.commit()
            return int(getattr(res, "rowcount", 0) or 0)
        finally:
            db.close()

    updated_rows = await asyncio.get_event_loop().run_in_executor(None, _claim_accept)

    if updated_rows == 0:
        return {"success": True, "recipient_session_id": str(recipient_session_id)}

    async def _finalize_acceptance():
        try:
            pre_created_id_str = req.get("created_message_id")
            if pre_created_id_str:
                await update_session_last_message(session_id=recipient_session_id, content=req.get("content", ""))
                await touch_session(session_id=recipient_session_id)
                return

            partner_text = req.get("content", "")
            annotated = json.dumps({"_talktome": {"type": "partner_received", "text": partner_text}, "body": ""})
            created = await save_message(user_id=user_uuid, session_id=recipient_session_id, role="assistant", content=annotated)
            await update_session_last_message(session_id=recipient_session_id, content=req.get("content", ""))
            await touch_session(session_id=recipient_session_id)
            await mark_accepted_and_attach(
                request_id=request_id,
                recipient_session_id=recipient_session_id,
                created_message_id=_as_uuid(created["id"]),  # type: ignore[index]
            )
        except Exception:
            pass

    background_tasks.add_task(_finalize_acceptance)
    return {"success": True, "recipient_session_id": str(recipient_session_id)}


@router.post("/request/stream")
async def partner_request_stream(body: PartnerRequestBody, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    await assert_session_owned_by_user(user_id=user_uuid, session_id=body.session_id)

    linked, relationship_id, _ = await get_link_status_for_user(user_id=user_uuid)
    if not linked or not relationship_id:
        raise HTTPException(status_code=400, detail="User is not linked to a partner")
    partner_user_id = await get_partner_user_id(user_id=user_uuid)
    if not partner_user_id:
        raise HTTPException(status_code=400, detail="Could not find partner for the linked relationship")

    linked_row = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=body.session_id
    )
    recipient_session_id: uuid.UUID | None = None
    if not linked_row:
        await create_linked_session(
            relationship_id=relationship_id,
            user_a_id=user_uuid,
            user_b_id=partner_user_id,
            user_a_personal_session_id=body.session_id,
            user_b_personal_session_id=None,
        )
    else:
        try:
            src = str(body.session_id)
            a_sid = linked_row.get("user_a_personal_session_id")
            b_sid = linked_row.get("user_b_personal_session_id")
            if a_sid and src == a_sid and b_sid:
                recipient_session_id = uuid.UUID(b_sid)
            elif b_sid and src == b_sid and a_sid:
                recipient_session_id = uuid.UUID(a_sid)
            else:
                recipient_session_id = None
        except Exception:
            recipient_session_id = None

    try:
        _ = await list_messages_for_session(user_id=user_uuid, session_id=body.session_id, limit=50)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load chat history: {e}")

    created_request_id: uuid.UUID | None = None
    if recipient_session_id is None:
        try:
            existing_req = await get_latest_pending_for_context(
                relationship_id=relationship_id,
                sender_user_id=user_uuid,
                recipient_user_id=partner_user_id,
                sender_session_id=body.session_id,
            )
            if existing_req:
                try:
                    existing_recipient_sid = existing_req.get("recipient_session_id")
                    if existing_recipient_sid:
                        try:
                            recipient_session_id = _as_uuid(existing_recipient_sid)
                        except Exception:
                            recipient_session_id = None
                    await update_content(request_id=_as_uuid(existing_req["id"]), content=body.message.strip())
                except Exception:
                    pass
                created_request_id = _as_uuid(existing_req["id"])  # type: ignore[index]
            else:
                created_req = await create_partner_request(
                    relationship_id=relationship_id,
                    sender_user_id=user_uuid,
                    recipient_user_id=partner_user_id,
                    sender_session_id=body.session_id,
                    content=body.message.strip(),
                )
                created_request_id = _as_uuid(created_req["id"])  # type: ignore[index]
                try:
                    meta = current_user.get("user_metadata") or {}
                    sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
                    await send_partner_request_notification_to_user(
                        recipient_user_id=partner_user_id,
                        request_id=created_request_id,
                        relationship_id=relationship_id,
                        preview=body.message.strip(),
                        sender_name=sender_name,
                    )
                except Exception:
                    pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create or reuse partner request: {e}")

    def iter_sse():
        yield (":" + " " * 2048 + "\n\n").encode()
        parts = []
        final_text = ""
        try:
            final_text = body.message.strip()
            if final_text:
                words = final_text.split(" ")
                for i, word in enumerate(words):
                    chunk = word + (" " if i < len(words) - 1 else "")
                    parts.append(chunk)
                    yield f"event: token\ndata: {json.dumps(chunk)}\n\n".encode()

            final_content = (final_text or "").strip() or body.message.strip()
            if created_request_id is not None:
                try:
                    try:
                        sender_session_row = asyncio.run(get_session_by_id(user_id=user_uuid, session_id=body.session_id))
                        mirrored_title = (sender_session_row or {}).get("title")
                    except Exception:
                        mirrored_title = None
                    new_session = asyncio.run(create_session(user_id=partner_user_id, title=mirrored_title or "New Chat"))
                    recipient_session_id_created = _as_uuid(new_session["id"])  # type: ignore[index]
                    try:
                        asyncio.run(
                            update_linked_session_partner_session_for_source(
                                relationship_id=relationship_id,
                                source_session_id=body.session_id,
                                partner_session_id=recipient_session_id_created,
                            )
                        )
                    except Exception:
                        pass
                    annotated = json.dumps({"_talktome": {"type": "partner_received", "text": final_content}, "body": ""})
                    created = asyncio.run(
                        save_message(
                            user_id=partner_user_id,
                            session_id=recipient_session_id_created,  # type: ignore[arg-type]
                            role="assistant",
                            content=annotated,
                        )
                    )
                    asyncio.run(update_session_last_message(session_id=recipient_session_id_created, content=final_content))  # type: ignore[arg-type]
                    asyncio.run(touch_session(session_id=recipient_session_id_created))  # type: ignore[arg-type]
                    asyncio.run(
                        attach_session_and_message_on_pending(
                            request_id=created_request_id,
                            recipient_session_id=recipient_session_id_created,
                            created_message_id=_as_uuid(created["id"]),  # type: ignore[index]
                        )
                    )
                    try:
                        meta = current_user.get("user_metadata") or {}
                        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
                        asyncio.run(
                            send_partner_message_notification_to_user(
                                recipient_user_id=partner_user_id,
                                session_id=recipient_session_id_created,  # type: ignore[arg-type]
                                preview=final_content,
                                sender_name=sender_name,
                            )
                        )
                    except Exception:
                        pass
                except Exception:
                    try:
                        asyncio.run(update_content(request_id=created_request_id, content=final_content))
                    except Exception:
                        pass
            else:
                try:
                    annotated = json.dumps({"_talktome": {"type": "partner_received", "text": final_content}, "body": ""})
                    created = asyncio.run(
                        save_message(
                            user_id=partner_user_id,
                            session_id=recipient_session_id,  # type: ignore[arg-type]
                            role="assistant",
                            content=annotated,
                        )
                    )
                    asyncio.run(update_session_last_message(session_id=recipient_session_id, content=final_content))  # type: ignore[arg-type]
                    asyncio.run(touch_session(session_id=recipient_session_id))  # type: ignore[arg-type]
                    try:
                        meta = current_user.get("user_metadata") or {}
                        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
                        asyncio.run(
                            send_partner_message_notification_to_user(
                                recipient_user_id=partner_user_id,
                                session_id=recipient_session_id,  # type: ignore[arg-type]
                                preview=final_content,
                                sender_name=sender_name,
                            )
                        )
                    except Exception:
                        pass
                    _ = created
                except Exception as e:
                    yield f"event: error\ndata: {json.dumps(str(e))}\n\n".encode()
                    return

            yield b"event: done\ndata: {}\n\n"
        except Exception as e:
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
    )