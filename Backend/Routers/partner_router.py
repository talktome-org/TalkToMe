import uuid
import json
import asyncio
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from ..auth import get_current_user
from ..Database.link_repo import get_link_status_for_user, get_partner_user_id
from ..Database.session_repo import create_session, assert_session_owned_by_user, touch_session, delete_session, get_session_by_id
from ..Database.chat_repo import save_message, list_messages_for_session, update_session_last_message
from ..Database.linked_sessions_repo import (
    create_linked_session,
    get_linked_session_by_relationship_and_source_session,
    update_linked_session_partner_session_for_source,
)
from ..Database.partner_requests_repo import (
    create_partner_request,
    list_pending_for_user,
    mark_delivered,
    mark_accepted_and_attach,
    get_request_by_id,
    update_content,
    attach_session_and_message_on_pending,
    get_latest_pending_for_context,
)
from ..Database.supabase_client import supabase as _sp
from ..APNS.apns import (
    send_partner_request_notification_to_user,
    send_partner_message_notification_to_user,
)

from ..Models.requests import (
    PartnerRequestBody,
    PartnerRequestResponse,
    PartnerPendingRequestsResponse,
    PartnerPendingRequestDTO,
)

router = APIRouter(prefix="/partner", tags=["partner"])


@router.post("/request", response_model=PartnerRequestResponse)
async def create_partner_request_endpoint(body: PartnerRequestBody, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    # Verify session ownership
    await assert_session_owned_by_user(user_id=user_uuid, session_id=body.session_id)

    # Link + partner
    linked, relationship_id, _ = await get_link_status_for_user(user_id=user_uuid)
    if not linked or not relationship_id:
        raise HTTPException(status_code=400, detail="User is not linked to a partner")
    partner_user_id = await get_partner_user_id(user_id=user_uuid)
    if not partner_user_id:
        raise HTTPException(status_code=400, detail="Could not find partner for the linked relationship")

    # Ensure linked_sessions row exists for this source session
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

    # Create request row
    row = await create_partner_request(
        relationship_id=relationship_id,
        sender_user_id=user_uuid,
        recipient_user_id=partner_user_id,
        sender_session_id=body.session_id,
        content=body.message.strip(),
    )
    # Fire APNs notification to recipient (best-effort)
    try:
        meta = current_user.get("user_metadata") or {}
        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
        await send_partner_request_notification_to_user(
            recipient_user_id=partner_user_id,
            request_id=uuid.UUID(row["id"]),
            relationship_id=relationship_id,
            preview=body.message.strip(),
            sender_name=sender_name,
        )
    except Exception:
        pass
    return PartnerRequestResponse(success=True, request_id=uuid.UUID(row["id"]))


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
                id=uuid.UUID(r["id"]),
                sender_user_id=uuid.UUID(r["sender_user_id"]),
                sender_session_id=uuid.UUID(r["sender_session_id"]),
                content=r["content"],
                created_at=r["created_at"],
                status=r["status"],
                recipient_session_id=(uuid.UUID(r["recipient_session_id"]) if r.get("recipient_session_id") else None),
                created_message_id=(uuid.UUID(r["created_message_id"]) if r.get("created_message_id") else None),
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
async def accept_request_endpoint(request_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")

    req = await get_request_by_id(request_id=request_id)
    if not req or req.get("recipient_user_id") != str(user_uuid):
        raise HTTPException(status_code=404, detail="Request not found")

    # Idempotency: if already accepted, return existing session id
    if (req.get("status") == "accepted") and req.get("recipient_session_id"):
        try:
            existing_session_id = uuid.UUID(req["recipient_session_id"])  # type: ignore[index]
        except Exception:
            existing_session_id = None  # type: ignore[assignment]
        if existing_session_id:
            return {"success": True, "recipient_session_id": str(existing_session_id)}

    relationship_id = uuid.UUID(req["relationship_id"])  # type: ignore[arg-type]
    sender_session_id = uuid.UUID(req["sender_session_id"])  # type: ignore[arg-type]

    # Find or create recipient personal session
    linked_row = await get_linked_session_by_relationship_and_source_session(
        relationship_id=relationship_id, source_session_id=sender_session_id
    )
    recipient_session_id: uuid.UUID

    # Determine which session belongs to the recipient based on who is the sender
    if linked_row:
        sender_user_id = uuid.UUID(req["sender_user_id"])  # type: ignore[arg-type]
        if linked_row.get("user_a_id") == str(sender_user_id):
            # Sender is user_a, so recipient is user_b
            recipient_session_str = linked_row.get("user_b_personal_session_id")
        else:
            # Sender is user_b, so recipient is user_a
            recipient_session_str = linked_row.get("user_a_personal_session_id")

        if recipient_session_str:
            recipient_session_id = uuid.UUID(recipient_session_str)
        else:
            linked_row = None  # Force creation of new session

    if not linked_row or not recipient_session_str:
        # Mirror the sender's session title for the recipient
        sender_user_id = uuid.UUID(req["sender_user_id"])  # type: ignore[arg-type]
        try:
            sender_session_row = await get_session_by_id(user_id=sender_user_id, session_id=sender_session_id)
            mirrored_title = (sender_session_row or {}).get("title")
        except Exception:
            mirrored_title = None

        new_session = await create_session(user_id=user_uuid, title=mirrored_title or "New Chat")
        candidate_session_id = uuid.UUID(new_session["id"])  # type: ignore[index]
        await update_linked_session_partner_session_for_source(
            relationship_id=relationship_id, source_session_id=sender_session_id, partner_session_id=candidate_session_id
        )
        # Re-read mapping to confirm final session id
        refreshed = await get_linked_session_by_relationship_and_source_session(
            relationship_id=relationship_id, source_session_id=sender_session_id
        )
        final_id_str = (refreshed or {}).get("user_b_personal_session_id")
        if final_id_str and final_id_str != str(candidate_session_id):
            # Lost the race; delete duplicate session and use the winner
            try:
                await delete_session(user_id=user_uuid, session_id=candidate_session_id)
            except Exception:
                pass
            recipient_session_id = uuid.UUID(final_id_str)
        else:
            recipient_session_id = candidate_session_id

    # Atomically claim acceptance. If another worker already accepted, skip inserting another message.
    def _claim_accept():
        return (
            _sp
            .table("partner_requests")
            .update({
                "status": "accepted",
                "accepted_at": datetime.now(timezone.utc).isoformat(),
                "recipient_session_id": str(recipient_session_id),
            })
            .eq("id", str(request_id))
            .in_("status", ["pending", "delivered"])  # only transition once
            .execute()
        )

    res = await asyncio.get_event_loop().run_in_executor(None, _claim_accept)
    try:
        updated_rows = len(getattr(res, "data", []) or [])
    except Exception:
        updated_rows = 0

    if updated_rows == 0:
        # Already accepted elsewhere → return the mapped session id without adding another message
        return {"success": True, "recipient_session_id": str(recipient_session_id)}

    # Winner: finalize acceptance in the background to return immediately
    async def _finalize_acceptance():
        try:
            pre_created_id_str = req.get("created_message_id")
            if pre_created_id_str:
                await update_session_last_message(session_id=recipient_session_id, content=req.get("content", ""))
                await touch_session(session_id=recipient_session_id)
                return

            partner_text = req.get("content", "")
            annotated = json.dumps({
                "_talktome": {"type": "partner_received", "text": partner_text},
                "body": ""
            })
            print(f"[PartnerAccept] Saving partner message with annotation: {annotated[:100]}...")
            created = await save_message(
                user_id=user_uuid, session_id=recipient_session_id, role="assistant", content=annotated
            )
            await update_session_last_message(session_id=recipient_session_id, content=req.get("content", ""))
            await touch_session(session_id=recipient_session_id)
            await mark_accepted_and_attach(
                request_id=request_id,
                recipient_session_id=recipient_session_id,
                created_message_id=uuid.UUID(created["id"])  # type: ignore[index]
            )
        except Exception:
            pass

    background_tasks.add_task(_finalize_acceptance)


    # Return immediately with the session id so the client can navigate without delay
    return {"success": True, "recipient_session_id": str(recipient_session_id)}


@router.post("/request/stream")
async def partner_request_stream(body: PartnerRequestBody, current_user: dict = Depends(get_current_user)):
    try:
        user_uuid = uuid.UUID(current_user.get("sub"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user ID in token")
    print(f"[PartnerStream] START user={user_uuid} session={body.session_id} msg_len={len((body.message or '').strip())}")

    # Guard: session ownership
    await assert_session_owned_by_user(user_id=user_uuid, session_id=body.session_id)

    # Relationship + partner
    linked, relationship_id, _ = await get_link_status_for_user(user_id=user_uuid)
    if not linked or not relationship_id:
        raise HTTPException(status_code=400, detail="User is not linked to a partner")
    partner_user_id = await get_partner_user_id(user_id=user_uuid)
    if not partner_user_id:
        raise HTTPException(status_code=400, detail="Could not find partner for the linked relationship")
    print(f"[PartnerStream] LINK OK relationship={relationship_id} partner_user_id={partner_user_id}")

    # Ensure mapping row; detect if recipient session already exists (direct delivery mode)
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
        print("[PartnerStream] Linked session row created (source->partner mapping stub)")
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

    # Load context (sender recent messages)
    try:
        current_history = await list_messages_for_session(user_id=user_uuid, session_id=body.session_id, limit=50)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load chat history: {e}")

    # Mode select: if recipient session already linked → direct delivery; else pre-create request
    created_request_id: uuid.UUID | None = None
    if recipient_session_id is None:
        try:
            # Reuse existing pending request for this context if it exists
            existing_req = await get_latest_pending_for_context(
                relationship_id=relationship_id,
                sender_user_id=user_uuid,
                recipient_user_id=partner_user_id,
                sender_session_id=body.session_id,
            )
            if existing_req:
                try:
                    # If an attached recipient session already exists on the request, prefer direct mode
                    existing_recipient_sid = existing_req.get("recipient_session_id")
                    if existing_recipient_sid:
                        try:
                            recipient_session_id = uuid.UUID(existing_recipient_sid)
                        except Exception:
                            recipient_session_id = None
                    # Update the request's preview content with the latest message
                    await update_content(request_id=uuid.UUID(existing_req["id"]), content=body.message.strip())
                except Exception:
                    pass
                created_request_id = uuid.UUID(existing_req["id"])  # type: ignore[index]
                print(f"[PartnerStream] REUSING EXISTING PENDING REQUEST id={created_request_id}")
            else:
                created_req = await create_partner_request(
                    relationship_id=relationship_id,
                    sender_user_id=user_uuid,
                    recipient_user_id=partner_user_id,
                    sender_session_id=body.session_id,
                    content=body.message.strip(),
                )
                created_request_id = uuid.UUID(created_req["id"])  # type: ignore[index]
                print(f"[PartnerStream] REQUEST PRE-CREATED id={created_request_id}")
                # Best-effort APNs notify
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
    else:
        print(f"[PartnerStream] DIRECT MODE recipient_session_id={recipient_session_id}")

    def iter_sse():
        # Anti-buffering prelude
        yield (":" + " " * 2048 + "\n\n").encode()
        parts = []
        final_text = ""
        try:
            # Pass through the already-formatted partner message
            final_text = body.message.strip()
            print(f"[PartnerStream] GENERATED partner message len={len(final_text)}")
            # Stream the generated text to keep client UX consistent
            if final_text:
                words = final_text.split(' ')
                for i, word in enumerate(words):
                    chunk = word + (' ' if i < len(words) - 1 else '')
                    parts.append(chunk)
                    yield f"event: token\ndata: {json.dumps(chunk)}\n\n".encode()
                    if len(parts) % 16 == 0:
                        print(f"[PartnerStream] tokens emitted so far: {len(parts)}")

            # Deliver based on mode
            final_content = (final_text or "").strip() or body.message.strip()
            if created_request_id is not None:
                # Create recipient session now and attach the partner message to it, while keeping request pending
                try:
                    try:
                        sender_session_row = asyncio.run(get_session_by_id(user_id=user_uuid, session_id=body.session_id))
                        mirrored_title = (sender_session_row or {}).get("title")
                    except Exception:
                        mirrored_title = None
                    new_session = asyncio.run(create_session(user_id=partner_user_id, title=mirrored_title or "New Chat"))
                    recipient_session_id_created = uuid.UUID(new_session["id"])  # type: ignore[index]
                    try:
                        asyncio.run(update_linked_session_partner_session_for_source(
                            relationship_id=relationship_id,
                            source_session_id=body.session_id,
                            partner_session_id=recipient_session_id_created,
                        ))
                    except Exception:
                        pass
                    annotated = json.dumps({
                        "_talktome": {"type": "partner_received", "text": final_content},
                        "body": ""
                    })
                    created = asyncio.run(save_message(
                        user_id=partner_user_id,
                        session_id=recipient_session_id_created,  # type: ignore[arg-type]
                        role="assistant",
                        content=annotated,
                    ))
                    asyncio.run(update_session_last_message(session_id=recipient_session_id_created, content=final_content))  # type: ignore[arg-type]
                    asyncio.run(touch_session(session_id=recipient_session_id_created))  # type: ignore[arg-type]
                    asyncio.run(attach_session_and_message_on_pending(
                        request_id=created_request_id,
                        recipient_session_id=recipient_session_id_created,
                        created_message_id=uuid.UUID(created["id"])  # type: ignore[index]
                    ))
                    try:
                        meta = current_user.get("user_metadata") or {}
                        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
                        asyncio.run(send_partner_message_notification_to_user(
                            recipient_user_id=partner_user_id,
                            session_id=recipient_session_id_created,  # type: ignore[arg-type]
                            preview=final_content,
                            sender_name=sender_name,
                        ))
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[PartnerStream] ATTACH ON PENDING ERROR: {e}")
                    try:
                        asyncio.run(update_content(request_id=created_request_id, content=final_content))
                    except Exception:
                        pass
            else:
                # Direct insert into recipient's personal session
                try:
                    annotated = json.dumps({
                        "_talktome": {"type": "partner_received", "text": final_content},
                        "body": ""
                    })
                    print(f"[PartnerStream] DIRECT MODE: Saving with annotation: {annotated[:100]}...")
                    created = asyncio.run(save_message(
                        user_id=partner_user_id,
                        session_id=recipient_session_id,  # type: ignore[arg-type]
                        role="assistant",
                        content=annotated,
                    ))
                    asyncio.run(update_session_last_message(session_id=recipient_session_id, content=final_content))  # type: ignore[arg-type]
                    asyncio.run(touch_session(session_id=recipient_session_id))  # type: ignore[arg-type]
                    print(f"[PartnerStream] DIRECT DELIVERED message_id={created.get('id')}")
                    try:
                        meta = current_user.get("user_metadata") or {}
                        sender_name = meta.get("full_name") or meta.get("name") or meta.get("display_name")
                        asyncio.run(send_partner_message_notification_to_user(
                            recipient_user_id=partner_user_id,
                            session_id=recipient_session_id,  # type: ignore[arg-type]
                            preview=final_content,
                            sender_name=sender_name,
                        ))
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[PartnerStream] DIRECT DELIVERY ERROR: {e}")
                    yield f"event: error\ndata: {json.dumps(str(e))}\n\n".encode()
                    return

            yield b"event: done\ndata: {}\n\n"
            print("[PartnerStream] DONE sent to client")
        except Exception as e:
            print(f"[PartnerStream] ERROR: {e}")
            yield f"event: error\ndata: {json.dumps(str(e))}\n\n".encode()
        finally:
            print("[PartnerStream] STREAM CLOSED (client disconnect or finished)")

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


