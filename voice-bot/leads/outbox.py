"""
Outbox Worker for CRM and Google Sheets Synchronization.

Drains outbox table items with exponential backoff and error tracking.
"""

from datetime import datetime, timedelta, timezone
import os
from typing import Any

from loguru import logger
from sqlalchemy import select

from leads.db import get_session
from leads.models import OutboxItem


async def queue_outbox_item(
    lead_id: Any,
    target: str,
    payload: dict[str, Any],
    session: Any = None,
) -> OutboxItem:
    """Inserts a new pending outbox item with call_id idempotency."""
    now = datetime.now(timezone.utc)
    call_id = payload.get("call_id") if isinstance(payload, dict) else None

    if session is not None:
        if call_id:
            existing = (await session.execute(
                select(OutboxItem).where(OutboxItem.target == target)
            )).scalars().all()
            for it in existing:
                if isinstance(it.payload, dict) and it.payload.get("call_id") == call_id and it.payload.get("action", "location") == payload.get("action", "location"):
                    logger.info("queue_outbox_item: idempotent skip for target={} call_id={}", target, call_id)
                    return it

        item = OutboxItem(
            lead_id=lead_id,
            target=target,
            payload=payload,
            status="pending",
            attempts=0,
            created_at=now,
        )
        session.add(item)
        return item

    async with get_session() as sess:
        if call_id:
            existing = (await sess.execute(
                select(OutboxItem).where(OutboxItem.target == target)
            )).scalars().all()
            for it in existing:
                if isinstance(it.payload, dict) and it.payload.get("call_id") == call_id and it.payload.get("action", "location") == payload.get("action", "location"):
                    logger.info("queue_outbox_item: idempotent skip for target={} call_id={}", target, call_id)
                    return it

        item = OutboxItem(
            lead_id=lead_id,
            target=target,
            payload=payload,
            status="pending",
            attempts=0,
            created_at=now,
        )
        sess.add(item)
        await sess.flush()
        return item


_outbox_worker_task: Any = None
_outbox_worker_running: bool = False


def is_outbox_worker_running() -> bool:
    """Returns True if the background outbox worker loop is actively running."""
    global _outbox_worker_running, _outbox_worker_task
    return _outbox_worker_running and _outbox_worker_task is not None and not _outbox_worker_task.done()


async def start_outbox_worker() -> None:
    """Starts the background loop to drain outbox periodically."""
    global _outbox_worker_task, _outbox_worker_running
    if is_outbox_worker_running():
        return

    from leads.db import ensure_call_sheet_schema
    await ensure_call_sheet_schema()
    _outbox_worker_running = True

    async def _worker_loop():
        global _outbox_worker_running
        logger.info("CRM/WhatsApp outbox worker started (drain_outbox active)")
        while _outbox_worker_running:
            try:
                try:
                    await drain_call_sheet_exports()
                except Exception as exc:
                    logger.error("Sheets worker error (other outbox continues): {}", exc)
                await drain_outbox()
            except Exception as e:
                logger.error("Outbox worker loop error: {}", e)
            await asyncio.sleep(5)

    import asyncio
    _outbox_worker_task = asyncio.create_task(_worker_loop())


async def stop_outbox_worker() -> None:
    global _outbox_worker_running, _outbox_worker_task
    _outbox_worker_running = False
    if _outbox_worker_task and not _outbox_worker_task.done():
        _outbox_worker_task.cancel()
        _outbox_worker_task = None


async def drain_outbox() -> int:
    """Processes pending outbox items without fake simulated success."""
    now = datetime.now(timezone.utc)
    processed_count = 0

    async with get_session() as session:
        stmt = (
            select(OutboxItem)
            .where(
                OutboxItem.status == "pending",
                (OutboxItem.next_attempt_at == None) | (OutboxItem.next_attempt_at <= now),  # noqa: E711
            )
            .limit(50)
        )
        res = await session.execute(stmt)
        items = res.scalars().all()

        for item in items:
            item.attempts += 1
            success = False
            error_msg = None

            try:
                if item.target == "whatsapp":
                    from services.whatsapp_sender import send_whatsapp_location
                    payload = item.payload or {}
                    to_phone = payload.get("phone", "")
                    v_date = payload.get("visit_date_iso") or payload.get("visit_date", "")
                    v_time = payload.get("time_slot") or payload.get("visit_time", "")
                    c_name = payload.get("name") or payload.get("client_name", "Valued Customer")
                    if payload.get("action") == "send_brochure":
                        from call_repairs import dispatch_brochure
                        res_wa = await dispatch_brochure(payload)
                    else:
                        res_wa = await send_whatsapp_location(
                            to_phone,
                            visit_date=v_date,
                            visit_time=v_time,
                            client_name=c_name,
                        )
                    if res_wa.get("ok") and res_wa.get("message_id"):
                        success = True
                        item.payload = {**payload, "message_id": res_wa.get("message_id")}
                    else:
                        success = False
                        error_msg = res_wa.get("error") or "WhatsApp send failed or unconfigured"
                        if res_wa.get("status") == "uncertain":
                            error_msg = "outcome unknown; manual reconciliation required"

                elif item.target == "sheets":
                    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
                    if spreadsheet_id:
                        import google_sheets_export
                        import inspect
                        call_dict = item.payload or {}
                        analysis_dict = call_dict.get("analysis")
                        res_sh = False
                        if hasattr(google_sheets_export, "export_call_to_sheet"):
                            fn = getattr(google_sheets_export, "export_call_to_sheet")
                            res_sh = fn(call_dict, analysis_dict)
                            if inspect.iscoroutine(res_sh):
                                res_sh = await res_sh
                        elif hasattr(google_sheets_export, "append_call_row"):
                            import asyncio
                            res_sh = await asyncio.to_thread(google_sheets_export.append_call_row, call_dict, analysis_dict)
                        if res_sh:
                            success = True
                        else:
                            success = False
                            error_msg = "Google Sheets export failed or unconfigured credentials"
                    else:
                        error_msg = "GOOGLE_SHEETS_SPREADSHEET_ID not configured; pending_not_configured"
                        success = False

                elif item.target == "webhook":
                    raw_url = os.getenv("CRM_WEBHOOK_URL", "")
                    webhook_url = raw_url.split("#")[0].strip()
                    if webhook_url and (webhook_url.startswith("http://") or webhook_url.startswith("https://")):
                        import httpx
                        async with httpx.AsyncClient(timeout=10.0) as client:
                            resp = await client.post(webhook_url, json=item.payload)
                            if resp.status_code < 400:
                                success = True
                            else:
                                error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    else:
                        error_msg = "CRM_WEBHOOK_URL not configured; pending_not_configured"
                        success = False
                else:
                    error_msg = f"Unknown outbox target: {item.target}"
                    success = False

            except Exception as exc:
                error_msg = str(exc)
                logger.error("Outbox item {} failed on attempt {}: {}", item.id, item.attempts, exc)

            if success:
                item.status = "done"
                item.last_error = None
                processed_count += 1
            else:
                item.last_error = error_msg
                is_permanent = (
                    item.attempts >= 5
                    or "outcome unknown" in (error_msg or "").lower()
                    or "not configured" in (error_msg or "").lower()
                    or "131031" in (error_msg or "")
                    or "authentication" in (error_msg or "").lower()
                    or "oauth" in (error_msg or "").lower()
                    or "401" in (error_msg or "")
                    or "403" in (error_msg or "")
                )
                if is_permanent:
                    item.status = "failed"
                else:
                    backoff_mins = 2 ** item.attempts
                    item.next_attempt_at = now + timedelta(minutes=backoff_mins)

    return processed_count


async def update_outbox_whatsapp_delivery_status(
    message_id: str,
    status: str,
    error_info: str | None = None,
) -> bool:
    """Updates OutboxItem and SiteVisit status when a delivery report webhook arrives from Meta."""
    from leads.models import SiteVisit
    async with get_session() as session:
        # Update OutboxItem
        res = await session.execute(
            select(OutboxItem).where(OutboxItem.target == "whatsapp")
        )
        items = res.scalars().all()
        updated = False
        for item in items:
            if isinstance(item.payload, dict) and item.payload.get("message_id") == message_id:
                if status == "failed":
                    item.status = "failed"
                    item.last_error = error_info or "Meta delivery failed"
                elif status in ("delivered", "read", "sent"):
                    item.status = "done"
                updated = True

        # Update SiteVisit
        res_sv = await session.execute(
            select(SiteVisit).where(SiteVisit.whatsapp_message_id == message_id)
        )
        sv = res_sv.scalars().first()
        if sv:
            sv.whatsapp_status = status
            updated = True

        return updated




async def queue_call_sheet_export(call_id: str) -> None:
    """Persist without scoring/Redis or a Lead; primary key enforces queue idempotency."""
    from leads.models import CallSheetExport
    from leads.db import ensure_call_sheet_schema
    await ensure_call_sheet_schema()
    from sqlalchemy.exc import IntegrityError
    try:
        async with get_session() as session:
            if await session.get(CallSheetExport, call_id) is None:
                session.add(CallSheetExport(call_id=call_id))
                await session.flush()
        logger.info("[{}] SHEETS_QUEUED (durable finished-call export)", call_id)
    except IntegrityError:
        logger.info("[{}] SHEETS_QUEUE_EXISTS", call_id)


_sheet_drain_lock = None


async def drain_call_sheet_exports() -> int:
    import asyncio
    import lead_state
    import google_sheets_export
    from leads.models import CallSheetExport
    global _sheet_drain_lock
    if _sheet_drain_lock is None:
        _sheet_drain_lock = asyncio.Lock()
    if _sheet_drain_lock.locked():
        return 0
    done = 0
    async with _sheet_drain_lock:
        async with get_session() as session:
            now = datetime.now(timezone.utc)
            # PostgreSQL workers claim rows using DB locks. SQLite supported single-process only.
            rows = (await session.execute(select(CallSheetExport).where(
                CallSheetExport.status == "pending",
                (CallSheetExport.next_attempt_at == None) | (CallSheetExport.next_attempt_at <= now),
            ).with_for_update(skip_locked=True).limit(20))).scalars().all()
            for job in rows:
                job.attempts += 1
                try:
                    call = await lead_state.get_call_async(job.call_id)
                    if not call:
                        raise RuntimeError("Call record unavailable")
                    analysis = call.get("analysis") or call.get("analysis_json") or {}
                    if isinstance(analysis, str):
                        import json
                        analysis = json.loads(analysis)
                    receipt = await asyncio.to_thread(google_sheets_export.append_call_row_verified, call, analysis)
                    if not receipt.get("verified"):
                        raise RuntimeError("Sheets export returned no verified receipt")
                    job.receipt = receipt
                    job.status = "done"
                    job.last_error = None
                    done += 1
                    logger.info("[{}] SHEETS_VERIFIED {}", job.call_id, receipt)
                except Exception as exc:
                    job.last_error = str(exc)
                    if job.attempts >= 5:
                        job.status = "failed"
                    else:
                        job.next_attempt_at = now + timedelta(minutes=2 ** job.attempts)
                    logger.error("[{}] SHEETS_EXPORT_FAILED attempt={} status={} error={}", job.call_id, job.attempts, job.status, exc)
    return done
