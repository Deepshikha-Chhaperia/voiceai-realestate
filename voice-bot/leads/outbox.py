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
                if isinstance(it.payload, dict) and it.payload.get("call_id") == call_id:
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
                if isinstance(it.payload, dict) and it.payload.get("call_id") == call_id:
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

    _outbox_worker_running = True

    async def _worker_loop():
        global _outbox_worker_running
        logger.info("CRM/WhatsApp outbox worker started (drain_outbox active)")
        while _outbox_worker_running:
            try:
                await drain_outbox()
            except Exception as e:
                logger.debug("Outbox worker loop error: {}", e)
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


