"""
Outbox Worker for CRM and Google Sheets Synchronization.

Drains outbox table items with exponential backoff and error tracking.
"""

from datetime import datetime, timedelta
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
    """Inserts a new pending outbox item."""
    if session is not None:
        item = OutboxItem(
            lead_id=lead_id,
            target=target,
            payload=payload,
            status="pending",
            attempts=0,
            created_at=datetime.utcnow(),
        )
        session.add(item)
        return item

    async with get_session() as sess:
        item = OutboxItem(
            lead_id=lead_id,
            target=target,
            payload=payload,
            status="pending",
            attempts=0,
            created_at=datetime.utcnow(),
        )
        sess.add(item)
        await sess.flush()
        return item


async def drain_outbox() -> int:
    """Processes pending outbox items."""
    now = datetime.utcnow()
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
                if item.target == "sheets":
                    # Use existing google_sheets_export if configured
                    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
                    if spreadsheet_id:
                        import google_sheets_export
                        import inspect
                        call_dict = item.payload or {}
                        analysis_dict = call_dict.get("analysis")
                        if hasattr(google_sheets_export, "export_call_to_sheet"):
                            fn = getattr(google_sheets_export, "export_call_to_sheet")
                            res = fn(call_dict, analysis_dict)
                            if inspect.iscoroutine(res):
                                await res
                        elif hasattr(google_sheets_export, "append_call_row"):
                            import asyncio
                            await asyncio.to_thread(google_sheets_export.append_call_row, call_dict, analysis_dict)
                    else:
                        logger.debug("Sheets outbox: no GOOGLE_SHEETS_SPREADSHEET_ID configured; marked as simulated success")
                    success = True
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
                        logger.debug("CRM Webhook outbox: no valid CRM_WEBHOOK_URL configured; marked as simulated success")
                        success = True
                else:
                    success = True
            except Exception as exc:
                error_msg = str(exc)
                logger.error("Outbox item {} failed on attempt {}: {}", item.id, item.attempts, exc)

            if success:
                item.status = "done"
                item.last_error = None
                processed_count += 1
            else:
                item.last_error = error_msg
                if item.attempts >= 5:
                    item.status = "failed"
                else:
                    backoff_mins = 2 ** item.attempts
                    item.next_attempt_at = now + timedelta(minutes=backoff_mins)

    return processed_count
