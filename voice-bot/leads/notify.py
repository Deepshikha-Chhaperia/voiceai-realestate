"""
Sales Rep Notification Service (Telegram Bot API).

Sends instant alerts for HOT leads.
Plain text format:
HOT lead: <name> <masked phone>
<score_reason>
Visit: <day time or "not booked">
Summary: <2 sentences>
Call: tel:<phone>   Dashboard: <url>/dashboard/leads/<id>
"""

import os
from typing import Any

import httpx
from loguru import logger


def mask_phone(phone: str) -> str:
    """Masks all but the last 4 digits of a phone number."""
    if not phone:
        return "Unknown"
    clean = phone.strip()
    if len(clean) <= 4:
        return clean
    return "*" * (len(clean) - 4) + clean[-4:]


async def send_telegram_alert(
    *,
    lead_id: str,
    name: str | None,
    phone: str,
    score_reason: str | None,
    visit_info: str | None,
    summary: str | None,
) -> bool:
    """Dispatches a structured alert message to the sales team's Telegram chat."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    base_url = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if not base_url:
        public_host = os.getenv("PUBLIC_HOST", "").strip()
        base_url = f"https://{public_host}" if public_host else "http://localhost:8000"

    display_name = name or "Lead"
    masked = mask_phone(phone)
    reason = score_reason or "High purchase intent"
    visit_str = visit_info or "not booked"
    sum_str = summary or "Engaged with AI voice advisor."

    msg = (
        f"🔥 HOT lead: {display_name} {masked}\n"
        f"📌 {reason}\n"
        f"📅 Visit: {visit_str}\n"
        f"📝 Summary: {sum_str}\n\n"
        f"📞 Call: tel:{phone}\n"
        f"🔗 Dashboard: {base_url}/dashboard/leads/{lead_id}"
    )

    if not token or not chat_id:
        logger.info(
            "Telegram alert (mock - no TELEGRAM_BOT_TOKEN/CHAT_ID configured):\n{}",
            msg,
        )
        return True

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": msg,
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info("Telegram alert sent for lead_id={}", lead_id)
                return True
            else:
                logger.warning(
                    "Telegram alert failed status={}: {}",
                    resp.status_code,
                    resp.text,
                )
                return False
    except Exception as exc:
        logger.error("Failed to dispatch Telegram alert for lead_id={}: {}", lead_id, exc)
        return False
