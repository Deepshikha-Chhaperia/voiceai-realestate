"""
Meta WhatsApp Cloud API integration for location and site visit dispatch.

Sends site visit location and booking details directly to caller via Meta Graph API.
Never logs the access token. Enforces strict timeouts and single-retry resilience.
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any

import httpx
from loguru import logger


def normalize_phone_e164(phone: str) -> str:
    """Normalize phone number to international E.164 format (+91...)."""
    if not phone:
        return ""
    clean = re.sub(r"[^\d+]", "", phone.strip())
    if clean.startswith("+"):
        return clean
    if len(clean) == 10:
        return f"+91{clean}"
    if len(clean) == 12 and clean.startswith("91"):
        return f"+{clean}"
    return f"+{clean}" if clean else ""


async def send_whatsapp_location(
    to_phone: str,
    *,
    visit_date: str,
    visit_time: str,
    project_name: str = "Meridian Crest",
    client_name: str = "Valued Customer",
    **kwargs: Any,
) -> dict[str, Any]:
    """Sends WhatsApp location and booking confirmation via Meta Cloud API.

    Returns dict with status: 'sent', 'failed', or 'not_configured'.
    Never raises an uncaught exception.
    """
    token = os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
    phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
    api_version = os.getenv("WHATSAPP_API_VERSION", "v21.0").strip()
    template_name = os.getenv("WHATSAPP_TEMPLATE_NAME", "").strip()
    template_lang = os.getenv("WHATSAPP_TEMPLATE_LANG", "en_US").strip()
    location_url = os.getenv("SITE_LOCATION_URL", "https://maps.google.com/?q=Meridian+Crest+Noida").strip()
    test_recipient = os.getenv("WHATSAPP_TEST_RECIPIENT", "").strip()

    recipient = normalize_phone_e164(test_recipient or to_phone)
    # Strip leading '+' for Meta Cloud API
    meta_recipient = recipient.lstrip("+")

    if not token or not phone_number_id:
        logger.warning(
            "WhatsApp sender: WHATSAPP_ACCESS_TOKEN or WHATSAPP_PHONE_NUMBER_ID missing; status=not_configured"
        )
        return {
            "status": "not_configured",
            "ok": False,
            "error": "WhatsApp credentials not configured in environment",
            "recipient": recipient,
        }

    url = f"https://graph.facebook.com/{api_version}/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if template_name:
        # Template message dispatch
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": meta_recipient,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": template_lang},
            },
        }
        # If not the default hello_world template, add parameters
        if template_name != "hello_world":
            payload["template"]["components"] = [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": client_name},
                        {"type": "text", "text": project_name},
                        {"type": "text", "text": visit_date},
                        {"type": "text", "text": visit_time},
                        {"type": "text", "text": location_url},
                    ],
                }
            ]
    else:
        # Standard interactive text message
        text_body = (
            f"Hello {client_name}!\n\n"
            f"Your site visit to *{project_name}* is booked for *{visit_date}* at *{visit_time}*.\n\n"
            f"📍 *Site Location & Directions:*\n{location_url}\n\n"
            f"Our sales manager will be awaiting your arrival. We look forward to hosting you!"
        )
        payload = {
            "messaging_product": "whatsapp",
            "to": meta_recipient,
            "type": "text",
            "text": {"preview_url": True, "body": text_body},
        }

    # Attempt with 1.5s timeout and 1 retry
    last_error: str | None = None
    last_error_code: int | None = None
    for attempt in range(1, 3):
        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code in (200, 201):
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}
                    msg_id = None
                    if isinstance(data, dict) and "messages" in data and len(data["messages"]) > 0:
                        msg_id = data["messages"][0].get("id")
                    if msg_id:
                        logger.info(
                            "WhatsApp sent successfully to {} (attempt={}, msg_id={})",
                            recipient,
                            attempt,
                            msg_id,
                        )
                        return {
                            "status": "sent",
                            "ok": True,
                            "message_id": msg_id,
                            "recipient": recipient,
                        }
                    else:
                        last_error = f"HTTP {resp.status_code}: missing message_id in Meta response"
                        last_error_code = None
                        logger.warning(
                            "WhatsApp API call attempt {} returned {} without message_id for {}: {}",
                            attempt,
                            resp.status_code,
                            recipient,
                            data,
                        )
                else:
                    try:
                        err_json = resp.json()
                        if isinstance(err_json, dict) and "error" in err_json:
                            err_info = err_json["error"]
                            last_error_code = err_info.get("code")
                            if last_error_code == 131031:
                                last_error = f"Meta error 131031 (business account locked): {err_info.get('message')}"
                            else:
                                last_error = f"Meta error {last_error_code}: {err_info.get('message')}"
                        else:
                            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    except Exception:
                        last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    if resp.status_code in (401, 403):
                        last_error = f"Authentication error (HTTP {resp.status_code}): {last_error}"
                    logger.warning(
                        "WhatsApp API call attempt {} failed for {}: {}",
                        attempt,
                        recipient,
                        last_error,
                    )
        except Exception as exc:
            last_error = str(exc)
            logger.warning(
                "WhatsApp API call attempt {} exception for {}: {}",
                attempt,
                recipient,
                exc,
            )

        if attempt == 1:
            await asyncio.sleep(0.2)

    return {
        "status": "failed",
        "ok": False,
        "error": last_error,
        "error_code": last_error_code,
        "message_id": None,
        "recipient": recipient,
    }
