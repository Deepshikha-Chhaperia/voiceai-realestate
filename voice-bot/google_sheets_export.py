"""Finished-call Sheets export, driven by leads.outbox's durable call_sheet_exports queue.
Existing credentials only. Call-ID lookup reconciles retry-after-timeout before append.
RAW values prevent formula injection from caller text. Verified row readback is the receipt.
Single-process SQLite or PostgreSQL row locks required; Sheets has no atomic unique key.
"""

from __future__ import annotations

import json
import re
import os
from typing import Any

from loguru import logger

SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEETS_SHEET_NAME", "Calls")

# Column order the sheet is appended in. Keep this in sync with the header
# row you put in row 1 of the actual sheet -- this module appends values
# positionally, it does not read or match header names.
COLUMNS = [
    "call_id", "campaign_id", "phone", "customer_name",
    "disposition", "sentiment", "budget", "configuration", "location",
    "site_visit_interest", "main_issue_or_intent", "resolution_summary",
    "avg_voice_latency_ms", "median_voice_latency_ms", "cost_usd", "started_at_iso", "ended_at_iso",
]

_client = None  # lazily built, module-level cache -- avoid re-authenticating every call


def _get_client():
    """Returns an authenticated gspread client, or None if not configured.
    Import of gspread/google-auth is deferred into this function so the
    rest of the codebase doesn't hard-require these packages when Sheets
    export isn't in use."""
    global _client
    if _client is not None:
        return _client
    if not SPREADSHEET_ID:
        return None

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.error(
            "GOOGLE_SHEETS_SPREADSHEET_ID is set but gspread/google-auth "
            "are not installed. Add `gspread` and `google-auth` to "
            "requirements.txt, or unset GOOGLE_SHEETS_SPREADSHEET_ID to "
            "disable Sheets export."
        )
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
    try:
        if creds_json:
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            creds = Credentials.from_service_account_file(
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
            )
        else:
            logger.error(
                "GOOGLE_SHEETS_SPREADSHEET_ID is set but no credentials "
                "found. Set GOOGLE_SHEETS_CREDENTIALS_JSON or "
                "GOOGLE_APPLICATION_CREDENTIALS."
            )
            return None
        _client = gspread.authorize(creds)
        return _client
    except Exception as e:
        logger.error("Failed to authenticate to Google Sheets: {}", e)
        return None


def _row_from(call: dict[str, Any], analysis: dict[str, Any]) -> list[Any]:
    lead_fields = call.get("lead_fields")
    lead_fields = json.loads(lead_fields) if isinstance(lead_fields, str) else (lead_fields or {})

    import datetime as _dt

    def _iso(ts: float | None) -> str:
        return _dt.datetime.fromtimestamp(ts).isoformat(timespec="seconds") if ts else ""

    values = {
        "call_id": call.get("call_id", ""),
        "campaign_id": call.get("campaign_id", ""),
        "phone": call.get("phone", ""),
        "customer_name": call.get("customer_name", ""),
        "disposition": call.get("disposition", ""),
        "sentiment": analysis.get("sentiment", ""),
        "budget": lead_fields.get("budget", analysis.get("budget", "")),
        "configuration": lead_fields.get("configuration", analysis.get("configuration", "")),
        "location": lead_fields.get("location", analysis.get("location", "")),
        "site_visit_interest": lead_fields.get("site_visit_interest", ""),
        "main_issue_or_intent": analysis.get("main_issue_or_intent", ""),
        "resolution_summary": analysis.get("resolution_summary", ""),
        "avg_voice_latency_ms": call.get("avg_voice_latency_ms", ""),
        "median_voice_latency_ms": call.get("median_voice_latency_ms", ""),
        "cost_usd": call.get("cost_usd", ""),
        "started_at_iso": _iso(call.get("started_at")),
        "ended_at_iso": _iso(call.get("ended_at")),
    }
    return [values[c] for c in COLUMNS]


def append_call_row_verified(call: dict[str, Any], analysis: dict[str, Any] | None = None) -> dict:
    call_id = str(call.get("call_id") or "")
    if not call_id:
        raise ValueError("Missing call_id")
    client = _get_client()
    if client is None:
        raise RuntimeError("Sheets not configured or credentials unavailable; inspect authentication log")
    import gspread
    sheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = sheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=len(COLUMNS))
        worksheet.append_row(COLUMNS, value_input_option="RAW")
    if worksheet.row_values(1) != COLUMNS:
        raise RuntimeError("Calls sheet header/order mismatch; expected " + repr(COLUMNS))
    ids = worksheet.col_values(1)
    if call_id in ids:
        row = ids.index(call_id) + 1
        values = worksheet.row_values(row)
        if not values or values[0] != call_id:
            raise RuntimeError("Existing row readback failed")
        return {"call_id": call_id, "row": row, "status": "already_present", "verified": True}
    # If append times out, the next attempt looks up call_id before appending again.
    response = worksheet.append_row(_row_from(call, analysis or {}), value_input_option="RAW")
    updated_range = (response or {}).get("updates", {}).get("updatedRange")
    if not updated_range:
        raise RuntimeError("Append response missing updatedRange; reconcile call_id on retry")
    # Worksheet.get qualifies its own title. API updatedRange already has one.
    relative_range = updated_range.rsplit("!", 1)[-1]
    if not re.fullmatch(r"[A-Z]+[1-9]\d*(?::[A-Z]+[1-9]\d*)?", relative_range):
        raise RuntimeError("Invalid append updatedRange; reconcile call_id on retry")
    values = worksheet.get(relative_range)
    if not values or not values[0] or str(values[0][0]) != call_id:
        raise RuntimeError("Appended row readback failed; reconcile call_id on retry")
    return {"call_id": call_id, "range": updated_range, "status": "appended", "verified": True}


def append_call_row(call: dict[str, Any], analysis: dict[str, Any] | None = None) -> bool:
    try:
        receipt = append_call_row_verified(call, analysis)
        logger.info("[{}] SHEETS_VERIFIED {}", call.get("call_id"), receipt)
        return True
    except Exception as exc:
        logger.error("[{}] Sheets export failed: {}", call.get("call_id"), exc)
        return False


async def export_call_to_sheet(call: dict[str, Any], analysis: dict[str, Any] | None = None) -> bool:
    """Async wrapper for append_call_row to satisfy outbox calls."""
    import asyncio
    return await asyncio.to_thread(append_call_row, call, analysis)

