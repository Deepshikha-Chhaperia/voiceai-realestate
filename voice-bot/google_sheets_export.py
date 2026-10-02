"""
Google Sheets export -- additive module, called from call_analytics.py's
existing _push_to_crm() hook, not a replacement for it.

WHY A DEDICATED MODULE INSTEAD OF JUST THE GENERIC CRM_WEBHOOK_URL: you
asked specifically for "a google sheet", not "a webhook that could point
somewhere". A generic webhook still needs something on the other end
(Zapier, Make, your own receiver) to actually land rows in a Sheet -- this
module talks to the Sheets API directly, so a spreadsheet ID and a service
account are the entire dependency, nothing else to stand up.

HOW AUTH WORKS, and why this is fine on any cloud:
  A Google service account is just a JSON key file / JSON blob -- not tied
  to AWS, Azure, or GCP in any way. Two ways to supply it, pick whichever
  fits your deployment:
    - GOOGLE_SHEETS_CREDENTIALS_JSON: the full JSON key content, as an
      environment variable. Cleanest for containers (ECS/Cloud Run/Azure
      Container Apps task definitions, k8s secrets, etc.) -- no file to
      mount.
    - GOOGLE_APPLICATION_CREDENTIALS: a file path, standard Google-auth
      convention, for when you'd rather mount a secret as a file.
  Share the target Google Sheet with that service account's email address
  (found inside the JSON key as `client_email`) with Editor access -- the
  API call fails otherwise, and that failure is caught and logged, not
  swallowed silently.

LOCAL / DEMO BEHAVIOR: if GOOGLE_SHEETS_SPREADSHEET_ID isn't set, every
call below is a fast no-op that logs once and returns -- the rest of the
pipeline (including the generic CRM_WEBHOOK_URL push) is completely
unaffected. This is the same "optional, degrades to a no-op" pattern
CRM_WEBHOOK_URL already uses.

DISPOSITION: included as its own column, and this is the ONE place a
freshly-computed disposition from post-call analysis becomes visible to a
human outside the system -- see call_analytics.py, which only ever
*upgrades* a null disposition from analysis, never overwrites one the live
call already set via the enum-enforced set_disposition().

IDEMPOTENCY: guarded by lead_state's existing crm_pushed flag (set by the
caller in call_analytics.py) -- this module itself does not check it, to
keep it a plain "append this row" primitive; the check belongs where the
decision to call it is made.
"""

from __future__ import annotations

import json
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


def append_call_row(call: dict[str, Any], analysis: dict[str, Any] | None = None) -> bool:
    """Appends one row for a finished call. Returns True on success, False
    on any failure (including 'not configured') -- never raises, since a
    Sheets outage must not be able to affect call teardown or crash the
    fire-and-forget analytics task that calls this."""
    client = _get_client()
    if client is None:
        return False

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        try:
            worksheet = sheet.worksheet(SHEET_NAME)
        except Exception:
            # First run: sheet tab doesn't exist yet -- create it with a
            # header row rather than fail. Safe to run every time; gspread
            # raises (caught above) if it already exists.
            worksheet = sheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=len(COLUMNS))
            worksheet.append_row(COLUMNS)

        worksheet.append_row(_row_from(call, analysis or {}), value_input_option="USER_ENTERED")
        logger.info("[{}] Appended row to Google Sheet", call.get("call_id"))
        return True
    except Exception as e:
        logger.warning("[{}] Google Sheets append failed: {}", call.get("call_id"), e)
        return False


async def export_call_to_sheet(call: dict[str, Any], analysis: dict[str, Any] | None = None) -> bool:
    """Async wrapper for append_call_row to satisfy outbox calls."""
    import asyncio
    return await asyncio.to_thread(append_call_row, call, analysis)

