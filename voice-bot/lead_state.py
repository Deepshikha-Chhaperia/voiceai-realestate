"""
Persistent storage for call metadata, captured lead information, and metrics.

Supports SQLite (default) and PostgreSQL (via DATABASE_URL).
Provides non-blocking async wrappers (asyncio.to_thread) for the live call path.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Iterator

from loguru import logger

_DEFAULT_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs", "calls.db")
_raw_db_path = os.getenv("LEAD_DB_PATH", _DEFAULT_DB_FILE)
DB_PATH = _raw_db_path if os.path.isabs(_raw_db_path) else os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), _raw_db_path))
DATABASE_URL = os.getenv("DATABASE_URL")  # e.g. postgresql://user:pass@host:5432/db

_BACKEND = "sqlite"
if DATABASE_URL:
    try:
        import psycopg  # psycopg3; add `psycopg[binary]` to requirements to use this path
        _BACKEND = "postgres"
    except ImportError:
        raise RuntimeError(
            "DATABASE_URL is set but psycopg is not installed. "
            "Add `psycopg[binary]` to requirements.txt, or unset DATABASE_URL "
            "to fall back to the zero-config SQLite backend."
        )


class Disposition(str, Enum):
    QUALIFIED = "QUALIFIED"
    SITE_VISIT_REQUESTED = "SITE_VISIT_REQUESTED"
    SITE_VISIT_BOOKED = "SITE_VISIT_BOOKED"
    CALLBACK_REQUESTED = "CALLBACK_REQUESTED"
    NOT_INTERESTED = "NOT_INTERESTED"
    WRONG_NUMBER = "WRONG_NUMBER"
    DNC_REQUESTED = "DNC_REQUESTED"
    NO_RESPONSE = "NO_RESPONSE"
    INCOMPLETE = "INCOMPLETE"
    ESCALATED_TO_HUMAN = "ESCALATED_TO_HUMAN"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"


LEAD_FIELDS = {
    "name", "phone", "location", "property_type", "configuration",
    "budget", "purpose", "timeline", "preferred_project",
    "site_visit_interest", "preferred_visit_date", "preferred_visit_time",
    "spoken_name", "registered_name", "identity_discrepancy", "site_visit", "whatsapp",
}

_lock = threading.RLock()  # Reentrant lock: safe for nested connection & init calls across threads
_db_initialized = False


@contextmanager
def _sqlite_conn() -> Iterator[sqlite3.Connection]:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


_pg_pool = None

@contextmanager
def _pg_conn():
    global _pg_pool
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
    if _pg_pool is None:
        _pg_pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, kwargs={"row_factory": dict_row})
    with _pg_pool.connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _raw_conn():
    return _pg_conn() if _BACKEND == "postgres" else _sqlite_conn()


def _conn():
    global _db_initialized
    if not _db_initialized:
        init_db()
    return _raw_conn()


def _ph(n: int) -> str:
    """Placeholder style differs: sqlite uses '?', postgres uses '%s'."""
    mark = "%s" if _BACKEND == "postgres" else "?"
    return ", ".join([mark] * n)



# Every column ever added to `calls`, with its SQL type -- the single
# source of truth both CREATE TABLE and the migration step below read
# from, so a new column only ever needs to be added in one place.
_COLUMNS: dict[str, str] = {
    "call_id": "TEXT PRIMARY KEY",
    "lead_id": "TEXT",
    "campaign_id": "TEXT",
    "provider_call_id": "TEXT",
    "phone": "TEXT",
    "customer_name": "TEXT",
    "started_at": "REAL",
    "ended_at": "REAL",
    "conversation_stage": "TEXT DEFAULT 'call_started'",
    "disposition": "TEXT",
    "lead_fields": "TEXT DEFAULT '{}'",
    "analysis_json": "TEXT",
    "crm_pushed": "INTEGER DEFAULT 0",
    "avg_voice_latency_ms": "REAL",
    "median_voice_latency_ms": "REAL",
    "p90_voice_latency_ms": "REAL",
    "avg_llm_ttft_ms": "REAL",
    "avg_tts_ttfa_ms": "REAL",
    "turns": "INTEGER",
    "cost_usd": "REAL",
    "cost_breakdown_json": "TEXT",
    "stt_provider": "TEXT",
    "llm_provider": "TEXT",
    "tts_provider": "TEXT",
}


def init_db() -> None:
    """Call once at process startup (main.py lifespan) or lazily on first access."""
    global _db_initialized
    with _lock, _raw_conn() as conn:
        cur = conn.cursor()
        col_defs = ",\n".join(f"{name} {sql_type}" for name, sql_type in _COLUMNS.items())
        type_map = (lambda s: s.replace("INTEGER DEFAULT", "INTEGER DEFAULT")) if _BACKEND == "sqlite" else (lambda s: s.replace("REAL", "DOUBLE PRECISION"))
        cur.execute(f"CREATE TABLE IF NOT EXISTS calls (\n{type_map(col_defs)}\n)")

        if _BACKEND == "postgres":
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'calls'"
            )
            existing = {row["column_name"] for row in cur.fetchall()}
        else:
            cur.execute("PRAGMA table_info(calls)")
            existing = {row["name"] for row in cur.fetchall()}

        for name, sql_type in _COLUMNS.items():
            if name in existing:
                continue
            add_type = sql_type.replace("PRIMARY KEY", "").strip()
            if _BACKEND == "postgres":
                add_type = add_type.replace("REAL", "DOUBLE PRECISION")
            logger.info("[lead_state] Migrating calls table: adding column {} {}", name, add_type)
            cur.execute(f"ALTER TABLE calls ADD COLUMN {name} {add_type}")
        _db_initialized = True



def upsert_call(
    call_id: str,
    *,
    campaign_id: str | None = None,
    provider_call_id: str | None = None,
    phone: str | None = None,
    customer_name: str | None = None,
    lead_id: str | None = None,
) -> None:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        if _BACKEND == "postgres":
            cur.execute(
                """
                INSERT INTO calls (call_id, campaign_id, provider_call_id, phone, customer_name, lead_id, started_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (call_id) DO UPDATE SET
                    campaign_id = COALESCE(EXCLUDED.campaign_id, calls.campaign_id),
                    provider_call_id = COALESCE(EXCLUDED.provider_call_id, calls.provider_call_id),
                    phone = COALESCE(EXCLUDED.phone, calls.phone),
                    customer_name = COALESCE(EXCLUDED.customer_name, calls.customer_name),
                    lead_id = COALESCE(EXCLUDED.lead_id, calls.lead_id)
                """,
                (call_id, campaign_id, provider_call_id, phone, customer_name, lead_id, time.time()),
            )
        else:
            cur.execute(
                """
                INSERT INTO calls (call_id, campaign_id, provider_call_id, phone, customer_name, lead_id, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(call_id) DO UPDATE SET
                    campaign_id = COALESCE(excluded.campaign_id, calls.campaign_id),
                    provider_call_id = COALESCE(excluded.provider_call_id, calls.provider_call_id),
                    phone = COALESCE(excluded.phone, calls.phone),
                    customer_name = COALESCE(excluded.customer_name, calls.customer_name),
                    lead_id = COALESCE(excluded.lead_id, calls.lead_id)
                """,
                (call_id, campaign_id, provider_call_id, phone, customer_name, lead_id, time.time()),
            )


def record_field(call_id: str, field: str, value: Any) -> dict[str, Any]:
    if field not in LEAD_FIELDS:
        return {}
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT lead_fields FROM calls WHERE call_id = {_ph(1)}", (call_id,))
        row = cur.fetchone()
        raw = row["lead_fields"] if row else None
        current = json.loads(raw) if raw else {}
        current[field] = value
        cur.execute(
            f"UPDATE calls SET lead_fields = {_ph(1)} WHERE call_id = {_ph(1)}",
            (json.dumps(current), call_id),
        )
        return current


def record_fields(call_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    valid_updates = {k: v for k, v in fields.items() if k in LEAD_FIELDS}
    if not valid_updates:
        return {}
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT lead_fields FROM calls WHERE call_id = {_ph(1)}", (call_id,))
        row = cur.fetchone()
        raw = row["lead_fields"] if row else None
        current = json.loads(raw) if raw else {}
        current.update(valid_updates)
        cur.execute(
            f"UPDATE calls SET lead_fields = {_ph(1)} WHERE call_id = {_ph(1)}",
            (json.dumps(current), call_id),
        )
        return current


def infer_deterministic_disposition(
    lead_memory: dict[str, Any] | None = None,
    messages: list[dict] | None = None,
) -> str:
    """Zero-latency (<0.1ms), deterministic disposition inference.
    
    Guarantees calls always have an accurate, meaningful disposition
    (SITE_VISIT_BOOKED, QUALIFIED, NOT_INTERESTED, etc.) without
    relying on slow, non-deterministic external LLMs.
    """
    mem = lead_memory or {}
    transcript_text = ""
    user_turns = 0
    user_lines = []
    agent_lines = []
    if messages:
        dialog_lines = []
        for m in messages:
            role = m.get("role")
            content = m.get("content")
            if isinstance(content, list):
                content = " ".join(part.get("text", "") for part in content if isinstance(part, dict))
            if isinstance(content, str) and content.strip():
                if role == "user":
                    user_turns += 1
                    user_lines.append(content.lower())
                    dialog_lines.append(f"user: {content.lower()}")
                elif role == "assistant":
                    agent_lines.append(content.lower())
                    dialog_lines.append(f"agent: {content.lower()}")
        transcript_text = " ".join(dialog_lines)

    user_text = " ".join(user_lines)

    # 1. Do Not Call
    if any(k in user_text for k in ("do not call", "stop calling", "remove my number", "dnc", "don't call again")):
        return Disposition.DNC_REQUESTED.value

    # 2. Wrong Number
    if any(k in user_text for k in ("wrong number", "wrong person")):
        return Disposition.WRONG_NUMBER.value

    # 3. Explicit Refusal / Not Interested
    _REFUSAL_RE = re.compile(
        r"\bnot\s+(?:at\s+all\s+|that\s+|really\s+|so\s+|much\s+|very\s+)?interested\b|"
        r"\b(?:don'?t|dont)\s+(?:feel\s+interested|want\s+any|want\s+to\s+buy|want\s+this|need)\b|"
        r"\bnot\s+(?:looking|ready|buying|for\s+me)\b|"
        r"\bno\s+(?:interest|need|thanks?|requirement)\b|"
        r"\bnot\s+now\b|"
        r"\b(?:nahi|nahin|na)\s+(?:chahiye|lena|interest|dekh\s+rahe)\b|"
        r"\binterest\s+nahi\b|"
        r"\bkuch\s+nahi\b|"
        r"\b(?:not|nahi|nahin|na)\b.*?\b(?:happening|lively|vibrant|social)\b|"
        r"\b(?:too|very)\s+(?:far|corporatish|expensive|costly|high)\b|"
        r"\bdrop\s+(?:the\s+plan|it)\b",
        re.IGNORECASE,
    )
    has_user_refusal = bool(_REFUSAL_RE.search(user_text)) or any(
        k in user_text for k in ("not interested", "nahi chahiye", "don't want", "no requirement", "not look", "dont want")
    )

    # 4. Site Visit Booked / Confirmed
    sv_mem = str(mem.get("site_visit", "")).lower()
    has_date_time = bool(mem.get("preferred_visit_date") or mem.get("preferred_visit_time"))
    user_affirmed_visit = any(
        k in user_text for k in ("book the visit", "schedule the visit", "visit tomorrow", "coming tomorrow", "come tomorrow", "see you tomorrow", "visit confirmed")
    ) or (
        has_date_time and any(aff in user_text for aff in ("yes", "sure", "okay", "ok", "theek hai", "chalega", "done"))
    )

    if ("confirmed" in sv_mem or "scheduled" in sv_mem or user_affirmed_visit) and "decline" not in sv_mem:
        # If user had a refusal, only accept booking if they explicitly affirmed/booked afterwards
        if not has_user_refusal or user_affirmed_visit:
            return Disposition.SITE_VISIT_BOOKED.value

    # 5. Callback Requested or Busy Right Now
    has_explicit_callback = any(
        k in user_text
        for k in (
            "call me later", "call later", "call back", "call tomorrow",
            "call in the evening", "call after", "baad mein phone",
            "baad mein call", "busy right now", "busy now", "driving",
            "in a meeting", "meeting mein", "connect later", "talk later",
            "call me at", "call after 5"
        )
    )
    if has_explicit_callback:
        return Disposition.CALLBACK_REQUESTED.value

    # 6. WhatsApp Follow-Up / Brochure Confirmed
    wa_mem = str(mem.get("whatsapp", "")).lower()
    has_whatsapp_confirmed = (
        "confirm" in wa_mem
        or "sent" in wa_mem
        or any(k in transcript_text for k in ("brochure shortly", "brochure with you shortly", "share the brochure", "details on whatsapp", "share the floor plan"))
        or any(
            k in user_text
            for k in ("send on whatsapp", "whatsapp pe bhej", "bhej do", "bhej dijiye", "haan bhej", "theek hai fine", "haan theek hai fine", "chalega")
            if any(ag in transcript_text for ag in ("whatsapp", "brochure", "floor plan", "leisure"))
        )
    )

    # Check for firm, unrecovered refusal (e.g. caller explicitly rejected WhatsApp or repeated refusal at end)
    has_firm_unrecovered_refusal = any(
        k in user_text
        for k in (
            "don't send", "dont send", "mat bhejo", "no whatsapp",
            "kuch mat bhejo", "no need", "not interested at all", "don't want anything"
        )
    )

    # If caller objected/hesitated but agreed to follow-up via WhatsApp brochure -> Consultative recovery: CALLBACK_REQUESTED
    if has_user_refusal and has_whatsapp_confirmed and not has_firm_unrecovered_refusal:
        return Disposition.CALLBACK_REQUESTED.value

    # If user refused and did not book a visit or agree to follow-up, it's NOT_INTERESTED
    if has_user_refusal or has_firm_unrecovered_refusal:
        return Disposition.NOT_INTERESTED.value

    # 7. Site Visit Requested (interest expressed without finalized slot)
    if (
        "tentative" in sv_mem
        or "interested" in sv_mem
        or any(k in user_text for k in ("site visit", "visit the property", "make a site visit", "come and see"))
    ):
        if "decline" not in sv_mem and "not free" not in sv_mem:
            return Disposition.SITE_VISIT_REQUESTED.value

    # 8. Escalated to human (only if user requested human/manager)
    if any(k in user_text for k in ("talk to a human", "connect me to a person", "speak with a person", "talk to manager", "connect to agent")):
        return Disposition.ESCALATED_TO_HUMAN.value

    # 9. Qualified (Captured configuration, budget, or WhatsApp brochure confirmed)
    if (
        mem.get("configuration")
        or mem.get("budget")
        or has_whatsapp_confirmed
        or any(k in transcript_text for k in ("share the floor plan", "brochure with you shortly", "details on whatsapp"))
    ):
        return Disposition.QUALIFIED.value

    # 10. No response / early abandonment
    if user_turns <= 1:
        return Disposition.NO_RESPONSE.value

    return Disposition.INCOMPLETE.value


def set_disposition(call_id: str, disposition: str) -> bool:
    if disposition not in {d.value for d in Disposition}:
        return False
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE calls SET disposition = {_ph(1)} WHERE call_id = {_ph(1)}",
            (disposition, call_id),
        )
        return True


def set_stage(call_id: str, stage: str) -> None:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE calls SET conversation_stage = {_ph(1)} WHERE call_id = {_ph(1)}",
            (stage, call_id),
        )


def record_call_stats(
    call_id: str,
    *,
    avg_voice_latency_ms: float | None,
    median_voice_latency_ms: float | None = None,
    p90_voice_latency_ms: float | None = None,
    avg_llm_ttft_ms: float | None = None,
    avg_tts_ttfa_ms: float | None = None,
    turns: int | None,
    cost_usd: float | None,
    cost_breakdown: dict[str, float] | None,
    stt_provider: str | None = None,
    llm_provider: str | None = None,
    tts_provider: str | None = None,
    **kwargs: Any,
) -> None:
    """Written once at call end from the metrics summary -- see
    metrics_collector.CallMetricsCollector.summary()."""
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""UPDATE calls SET avg_voice_latency_ms = {_ph(1)}, median_voice_latency_ms = {_ph(1)},
                p90_voice_latency_ms = {_ph(1)}, avg_llm_ttft_ms = {_ph(1)}, avg_tts_ttfa_ms = {_ph(1)},
                turns = {_ph(1)}, cost_usd = {_ph(1)}, cost_breakdown_json = {_ph(1)},
                stt_provider = {_ph(1)}, llm_provider = {_ph(1)}, tts_provider = {_ph(1)} WHERE call_id = {_ph(1)}""",
            (
                avg_voice_latency_ms,
                median_voice_latency_ms,
                p90_voice_latency_ms,
                avg_llm_ttft_ms,
                avg_tts_ttfa_ms,
                turns,
                cost_usd,
                json.dumps(cost_breakdown) if cost_breakdown else None,
                stt_provider,
                llm_provider,
                tts_provider,
                call_id,
            ),
        )


def finalize_call(call_id: str, analysis: dict[str, Any] | None = None) -> None:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT disposition FROM calls WHERE call_id = {_ph(1)}", (call_id,))
        row = cur.fetchone()
        if row is not None and not row["disposition"]:
            cur.execute(
                f"UPDATE calls SET disposition = {_ph(1)} WHERE call_id = {_ph(1)}",
                (Disposition.INCOMPLETE.value, call_id),
            )
        cur.execute(
            f"UPDATE calls SET ended_at = {_ph(1)}, analysis_json = {_ph(1)} WHERE call_id = {_ph(1)}",
            (time.time(), json.dumps(analysis) if analysis else None, call_id),
        )


def mark_crm_pushed(call_id: str, ok: bool) -> None:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE calls SET crm_pushed = {_ph(1)} WHERE call_id = {_ph(1)}",
            (1 if ok else 0, call_id),
        )


def get_call(call_id: str) -> dict[str, Any] | None:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM calls WHERE call_id = {_ph(1)}", (call_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_calls(limit: int = 200, campaign_id: str | None = None) -> list[dict[str, Any]]:
    with _lock, _conn() as conn:
        cur = conn.cursor()
        if campaign_id:
            cur.execute(
                f"SELECT * FROM calls WHERE campaign_id = {_ph(1)} ORDER BY started_at DESC LIMIT {_ph(1)}",
                (campaign_id, limit),
            )
        else:
            cur.execute(f"SELECT * FROM calls ORDER BY started_at DESC LIMIT {_ph(1)}", (limit,))
        return [dict(r) for r in cur.fetchall()]


def campaign_summary(campaign_id: str) -> dict[str, Any]:
    """Backs the client-facing 'what did this campaign cost, and how fast
    was the bot' view -- exactly the per-campaign latency/cost visibility
    asked for."""
    rows = list_calls(limit=100000, campaign_id=campaign_id)
    n = len(rows)
    if n == 0:
        return {"campaign_id": campaign_id, "calls": 0}
    latencies = [r["avg_voice_latency_ms"] for r in rows if r.get("avg_voice_latency_ms") is not None]
    costs = [r["cost_usd"] for r in rows if r.get("cost_usd") is not None]
    dispositions: dict[str, int] = {}
    for r in rows:
        d = r.get("disposition") or "UNKNOWN"
        dispositions[d] = dispositions.get(d, 0) + 1
    return {
        "campaign_id": campaign_id,
        "calls": n,
        "avg_voice_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
        "total_cost_usd": round(sum(costs), 4) if costs else None,
        "avg_cost_usd_per_call": round(sum(costs) / len(costs), 4) if costs else None,
        "disposition_breakdown": dispositions,
    }


# Non-blocking async wrappers for use within live call event loops
async def upsert_call_async(*args, **kwargs) -> None:
    await asyncio.to_thread(upsert_call, *args, **kwargs)


async def record_field_async(call_id: str, field: str, value: Any) -> dict[str, Any]:
    return await asyncio.to_thread(record_field, call_id, field, value)


async def record_fields_async(call_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(record_fields, call_id, fields)


async def set_disposition_async(call_id: str, disposition: str) -> bool:
    return await asyncio.to_thread(set_disposition, call_id, disposition)


async def infer_deterministic_disposition_async(*args, **kwargs) -> str:
    return await asyncio.to_thread(infer_deterministic_disposition, *args, **kwargs)


async def record_call_stats_async(*args, **kwargs) -> None:
    await asyncio.to_thread(record_call_stats, *args, **kwargs)


async def finalize_call_async(call_id: str, analysis: dict[str, Any] | None = None) -> None:
    await asyncio.to_thread(finalize_call, call_id, analysis)


async def mark_crm_pushed_async(call_id: str, ok: bool) -> None:
    await asyncio.to_thread(mark_crm_pushed, call_id, ok)


async def get_call_async(call_id: str) -> dict[str, Any] | None:
    return await asyncio.to_thread(get_call, call_id)


# ---------------------------------------------------------------------------
UPDATE_LEAD_INFO_TOOL = {
    "type": "function",
    "function": {
        "name": "update_lead_info",
        "description": (
            "Record one piece of information the caller has confirmed "
            "(budget, configuration, location, timeline, etc). Call this "
            "as soon as a fact is confirmed, not just at the end of the "
            "call. Never call this to record a guess -- only confirmed "
            "facts the caller actually stated."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "field": {"type": "string", "enum": sorted(LEAD_FIELDS)},
                "value": {"type": "string"},
            },
            "required": ["field", "value"],
            "additionalProperties": False,
        },
    },
}
