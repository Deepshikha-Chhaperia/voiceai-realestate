"""
Background worker (arq / Redis) for Speed-to-Lead execution, call analysis, scoring, and retries.
"""

from datetime import datetime, time as dt_time, timedelta, timezone
import json
import os
import re
import uuid
from typing import Any
import zoneinfo

from arq import create_pool, cron
from arq.connections import RedisSettings
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from leads.db import get_session
from leads.models import DoNotCall, Lead, SiteVisit, Touchpoint
from leads.notify import send_telegram_alert
from leads.outbox import drain_outbox, queue_outbox_item
from leads.scoring import score
from settings import is_local_demo


def get_redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(os.environ["REDIS_URL"])


def normalize_visit_date(raw: str, now_ist: datetime) -> tuple[str, str] | None:
    """Map a spoken visit date (English or Hindi/Hinglish) to (iso_date, human_label).

    Returns None when the input cannot be confidently mapped to a specific date.
    Never falls back to a guessed default — callers must handle None explicitly.
    """
    v = raw.strip().lower()
    if not v:
        return None

    # today: today / aaj / आज
    if re.search(r"\b(today|aaj)\b", v) or "आज" in v:
        d = now_ist.date()
        return (d.isoformat(), f"Today — {d.strftime('%A, %d %b %Y')}")

    # tomorrow: tomorrow / kal / कल
    if re.search(r"\b(tomorrow|kal)\b", v) or "कल" in v:
        d = (now_ist + timedelta(days=1)).date()
        logger.info("normalize_visit_date: 'कल' / 'kal' resolved as tomorrow for future site visit -> {}", d.isoformat())
        return (d.isoformat(), f"Tomorrow — {d.strftime('%A, %d %b %Y')}")

    # day after tomorrow: parso / परसों / day after tomorrow
    if re.search(r"\b(parso|parson|day after tomorrow)\b", v) or "परसों" in v:
        d = (now_ist + timedelta(days=2)).date()
        return (d.isoformat(), f"{d.strftime('%A, %d %b %Y')}")

    # weekday names (English and Hindi)
    weekday_map = {
        "monday": 0, "somwar": 0, "सोमवार": 0,
        "tuesday": 1, "mangalwar": 1, "मंगलवार": 1,
        "wednesday": 2, "budhwar": 2, "बुधवार": 2,
        "thursday": 3, "guruwar": 3, "brihaspatiwar": 3, "गुरुवार": 3,
        "friday": 4, "shukrawar": 4, "शुक्रवार": 4,
        "saturday": 5, "shaniwar": 5, "शनिवार": 5,
        "sunday": 6, "ravivar": 6, "रविवार": 6,
    }
    for day_name, day_idx in weekday_map.items():
        if (day_name in ("सोमवार", "मंगलवार", "बुधवार", "गुरुवार", "शुक्रवार", "शनिवार", "रविवार") and day_name in v) or re.search(r"\b" + re.escape(day_name) + r"\b", v):
            days_ahead = (day_idx - now_ist.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7  # Upcoming occurrence, not today
            d = (now_ist + timedelta(days=days_ahead)).date()
            return (d.isoformat(), f"{d.strftime('%A, %d %b %Y')}")

    # ISO date literal (YYYY-MM-DD passed through from LLM)
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        from datetime import date as _date
        try:
            d = _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return (d.isoformat(), f"{d.strftime('%A, %d %b %Y')}")
        except ValueError:
            return None

    return None


def format_spoken_date(date_val: Any, now_ist: Any, language: str = "en") -> str:
    """Format a date into natural conversational speech.
    Never outputs year or ISO format with hyphens.
    - today -> "today" / "aaj"
    - tomorrow -> "tomorrow" / "kal"
    - day after tomorrow -> "day after tomorrow" / "parso"
    - within 6 days -> weekday name (e.g. "Sunday", "Sunday ko")
    - otherwise -> ordinal day (e.g. "9th", "9 tareekh ko") if current month,
      or day + month (e.g. "15th of November", "15 November ko") if different month.
    """
    if isinstance(date_val, str):
        from datetime import date as _date
        parts = [int(p) for p in date_val.split("-")]
        d = _date(parts[0], parts[1], parts[2])
    elif hasattr(date_val, "date"):
        d = date_val.date()
    else:
        d = date_val

    ref_date = now_ist.date() if hasattr(now_ist, "date") else now_ist
    delta = (d - ref_date).days

    is_hi = isinstance(language, str) and language.lower().startswith("hi")

    if delta == 0:
        return "aaj" if is_hi else "today"
    elif delta == 1:
        return "kal" if is_hi else "tomorrow"
    elif delta == 2:
        return "parso" if is_hi else "day after tomorrow"
    elif 3 <= delta <= 6:
        weekday = d.strftime("%A")
        return f"{weekday} ko" if is_hi else weekday
    else:
        day = d.day
        if 11 <= (day % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

        month_name = d.strftime("%B")
        if d.month == ref_date.month and d.year == ref_date.year:
            return f"{day} tareekh ko" if is_hi else f"{day}{suffix}"
        else:
            return f"{day} {month_name} ko" if is_hi else f"{day}{suffix} of {month_name}"


def format_spoken_time(time_str: str, language: str = "en") -> str:
    """Format a normalized time string like '11:00 AM' into natural conversational speech."""
    if not time_str or not isinstance(time_str, str):
        return ""
    t = time_str.strip()
    is_hi = isinstance(language, str) and language.lower().startswith("hi")

    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?$", t, re.IGNORECASE)
    if not m:
        return t

    hour = int(m.group(1))
    minute = int(m.group(2)) if m.group(2) else 0
    ampm = (m.group(3) or "").upper()
    if not ampm:
        if hour >= 12:
            ampm = "PM"
            if hour > 12:
                hour = hour - 12
        else:
            ampm = "AM"
            if hour == 0:
                hour = 12
    elif hour > 12:
        hour = hour - 12

    time_part = f"{hour}" if minute == 0 else f"{hour}:{minute:02d}"

    if is_hi:
        if ampm == "AM":
            period = "subah " if hour < 12 else ""
        elif ampm == "PM":
            period = "dopahar " if (hour == 12 or hour < 4) else "shaam "
        else:
            period = ""
        return f"{period}{time_part} baje".strip()
    else:
        if ampm:
            return f"{time_part} {ampm.lower()}"
        return time_part


def normalize_visit_time(raw: str) -> str | None:
    """Normalize spoken time in English or Hindi into a standard format (e.g. '5:00 PM').
    Supports 'शाम 5 बजे', '5 baje', 'evening 5', '5 pm', 'subah 10', 'dopahar 2 baje', etc.
    """
    if not raw:
        return None
    s = raw.strip().lower()

    # Reject property specifications and non-time numerical entities:
    # 3 BHK, 2 BHK, 4 BHK, 95 Lakh, 1.7 Cr, 1580 sq ft, 2 balconies
    has_explicit_time_indicator = any(
        w in s for w in ("baje", "बजे", "o'clock", "oclock", "shaam", "shyam", "sham", "dopahar", "subah", "morning", "evening", "शाम", "सुबह", "दोपहर", "कल", "kal", "tomorrow")
    ) or bool(re.search(r"\b\d{1,2}\s*(?:am|a\.m\.|pm|p\.m\.)\b", s)) or bool(re.search(r"\b(?:at|around)\s+\d{1,2}\b", s))
    if re.search(r"\b\d+\s*(?:bhk|bed|bedroom|bedrooms|lakh|lakhs|lac|lacs|cr|crore|crores|sq\s*ft|sqft|balcon|balconies|floor|tower)\b", s):
        if not has_explicit_time_indicator:
            return None

    is_pm = bool(re.search(r"\b\d{1,2}\s*(?:pm|p\.m\.)\b", s)) or any(w in s for w in ("pm", "p.m.", "shaam", "shyam", "sham", "dopahar", "raat", "evening", "afternoon", "night", "शाम", "दोपहर", "रात"))
    is_am = bool(re.search(r"\b\d{1,2}\s*(?:am|a\.m\.)\b", s)) or any(w in s for w in ("subah", "morning", "सुबह"))

    m = re.search(r"(\b\d{1,2})(?:[:.](\d{2}))?\s*(?:baje|बजे|pm|am|o'?clock)?\b", s)
    if not m:
        # Check word numbers like 'two', 'three', etc.
        word_match = re.search(r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|ek|do|teen|chaar|paanch|chhe|saat|aath|nau|das|gyarah|baarah)\b(?:\s*(?:baje|बजे|pm|am|o'?clock))?", s)
        if word_match:
            word_map = {
                "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
                "ek": 1, "do": 2, "teen": 3, "chaar": 4, "paanch": 5, "chhe": 6,
                "saat": 7, "aath": 8, "nau": 9, "das": 10, "gyarah": 11, "baarah": 12,
            }
            hour = word_map[word_match.group(1)]
            minute = 0
        else:
            return None
    else:
        hour = int(m.group(1))
        minute = int(m.group(2)) if m.group(2) else 0

    if hour > 24 or minute > 59:
        return None

    # If the token is just a bare number without explicit time indicator, only accept valid hour slots (1-12)
    # when the input is a concise time expression (e.g. "at 2", "two", "2", "around 4")
    if not has_explicit_time_indicator and not re.search(r"^(?:at\s+|around\s+)?(?:\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|do|teen|chaar|paanch)(?:[:.]\d{2})?\.?$", s):
        return None

    if is_pm and hour < 12:
        meridiem = "PM"
    elif is_am and hour < 12:
        meridiem = "AM"
    elif 12 <= hour < 24:
        meridiem = "PM"
        if hour > 12:
            hour -= 12
    elif hour <= 7 and not is_am:
        # Typical real-estate visit hours: 2 baje -> 2 PM, 5 baje -> 5 PM, not 5 AM
        meridiem = "PM"
    else:
        meridiem = "AM" if (hour < 12 and not is_pm) else "PM"

    return f"{hour}:{minute:02d} {meridiem}"


def _resolve_visit_datetime(date_str: str | None, time_str: str | None) -> datetime:
    """Helper to convert spoken day/time into a realistic upcoming datetime slot (IST-aware)."""
    try:
        tz_ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    except Exception:
        tz_ist = zoneinfo.ZoneInfo("UTC")
    now_ist = datetime.now(tz_ist)
    date_val = str(date_str or "").strip().lower()
    time_val = str(time_str or "").strip().lower()

    # Determine hour and minute using token regex matching
    hour = 14  # Default 2:00 PM
    minute = 0

    time_match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", time_val)
    if time_match:
        h = int(time_match.group(1))
        m_min = int(time_match.group(2) or 0)
        ampm = (time_match.group(3) or "").lower()
        if ampm == "pm" and h < 12:
            h += 12
        elif ampm == "am" and h == 12:
            h = 0
        elif not ampm:
            # 1 to 6 without am/pm are usually afternoon site visits in real estate
            if 1 <= h <= 6:
                h += 12
        if 0 <= h <= 23:
            hour = h
            minute = m_min

    # Try normalize_visit_date (handles English + Hindi/Hinglish + ISO)
    result = normalize_visit_date(date_val, now_ist)
    if result is not None:
        from datetime import date as _date
        iso_date, _ = result
        d = _date.fromisoformat(iso_date)
        return now_ist.replace(year=d.year, month=d.month, day=d.day,
                               hour=hour, minute=minute, second=0, microsecond=0)

    # Fallback: days_ahead=2 — should never fire for new bookings after A3.
    # book_site_visit returns needs_confirmation when normalize_visit_date returns None,
    # so reaching here means the date arrived via a legacy path (e.g. old DB row or
    # manual retry queue). Log loudly so it is visible in call logs.
    logger.warning(
        "resolve_visit_datetime: unparseable date {!r} — falling back to days_ahead=2. "
        "This should never happen for new bookings; book_site_visit should have returned "
        "needs_confirmation instead of passing this value.",
        date_val,
    )
    target = now_ist + timedelta(days=2)
    return target.replace(hour=hour, minute=minute, second=0, microsecond=0)



def get_next_calling_window_start(tz_name: str = "Asia/Kolkata", start_str: str = "09:00") -> datetime:
    """Returns UTC datetime for the next start of legal calling hours."""
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = zoneinfo.ZoneInfo("Asia/Kolkata")
    now_local = datetime.now(tz)
    s_h, s_m = map(int, start_str.split(":"))
    today_start = now_local.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
    if now_local < today_start:
        target = today_start
    else:
        target = today_start + timedelta(days=1)
    return target.astimezone(timezone.utc)


_arq_pool = None


async def get_arq_pool():
    global _arq_pool
    if _arq_pool is None and os.getenv("REDIS_URL"):
        try:
            _arq_pool = await create_pool(get_redis_settings())
        except Exception as exc:
            if not is_local_demo():
                raise RuntimeError(f"REDIS_URL is set but Redis is unreachable: {exc}")
            logger.warning("Redis arq pool unreachable ({}); LOCAL_DEMO runs worker jobs synchronously.", exc)
    return _arq_pool


async def enqueue_process_new_lead(lead_id: str):
    pool = await get_arq_pool()
    if pool:
        await pool.enqueue_job("process_new_lead", lead_id)
    else:
        # Dev fallback: run inline
        import asyncio
        asyncio.create_task(process_new_lead({}, lead_id))


async def enqueue_on_call_finished(call_id: str):
    pool = await get_arq_pool()
    if pool:
        await pool.enqueue_job("on_call_finished", call_id)
    else:
        import asyncio
        asyncio.create_task(on_call_finished({}, call_id))


def is_within_calling_hours(tz_name: str = "Asia/Kolkata", start_str: str = "09:00", end_str: str = "21:00") -> bool:
    """Checks if current time is within legal calling hours for the target market."""
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = zoneinfo.ZoneInfo("UTC")
    now_local = datetime.now(tz)
    s_h, s_m = map(int, start_str.split(":"))
    e_h, e_m = map(int, end_str.split(":"))
    start_t = dt_time(s_h, s_m)
    end_t = dt_time(e_h, e_m)
    return start_t <= now_local.time() <= end_t


async def process_new_lead(ctx: dict, lead_id: str) -> None:
    """Speed to lead job: places outbound call within 60s during calling hours."""
    logger.info("Executing process_new_lead for lead_id={}", lead_id)
    async with get_session() as session:
        stmt = select(Lead).where(Lead.id == uuid.UUID(lead_id)).options(selectinload(Lead.project))
        res = await session.execute(stmt)
        lead = res.scalar_one_or_none()
        if not lead or lead.status in ("dead", "visit_booked"):
            return

        # Market-specific calling hours check
        project_config = (lead.project.config if lead.project else {}) or {}
        tz_name = project_config.get("timezone", "Asia/Kolkata")
        calling_hours = project_config.get("calling_hours", {"start": "09:00", "end": "21:00"})

        if not is_within_calling_hours(tz_name, calling_hours.get("start", "09:00"), calling_hours.get("end", "21:00")):
            logger.info("Outside calling hours ({}) for lead_id={}; scheduling for next morning", tz_name, lead_id)
            lead.next_attempt_at = get_next_calling_window_start(tz_name, calling_hours.get("start", "09:00"))
            return

        # Start outbound call via extracted function in main.py
        from main import initiate_outbound_call
        campaign = lead.campaign_id or (lead.project.name if lead.project else "web-test")

        try:
            call_res = await initiate_outbound_call(
                to_phone=lead.phone,
                customer_name=lead.name or "Alex",
                campaign_id=campaign,
                lead_id=str(lead.id),
            )
            if call_res.get("status") == "blocked":
                reason = call_res.get("reason")
                logger.info("Outbound call blocked for lead_id={}: {}", lead.id, reason)
                if reason == "do_not_call":
                    lead.status = "dead"
                    lead.score_reason = "Do Not Call list"
                    lead.next_attempt_at = None
                elif reason == "outside_calling_hours":
                    lead.next_attempt_at = get_next_calling_window_start(tz_name, calling_hours.get("start", "09:00"))
                return
            if call_res.get("simulated"):
                raise RuntimeError(
                    f"Outbound call for lead_id={lead.id} ({lead.phone}) was simulated. Vobiz telephony credentials are not configured!"
                )
            call_id = call_res.get("call_id")
            lead.status = "contacting"
            lead.attempts += 1
            now_utc = datetime.now(timezone.utc)
            lead.last_touch_at = now_utc

            tp = Touchpoint(
                lead_id=lead.id,
                kind="call_out",
                call_id=call_id,
                summary=f"Outbound AI call initiated (attempt {lead.attempts})",
                payload={"call_res": call_res},
                occurred_at=now_utc,
            )
            session.add(tp)
            logger.info("Outbound call triggered for lead_id={} call_id={}", lead.id, call_id)
        except Exception as exc:
            logger.error("Failed to start outbound call for lead_id={}: {}", lead.id, exc)
            raise


async def on_call_finished(ctx: dict, call_id: str) -> None:
    """Post-call analytics, scoring, retry scheduling, and notification job."""
    logger.info("Executing on_call_finished for call_id={}", call_id)
    from call_analytics import analyze_call
    analysis_data = await analyze_call(call_id)
    if not analysis_data:
        logger.warning("No analysis generated for call_id={}", call_id)
        return

    analysis = analysis_data.get("analysis", {})
    call_record = analysis_data.get("call", {})
    disposition = call_record.get("disposition") or analysis.get("disposition")

    # Enrich analysis with captured lead fields from call working memory
    lead_fields = json.loads(call_record.get("lead_fields") or "{}") if isinstance(call_record.get("lead_fields"), str) else (call_record.get("lead_fields") or {})
    if lead_fields:
        for k, v in lead_fields.items():
            if k not in analysis or not analysis[k]:
                analysis[k] = v
        if "site_visit" in lead_fields and not analysis.get("visit_intent"):
            analysis["visit_intent"] = "booked" if any(w in str(lead_fields["site_visit"]).lower() for w in ("confirm", "saturday", "sunday", "morning", "evening", ":")) else "requested"

    dur = float(call_record.get("duration", 0.0))
    if dur <= 0.0 and call_record.get("ended_at") and call_record.get("started_at"):
        try:
            dur = max(0.0, float(call_record["ended_at"]) - float(call_record["started_at"]))
        except Exception:
            dur = 0.0

    call_stats = {
        "turns": call_record.get("turns", 0),
        "caller_turns": call_record.get("caller_turns", call_record.get("turns", 0)),
        "duration_s": dur,
        "talk_time_s": float(call_record.get("talk_time_s") or dur),
    }

    lead_id_str = call_record.get("lead_id")
    async with get_session() as session:
        lead = None
        if lead_id_str:
            try:
                stmt = select(Lead).where(Lead.id == uuid.UUID(lead_id_str)).options(selectinload(Lead.project))
                res = await session.execute(stmt)
                lead = res.scalar_one_or_none()
            except Exception:
                pass

        if not lead and call_record.get("phone"):
            stmt = select(Lead).where(Lead.phone == call_record.get("phone")).options(selectinload(Lead.project)).limit(1)
            res = await session.execute(stmt)
            lead = res.scalar_one_or_none()

        # Fallback for demo web test calls: find the most recent active/pending lead
        if not lead:
            stmt = select(Lead).where(Lead.status.in_(["pending", "contacting", "new"])).options(selectinload(Lead.project)).order_by(Lead.created_at.desc()).limit(1)
            res = await session.execute(stmt)
            lead = res.scalar_one_or_none()

        if not lead:
            logger.info("No corresponding lead row found for call_id={}", call_id)
            return

        # Ensure attempt count is recorded (at least 1 attempt made for this call)
        if (lead.attempts or 0) == 0:
            lead.attempts = 1
        elif lead.status != "contacting":
            lead.attempts += 1

        # Compute deterministic score and tier
        lead_score, lead_tier, score_reason, visit_genuine = score(lead, analysis, disposition, call_stats)

        lead.score = lead_score
        lead.tier = lead_tier
        lead.score_reason = score_reason
        lead.visit_genuine = visit_genuine
        lead.last_touch_at = datetime.now(timezone.utc)

        turns = int(call_stats.get("turns", 0))
        if turns >= 1:
            lead.first_contact_at = lead.first_contact_at or datetime.now(timezone.utc)
            lead.status = "conversed"

        # Check if DNC requested
        if disposition == "DNC_REQUESTED":
            lead.status = "dead"
            lead.score_reason = "Caller requested DNC"
            lead.next_attempt_at = None
            if lead.phone:
                stmt_dnc = select(DoNotCall).where(DoNotCall.phone == lead.phone).limit(1)
                res_dnc = await session.execute(stmt_dnc)
                if not res_dnc.scalar_one_or_none():
                    session.add(DoNotCall(phone=lead.phone, reason="Caller requested DNC during call"))
                    logger.info("Added {} to DoNotCall list", lead.phone)

        # Check if site visit is booked
        stmt_v = select(SiteVisit).where(SiteVisit.lead_id == lead.id).limit(1)
        res_v = await session.execute(stmt_v)
        visit_obj = res_v.scalar_one_or_none()

        if visit_obj:
            lead.status = "visit_booked"
            lead.visit_genuine = True

        # Handle retries on unanswered calls
        # DEMO_FAST_RETRY=true: 1 min / 2 min (demo only, never default)
        # Default (false): 30 min / 3 hr / 24 hr
        fast_retry = os.getenv("DEMO_FAST_RETRY", "false").lower() == "true"
        now_utc = datetime.now(timezone.utc)
        if disposition in ("NO_RESPONSE", "INCOMPLETE") or turns == 0:
            if lead.attempts < 4 and lead.status != "dead":
                if fast_retry:
                    delay_mins = 1 if lead.attempts == 1 else 2
                else:
                    delays = {1: 30, 2: 180, 3: 1440}
                    delay_mins = delays.get(lead.attempts, 60)
                scheduled_retry = now_utc + timedelta(minutes=delay_mins)

                # Calling-hours check (IST): if retry lands outside calling hours, schedule for 09:00 IST next day
                project_config = (lead.project.config if lead.project else {}) or {}
                tz_name = project_config.get("timezone", "Asia/Kolkata")
                calling_hours = project_config.get("calling_hours", {"start": "09:00", "end": "21:00"})
                try:
                    tz_lead = zoneinfo.ZoneInfo(tz_name)
                except Exception:
                    tz_lead = zoneinfo.ZoneInfo("Asia/Kolkata")
                scheduled_local = scheduled_retry.astimezone(tz_lead)
                s_h, s_m = map(int, calling_hours.get("start", "09:00").split(":"))
                e_h, e_m = map(int, calling_hours.get("end", "21:00").split(":"))
                if not (dt_time(s_h, s_m) <= scheduled_local.time() <= dt_time(e_h, e_m)):
                    scheduled_retry = get_next_calling_window_start(tz_name, calling_hours.get("start", "09:00"))

                lead.next_attempt_at = scheduled_retry
                logger.info(
                    "Scheduled call retry for lead_id={} attempt {} at {}",
                    lead.id,
                    lead.attempts + 1,
                    scheduled_retry,
                )
            else:
                lead.next_attempt_at = None
                if lead.status != "dead":
                    lead.score_reason = f"No answer after {lead.attempts} attempts"

        # Add call touchpoint
        tp = Touchpoint(
            lead_id=lead.id,
            kind="call_out",
            call_id=call_id,
            summary=f"Call completed ({disposition}) - Tier: {lead_tier.upper()} ({score_reason})",
            payload=analysis_data,
            occurred_at=now_utc,
        )
        session.add(tp)

        # Telegram Alert for HOT leads
        if lead_tier == "hot":
            if visit_obj:
                visit_info = f"{visit_obj.slot_start.strftime('%A, %b %d at %I:%M %p')}"
            else:
                visit_info = "not booked"

            summary_text = analysis.get("summary") or f"{score_reason}. Spoke for {int(call_stats.get('duration_s', 0))}s."
            await send_telegram_alert(
                lead_id=str(lead.id),
                name=lead.name,
                phone=lead.phone,
                score_reason=score_reason,
                visit_info=visit_info,
                summary=summary_text,
            )

        # Queue Outbox Items (Sheets & Webhook)
        await queue_outbox_item(lead.id, "sheets", call_record, session=session)
        await queue_outbox_item(lead.id, "webhook", {"lead_id": str(lead.id), "analysis": analysis, "score": lead_score}, session=session)

    # Drain pending outbox tasks
    try:
        await drain_outbox()
    except Exception as exc:
        logger.warning("Drain outbox error: {}", exc)


async def requeue_due_leads(ctx: dict) -> int:
    """Cron task: scans for due leads with next_attempt_at <= now and enqueues them."""
    now_utc = datetime.now(timezone.utc)
    count = 0
    async with get_session() as session:
        stmt = (
            select(Lead)
            .where(
                Lead.status.in_(["pending", "contacting", "new", "conversed"]),
                Lead.next_attempt_at != None,  # noqa: E711
                Lead.next_attempt_at <= now_utc,
                Lead.attempts < 4,
            )
            .options(selectinload(Lead.project))
        )
        res = await session.execute(stmt)
        due_leads = res.scalars().all()
        for lead in due_leads:
            lead.next_attempt_at = None
            await enqueue_process_new_lead(str(lead.id))
            count += 1
    if count > 0:
        logger.info("Cron re-queued {} due leads for retry", count)
    return count


class WorkerSettings:
    functions = [process_new_lead, on_call_finished, requeue_due_leads]
    cron_jobs = [cron(requeue_due_leads, minute=None, second=0)]
    redis_settings = get_redis_settings() if os.getenv("REDIS_URL") else None  # arq worker process requires REDIS_URL
    on_startup = None
    on_shutdown = None
    max_jobs = 20
