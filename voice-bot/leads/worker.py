"""
Background worker (arq / Redis) for Speed-to-Lead execution, call analysis, scoring, and retries.
"""

from datetime import datetime, time as dt_time, timedelta
import json
import os
import uuid
from typing import Any
import zoneinfo

from arq import create_pool
from arq.connections import RedisSettings
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from leads.db import get_session
from leads.models import Lead, SiteVisit, Touchpoint
from leads.notify import send_telegram_alert
from leads.outbox import drain_outbox, queue_outbox_item
from leads.scoring import score


def get_redis_settings() -> RedisSettings:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return RedisSettings.from_dsn(redis_url)


def _resolve_visit_datetime(date_str: str | None, time_str: str | None) -> datetime:
    """Helper to convert spoken day/time into a realistic upcoming datetime slot."""
    now = datetime.utcnow()
    date_val = str(date_str or "").strip().lower()
    time_val = str(time_str or "").strip().lower()

    # Determine hour and minute
    hour = 14  # Default 2:00 PM
    minute = 0
    if "10" in time_val:
        hour = 10
    elif "11" in time_val:
        hour = 11
    elif "12" in time_val:
        hour = 12
    elif "1" in time_val and "pm" in time_val:
        hour = 13
    elif "2" in time_val or "14" in time_val:
        hour = 14
    elif "3" in time_val:
        hour = 15
    elif "4" in time_val:
        hour = 16
    elif "5" in time_val:
        hour = 17

    # Determine day
    days_ahead = 2
    weekday_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6
    }
    for day_name, day_idx in weekday_map.items():
        if day_name in date_val:
            days_ahead = (day_idx - now.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            break
    if "tomorrow" in date_val:
        days_ahead = 1

    target = now + timedelta(days=days_ahead)
    return target.replace(hour=hour, minute=minute, second=0, microsecond=0)


_arq_pool = None


async def get_arq_pool():
    global _arq_pool
    if _arq_pool is None:
        try:
            _arq_pool = await create_pool(get_redis_settings())
        except Exception as exc:
            env = os.getenv("ENV", "dev")
            if env == "prod":
                raise RuntimeError(f"Could not connect to Redis in prod: {exc}")
            logger.warning("Could not connect to Redis arq pool in dev mode ({}); worker jobs will run synchronously.", exc)
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
            # Schedule next attempt at 09:00 next day
            now = datetime.utcnow()
            lead.next_attempt_at = now + timedelta(hours=10)
            return

        # Start outbound call via extracted function in main.py
        from main import start_outbound_call
        campaign = lead.campaign_id or (lead.project.name if lead.project else "web-test")
        
        try:
            call_res = await start_outbound_call(
                to_phone=lead.phone,
                customer_name=lead.name or "Alex",
                campaign_id=campaign,
                lead_id=str(lead.id),
            )
            call_id = call_res.get("call_id")
            lead.status = "contacting"
            lead.attempts += 1
            lead.last_touch_at = datetime.utcnow()

            tp = Touchpoint(
                lead_id=lead.id,
                kind="call_out",
                call_id=call_id,
                summary=f"Outbound AI call initiated (attempt {lead.attempts})",
                payload={"call_res": call_res},
                occurred_at=datetime.utcnow(),
            )
            session.add(tp)
            logger.info("Outbound call triggered for lead_id={} call_id={}", lead.id, call_id)
        except Exception as exc:
            logger.error("Failed to start outbound call for lead_id={}: {}", lead.id, exc)


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
        lead.last_touch_at = datetime.utcnow()

        turns = int(call_stats.get("turns", 0))
        if turns >= 1:
            lead.first_contact_at = lead.first_contact_at or datetime.utcnow()
            lead.status = "conversed"

        # Check if site visit is booked
        stmt_v = select(SiteVisit).where(SiteVisit.lead_id == lead.id).limit(1)
        res_v = await session.execute(stmt_v)
        visit_obj = res_v.scalar_one_or_none()

        # If call confirmed a site visit but no DB row exists yet, create it now!
        has_visit_booking = (
            disposition in ("SITE_VISIT_BOOKED", "SITE_VISIT_REQUESTED")
            or analysis.get("visit_intent") in ("booked", "requested")
            or any(w in str(lead_fields.get("site_visit", "")).lower() for w in ("confirm", "booked", "saturday", "sunday"))
            or "site visit successfully booked" in str(analysis.get("summary", "")).lower()
            or "site visit requested" in str(score_reason).lower()
        )

        if not visit_obj and has_visit_booking:
            v_date = lead_fields.get("preferred_visit_date") or analysis.get("preferred_visit_date") or "Saturday"
            v_time = lead_fields.get("preferred_visit_time") or analysis.get("preferred_visit_time") or "2:00 PM"
            slot_start = _resolve_visit_datetime(v_date, v_time)
            visit_obj = SiteVisit(
                lead_id=lead.id,
                slot_start=slot_start,
                status="booked",
            )
            session.add(visit_obj)
            logger.info("Auto-created SiteVisit row for lead_id={} slot={}", lead.id, slot_start)

        if visit_obj or has_visit_booking:
            lead.status = "visit_booked"
            lead.visit_genuine = True

        # Handle retries on unanswered calls
        # DEMO_FAST_RETRY=true: 1 min / 2 min (demo only, never default)
        # Default (false): 30 min / 3 hr / 24 hr
        fast_retry = os.getenv("DEMO_FAST_RETRY", "false").lower() == "true"
        now = datetime.utcnow()
        if disposition in ("NO_RESPONSE", "INCOMPLETE") or turns == 0:
            if lead.attempts < 4:
                if fast_retry:
                    delay_mins = 1 if lead.attempts == 1 else 2
                else:
                    delays = {1: 30, 2: 180, 3: 1440}
                    delay_mins = delays.get(lead.attempts, 60)
                lead.next_attempt_at = now + timedelta(minutes=delay_mins)
                logger.info(
                    "Scheduled call retry for lead_id={} attempt {} at +{} mins",
                    lead.id,
                    lead.attempts + 1,
                    delay_mins,
                )
            else:
                lead.next_attempt_at = None
                lead.score_reason = f"No answer after {lead.attempts} attempts"

        # Add call touchpoint
        tp = Touchpoint(
            lead_id=lead.id,
            kind="call_out",
            call_id=call_id,
            summary=f"Call completed ({disposition}) - Tier: {lead_tier.upper()} ({score_reason})",
            payload=analysis_data,
            occurred_at=now,
        )
        session.add(tp)

        # Telegram Alert for HOT leads
        if lead_tier == "hot":
            if visit_obj:
                visit_info = f"{visit_obj.slot_start.strftime('%A, %b %d at %I:%M %p')}"
            elif has_visit_booking:
                v_date = lead_fields.get("preferred_visit_date") or analysis.get("preferred_visit_date") or "Saturday"
                v_time = lead_fields.get("preferred_visit_time") or analysis.get("preferred_visit_time") or "2:00 PM"
                visit_info = f"Booked ({v_date} at {v_time})"
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


class WorkerSettings:
    functions = [process_new_lead, on_call_finished]
    redis_settings = get_redis_settings()
    on_startup = None
    on_shutdown = None
    max_jobs = 20
