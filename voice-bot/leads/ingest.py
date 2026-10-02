"""
Lead Ingestion & Deduplication Service.

Normalizes phone to E.164, saves raw payload, writes a form touchpoint,
dedupes against existing leads by (phone, project_id) or (source, external_id),
and enqueues background processing.
"""

from datetime import datetime, timezone
import re
import uuid
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import select

from leads.db import get_session
from leads.models import Lead, Project, Touchpoint


class LeadIn(BaseModel):
    name: str | None = None
    phone: str
    source: str = "website"  # website | meta | whatsapp | inbound_call | manual | seed
    external_id: str | None = None
    project_id: str | None = None
    campaign_id: str | None = None
    adset_id: str | None = None
    ad_id: str | None = None
    utm: dict[str, Any] | None = None
    consent: bool = True
    raw: dict[str, Any] | None = None


def normalize_phone_e164(raw_phone: str) -> str:
    """Normalizes phone numbers to standard E.164 format (defaults to India +91 if 10 digits)."""
    if not raw_phone:
        return ""
    digits = re.sub(r"[^\d+]", "", raw_phone)
    if digits.startswith("+"):
        return digits
    # 10-digit Indian mobile numbers (starts with 6,7,8,9)
    if len(digits) == 10 and digits[0] in "6789":
        return f"+91{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return f"+{digits}"
    return f"+{digits}"


async def get_or_create_default_project(session) -> Project:
    """Ensures at least one default project exists in DB."""
    stmt = select(Project).limit(1)
    res = await session.execute(stmt)
    proj = res.scalar_one_or_none()
    if not proj:
        proj = Project(
            name="Meridian Residences",
            market="india",
            config={
                "unit_types": ["2BHK", "3BHK", "3BHK_LARGE"],
                "min_price_lakhs": 95.0,
                "max_price_lakhs": 180.0,
                "currency": "INR",
                "brochure_url": "https://meridian.example.com/residences-brochure.pdf",
                "visit_days": ["Saturday", "Sunday"],
                "visit_hours": ["10:00 AM", "12:00 PM", "2:00 PM", "4:00 PM"],
            },
        )
        session.add(proj)
        await session.flush()
    return proj


async def ingest_lead(data: LeadIn) -> Lead:
    """Ingests a new lead payload with deduplication and idempotency."""
    phone_clean = normalize_phone_e164(data.phone)
    if not phone_clean:
        raise ValueError("Invalid phone number")

    async with get_session() as session:
        # Resolve project
        proj = None
        if data.project_id:
            try:
                p_uuid = uuid.UUID(data.project_id)
                proj = await session.get(Project, p_uuid)
            except (ValueError, TypeError):
                pass
        if not proj:
            proj = await get_or_create_default_project(session)

        # Idempotency Check: (source, external_id)
        if data.external_id:
            stmt = select(Lead).where(
                Lead.source == data.source,
                Lead.external_id == data.external_id,
            )
            res = await session.execute(stmt)
            existing_by_ext = res.scalar_one_or_none()
            if existing_by_ext:
                logger.info(
                    "LeadIn idempotency hit: source={}, ext_id={} -> returning lead_id={}",
                    data.source,
                    data.external_id,
                    existing_by_ext.id,
                )
                return existing_by_ext

        # Deduplication Check: (phone, project_id)
        stmt = select(Lead).where(
            Lead.phone == phone_clean,
            Lead.project_id == proj.id,
        )
        res = await session.execute(stmt)
        existing_lead = res.scalar_one_or_none()

        now = datetime.now(timezone.utc)
        if existing_lead:
            # Attach new touchpoint to existing lead
            tp = Touchpoint(
                lead_id=existing_lead.id,
                kind="form",
                summary=f"Re-engaged via {data.source}",
                payload=data.raw or data.model_dump(),
                occurred_at=now,
            )
            session.add(tp)
            existing_lead.last_touch_at = now
            if data.name and not existing_lead.name:
                existing_lead.name = data.name
            logger.info(
                "Lead deduplication match on phone={}: added touchpoint to lead_id={}",
                phone_clean[-4:].rjust(len(phone_clean), "*"),
                existing_lead.id,
            )
            lead_obj = existing_lead
        else:
            # Create new Lead
            lead_obj = Lead(
                phone=phone_clean,
                name=data.name,
                source=data.source,
                external_id=data.external_id,
                project_id=proj.id,
                campaign_id=data.campaign_id,
                adset_id=data.adset_id,
                ad_id=data.ad_id,
                utm=data.utm,
                status="new",
                tier="pending",
                score=0,
                score_reason="New lead registered",
                visit_genuine=False,
                consent_at=now if data.consent else None,
                attempts=0,
                last_touch_at=now,
                raw=data.raw or data.model_dump(),
                created_at=now,
            )
            session.add(lead_obj)
            await session.flush()

            # First touchpoint
            tp = Touchpoint(
                lead_id=lead_obj.id,
                kind="form",
                summary=f"Lead submitted via {data.source}",
                payload=data.raw or data.model_dump(),
                occurred_at=now,
            )
            session.add(tp)
            logger.info(
                "Created new lead_id={} phone={}",
                lead_obj.id,
                phone_clean[-4:].rjust(len(phone_clean), "*"),
            )

        # Enqueue background processing if lead is not dead
        if lead_obj.status != "dead":
            try:
                from leads.worker import enqueue_process_new_lead
                await enqueue_process_new_lead(str(lead_obj.id))
            except Exception as exc:
                logger.warning("Could not enqueue process_new_lead: {}", exc)

        return lead_obj
