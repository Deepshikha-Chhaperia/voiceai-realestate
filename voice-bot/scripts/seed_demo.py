"""
Seed script: Populates realistic demo leads, projects, site visit slots,
and touchpoint history into PostgreSQL for the Lead Management Dashboard.

Usage:
    python scripts/seed_demo.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import random
import sys
from pathlib import Path
import uuid

# Ensure project root is in sys.path when running scripts/seed_demo.py directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select

from leads.db import get_session, init_models
from leads.models import Lead, Project, SiteVisit, Slot, Touchpoint


CAMPAIGNS = [
    "meta-corridor-launch",
    "google-sem-meridian",
    "website-organic",
    "magicbricks-portal",
]

LEAD_DATA = [
    # Hot Leads (Visits booked / High intent)
    ("Rajesh Kumar", "+919845012345", "hot", 95, "Site visit booked for Saturday 11 AM. Looking for 3 BHK Large (2100 sqft), budget 1.8 Cr.", "visit_booked", True, "meta-corridor-launch"),
    ("Priya Sharma", "+919886023456", "hot", 90, "Requested 3 BHK Standard on high floor. Visit confirmed Sunday 3 PM.", "visit_booked", True, "google-sem-meridian"),
    ("Amitabh Sen", "+919740034567", "hot", 85, "Ready buyer with approved SBI home loan. Scheduled Saturday 11 AM.", "visit_booked", True, "website-organic"),
    ("Sneha Reddy", "+919900045678", "hot", 88, "Immediate possession query, 2 BHK 95L budget. Visit booked Sunday 11 AM.", "visit_booked", True, "meta-corridor-launch"),
    ("Vikram Malhotra", "+919811056789", "hot", 82, "East facing 3 BHK preferred. Family visit scheduled Saturday 2 PM.", "visit_booked", True, "google-sem-meridian"),
    ("Ananya Deshmukh", "+919945067890", "hot", 80, "Senior advisor handoff requested for custom duplex inquiry.", "conversed", False, "website-organic"),

    # Warm Leads (Brochure sent / Callback / Followup)
    ("Rohan Varma", "+919820078901", "warm", 65, "Brochure and floor plans sent on WhatsApp. Interested in 2 BHK.", "conversed", False, "meta-corridor-launch"),
    ("Kavita Nair", "+919841089012", "warm", 60, "Discussing with spouse about 3 BHK Standard pricing. Callback requested.", "conversed", False, "google-sem-meridian"),
    ("Deepak Joshi", "+919711090123", "warm", 55, "Comparing with Prime Tech Corridor alternatives. Brochure shared.", "conversed", False, "magicbricks-portal"),
    ("Sunil Chawla", "+919871012340", "warm", 58, "Interested in 10% booking payment plan details. Followup next week.", "conversed", False, "meta-corridor-launch"),
    ("Pooja Hegde", "+919980023451", "warm", 62, "Liked 3 BHK layout. Requested WhatsApp floor plan and pricing chart.", "conversed", False, "website-organic"),
    ("Manish Gupta", "+919822034562", "warm", 50, "Investment purpose. Evaluating rental yield in Prime Tech Corridor.", "conversed", False, "google-sem-meridian"),
    ("Meera Patel", "+919890045673", "warm", 52, "Inquired about possession timeline (late 2027). Brochure sent.", "conversed", False, "magicbricks-portal"),
    ("Karan Oberoi", "+919810056784", "warm", 68, "Positive response to Metro connectivity; reviewing price breakdown.", "conversed", False, "meta-corridor-launch"),

    # Pending / Contacting Leads (New inquiries or retry pending)
    ("Siddharth Rao", "+919845067895", "pending", 30, "Speed-to-lead call queued. Attempt 1 pending.", "contacting", False, "meta-corridor-launch"),
    ("Neha Saxena", "+919886078906", "pending", 25, "No response on attempt 1. Scheduled retry attempt 2.", "contacting", False, "google-sem-meridian"),
    ("Arun Menon", "+919740089017", "pending", 20, "Call disconnected after 1 ring. Auto-retry scheduled.", "contacting", False, "website-organic"),
    ("Divya Iyer", "+919900090128", "pending", 35, "New web lead from landing page /lp. Awaiting callback.", "new", False, "website-organic"),
    ("Gaurav Bansal", "+919811001239", "pending", 15, "No response on attempt 2. Next retry scheduled tomorrow morning.", "contacting", False, "meta-corridor-launch"),
    ("Shreya Singhal", "+919945012341", "pending", 28, "Meta Lead Ads instant form submission.", "new", False, "meta-corridor-launch"),

    # Dead Leads (Not interested / Invalid / Hostile / DNC)
    ("Ashok Mehta", "+919820023452", "dead", 0, "Looking strictly under 50 Lakhs budget (Meridian starts at 95L).", "dead", False, "magicbricks-portal"),
    ("Tanvi Roy", "+919841034563", "dead", 0, "Wrong number; callee is not looking for real estate.", "dead", False, "google-sem-meridian"),
    ("Ramesh Bhat", "+919711045674", "dead", 0, "Requested DNC on first call.", "dead", False, "meta-corridor-launch"),
    ("Sanjay Kaul", "+919871056785", "dead", 5, "Already purchased in another project last month.", "dead", False, "website-organic"),
    ("Harish Prasad", "+919980067896", "dead", 0, "No answer after 4 attempts across 3 days.", "dead", False, "magicbricks-portal"),
]


async def seed():
    print("Initializing DB tables...")
    await init_models()

    async with get_session() as session:
        # 1. Seed or retrieve default projects
        stmt = select(Project).where(Project.market == "india").limit(1)
        res = await session.execute(stmt)
        project_india = res.scalar_one_or_none()

        if not project_india:
            project_india = Project(
                id=uuid.uuid4(),
                name="Meridian Residences",
                market="india",
                config={
                    "timezone": "Asia/Kolkata",
                    "currency": "INR",
                    "calling_hours": {"start": "09:00", "end": "21:00"},
                    "location": "Prime Tech Corridor",
                },
            )
            session.add(project_india)
            await session.flush()
            print(f"Created India Project: {project_india.name} ({project_india.id})")


        # 2. Seed site visit slots for the upcoming weekend
        now = datetime.now(timezone.utc)
        # Find next Saturday
        days_ahead = 5 - now.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        sat_date = (now + timedelta(days=days_ahead)).replace(hour=11, minute=0, second=0, microsecond=0)
        sun_date = sat_date + timedelta(days=1)

        slot_times = [
            sat_date.replace(hour=11, minute=0),
            sat_date.replace(hour=14, minute=0),
            sat_date.replace(hour=16, minute=0),
            sun_date.replace(hour=11, minute=0),
            sun_date.replace(hour=15, minute=0),
        ]

        slots = []
        for st in slot_times:
            stmt_s = select(Slot).where(Slot.project_id == project_india.id, Slot.slot_start == st).limit(1)
            res_s = await session.execute(stmt_s)
            s_obj = res_s.scalar_one_or_none()
            if not s_obj:
                s_obj = Slot(
                    id=uuid.uuid4(),
                    project_id=project_india.id,
                    slot_start=st,
                    capacity=4,
                    booked=0,
                )
                session.add(s_obj)
                await session.flush()
            slots.append(s_obj)

        print(f"Verified {len(slots)} site visit slots.")

        # 3. Seed leads, touchpoints, and visits
        created_leads = 0
        for i, (name, phone, tier, score_val, reason, status, genuine, campaign) in enumerate(LEAD_DATA):
            stmt_l = select(Lead).where(Lead.phone == phone).limit(1)
            res_l = await session.execute(stmt_l)
            lead = res_l.scalar_one_or_none()

            lead_created_at = now - timedelta(
                hours=random.randint(1, 48),
                minutes=random.randint(1, 59),
                seconds=random.randint(1, 59),
            )
            # Realistic call latency in seconds (most within 14-46s, occasional 72s)
            contact_delay_sec = 72 if i == 7 else random.randint(14, 46)
            lead_first_call_at = (
                lead_created_at + timedelta(seconds=contact_delay_sec)
                if status in ("conversed", "visit_booked", "dead")
                else None
            )

            if lead:
                lead.source = "seed"
                lead.created_at = lead_created_at
                lead.first_contact_at = lead_first_call_at
                lead.status = status
                lead.tier = tier
                lead.score = score_val
                lead.score_reason = reason
                lead.visit_genuine = genuine
                created_leads += 1
            else:
                lead = Lead(
                    id=uuid.uuid4(),
                    name=name,
                    phone=phone,
                    source="seed",
                    project_id=project_india.id,
                    campaign_id=campaign,
                    status=status,
                    tier=tier,
                    score=score_val,
                    score_reason=reason,
                    visit_genuine=genuine,
                    consent_at=lead_created_at,
                    attempts=1 if status != "new" else 0,
                    first_contact_at=lead_first_call_at,
                    last_touch_at=lead_created_at + timedelta(minutes=5),
                    created_at=lead_created_at,
                )
                session.add(lead)
                await session.flush()
                created_leads += 1

                # Ingestion touchpoint
                tp_in = Touchpoint(
                    lead_id=lead.id,
                    kind="ingest",
                    summary=f"Lead captured from {campaign}",
                    payload={"name": name, "phone": phone, "campaign": campaign},
                    occurred_at=lead_created_at,
                )
                session.add(tp_in)

                # Call touchpoint if contacted
                if lead_first_call_at:
                    tp_call = Touchpoint(
                        lead_id=lead.id,
                        kind="call_out",
                        call_id=f"call-{str(lead.id)[:8]}",
                        summary=f"AI Advisor call: {reason}",
                        payload={"duration_s": random.randint(45, 180), "tier": tier, "score": score_val},
                        occurred_at=lead_first_call_at,
                    )
                    session.add(tp_call)

                # WhatsApp brochure touchpoint for warm/hot leads
                if "brochure" in reason.lower() or "whatsapp" in reason.lower() or tier in ("hot", "warm"):
                    tp_wa = Touchpoint(
                        lead_id=lead.id,
                        kind="whatsapp_out",
                        summary="Brochure and floor plans dispatched via WhatsApp",
                        payload={"template": "meridian_brochure_v1"},
                        occurred_at=lead_created_at + timedelta(minutes=3),
                    )
                    session.add(tp_wa)

                # Site visit booking for hot leads
                if genuine and status == "visit_booked" and slots:
                    chosen_slot = random.choice(slots)
                    chosen_slot.booked += 1
                    sv = SiteVisit(
                        lead_id=lead.id,
                        slot_id=chosen_slot.id,
                        slot_start=chosen_slot.slot_start,
                        status="booked",
                        created_at=lead_created_at + timedelta(minutes=4),
                    )
                    session.add(sv)

                    tp_v = Touchpoint(
                        lead_id=lead.id,
                        kind="site_visit",
                        summary=f"Site visit confirmed for {chosen_slot.slot_start.strftime('%A %I:%M %p')}",
                        occurred_at=lead_created_at + timedelta(minutes=4),
                    )
                    session.add(tp_v)

        await session.commit()
        print(f"Successfully seeded {created_leads} leads into the database.")


if __name__ == "__main__":
    asyncio.run(seed())
