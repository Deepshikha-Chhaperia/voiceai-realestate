import statistics
from datetime import datetime
import os
from pathlib import Path
import uuid

from fastapi import APIRouter, Cookie, Depends, HTTPException, Header, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from leads.db import get_session
from leads.models import Lead, Project, SiteVisit, Touchpoint
from leads.notify import mask_phone
from settings import safe_eq


router = APIRouter(prefix="/dashboard", tags=["Dashboard"])
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def verify_dashboard_auth(
    request: Request,
    dashboard_key_cookie: str | None = Cookie(None, alias="dashboard_key"),
    authorization: str | None = Header(None),
) -> bool:
    """Verifies dashboard access key with secure constant-time comparison."""
    expected_key = os.getenv("DASHBOARD_API_KEY", "").strip()
    if not expected_key:
        raise HTTPException(status_code=401, detail="Unauthorized dashboard access")

    provided_key = dashboard_key_cookie or ""
    if not provided_key and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() in ("bearer", "apikey"):
            provided_key = parts[1]
        else:
            provided_key = authorization

    if safe_eq(provided_key, expected_key):
        return True
    raise HTTPException(status_code=401, detail="Unauthorized dashboard access")


@router.get("", response_class=HTMLResponse)
async def dashboard_overview(request: Request, _auth=Depends(verify_dashboard_auth)):
    """Overview metrics page computed directly from the database."""
    market = os.getenv("MARKET", "india").lower()
    async with get_session() as session:
        # Lead counts by tier
        stmt_counts = select(Lead.tier, func.count(Lead.id)).group_by(Lead.tier)
        res_counts = await session.execute(stmt_counts)
        tier_counts = dict(res_counts.all())

        hot_c = tier_counts.get("hot", 0)
        warm_c = tier_counts.get("warm", 0)
        pending_c = tier_counts.get("pending", 0)
        dead_c = tier_counts.get("dead", 0)

        # Site visits count from site_visits table
        stmt_v = select(func.count(SiteVisit.id)).where(SiteVisit.status.in_(("booked", "done")))
        visits_booked = (await session.execute(stmt_v)).scalar_one() or 0

        # Total leads and contact rate computed from leads table
        total_leads = hot_c + warm_c + pending_c + dead_c
        conversed_stmt = select(func.count(Lead.id)).where(Lead.status.in_(("conversed", "visit_booked")))
        conversed_c = (await session.execute(conversed_stmt)).scalar_one() or 0
        contact_rate = round((conversed_c / total_leads * 100), 1) if total_leads > 0 else 0.0

        # Check for presence of seed sample data
        stmt_sample = select(func.count(Lead.id)).where(Lead.source == "seed")
        has_sample_data = ((await session.execute(stmt_sample)).scalar_one() or 0) > 0

        # Compute speed-to-lead and SLA adherence from actual call timestamps
        stmt_timing = select(Lead.created_at, Lead.first_contact_at).where(
            Lead.first_contact_at.isnot(None),
            Lead.created_at.isnot(None),
        )
        timing_rows = (await session.execute(stmt_timing)).all()

        call_diffs_sec = []
        for cr_at, fc_at in timing_rows:
            if cr_at and fc_at:
                diff = (fc_at - cr_at).total_seconds()
                if diff >= 0:
                    call_diffs_sec.append(diff)

        median_contact_sec = (
            round(statistics.median(call_diffs_sec)) if call_diffs_sec else None
        )
        sla_adherence = (
            round((sum(1 for d in call_diffs_sec if d <= 60.0) / len(call_diffs_sec) * 100), 1)
            if call_diffs_sec
            else None
        )

        # Campaign statistics rollup from database
        stmt_campaigns = (
            select(
                Lead.campaign_id,
                func.count(Lead.id).label("total_leads"),
                func.count(Lead.id).filter(Lead.status.in_(("conversed", "visit_booked"))).label("contacted"),
                func.count(Lead.id).filter(Lead.tier == "hot").label("hot_count"),
                func.count(Lead.id).filter(Lead.status == "visit_booked").label("visits_booked"),
            )
            .group_by(Lead.campaign_id)
            .order_by(func.count(Lead.id).desc())
        )
        camp_res = await session.execute(stmt_campaigns)
        campaign_stats = []
        for r in camp_res.all():
            t_leads = r.total_leads or 0
            cnt = r.contacted or 0
            c_rate = round((cnt / t_leads * 100), 1) if t_leads > 0 else 0.0
            campaign_stats.append({
                "campaign_id": r.campaign_id,
                "total_leads": t_leads,
                "contact_rate": c_rate,
                "hot_count": r.hot_count or 0,
                "visits_booked": r.visits_booked or 0,
            })

    stats = {
        "hot_count": hot_c,
        "warm_count": warm_c,
        "pending_count": pending_c,
        "dead_count": dead_c,
        "visits_booked": visits_booked,
        "contact_rate": contact_rate,
        "median_contact_sec": median_contact_sec,
        "sla_adherence": sla_adherence,
        "has_sample_data": has_sample_data,
    }

    return templates.TemplateResponse(
        request=request,
        name="overview.html",
        context={
            "market": market,
            "active_tab": "overview",
            "poll_url": "/dashboard",
            "stats": stats,
            "campaign_stats": campaign_stats,
            "has_sample_data": has_sample_data,
        },
    )


@router.get("/board", response_class=HTMLResponse)
async def dashboard_board(request: Request, _auth=Depends(verify_dashboard_auth)):
    """Kanban board page."""
    market = os.getenv("MARKET", "india").lower()
    async with get_session() as session:
        stmt = (
            select(Lead)
            .where(Lead.tier.in_(("hot", "warm", "pending")))
            .order_by(Lead.score.desc(), Lead.created_at.desc())
        )
        res = await session.execute(stmt)
        leads = res.scalars().all()

        hot_leads = []
        warm_leads = []
        pending_leads = []

        for lead in leads:
            lead.masked_phone = mask_phone(lead.phone)
            if lead.tier == "hot":
                hot_leads.append(lead)
            elif lead.tier == "warm":
                warm_leads.append(lead)
            else:
                pending_leads.append(lead)

    return templates.TemplateResponse(
        request=request,
        name="board.html",
        context={
            "market": market,
            "active_tab": "board",
            "poll_url": "/dashboard/board",
            "hot_leads": hot_leads,
            "warm_leads": warm_leads,
            "pending_leads": pending_leads,
        },
    )


@router.get("/visits", response_class=HTMLResponse)
async def dashboard_visits(request: Request, _auth=Depends(verify_dashboard_auth)):
    """Site visits queue page."""
    market = os.getenv("MARKET", "india").lower()
    async with get_session() as session:
        # Priority Call Now: Hot leads with visit_genuine without site_visits row
        stmt_call_now = (
            select(Lead)
            .where(
                Lead.tier == "hot",
                Lead.visit_genuine == True,  # noqa: E712
                Lead.status != "visit_booked",
            )
            .order_by(Lead.score.desc())
        )
        res_cn = await session.execute(stmt_call_now)
        call_now_leads = res_cn.scalars().all()
        for l in call_now_leads:
            l.masked_phone = mask_phone(l.phone)

        # Upcoming booked site visits
        stmt_v = (
            select(SiteVisit)
            .options(selectinload(SiteVisit.lead).selectinload(Lead.project))
            .where(SiteVisit.status == "booked")
            .order_by(SiteVisit.slot_start.asc())
        )
        res_v = await session.execute(stmt_v)
        upcoming_visits = res_v.scalars().all()

    return templates.TemplateResponse(
        request=request,
        name="visits.html",
        context={
            "market": market,
            "active_tab": "visits",
            "poll_url": "/dashboard/visits",
            "call_now_leads": call_now_leads,
            "upcoming_visits": upcoming_visits,
        },
    )


@router.post("/visits/{visit_id}/{action}")
async def update_visit_status(
    visit_id: str,
    action: str,
    request: Request,
    _auth=Depends(verify_dashboard_auth),
):
    """Actions on booked visits (done, no-show, cancel)."""
    valid_actions = {"done": "done", "no-show": "no_show", "cancel": "cancelled"}
    if action not in valid_actions:
        raise HTTPException(status_code=400, detail="Invalid action")

    status_val = valid_actions[action]
    async with get_session() as session:
        visit = await session.get(SiteVisit, uuid.UUID(visit_id))
        if visit:
            visit.status = status_val

    return await dashboard_visits(request, _auth)


@router.get("/leads/{lead_id}", response_class=HTMLResponse)
async def dashboard_lead_detail(lead_id: str, request: Request, _auth=Depends(verify_dashboard_auth)):
    """Lead detail timeline and analysis page."""
    market = os.getenv("MARKET", "india").lower()
    async with get_session() as session:
        lead = await session.get(Lead, uuid.UUID(lead_id))
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")

        stmt_tp = select(Touchpoint).where(Touchpoint.lead_id == lead.id).order_by(Touchpoint.occurred_at.desc())
        res_tp = await session.execute(stmt_tp)
        touchpoints = res_tp.scalars().all()
        for tp in touchpoints:
            tp.transcript = None
            if tp.call_id:
                rep_path = Path(__file__).parent.parent / "outputs" / "call_reports" / f"{tp.call_id}.md"
                if rep_path.exists():
                    try:
                        content = rep_path.read_text(encoding="utf-8")
                        if "## Transcript" in content:
                            tp.transcript = content.split("## Transcript")[1].replace("```", "").strip()
                    except Exception:
                        pass

    return templates.TemplateResponse(
        request=request,
        name="lead_detail.html",
        context={
            "market": market,
            "active_tab": "board",
            "poll_url": f"/dashboard/leads/{lead_id}",
            "lead": lead,
            "touchpoints": touchpoints,
        },
    )
