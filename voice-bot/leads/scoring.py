"""
Deterministic Lead Scoring Engine.

Pure function: score(lead, analysis, disposition, call_stats) -> (score, tier, reason, genuine)
No I/O, fully unit tested.
"""

from typing import Any


def score(
    lead: Any,
    analysis: dict[str, Any] | None,
    disposition: str | None,
    call_stats: dict[str, Any] | None,
    project_config: dict[str, Any] | None = None,
) -> tuple[int, str, str, bool]:
    """Scores a lead deterministically according to business rules.
    
    Returns:
        tuple of (score: int, tier: str, score_reason: str, visit_genuine: bool)
    """
    analysis = analysis or {}
    call_stats = call_stats or {}
    disp = (disposition or "").strip().upper()
    turns = int(call_stats.get("caller_turns", call_stats.get("turns", 0)))
    duration_s = float(call_stats.get("talk_time_s", call_stats.get("duration_s", 0.0)))

    # Rule 1: Explicit Dead / Negative Dispositions
    if disp in ("DNC_REQUESTED", "WRONG_NUMBER", "NOT_INTERESTED", "NOT_LOOKING"):
        reason = f"Caller declined ({disp.replace('_', ' ').lower()})"
        return (0, "dead", reason, False)

    # Rule 2: Pending / Incomplete / No Conversation yet
    if turns < 3 or disp in ("NO_RESPONSE", "CALLBACK_REQUESTED", "INCOMPLETE", "LEFT_VOICEMAIL", ""):
        reason = "Pending contact" if turns == 0 else f"Incomplete conversation ({turns} turns)"
        if disp == "CALLBACK_REQUESTED":
            reason = "Callback requested by caller"
        return (0, "pending", reason, False)

    # Step 3: Compute Score Points
    pts = 0
    reasons: list[str] = []

    # Project price range defaults (INR in Lakhs)
    if project_config is None:
        project_config = {}
        if isinstance(lead, dict):
            project_config = lead.get("project_config") or {}
        else:
            # Check __dict__ to avoid triggering SQLAlchemy async lazy load
            lead_dict = getattr(lead, "__dict__", {})
            proj = lead_dict.get("project")
            if proj is not None and hasattr(proj, "config"):
                project_config = proj.config or {}
            elif hasattr(lead, "project_config"):
                project_config = lead.project_config or {}

    if project_config.get("enterprise_scoring"):
        from enterprise.scoring import qualification
        return qualification(analysis, project_config, booking_verified=(disp == "SITE_VISIT_BOOKED"))

    min_project_price = float(project_config.get("min_price_lakhs", 95.0))
    max_project_price = float(project_config.get("max_price_lakhs", 180.0))

    # A) Budget overlap (+30)
    b_min = analysis.get("budget_min_lakhs")
    b_max = analysis.get("budget_max_lakhs")
    budget_contradicts = False

    if b_min is not None or b_max is not None:
        b_low = float(b_min) if b_min is not None else float(b_max)
        b_high = float(b_max) if b_max is not None else float(b_min)
        if b_high < min_project_price * 0.7:
            budget_contradicts = True
        elif b_low > max_project_price * 1.5:
            budget_contradicts = True
        else:
            pts += 30
            reasons.append("budget matches range")

    # B) Configuration in inventory (+20)
    config = (analysis.get("configuration") or "").strip().upper()
    valid_configs = project_config.get("unit_types", ["2BHK", "3BHK", "3BHK_LARGE", "2 BR", "3 BR", "4 BR"])
    if config and (config in valid_configs or any(c in config for c in ("2", "3", "4", "BHK", "BR"))):
        pts += 20
        reasons.append(f"{config} requested")

    # C) Timeline (<=3 mo: +25; <=6 mo: +10)
    timeline = analysis.get("timeline_months")
    if timeline is not None:
        try:
            t_val = int(timeline)
            if t_val <= 3:
                pts += 25
                reasons.append(f"{t_val}mo timeline")
            elif t_val <= 6:
                pts += 10
                reasons.append(f"{t_val}mo timeline")
        except (ValueError, TypeError):
            pass

    # D) Visit Intent (+50 for booked, +40 for requested, +15 for maybe)
    visit_intent = (analysis.get("visit_intent") or "none").strip().lower()
    if visit_intent == "maybe":
        pts += 15
        reasons.append("open to site visit")
    elif visit_intent == "booked":
        pts += 50
        reasons.append("site visit requested")
    elif visit_intent == "requested":
        pts += 40
        reasons.append("site visit requested")

    # E) Asked price or plan (+10)
    if analysis.get("asked_price_or_plan"):
        pts += 10
        reasons.append("asked payment plan/pricing")

    # F) 2 or more objections (-20)
    objections = analysis.get("objections") or []
    if len(objections) >= 2:
        pts = max(0, pts - 20)
        reasons.append(f"{len(objections)} objections raised")

    # Step 4: Tier Decision
    if visit_intent in ("requested", "booked") or pts >= 70:
        tier = "hot"
        # Ensure any Hot lead strictly meets the column threshold (>= 70)
        pts = max(pts, 70)
    else:
        tier = "warm"

    # Step 5: Visit Genuine Check
    visit_genuine = (
        visit_intent in ("requested", "booked")
        and turns >= 3
        and duration_s >= 25.0
        and not budget_contradicts
    )

    # Step 6: Human Score Reason
    if not reasons:
        reasons.append("conversational inquiry")
    score_reason = ", ".join(reasons)

    return (pts, tier, score_reason, visit_genuine)
