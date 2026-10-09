"""Reviewed evidence scoring, no guesses for missing budget or timeline."""
import math


def qualification(analysis,project_config,booking_verified=False):
    analysis=analysis or {};cfg=project_config or {};points=0;reasons=[];known=0
    def finite(v):return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>=0
    low=analysis.get('budget_min_lakhs');high=analysis.get('budget_max_lakhs')
    minimum=cfg.get('min_price_lakhs');maximum=cfg.get('max_price_lakhs')
    if finite(low) or finite(high):
        b_low=low if finite(low) else high;b_high=high if finite(high) else low
        if b_low>b_high:reasons.append('budget invalid; needs review')
        elif finite(minimum) and finite(maximum) and minimum<=maximum:
            known+=1
            if b_high>=minimum and b_low<=maximum:points+=30;reasons.append('budget overlaps project range')
            else:reasons.append('budget outside project range')
        else:reasons.append('project price range missing; budget unscored')
    else:reasons.append('budget unknown')
    timeline=analysis.get('timeline_months')
    if finite(timeline):
        known+=1
        if timeline<=3:points+=25;reasons.append('buying within3months')
        elif timeline<=6:points+=10;reasons.append('buying within6months')
        else:reasons.append('timeline beyond6months')
    else:reasons.append('timeline unknown')
    visit=analysis.get('visit_intent','none')
    if visit in {'requested','booked'}:
        known+=1;points+=40;reasons.append('verified visit booked' if booking_verified else 'visit requested, not a confirmed booking')
    elif visit=='maybe':known+=1;points+=10;reasons.append('visit undecided')
    config=str(analysis.get('configuration') or '').upper()
    if config and config in {str(c).upper() for c in cfg.get('unit_types',[])}:points+=5;reasons.append('unit in inventory')
    tier='pending' if known==0 else 'hot' if points>=70 else 'warm' if points>=30 else 'cold'
    return min(points,100),tier,'; '.join(reasons),bool(booking_verified and visit in {'requested','booked'})
