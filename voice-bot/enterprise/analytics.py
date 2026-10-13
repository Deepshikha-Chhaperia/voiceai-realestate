"""Enterprise scoring job with durable ownership and source-scoped evidence."""
import json
import uuid
from sqlalchemy import select
from leads.db import get_session
from leads.models import Lead,Project,Touchpoint,SiteVisit
from enterprise import store
from enterprise.scoring import qualification
from enterprise import crm
from enterprise.config import policy


def enqueue(tenant,call_id,lead_id,messages,retention_days,meta=None):
    import time
    m=meta if isinstance(meta,dict) else {}
    return store.enqueue('analytics',tenant,call_id,{'lead_id':lead_id,'messages':messages,
      'early_intent':m.get('early_intent'),'repeat':m.get('repeat')},f'{tenant}:{call_id}:analytics',expires=time.time()+retention_days*86400)


async def drain(tenant=None,config=None):
    item=store.claim('analytics',tenant)
    if not item:return
    snap=None
    try:
        from call_analytics import analyze_call
        data=await analyze_call(item['session_id'],item['payload']['messages'])
        async with get_session() as db:
            lead=await db.get(Lead,uuid.UUID(item['payload']['lead_id']))
            if not lead:raise ValueError('Lead missing')
            project=await db.get(Project,lead.project_id)
            verified=(await db.execute(select(SiteVisit).where(SiteVisit.call_id==item['session_id'],SiteVisit.lead_id==lead.id,SiteVisit.status.in_(('booked','done'))))).scalars().first() is not None
            analysis=data.get('analysis',{})
            lead.score,lead.tier,lead.score_reason,lead.visit_genuine=qualification(analysis,project.config if project else {},verified)
            existing=(await db.execute(select(Touchpoint).where(Touchpoint.call_id==item['session_id'],Touchpoint.kind=='enterprise_qualification'))).scalars().first()
            call=data.get('call') or {}
            fields=call.get('lead_fields') or {}
            if isinstance(fields,str):
                try:fields=json.loads(fields)
                except ValueError:fields={}
            turns=int(call.get('caller_turns',call.get('turns',0)) or 0)
            pl=item['payload']
            if not existing:
                db.add(Touchpoint(lead_id=lead.id,call_id=item['session_id'],kind='enterprise_qualification',
                   summary=lead.score_reason,payload={'analysis':analysis,'score':lead.score,'tier':lead.tier,'source':'AI caller-role transcript',
                   'early_intent':pl.get('early_intent'),'repeat_caller':pl.get('repeat')}))
                crm.apply_outcome(lead,source='ai_call',call_id=item['session_id'],name=(fields or {}).get('name') or (fields or {}).get('customer_name'),
                   spoke=turns>=1,early_intent=pl.get('early_intent') or 'unknown',repeat=pl.get('repeat'),visit_booked=verified)
                visit_info=None
                if verified:
                    v=(await db.execute(select(SiteVisit).where(SiteVisit.call_id==item['session_id'],SiteVisit.lead_id==lead.id).limit(1))).scalars().first()
                    if v and v.slot_start:visit_info=v.slot_start.strftime('%A, %b %d at %I:%M %p')
                snap=crm.snapshot(lead,analysis=analysis,visit_info=visit_info,summary=analysis.get('summary'))
                await crm.queue_webhook(db,snap,item['session_id'])
        store.complete(item['id'],'completed',payload={'lead_id':item['payload']['lead_id'],'status':'scored'})
        if snap:await crm.dispatch(snap,config or {})
    except Exception as exc:store.complete(item['id'],'needs_operator',error=type(exc).__name__)
