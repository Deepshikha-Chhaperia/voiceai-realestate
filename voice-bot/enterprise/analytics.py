"""Enterprise scoring job with durable ownership and source-scoped evidence."""
import json
import uuid
from sqlalchemy import select
from leads.db import get_session
from leads.models import Lead,Project,Touchpoint,SiteVisit
from enterprise import store
from enterprise.scoring import qualification


def enqueue(tenant,call_id,lead_id,messages,retention_days):
    import time
    return store.enqueue('analytics',tenant,call_id,{'lead_id':lead_id,'messages':messages},f'{tenant}:{call_id}:analytics',expires=time.time()+retention_days*86400)


async def drain(tenant=None):
    item=store.claim('analytics',tenant)
    if not item:return
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
            if not existing:db.add(Touchpoint(lead_id=lead.id,call_id=item['session_id'],kind='enterprise_qualification',
               summary=lead.score_reason,payload={'analysis':analysis,'score':lead.score,'tier':lead.tier,'source':'AI caller-role transcript'}))
        store.complete(item['id'],'completed',payload={'lead_id':item['payload']['lead_id'],'status':'scored'})
    except Exception as exc:store.complete(item['id'],'needs_operator',error=type(exc).__name__)
