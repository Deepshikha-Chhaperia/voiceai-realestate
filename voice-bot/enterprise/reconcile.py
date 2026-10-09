"""Bounded source-of-truth call reconciliation; missing CDR keeps reservation held."""
import math
import os
import time
import httpx
from enterprise import store
from enterprise.config import policy,readiness


async def check_call(session_id,client_factory=None):
    row=store.session(session_id)
    if not row or row['state']!='active':return {'status':'not_active'}
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=8) as client:
            resp=await client.get(f"https://api.vobiz.ai/api/v1/Account/{os.getenv('VOBIZ_AUTH_ID')}/Call/{session_id}/",
              headers={'X-Auth-ID':os.getenv('VOBIZ_AUTH_ID'),'X-Auth-Token':os.getenv('VOBIZ_AUTH_TOKEN')})
            if resp.status_code==404:return {'status':'unverified','reason':'Final CDR not available;404 does not prove end'}
            resp.raise_for_status();data=resp.json()
            if data.get('call_uuid')!=session_id or not data.get('end_time'):return {'status':'unverified','reason':'Final matching CDR missing'}
            duration=data.get('call_duration')
            if not isinstance(duration,(int,float)) or isinstance(duration,bool) or not math.isfinite(duration) or duration<0:return {'status':'unverified','reason':'Final duration missing'}
            store.finish(session_id,'provider_cdr_end_verified',duration)
            store.event(row['tenant'],session_id,'provider_cdr',{'duration_seconds':duration,'bill_duration':data.get('bill_duration'),
              'billed_duration':data.get('billed_duration'),'parent_call_uuid':data.get('parent_call_uuid')},session_id+':cdr')
            return {'status':'finalized','duration_seconds':duration}
    except Exception as exc:return {'status':'unverified','reason':type(exc).__name__}


async def check_due(config):
    if not policy(config).get('enabled') or not readiness(config)['quota']['ready']:return
    with store.transaction() as db:
        rows=[dict(r) for r in db.execute("SELECT id,tenant,started,reserved FROM sessions WHERE state='active' AND tenant=? AND started+reserved<? ORDER BY started LIMIT 20",(policy(config)['tenant_id'],time.time()-60))]
    for row in rows:await check_call(row['id'])
