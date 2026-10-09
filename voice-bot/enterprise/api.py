"""Authenticated management endpoints and separately signed vendor callbacks."""
import asyncio
import csv
import io
import json
import math
import os
import time
import uuid
from datetime import datetime,timezone,timedelta
from xml.etree.ElementTree import Element,SubElement,tostring
from fastapi import APIRouter,Depends,HTTPException,Request
from fastapi.responses import Response,HTMLResponse
from sqlalchemy import select
from leads.dashboard import verify_dashboard_auth
from leads.db import get_session
from leads.models import Lead,Project,Touchpoint,SiteVisit
from enterprise import store,meta,routing,recordings,analytics,reconcile,delivery
from enterprise.config import policy,readiness
from enterprise.scoring import qualification
from enterprise.forms import provider_form

router=APIRouter(prefix='/enterprise',tags=['Enterprise'])
_CONFIG={};_worker=None

def configure(config):
    global _CONFIG
    _CONFIG=config


def config():return _CONFIG


def verify_vendor(request):
    # Reuse existing signed Vobiz handler, never trust path secrecy as authorization.
    from main import _verify_vobiz_signature
    _verify_vobiz_signature(request)


@router.get('/readiness')
async def get_readiness(auth=Depends(verify_dashboard_auth)):
    return {**readiness(_CONFIG),'whatsapp':meta.state(),'reporting':report_readiness(),'notifications':delivery.readiness(_CONFIG),
       'deployment':{'scope':'single_host','live_validated':False,
          'caveats':['Telephony/PSTN billing is not application quota seconds','Human speaker attribution requires review','Provider recording deletion must be configured separately']}}


@router.post('/whatsapp/refresh')
async def refresh_meta(auth=Depends(verify_dashboard_auth)):
    return await meta.refresh()


@router.get('/whatsapp/webhook')
async def meta_challenge(request:Request):
    settings=meta.meta_settings();q=request.query_params
    import hmac
    if not settings['verify_token'] or q.get('hub.mode')!='subscribe' or not hmac.compare_digest(q.get('hub.verify_token',''),settings['verify_token']):raise HTTPException(403)
    return Response(q.get('hub.challenge',''),media_type='text/plain')


@router.post('/whatsapp/webhook')
async def meta_status(request:Request):
    body=await request.body()
    if len(body)>3*1024*1024:raise HTTPException(413)
    if not meta.verify_signature(body,request.headers.get('x-hub-signature-256','')):raise HTTPException(403)
    try:payload=json.loads(body)
    except ValueError:raise HTTPException(400)
    meta.ingest_status(payload)
    return {'ok':True}


@router.post('/transfer/{session_id}')
async def transfer_xml(session_id:str,request:Request):
    verify_vendor(request)
    data=await provider_form(request);row=store.session(session_id)
    if not row or data.get('CallUUID') not in {session_id,row['id']}:raise HTTPException(404)
    return Response(routing.bridge_xml(session_id,_CONFIG,record=policy(_CONFIG).get('record_transfers',False)),media_type='application/xml')


@router.post('/dial/{session_id}')
async def dial_callback(session_id:str,request:Request):
    verify_vendor(request);data=await provider_form(request)
    if (data.get('DialALegUUID') or data.get('CallUUID'))!=session_id:raise HTTPException(403)
    routing.dial_event(session_id,data,_CONFIG)
    return {'ok':True}


@router.post('/dial-result/{session_id}')
async def dial_result(session_id:str,request:Request):
    verify_vendor(request);data=await provider_form(request)
    if (data.get('DialALegUUID') or data.get('CallUUID'))!=session_id:raise HTTPException(403)
    routing.dial_event(session_id,data,_CONFIG,final=True)
    root=Element('Response')
    if data.get('DialStatus') in {'busy','failed','cancel','timeout','no-answer'}:
        message=policy(_CONFIG).get('routing',{}).get('callback_message')
        if message:SubElement(root,'Speak').text=message
    SubElement(root,'Hangup')
    return Response(tostring(root,encoding='unicode'),media_type='application/xml')


@router.post('/recording/{session_id}')
async def recording_callback(session_id:str,request:Request):
    verify_vendor(request);data=await provider_form(request)
    if (data.get('CallUUID') or data.get('ALegUUID'))!=session_id:raise HTTPException(403)
    # Record-start callbacks are not evidence that media is ready.
    if not (data.get('RecordUrl') or data.get('RecordFile')):return {'status':'awaiting_recording'}
    return recordings.queue(session_id,data,_CONFIG)


@router.post('/call-status/{session_id}')
async def call_status(session_id:str,request:Request):
    verify_vendor(request);data=await provider_form(request)
    if data.get('CallUUID')!=session_id:raise HTTPException(403)
    row=store.session(session_id)
    if not row:raise HTTPException(404)
    status=data.get('CallStatus','')
    if status in {'completed','busy','failed','no-answer','canceled'}:
        store.finish(session_id,status)
        if status!='completed':
            store.event(row['tenant'],session_id,'missed_call',{'phone':row['phone'],'lead_id':row['lead_id'],'status':status},session_id+':missed')
    return {'ok':True}


@router.get('/work')
async def work_status(auth=Depends(verify_dashboard_auth)):
    tenant=policy(_CONFIG).get('tenant_id')
    with store.transaction() as db:
        items=[dict(r) for r in db.execute('SELECT id,kind,session_id,state,attempts,provider_id,last_error,created,expires FROM work WHERE tenant=? ORDER BY created DESC LIMIT 100',(tenant,))]
        events=[dict(r) for r in db.execute('SELECT id,session_id,kind,payload,created FROM events WHERE tenant=? ORDER BY created DESC LIMIT 100',(tenant,))]
    return {'work':items,'events':events,'alert_delivery':'local authenticated queue; no external notification sent'}


@router.post('/events/{event_id}/ack')
async def acknowledge(event_id:int,auth=Depends(verify_dashboard_auth)):
    with store.transaction() as db:
        row=db.execute('SELECT tenant,payload FROM events WHERE id=?',(event_id,)).fetchone()
        if not row or row['tenant']!=policy(_CONFIG).get('tenant_id'):raise HTTPException(404)
        payload=json.loads(row['payload']);payload['operator_ack_at']=time.time()
        db.execute('UPDATE events SET payload=? WHERE id=?',(json.dumps(payload),event_id))
    return {'status':'acknowledged'}


@router.post('/human-review/{lead_id}/{call_id}')
async def human_review(lead_id:str,call_id:str,request:Request,auth=Depends(verify_dashboard_auth)):
    body=await request.json()
    if not isinstance(body,dict) or body.get('caller_evidence_reviewed') is not True:raise HTTPException(422,detail='Review caller-specific budget/timeline/visit evidence; do not score rep statements')
    analysis={k:body.get(k) for k in ('budget_min_lakhs','budget_max_lakhs','timeline_months','configuration','visit_intent')}
    for k in ('budget_min_lakhs','budget_max_lakhs','timeline_months'):
        v=analysis[k]
        if v is not None and (not isinstance(v,(float,int)) or isinstance(v,bool) or not math.isfinite(v) or v<0):raise HTTPException(422)
    if analysis['visit_intent'] not in {'none','maybe','requested','booked'}:raise HTTPException(422)
    async with get_session() as db:
        try:lead=await db.get(Lead,uuid.UUID(lead_id))
        except ValueError:raise HTTPException(404)
        if not lead or str(lead.project_id)!=policy(_CONFIG).get('project_id'):raise HTTPException(404)
        transcript=(await db.execute(select(Touchpoint).where(Touchpoint.lead_id==lead.id,Touchpoint.call_id==call_id,Touchpoint.kind=='human_transcript'))).scalars().first()
        if not transcript or (transcript.payload or {}).get('recording_status')=='retention_expired':raise HTTPException(409,detail='Transcript missing or expired')
        project=await db.get(Project,lead.project_id)
        verified=(await db.execute(select(SiteVisit).where(SiteVisit.lead_id==lead.id,SiteVisit.call_id==call_id,SiteVisit.status.in_(('booked','done'))))).scalars().first() is not None
        lead.score,lead.tier,lead.score_reason,lead.visit_genuine=qualification(analysis,project.config if project else {},verified)
        db.add(Touchpoint(lead_id=lead.id,call_id=call_id,kind='qualification_review',summary=lead.score_reason,
          payload={'analysis':analysis,'source':'operator-reviewed caller evidence','tier':lead.tier,'score':lead.score}))
        return {'score':lead.score,'tier':lead.tier,'reason':lead.score_reason,'booking_verified':verified}


def report_readiness():
    r=policy(_CONFIG).get('reports',{}) or {}
    gaps=[k for k in ('brand','anchor_date','timezone') if not r.get(k)]
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo(r.get('timezone',''));datetime.fromisoformat(r.get('anchor_date',''))
    except (ValueError,KeyError,TypeError):gaps.append('valid anchor_date/timezone')
    return {'ready':not gaps,'gaps':gaps,'delivery':'private downloadable report only; recipient/channel unresolved, no automatic sends'}


async def build_report(since,until):
    p=policy(_CONFIG)
    report=store.summary(p.get('tenant_id',''),since,until)
    async with get_session() as db:
        try:project=uuid.UUID(p.get('project_id',''))
        except ValueError:return {**report,'crm':[],'gap':'project not configured'}
        leads=(await db.execute(select(Lead).where(Lead.project_id==project))).scalars().all()
        rows=[]
        for lead in leads:
            touches=(await db.execute(select(Touchpoint).where(Touchpoint.lead_id==lead.id,
              Touchpoint.occurred_at>=datetime.fromtimestamp(since,timezone.utc),Touchpoint.occurred_at<datetime.fromtimestamp(until,timezone.utc)))).scalars().all()
            if not touches:continue
            visits=(await db.execute(select(SiteVisit).where(SiteVisit.lead_id==lead.id))).scalars().all()
            rows.append({'phone':lead.phone,'name':lead.name,'tier':lead.tier,'score':lead.score,'reason':lead.score_reason,
              'touchpoints_in_period':len(touches),'visits':[{'day':v.visit_date_iso,'time':v.time_slot,'status':v.status} for v in visits]})
    return {**report,'brand':p.get('reports',{}).get('brand',''),'timezone':p.get('reports',{}).get('timezone') or 'UTC','crm':rows,'privacy':'Authenticated download includes caller PII; review before onward sharing'}


@router.get('/report')
async def download_report(since:float,until:float,auth=Depends(verify_dashboard_auth)):
    if not math.isfinite(since) or not math.isfinite(until) or since>=until or until-since>31*86400:raise HTTPException(422)
    return await build_report(since,until)


async def fortnightly():
    if not report_readiness()['ready']:return
    from zoneinfo import ZoneInfo
    r=policy(_CONFIG)['reports'];tz=ZoneInfo(r['timezone'])
    anchor=datetime.fromisoformat(r['anchor_date']).replace(tzinfo=tz)
    now=datetime.now(tz);period=int((now-anchor).total_seconds()//(14*86400))
    if period<1:return
    end=anchor+timedelta(days=period*14);start=end-timedelta(days=14)
    dedupe=f"{policy(_CONFIG)['tenant_id']}:report:{start.date()}:{end.date()}"
    with store.transaction() as db:
        existing=db.execute('SELECT id FROM work WHERE dedupe=?',(dedupe,)).fetchone()
    if existing:return
    report=await build_report(start.timestamp(),end.timestamp())
    item=store.enqueue('report',policy(_CONFIG)['tenant_id'],'',report,dedupe)
    store.complete(item['id'],'prepared_not_sent',payload=report)


@router.get('/reports/{work_id}')
async def stored_report(work_id:str,auth=Depends(verify_dashboard_auth)):
    with store.transaction() as db:
        row=db.execute("SELECT tenant,payload FROM work WHERE id=? AND kind='report'",(work_id,)).fetchone()
    if not row or row['tenant']!=policy(_CONFIG).get('tenant_id'):raise HTTPException(404)
    report=json.loads(row['payload'])
    if not report:raise HTTPException(410,detail='Report expired')
    return report


async def start_worker(config):
    global _worker
    configure(config)
    await meta.refresh()
    async def tick(kind):
        last_refresh=0;last_maintenance=0
        while True:
            try:
                if kind=='meta':
                    if time.time()-last_refresh>240:await meta.refresh();last_refresh=time.time()
                    await meta.drain()
                elif kind=='recording' and policy(config).get('enabled'):await recordings.drain(config)
                elif kind=='analytics' and policy(config).get('enabled'):await analytics.drain(policy(config).get('tenant_id'))
                elif kind=='maintenance' and time.time()-last_maintenance>60:
                    await reconcile.check_due(config);await recordings.prune(config);await fortnightly();delivery.sync(config);await delivery.drain(config);last_maintenance=time.time()
            except asyncio.CancelledError:raise
            except Exception:
                from loguru import logger
                logger.exception('Enterprise background work failed; inspect authenticated work queue')
            await asyncio.sleep(5)
    _worker=[asyncio.create_task(tick(kind)) for kind in ('meta','recording','analytics','maintenance')]


async def stop_worker():
    global _worker
    if _worker:
        for task in _worker:task.cancel()
        await asyncio.gather(*_worker,return_exceptions=True)
        _worker=None


@router.post('/sessions/{session_id}/reconcile')
async def reconcile_call(session_id:str,request:Request,auth=Depends(verify_dashboard_auth)):
    body=await request.json();row=store.session(session_id)
    if not row or row['tenant']!=policy(_CONFIG).get('tenant_id'):raise HTTPException(404)
    duration=body.get('provider_confirmed_duration_seconds')
    if body.get('provider_end_verified') is not True or not isinstance(duration,(float,int)) or isinstance(duration,bool) or not math.isfinite(duration) or duration<0:raise HTTPException(422)
    store.reconcile_session(session_id,'operator_provider_end_verified',duration)
    return {'status':'reconciled'}


@router.get('/crm')
async def crm_list(auth=Depends(verify_dashboard_auth)):
    p=policy(_CONFIG)
    try:project=uuid.UUID(p.get('project_id',''))
    except ValueError:return {'ready':False,'gaps':['Configure exact enterprise.project_id'],'leads':[]}
    async with get_session() as db:
        leads=(await db.execute(select(Lead).where(Lead.project_id==project).order_by(Lead.last_touch_at.desc()).limit(100))).scalars().all()
        return {'ready':True,'leads':[{'id':str(l.id),'phone':l.phone,'name':l.name,'tier':l.tier,'score':l.score,
          'reason':l.score_reason,'status':l.status,'last_touch_at':l.last_touch_at} for l in leads]}


@router.post('/visits/{visit_id}/status')
async def visit_status(visit_id:str,request:Request,auth=Depends(verify_dashboard_auth)):
    body=await request.json();status=body.get('status')
    if status not in {'done','no_show','canceled'}:raise HTTPException(422)
    async with get_session() as db:
        try:visit=await db.get(SiteVisit,uuid.UUID(visit_id))
        except ValueError:raise HTTPException(404)
        if not visit:raise HTTPException(404)
        lead=await db.get(Lead,visit.lead_id)
        if not lead or str(lead.project_id)!=policy(_CONFIG).get('project_id'):raise HTTPException(404)
        previous=visit.status;visit.status=status
        db.add(Touchpoint(lead_id=lead.id,call_id=visit.call_id,kind='visit_status',summary=f'Visit status {previous} -> {status}',payload={'visit_id':visit_id,'status':status}))
    return {'status':status}


@router.post('/call-ended')
async def app_call_end(request:Request):
    verify_vendor(request);data=await provider_form(request)
    session_id=data.get('CallUUID') or data.get('ALegUUID')
    row=store.session(session_id)
    if not row:return {'status':'untracked_call'}
    status=data.get('CallStatus','')
    if status not in {'completed','busy','failed','no-answer','canceled'}:return {'status':'awaiting_final_state'}
    store.finish(session_id,'signed_hangup_'+status)
    if status!='completed':store.event(row['tenant'],session_id,'missed_call',{'lead_id':row['lead_id'],'phone':row['phone'],'status':status},session_id+':missed')
    return {'status':'finalized'}


@router.get('/reports/{work_id}/html',response_class=HTMLResponse)
async def forwardable_report(work_id:str,auth=Depends(verify_dashboard_auth)):
    report=await stored_report(work_id,auth)
    from enterprise.report import report_html
    return HTMLResponse(report_html(report),headers={'Content-Security-Policy':"default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'",'Cache-Control':'no-store'})
