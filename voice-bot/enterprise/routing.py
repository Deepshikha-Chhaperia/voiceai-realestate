"""Telephony call control. Accepted is never reported as connected."""
import asyncio
import os
import time
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from zoneinfo import ZoneInfo
import httpx
from enterprise.config import policy, readiness, e164
from enterprise import store


def available(config,now=None):
    r=policy(config).get('routing',{})
    if not readiness(config)['routing']['ready']:return False
    current=now or datetime.now(ZoneInfo(r['timezone']))
    day=current.weekday();clock=current.strftime('%H:%M')
    # Overnight shifts associate after-midnight portion with previous weekday.
    if r['open']<r['close']:return day in r['weekdays'] and r['open']<=clock<r['close']
    if r['open']>r['close']:return (day in r['weekdays'] and clock>=r['open']) or ((day-1)%7 in r['weekdays'] and clock<r['close'])
    return False


def callback(session_id,config,reason):
    row=store.session(session_id)
    if row:
        store.event(row['tenant'],session_id,'callback_required',{'phone':row['phone'],'lead_id':row['lead_id'],
          'reason':reason,'status':'needs_operator','message':policy(config).get('routing',{}).get('callback_message','')},f'{session_id}:callback')
    return {'status':'callback_required','connected':False,'reason':reason}


def callback_root():
    return 'https://'+os.getenv('PUBLIC_HOST','').rstrip('/')


def bridge_xml(session_id,config,record=False):
    p=policy(config);root=Element('Response');row=store.session(session_id)
    if not row or row['state']!='active' or not available(config):
        message=p.get('routing',{}).get('callback_message')
        if message:SubElement(root,'Speak').text=message
        SubElement(root,'Hangup');return tostring(root,encoding='unicode')
    if record and readiness(config)['human_mode']['ready']:
        SubElement(root,'Speak').text=p['recording']['notice']
        SubElement(root,'Record',{'recordSession':'true','fileFormat':'mp3','maxLength':str(row['reserved']),
          'playBeep':'true','redirect':'false','action':callback_root()+'/enterprise/recording/'+session_id,
          'callbackUrl':callback_root()+'/enterprise/recording/'+session_id,'callbackMethod':'POST'})
    remaining=max(1,row['reserved']-int(time.time()-row['started']))
    ring_timeout=min(50,max(1,remaining-1))
    # One simultaneous Dial <=50s; no sequential retries that extend hold beyond1min.
    dial=SubElement(root,'Dial',{'callerId':os.getenv('VOBIZ_FROM_NUMBER',''),'timeout':str(ring_timeout),
      'timeLimit':str(max(1,remaining-ring_timeout)), 'callbackUrl':callback_root()+'/enterprise/dial/'+session_id,
      'action':callback_root()+'/enterprise/dial-result/'+session_id,'redirect':'true'})
    for number in p['routing']['rep_numbers']:SubElement(dial,'Number').text=number
    SubElement(root,'Hangup');return tostring(root,encoding='unicode')


async def transfer(provider_call_id,session_id,config,reason,client_factory=None):
    if not provider_call_id or not available(config):return callback(session_id,config,'routing_unavailable')
    row=store.session(session_id)
    if not row:return {'status':'not_ready','connected':False,'reason':'session_missing'}
    # Persist intent before network; atomically claim prevents two concurrent tool/ceiling attempts.
    item=store.enqueue('transfer',row['tenant'],session_id,{'reason':reason},session_id+':transfer')
    if item['state']!='pending':return {'status':item['state'],'connected':item['state']=='connected'}
    with store.transaction() as db:
        changed=db.execute("UPDATE work SET state='processing',lease_until=? WHERE id=? AND state='pending'",(time.time()+60,item['id'])).rowcount
    if not changed:return {'status':'processing','connected':False}
    extra=policy(config)['routing']['max_call_seconds']
    if not store.extend_for_transfer(session_id,extra):
        store.complete(item['id'],'blocked',error='Insufficient monthly minute reservation for human bridge')
        return callback(session_id,config,'quota_blocks_transfer')
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=8) as client:
            resp=await client.post(f"https://api.vobiz.ai/api/v1/Account/{os.getenv('VOBIZ_AUTH_ID')}/Call/{provider_call_id}/",
             headers={'X-Auth-ID':os.getenv('VOBIZ_AUTH_ID'),'X-Auth-Token':os.getenv('VOBIZ_AUTH_TOKEN')},
             json={'legs':'aleg','aleg_url':callback_root()+'/enterprise/transfer/'+session_id,'aleg_method':'POST'})
            if resp.status_code in {200,201,202}:
                store.complete(item['id'],'initiated',provider_call_id)
                store.event(row['tenant'],session_id,'transfer_initiated',{'reason':reason},session_id+':transfer_initiated')
                return {'status':'initiated','connected':False}
            store.complete(item['id'],'failed',error='Vobiz rejected transfer: HTTP '+str(resp.status_code))
            return callback(session_id,config,'transfer_failed')
    except Exception as exc:
        # Ambiguous write: do not retry or hang up a possibly bridged leg.
        store.complete(item['id'],'uncertain',error=type(exc).__name__)
        callback(session_id,config,'transfer_outcome_unknown')
        return {'status':'uncertain','connected':False}


def dial_event(session_id,data,config,final=False):
    row=store.session(session_id)
    if not row:return
    event=data.get('Event') or '';status=data.get('DialStatus') or ''
    store.event(row['tenant'],session_id,'dial_event',{'event':event,'status':status,
      'leg':data.get('DialBLegUUID')},f"{session_id}:{event}:{data.get('DialBLegUUID','')}:{status}")
    if event=='DialConnected':
        with store.transaction() as db:
            db.execute("UPDATE work SET state='connected' WHERE dedupe=?",(session_id+':transfer',))
    if final:
        if status in {'busy','failed','cancel','timeout','no-answer'}:callback(session_id,config,status)
        store.finish(session_id,'dial_'+status)


async def persist_caller(phone,config,provider_call_id,source='inbound_call'):
    """No arbitrary project fallback and no blanket marketing consent."""
    import uuid
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError
    from leads.db import get_session
    from leads.models import Project, Lead, Touchpoint
    if not e164(phone):raise ValueError('Invalid caller phone')
    project_id=uuid.UUID(policy(config)['project_id'])
    async def save():
        async with get_session() as db:
            if not await db.get(Project,project_id):raise ValueError('Configured project does not exist')
            lead=(await db.execute(select(Lead).where(Lead.phone==phone,Lead.project_id==project_id))).scalar_one_or_none()
            repeat=lead is not None
            if not lead:
                lead=Lead(phone=phone,project_id=project_id,source=source,consent_at=None)
                db.add(lead);await db.flush()
            tp=(await db.execute(select(Touchpoint).where(Touchpoint.call_id==provider_call_id,Touchpoint.kind=='call_received'))).scalars().first()
            if not tp:db.add(Touchpoint(lead_id=lead.id,kind='call_received',call_id=provider_call_id,
              summary='Call received; connection/outcome not yet confirmed',payload={'repeat':repeat}))
            return str(lead.id),repeat
    try:return await save()
    except IntegrityError:return await save()
