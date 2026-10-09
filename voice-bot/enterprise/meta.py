"""Dormant Meta transport with live read-only readiness and one-attempt sends.
No network during import. No free-form fallback, recipient override or fictitious URLs.
Supported templates: text BODY positional parameters only, no dynamic headers/buttons.
Unsupported schemas fail readiness instead of guessing a payload.
"""
import hashlib
import hmac
import json
import re
import time
import httpx
from enterprise.config import meta_settings, public_https, e164
from enterprise import store

_STATE={'ready':False,'gaps':['Automation disabled'],'checked_at':0}
_CONFIG_FINGERPRINT=''


def fingerprint(c):
    return hashlib.sha256(json.dumps(c,sort_keys=True).encode()).hexdigest()


def state():
    c=meta_settings()
    if fingerprint(c)!=_CONFIG_FINGERPRINT:return {'ready':False,'gaps':['Configuration changed; refresh readiness'],'checked_at':0}
    if time.time()-_STATE['checked_at']>300:return {'ready':False,'gaps':['Readiness stale; refresh required'],'checked_at':_STATE['checked_at']}
    return dict(_STATE)


def template_gaps(mapping,template):
    gaps=[]
    if template.get('status')!='APPROVED':gaps.append('template not APPROVED')
    if template.get('language')!=mapping.get('language'):gaps.append('template language mismatch')
    if template.get('parameter_format','POSITIONAL').upper()!='POSITIONAL':gaps.append('named template unsupported; configure an approved positional BODY template')
    components=template.get('components',[])
    for c in components:
        if c.get('type')=='HEADER' and (c.get('format')!='TEXT' or '{{' in c.get('text','')):gaps.append('dynamic/media HEADER unsupported')
        if c.get('type')=='BUTTONS' and any('{{' in str(b) for b in c.get('buttons',[])):gaps.append('dynamic BUTTONS unsupported')
    body=next((c.get('text','') for c in components if c.get('type')=='BODY'),'')
    placeholders=sorted({int(n) for n in re.findall(r'\{\{(\d+)\}\}',body)})
    fields=mapping.get('body_fields',[])
    if not isinstance(fields,list) or placeholders!=list(range(1,len(fields)+1)):gaps.append('BODY parameter map must exactly match approved template')
    allowed={'name','project_name','visit_date_iso','time_slot','brochure_url','floorplan_url','location_url'}
    if isinstance(fields,list) and not set(fields)<=allowed:gaps.append('unknown template body field')
    return gaps


async def refresh(client_factory=None):
    global _STATE,_CONFIG_FINGERPRINT
    c=meta_settings();gaps=[]
    if not c['enabled']:gaps.append('WHATSAPP_AUTOMATION_ENABLED=true not set; manual mode')
    for k in ('token','phone_id','waba_id','version','app_secret','verify_token'):
        if not c[k]:gaps.append('WhatsApp '+k+' missing')
    if __import__('os').getenv('WHATSAPP_WEBHOOK_SETUP_CONFIRMED','').lower()!='true':gaps.append('WHATSAPP_WEBHOOK_SETUP_CONFIRMED=true after verification/subscription/live-mode setup')
    if not re.fullmatch(r'v\d+\.\d+',c['version']):gaps.append('explicit supported API version required')
    if not isinstance(c['templates'],dict) or not all(k in c['templates'] for k in ('brochure','location')):gaps.append('template map requires brochure and location')
    if not isinstance(c['links'],dict):gaps.append('verified links must be an object')
    else:
        for k in ('brochure_url','floorplan_url','location_url'):
            v=c['links'].get(k,{})
            if not isinstance(v,dict) or v.get('verified') is not True or not public_https(v.get('url')):gaps.append(k+' requires verified HTTPS content URL')
    if not gaps:
        try:
            root=f"https://graph.facebook.com/{c['version']}"
            headers={'Authorization':'Bearer '+c['token']}
            async with (client_factory or httpx.AsyncClient)(timeout=8,follow_redirects=False) as client:
                # Check WABA phone membership, registration connectivity and exact approved assets.
                phones=[];url=f"{root}/{c['waba_id']}/phone_numbers?fields=id,display_phone_number,code_verification_status&limit=100"
                for _ in range(20):
                    response=await client.get(url,headers=headers);response.raise_for_status();data=response.json()
                    phones.extend(data.get('data',[]));url=data.get('paging',{}).get('next')
                    if not url:break
                    if not url.startswith(root+'/'):raise ValueError('unexpected Graph pagination host')
                else:raise ValueError('phone pagination limit reached')
                phone=next((p for p in phones if p.get('id')==c['phone_id']),None)
                if not phone:gaps.append('phone_number_id not in configured WABA')
                elif phone.get('code_verification_status')!='VERIFIED':gaps.append('business phone verification incomplete')
                for action,mapping in c['templates'].items():
                    if action not in {'brochure','location'}:continue
                    if not isinstance(mapping,dict) or not re.fullmatch(r'[a-z0-9_]+',mapping.get('name','')):
                        gaps.append(action+' template name invalid');continue
                    response=await client.get(f"{root}/{c['waba_id']}/message_templates",params={'name':mapping['name'],'fields':'name,status,language,components,parameter_format','limit':100},headers=headers)
                    response.raise_for_status();data=response.json()
                    candidates=[t for t in data.get('data',[]) if t.get('name')==mapping['name'] and t.get('language')==mapping.get('language')]
                    if not candidates:gaps.append(action+' template/language not found')
                    else:gaps.extend(action+': '+g for g in template_gaps(mapping,candidates[0]))
        except Exception as exc:
            # Never include server error bodies or secrets in readiness output.
            gaps.append('Meta read validation failed: '+type(exc).__name__)
    _CONFIG_FINGERPRINT=fingerprint(c)
    _STATE={'ready':not gaps,'gaps':gaps,'checked_at':time.time(),
            'scope':'configuration/template read validation, not a live delivery test',
            'webhook_note':'configure HTTPS webhook, messages subscription and app live mode; inbound signature check remains required'}
    return dict(_STATE)


def enqueue_postcall(tenant,call_id,phone,memory,telephony_session_id=None):
    actions=memory.get('_postcall_whatsapp_actions') or []
    if not e164(phone):return {'status':'manual','gaps':['Valid real recipient phone missing']}
    c=meta_settings()
    if not state()['ready']:return {'status':'manual','gaps':state()['gaps']}
    queued=[]
    for action in actions:
        if action not in {'brochure','location'}:continue
        # Only the recorded exact scoped action, never blanket inferred lead consent.
        payload={'telephony_session_id':telephony_session_id,'sender_phone_id':c['phone_id'],'sender_waba_id':c['waba_id'],'action':action,'phone':phone,'consent':True,'consent_scope':action,'name':memory.get('client') or '',
          'project_name':memory.get('_project_name') or '', 'visit_date_iso':'Not booked','time_slot':'Not booked'}
        if memory.get('disposition')=='SITE_VISIT_BOOKED' and str(memory.get('site_visit','')).startswith(('Confirmed (','Booked (')):
            payload.update(visit_date_iso=memory.get('visit_date_iso') or 'Not booked',time_slot=memory.get('time_slot') or 'Not booked')
        payload.update({k:v['url'] for k,v in c['links'].items() if isinstance(v,dict) and v.get('verified') is True})
        item=store.enqueue('whatsapp',tenant,call_id,payload,f'{tenant}:{call_id}:whatsapp:{action}',expires=time.time()+86400)
        queued.append(item['id'])
    return {'status':'queued' if queued else 'not_requested','work_ids':queued}


async def send(payload,client_factory=None):
    if not state()['ready']:return {'status':'manual','ok':False,'error':'Meta readiness not configured'}
    c=meta_settings();action=payload.get('action');mapping=c['templates'].get(action,{})
    if payload.get('sender_phone_id') not in {None,c['phone_id']} or payload.get('sender_waba_id') not in {None,c['waba_id']}:return {'status':'blocked','ok':False,'error':'Sender account changed since queueing'}
    if payload.get('consent') is not True or payload.get('consent_scope')!=action:return {'status':'blocked','ok':False,'error':'Exact scoped consent missing'}
    if not e164(payload.get('phone')):return {'status':'blocked','ok':False,'error':'Invalid recipient'}
    fields=mapping.get('body_fields',[])
    values=[payload.get(k) for k in fields]
    if not all(isinstance(v,str) and v.strip() for v in values):return {'status':'manual','ok':False,'error':'Template values missing'}
    for k in ('brochure_url','floorplan_url','location_url'):
        if k in fields and payload.get(k)!=(c['links'].get(k) or {}).get('url'):return {'status':'blocked','ok':False,'error':'Content URL differs from verified configuration'}
    request={'messaging_product':'whatsapp','to':payload['phone'].lstrip('+'),'type':'template',
      'template':{'name':mapping['name'],'language':{'code':mapping['language']},
      'components':[{'type':'body','parameters':[{'type':'text','text':v} for v in values]}] if fields else []}}
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=8) as client:
            response=await client.post(f"https://graph.facebook.com/{c['version']}/{c['phone_id']}/messages",
              headers={'Authorization':'Bearer '+c['token']},json=request)
            data=response.json();message_id=((data.get('messages') or [{}])[0]).get('id')
            if response.is_success and message_id:return {'status':'accepted','ok':True,'message_id':message_id}
            return {'status':'failed','ok':False,'error':'Meta send rejected','error_code':(data.get('error') or {}).get('code'),'http_status':response.status_code}
    except Exception as exc:return {'status':'uncertain','ok':False,'error':'Send outcome unknown: '+type(exc).__name__}


async def drain():
    if not state()['ready']:return
    item=store.claim('whatsapp')
    if not item:return
    call_id=item['payload'].get('telephony_session_id')
    row=store.session(call_id) if call_id else None
    if call_id and (not row or row['state']!='ended'):
        # Stream teardown may be a transfer, not call end. Never send during human continuation.
        store.complete(item['id'],'pending',error='Awaiting verified telephony end')
        return
    result=await send(item['payload'])
    from loguru import logger
    logger.info('[WA-SEND] action={} call={} status={} ok={} message_id={}',item['payload'].get('action'),call_id,result.get('status'),result.get('ok'),result.get('message_id'))
    store.complete(item['id'],result['status'],result.get('message_id'),result.get('error'))


def verify_signature(body,signature):
    secret=meta_settings()['app_secret']
    expected='sha256='+hmac.new(secret.encode(),body,hashlib.sha256).hexdigest()
    return bool(secret and hmac.compare_digest(expected,signature or ''))


def ingest_status(payload):
    c=meta_settings()
    if payload.get('object')!='whatsapp_business_account':return
    for entry in payload.get('entry',[]):
        if str(entry.get('id'))!=c['waba_id']:continue
        for change in entry.get('changes',[]):
            value=change.get('value',{})
            if str(value.get('metadata',{}).get('phone_number_id'))!=c['phone_id']:continue
            for status in value.get('statuses',[]):
                if status.get('status') in {'sent','delivered','read','failed'}:store.message_status(status.get('id'),status['status'])
