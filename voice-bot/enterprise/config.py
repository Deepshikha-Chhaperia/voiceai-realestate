"""Deployment readiness, not credential-presence optimism."""
import json
import os
import re
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


def policy(config=None):
    value=(config or {}).get('enterprise', {})
    return value if isinstance(value,dict) else {}


def public_https(value):
    parsed = urlparse(str(value or ''))
    return (parsed.scheme == 'https' and bool(parsed.hostname) and not parsed.username
            and not parsed.password and parsed.hostname not in {'localhost','127.0.0.1','::1'}
            and not parsed.hostname.endswith(('.invalid','.example','.test')))


def e164(value):
    return bool(re.fullmatch(r'\+[1-9]\d{7,14}', str(value or '')))


def readiness(config=None):
    p = policy(config)
    p={**p,**{k:(p.get(k) if isinstance(p.get(k),dict) else {}) for k in ('quota','routing','recording','reports')}}
    base = []
    if os.getenv('VERIFY_VOBIZ_SIGNATURE','true').lower()!='true':base.append('Vobiz signature verification must remain enabled')
    if os.getenv('LOCAL_DEMO','').lower() in {'1','true','yes'}:base.append('Enterprise activation disallowed in LOCAL_DEMO')
    if not p.get('tenant_id'): base.append('enterprise.tenant_id')
    try: __import__('uuid').UUID(str(p.get('project_id') or ''))
    except ValueError:base.append('enterprise.project_id (existing project UUID)')
    if p.get('deployment_scope') != 'single_host': base.append('single_host persistent ledger deployment required; distributed ledger not implemented')
    if not p.get('persistent_volume_confirmed'): base.append('persistent_volume_confirmed')
    if not p.get('legacy_retention_policy_confirmed'):base.append('legacy_retention_policy_confirmed: logs/lead_state/provider copies require approved lifecycle')
    if not p.get('provider_hangup_webhook_confirmed'): base.append('provider_hangup_webhook_confirmed: signed /enterprise/call-ended webhook configured on Vobiz application')
    retention=p.get('transcript_retention_days')
    if not isinstance(retention,int) or isinstance(retention,bool) or not 1 <= retention <= 365:base.append('enterprise.transcript_retention_days 1..365')
    quota = list(base)
    for k in ('monthly_calls','monthly_minutes'):
        v = p.get('quota', {}).get(k)
        if not isinstance(v,int) or isinstance(v,bool) or v <= 0: quota.append('quota.'+k)
    try: ZoneInfo(p.get('quota',{}).get('timezone',''))
    except (ValueError,KeyError,TypeError): quota.append('quota.timezone')
    if p.get('quota',{}).get('accounting') != 'all_routed_call_seconds': quota.append('quota.accounting=all_routed_call_seconds (not carrier invoice minutes)')
    routing = list(base)
    reps = p.get('routing',{}).get('rep_numbers',[])
    if not isinstance(reps,list) or not reps or not all(e164(n) for n in reps): routing.append('routing.rep_numbers (E.164)')
    for key in ('timezone','open','close','weekdays'):
        if not p.get('routing',{}).get(key): routing.append('routing.'+key)
    try:
        ZoneInfo(p.get('routing',{}).get('timezone',''))
        for k in ('open','close'):
            if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',p.get('routing',{}).get(k,'')): raise ValueError()
        days = p.get('routing',{}).get('weekdays',[])
        if not isinstance(days,list) or not all(type(d) is int and 0 <= d <= 6 for d in days): raise ValueError()
    except (ValueError,KeyError,TypeError): routing.append('routing hours/timezone invalid')
    if not e164(os.getenv('VOBIZ_FROM_NUMBER','')):routing.append('VOBIZ_FROM_NUMBER authorized E.164')
    for key in ('VOBIZ_AUTH_ID','VOBIZ_AUTH_TOKEN','VOBIZ_FROM_NUMBER'):
        if not os.getenv(key,'').strip(): routing.append(key)
    host = os.getenv('PUBLIC_HOST','')
    if not public_https('https://'+host) or os.getenv('FORCE_WSS','').lower()!='true': routing.append('public HTTPS callback host')
    if p.get('routing',{}).get('broker_job_policy') not in {'close','route'}: routing.append('routing.broker_job_policy')
    if not isinstance(p.get('routing',{}).get('callback_message'),str) or not p.get('routing',{}).get('callback_message'): routing.append('routing.callback_message (approved wording)')
    if not isinstance(p.get('routing',{}).get('max_call_seconds'),int) or not 60 <= p.get('routing',{}).get('max_call_seconds',0) <= 7200: routing.append('routing.max_call_seconds 60..7200 for human bridge')
    human = routing.copy()
    r = p.get('recording',{})
    for key in ('notice','retention_days','access_policy','allowed_hosts','sarvam_model','language_code','max_call_seconds'):
        if not r.get(key): human.append('recording.'+key)
    if r.get('consent_policy') != 'notice_continue': human.append('recording.consent_policy=notice_continue must be approved for deployment; explicit opt-in IVR not implemented')
    if not isinstance(r.get('retention_days'),int) or not 1 <= r.get('retention_days',0) <= 365: human.append('recording.retention_days 1..365')
    if not isinstance(r.get('max_call_seconds'),int) or not 60 <= r.get('max_call_seconds',0) <= 7200: human.append('recording.max_call_seconds 60..7200')
    if not os.getenv('SARVAM_API_KEY'): human.append('SARVAM_API_KEY')
    return {name:{'ready':not gaps,'gaps':list(dict.fromkeys(gaps))} for name,gaps in [('quota',quota),('routing',routing),('human_mode',human)]}


def meta_settings():
    def load(key):
        try: return json.loads(os.getenv(key,'{}'))
        except (ValueError,TypeError): return {}
    return {'enabled':os.getenv('WHATSAPP_AUTOMATION_ENABLED','').lower()=='true',
        'token':os.getenv('WHATSAPP_ACCESS_TOKEN','').strip(),
        'phone_id':os.getenv('WHATSAPP_PHONE_NUMBER_ID','').strip(),
        'waba_id':os.getenv('WHATSAPP_WABA_ID','').strip(),
        'version':os.getenv('WHATSAPP_API_VERSION','').strip(),
        'app_secret':os.getenv('WHATSAPP_APP_SECRET','').strip(),
        'verify_token':os.getenv('WHATSAPP_WEBHOOK_VERIFY_TOKEN','').strip(),
        'templates':load('WHATSAPP_TEMPLATE_MAP_JSON'),
        'links':load('WHATSAPP_VERIFIED_LINKS_JSON')}
