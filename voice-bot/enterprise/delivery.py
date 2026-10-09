"""External alert/report notification adapters. Dormant until the owner picks a channel AND confirms recipients.

- Channels: 'telegram' (TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID) or 'https_webhook' (ENTERPRISE_NOTIFY_WEBHOOK_URL).
- ONE attempt per notification. A timeout is 'uncertain' (it may have arrived) and is never retried.
  Failures become 'needs_operator'; operators see them in /enterprise/work.
- Reports are announced with a link only (authenticated dashboard URL). Caller PII never leaves this server
  through a notification.
- Nothing is sent at import time or by readiness checks.
"""
import json
import os
import time
import httpx
from enterprise import store
from enterprise.config import policy, public_https

CHANNELS = {'telegram', 'https_webhook'}


def settings(config):
    n = policy(config).get('notifications')
    return n if isinstance(n, dict) else {}


def readiness(config):
    n = settings(config)
    gaps = []
    ch = n.get('channel')
    if ch not in CHANNELS:
        gaps.append('notifications.channel (telegram|https_webhook)')
    if n.get('recipients_confirmed') is not True:
        gaps.append('notifications.recipients_confirmed (owner confirmed who receives alerts/reports)')
    if ch == 'telegram':
        for k in ('TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'):
            if not os.getenv(k, '').strip():
                gaps.append('env ' + k)
    if ch == 'https_webhook' and not public_https(os.getenv('ENTERPRISE_NOTIFY_WEBHOOK_URL', '')):
        gaps.append('env ENTERPRISE_NOTIFY_WEBHOOK_URL (public https)')
    base = os.getenv('PUBLIC_BASE_URL', '').strip() or ('https://' + os.getenv('PUBLIC_HOST', '').strip())
    if not public_https(base):
        gaps.append('PUBLIC_BASE_URL/PUBLIC_HOST (public https, for report links)')
    for k in ('alerts', 'reports'):
        if n.get(k) is not True:
            gaps.append(f'notifications.{k} (set true to deliver {k} externally)')
    return {'ready': not gaps, 'gaps': gaps}


def _base():
    return (os.getenv('PUBLIC_BASE_URL', '').strip() or 'https://' + os.getenv('PUBLIC_HOST', '').strip()).rstrip('/')


def sync(config):
    """Queue (idempotent) external notifications for quota alerts and prepared reports. No network."""
    n = settings(config)
    p = policy(config)
    tenant = p.get('tenant_id')
    if not tenant or not readiness(config)['ready']:
        return 0
    queued = 0
    with store.transaction() as db:
        alerts = [dict(r) for r in db.execute("SELECT id,payload FROM events WHERE tenant=? AND kind='quota_alert'", (tenant,))] if n.get('alerts') else []
        reports = [dict(r) for r in db.execute("SELECT id FROM work WHERE tenant=? AND kind='report' AND state='prepared_not_sent'", (tenant,))] if n.get('reports') else []
    for a in alerts:
        pl = json.loads(a['payload'])
        text = f"Voice bot usage reached {pl.get('threshold')}% of the monthly cap."
        store.enqueue('notify', tenant, '', {'text': text, 'ref': f"event:{a['id']}"}, f"{tenant}:notify:alert:{a['id']}")
        queued += 1
    for r in reports:
        text = f"Fortnightly report is ready: {_base()}/enterprise/reports/{r['id']}/html (sign in with the dashboard key)."
        store.enqueue('notify', tenant, '', {'text': text, 'ref': f"report:{r['id']}"}, f"{tenant}:notify:report:{r['id']}")
        queued += 1
    return queued


async def drain(config, client_factory=None):
    """Deliver ONE queued notification with ONE attempt."""
    if not readiness(config)['ready']:
        return None
    item = store.claim('notify', policy(config).get('tenant_id'))
    if not item:
        return None
    ch = settings(config)['channel']
    text = item['payload']['text']
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=10, follow_redirects=False) as c:
            if ch == 'telegram':
                r = await c.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN'].strip()}/sendMessage",
                                 json={'chat_id': os.environ['TELEGRAM_CHAT_ID'].strip(), 'text': text, 'disable_web_page_preview': True})
            else:
                r = await c.post(os.environ['ENTERPRISE_NOTIFY_WEBHOOK_URL'].strip(), json={'text': text, 'ref': item['payload']['ref']})
        if r.status_code == 200 or (ch == 'https_webhook' and 200 <= r.status_code < 300):
            store.complete(item['id'], 'delivered')
            return 'delivered'
        store.complete(item['id'], 'needs_operator', error=f'HTTP {r.status_code}')
        return 'needs_operator'
    except Exception as exc:  # outcome unknown: do not resend
        store.complete(item['id'], 'uncertain', error='outcome unknown: ' + type(exc).__name__)
        return 'uncertain'
