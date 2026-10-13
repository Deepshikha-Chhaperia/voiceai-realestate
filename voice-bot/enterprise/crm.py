"""Enterprise CRM writeback shared by AI-call analytics and operator-reviewed human calls.

- Lead lifecycle fields (name, first_contact_at, last_touch_at, status) are updated in the caller's DB transaction.
- Early intent and repeat-caller are stored in lead.raw['enterprise'] (JSON column that already exists), so no schema
  migration is needed on a running Postgres/SQLite database.
- External fan-out (CRM webhook outbox + hot-lead Telegram alert) mirrors leads/worker.py's legacy path and runs AFTER the
  transaction commits. It can never fail scoring: errors are logged and the scored lead stays saved.
"""
from datetime import datetime, timezone
from loguru import logger

INTENTS = {'buyer', 'broker', 'job', 'wrong_number', 'unknown'}


def clean_name(value):
    name = ' '.join(str(value or '').split())[:120]
    return name if name and name.lower() not in {'unknown', 'none', 'null', 'caller', 'n/a'} else None


def apply_outcome(lead, *, source, call_id, name=None, spoke=False, early_intent=None, repeat=None,
                  visit_booked=False, overwrite_name=False, now=None):
    now = now or datetime.now(timezone.utc)
    lead.last_touch_at = now
    n = clean_name(name)
    if n and (overwrite_name or not lead.name):
        lead.name = n
    if spoke:
        lead.first_contact_at = lead.first_contact_at or now
        if lead.status not in {'dead', 'visit_booked'}:
            lead.status = 'conversed'
    if visit_booked and lead.status != 'dead':
        lead.status = 'visit_booked'
    raw = dict(lead.raw or {})
    ent = dict(raw.get('enterprise') or {})
    if early_intent in INTENTS:
        ent['early_intent'] = early_intent
        ent['early_intent_call_id'] = call_id
    if repeat is not None:
        ent['repeat_caller'] = bool(repeat) or bool(ent.get('repeat_caller'))
    ent['calls_seen'] = int(ent.get('calls_seen') or 0) + 1
    ent['last_source'] = source
    ent['last_call_id'] = call_id
    raw['enterprise'] = ent  # reassign so SQLAlchemy sees the JSON change
    lead.raw = raw


def snapshot(lead, *, analysis, visit_info=None, summary=None):
    return {'lead_id': str(lead.id), 'name': lead.name, 'phone': lead.phone, 'tier': lead.tier, 'score': lead.score,
            'reason': lead.score_reason, 'analysis': analysis or {}, 'visit_info': visit_info, 'summary': summary}


async def queue_webhook(db, snap, call_id):
    import uuid
    from leads.outbox import queue_outbox_item
    await queue_outbox_item(uuid.UUID(snap['lead_id']), 'webhook',
                            {'lead_id': snap['lead_id'], 'call_id': call_id, 'analysis': snap['analysis'],
                             'score': snap['score'], 'tier': snap['tier'], 'source': 'enterprise'}, session=db)


async def dispatch(snap, config):
    """After commit: drain the webhook outbox and send the hot-lead alert. Never raises."""
    from enterprise.config import policy
    try:
        from leads.outbox import drain_outbox
        await drain_outbox()
    except Exception as exc:
        logger.warning('Enterprise CRM webhook drain failed (item stays queued): {}', type(exc).__name__)
    n = policy(config).get('notifications')
    if snap.get('tier') == 'hot' and not (isinstance(n, dict) and n.get('hot_lead_alerts') is False):
        try:
            from leads.notify import send_telegram_alert
            await send_telegram_alert(lead_id=snap['lead_id'], name=snap.get('name'), phone=snap['phone'],
                score_reason=snap.get('reason'), visit_info=snap.get('visit_info') or 'not booked',
                summary=snap.get('summary') or f"{snap.get('reason')}.")
        except Exception as exc:
            logger.warning('Enterprise hot-lead alert failed: {}', type(exc).__name__)
