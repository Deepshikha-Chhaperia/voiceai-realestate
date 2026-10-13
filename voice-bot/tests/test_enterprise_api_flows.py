import uuid, json
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

H = {'Authorization': 'Bearer test-key'}


@pytest.fixture
def env(monkeypatch):
    import asyncio
    from leads import db as leads_db
    from leads.models import Project, Lead, Touchpoint
    from enterprise import api, store
    pid = uuid.uuid4()
    cfg = {'enterprise': {'enabled': True, 'tenant_id': 't1', 'project_id': str(pid),
        'quota': {'monthly_calls': 100, 'monthly_minutes': 1000, 'timezone': 'Asia/Kolkata', 'accounting': 'all_routed_call_seconds'},
        'routing': {'timezone': 'Asia/Kolkata'}, 'notifications': {'hot_lead_alerts': False}}}
    api.configure(cfg)
    ids = {}

    async def seed():
        await leads_db.init_models()
        async with leads_db.get_session() as s:
            s.add(Project(id=pid, name='P', config={'min_price_lakhs': 100, 'max_price_lakhs': 200, 'unit_types': ['3BHK']}))
            await s.flush()
            l = Lead(phone='+919800000001', project_id=pid, source='inbound_call'); s.add(l); await s.flush()
            s.add(Touchpoint(lead_id=l.id, call_id='call1', kind='human_transcript', summary='x', payload={
                'transcript': 'hello', 'speaker_attribution': 'unverified', 'recording_status': 'transcribed',
                'entries': [{'speaker': '0', 'text': 'Hi, tell me about 3BHK'}, {'speaker': '1', 'text': 'Budget 1.5 crore, visit Sunday'}]}))
            ids['lead'] = str(l.id)
    asyncio.run(seed())
    app = FastAPI(); app.include_router(api.router)
    return TestClient(app), ids, pid


def test_callback_endpoints_need_auth_and_work(env):
    c, ids, pid = env
    from enterprise import store
    assert c.get('/enterprise/callbacks').status_code == 401
    store.reserve('sx', 't1', '+919811111111', {'monthly_calls': 10, 'monthly_minutes': 100, 'timezone': 'Asia/Kolkata'})
    cb = store.callback_create('t1', 'sx', '+919811111111', None, 'ai_ceiling', 30)
    r = c.get('/enterprise/callbacks', headers=H).json()['callbacks']
    assert r[0]['id'] == cb['id']
    assert c.post(f"/enterprise/callbacks/{cb['id']}/assign", headers=H, json={'assignee': 'Ravi', 'due_minutes': 20}).json()['assignee'] == 'Ravi'
    assert c.post(f"/enterprise/callbacks/{cb['id']}/assign", headers=H, json={'assignee': ''}).status_code == 422
    assert c.post(f"/enterprise/callbacks/{cb['id']}/complete", headers=H, json={'outcome': 'nope'}).status_code == 422
    assert c.post(f"/enterprise/callbacks/{cb['id']}/complete", headers=H, json={'outcome': 'reached', 'notes': 'ok'}).json()['status'] == 'done'
    assert c.post('/enterprise/callbacks/unknown/complete', headers=H, json={'outcome': 'reached'}).status_code == 404
    page = c.get('/enterprise/callbacks/ui', headers=H)
    assert page.status_code == 200 and 'script-src' in page.headers['content-security-policy']


def test_human_review_flow_end_to_end(env):
    c, ids, pid = env
    lead = ids['lead']
    items = c.get('/enterprise/human-reviews', headers=H).json()['items']
    assert items[0]['reviewed'] is False
    d = c.get(f'/enterprise/human-review/{lead}/call1', headers=H).json()
    assert len(d['entries']) == 2
    body = {'caller_evidence_reviewed': True, 'budget_min_lakhs': 140, 'budget_max_lakhs': 160, 'timeline_months': 2,
            'configuration': '3BHK', 'visit_intent': 'requested', 'caller_name': 'Suresh Rao'}
    assert c.post(f'/enterprise/human-review/{lead}/call1', headers=H, json={**body, 'caller_speaker': None}).status_code == 422  # must pick caller
    assert c.post(f'/enterprise/human-review/{lead}/call1', headers=H, json={**body, 'caller_speaker': '9'}).status_code == 422
    assert c.post(f'/enterprise/human-review/{lead}/call1', headers=H, json={**body, 'caller_speaker': '1', 'caller_evidence_reviewed': False}).status_code == 422
    r = c.post(f'/enterprise/human-review/{lead}/call1', headers=H, json={**body, 'caller_speaker': '1'})
    assert r.status_code == 200, r.text
    assert r.json()['tier'] in {'hot', 'warm'}
    items = c.get('/enterprise/human-reviews', headers=H).json()['items']
    assert items[0]['reviewed'] is True and items[0]['name'] == 'Suresh Rao'
    import asyncio
    from leads import db as leads_db
    from leads.models import Lead, Touchpoint, OutboxItem

    async def check():
        async with leads_db.get_session() as s:
            l = await s.get(Lead, uuid.UUID(lead))
            assert l.status == 'conversed' and l.first_contact_at and l.last_touch_at and l.name == 'Suresh Rao'
            tp = (await s.execute(select(Touchpoint).where(Touchpoint.kind == 'human_transcript'))).scalars().one()
            assert tp.payload['caller_speaker'] == '1' and tp.payload['speaker_attribution'] == 'operator_confirmed'
            outbox = (await s.execute(select(OutboxItem).where(OutboxItem.target == 'webhook'))).scalars().all()
            assert len(outbox) == 1 and outbox[0].payload['call_id'] == 'call1'
    asyncio.run(check())
    # corrected re-review must not queue a second CRM webhook
    assert c.post(f'/enterprise/human-review/{lead}/call1', headers=H, json={**body, 'caller_speaker': '1', 'timeline_months': 1}).status_code == 200
    asyncio.run(check())


def test_ai_analytics_writes_back_crm_and_fans_out(env, monkeypatch):
    import sys, types, asyncio
    from leads import db as leads_db
    from leads.models import Lead, OutboxItem
    from enterprise import analytics, store, api
    c, ids, pid = env
    async def fake_analyze(call_id, messages):
        return {'analysis': {'budget_min_lakhs': 140, 'budget_max_lakhs': 160, 'timeline_months': 2, 'visit_intent': 'requested', 'configuration': '3BHK', 'summary': 'Ready buyer'},
                'call': {'caller_turns': 6, 'lead_fields': json.dumps({'name': 'Meera Iyer'})}}
    monkeypatch.setitem(sys.modules, 'call_analytics', types.SimpleNamespace(analyze_call=fake_analyze))
    sent = []
    async def fake_alert(**kw): sent.append(kw); return True
    import leads.notify as notify
    monkeypatch.setattr(notify, 'send_telegram_alert', fake_alert)
    cfg = {'enterprise': {'tenant_id': 't1', 'notifications': {}}}
    analytics.enqueue('t1', 'callA', ids['lead'], [{'role': 'user', 'content': 'hi'}], 30, meta={'early_intent': 'buyer', 'repeat': True})
    asyncio.run(analytics.drain('t1', cfg))
    async def check():
        async with leads_db.get_session() as s:
            l = await s.get(Lead, uuid.UUID(ids['lead']))
            assert l.name == 'Meera Iyer' and l.status == 'conversed' and l.first_contact_at and l.last_touch_at
            assert l.raw['enterprise']['early_intent'] == 'buyer' and l.raw['enterprise']['repeat_caller'] is True
            assert l.tier == 'hot'
            n = (await s.execute(select(OutboxItem).where(OutboxItem.target == 'webhook'))).scalars().all()
            assert len(n) == 1
    asyncio.run(check())
    assert len(sent) == 1 and sent[0]['name'] == 'Meera Iyer'  # 95 points => hot => one alert
