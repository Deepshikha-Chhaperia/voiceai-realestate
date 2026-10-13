import asyncio, json, time, uuid
import pytest
from enterprise import store, routing, delivery, crm

Q = {'monthly_calls': 100, 'monthly_minutes': 1000, 'timezone': 'Asia/Kolkata', 'accounting': 'all_routed_call_seconds', 'daily_calls': 10}
CFG = {'enterprise': {'enabled': True, 'tenant_id': 't1', 'quota': Q, 'routing': {'timezone': 'Asia/Kolkata', 'callback_due_minutes': 15, 'callback_message': 'We will call back.'},
       'notifications': {'channel': 'https_webhook', 'recipients_confirmed': True, 'alerts': True, 'reports': True}}}


def alerts(scope=None):
    with store.transaction() as db:
        rows = [json.loads(r['payload']) for r in db.execute("SELECT payload FROM events WHERE kind='quota_alert'")]
    return [r for r in rows if scope is None or r['scope'] == scope]


def test_daily_cap_blocks_after_limit_and_alerts_once_each():
    for i in range(10):
        r = store.reserve(f'c{i}', 't1', '+919999900000', Q)
        assert r['allowed'], i
    r = store.reserve('c10', 't1', '+919999900000', Q)
    assert not r['allowed'] and r['reason'] == 'daily_cap_reached' and r['seconds'] == 0
    r = store.reserve('c11', 't1', '+919999900000', Q)  # blocked attempts do not consume, and do not duplicate alerts
    assert r['reason'] == 'daily_cap_reached'
    levels = sorted(a['threshold'] for a in alerts('daily'))
    assert levels == [70, 85, 100]
    assert store.session('c10')['state'] == 'blocked'


def test_blocked_by_daily_does_not_burn_monthly_quota():
    for i in range(11):
        store.reserve(f'd{i}', 't1', '+91', Q)
    with store.transaction() as db:
        calls = db.execute('SELECT calls,reserved FROM quota').fetchone()
    assert calls['calls'] == 10 and calls['reserved'] == 240 * 10


def test_no_daily_cap_when_unset():
    q = {k: v for k, v in Q.items() if k != 'daily_calls'}
    for i in range(15):
        assert store.reserve(f'e{i}', 't1', '+91', q)['allowed']
    assert alerts('daily') == []


def test_monthly_reason_wins_and_monthly_alerts_keep_scope():
    q = {**Q, 'monthly_calls': 2, 'daily_calls': 50}
    store.reserve('m1', 't1', '+91', q); store.reserve('m2', 't1', '+91', q)
    r = store.reserve('m3', 't1', '+91', q)
    assert r['reason'] == 'monthly_cap_reached'
    assert {a['threshold'] for a in alerts('monthly')} == {70, 85, 100}


def test_callback_workflow():
    store.reserve('s1', 't1', '+919811111111', Q, lead_id='L1')
    res = routing.callback('s1', CFG, 'transfer_failed')
    assert res['status'] == 'callback_required'
    routing.callback('s1', CFG, 'again')  # idempotent per session
    rows = store.callback_list('t1')
    assert len(rows) == 1 and rows[0]['status'] == 'open' and rows[0]['reason'] == 'transfer_failed'
    assert 14 * 60 < rows[0]['due_at'] - rows[0]['created'] <= 15 * 60 + 1
    cid = rows[0]['id']
    a = store.callback_assign('t1', cid, 'Ravi', due_minutes=5)
    assert a['status'] == 'assigned' and a['assignee'] == 'Ravi'
    with pytest.raises(ValueError):
        store.callback_assign('t1', cid, '  ')
    n = store.callback_complete('t1', cid, 'no_answer', 'rang out')
    assert n['status'] == 'assigned' and n['attempts'] == 1 and n['due_at'] > time.time() + 59 * 60
    d = store.callback_complete('t1', cid, 'reached', 'booked visit')
    assert d['status'] == 'done' and d['completed_at']
    assert store.callback_list('t1') == []
    with pytest.raises(ValueError):
        store.callback_complete('t1', cid, 'bogus')
    assert store.callback_get('other', cid) is None  # tenant isolation


def test_missed_call_creates_event_and_inbox_item():
    store.reserve('s2', 't1', '+919822222222', Q, lead_id='L2')
    routing.missed_call('s2', CFG, 'no-answer')
    assert store.callback_list('t1')[0]['reason'] == 'missed_call_no-answer'
    with store.transaction() as db:
        assert db.execute("SELECT count(*) FROM events WHERE kind='missed_call'").fetchone()[0] == 1


def test_overdue_flag():
    store.reserve('s3', 't1', '+91', Q)
    c = store.callback_create('t1', 's3', '+91', None, 'x', due_minutes=1)
    with store.transaction() as db:
        db.execute('UPDATE callbacks SET due_at=? WHERE id=?', (time.time() - 5, c['id']))
    assert store.callback_list('t1')[0]['overdue'] is True


class FakeResp:
    status_code = 200


class FakeClient:
    sent = []

    def __init__(self, **kw): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def post(self, url, json=None, **kw):
        FakeClient.sent.append(json); return FakeResp()


def test_delivery_notifies_reps_for_callbacks_without_phone_number(monkeypatch):
    monkeypatch.setenv('ENTERPRISE_NOTIFY_WEBHOOK_URL', 'https://hooks.client-notify.in/x')
    FakeClient.sent.clear()
    store.reserve('s4', 't1', '+919833333333', Q)
    routing.callback('s4', CFG, 'ai_ceiling')
    assert delivery.sync(CFG) >= 1
    delivery.sync(CFG)  # idempotent
    async def run():
        for _ in range(5):
            if await delivery.drain(CFG, client_factory=FakeClient) is None: break
    asyncio.run(run())
    texts = [m['text'] for m in FakeClient.sent]
    assert len([t for t in texts if 'callback request' in t]) == 1
    assert all('+919833333333' not in t for t in texts)
    assert any('/enterprise/callbacks/ui' in t for t in texts)
    c = store.callback_list('t1')[0]
    a = store.callback_assign('t1', c['id'], 'Asha')
    delivery.notify_assignment('t1', a, CFG)
    asyncio.run(run())
    assert any('assigned to Asha' in m['text'] for m in FakeClient.sent)


def test_daily_alert_text_says_daily(monkeypatch):
    monkeypatch.setenv('ENTERPRISE_NOTIFY_WEBHOOK_URL', 'https://hooks.client-notify.in/x')
    FakeClient.sent.clear()
    for i in range(8):
        store.reserve(f'z{i}', 't1', '+91', Q)
    delivery.sync(CFG)
    asyncio.run(delivery.drain(CFG, client_factory=FakeClient))
    asyncio.run(delivery.drain(CFG, client_factory=FakeClient))
    assert any('daily call cap' in m['text'] for m in FakeClient.sent)


class L:  # minimal lead stand-in
    def __init__(self, **kw):
        self.name = None; self.status = 'new'; self.first_contact_at = None; self.last_touch_at = None; self.raw = None
        self.__dict__.update(kw)


def test_crm_apply_outcome():
    l = L()
    crm.apply_outcome(l, source='ai_call', call_id='c1', name='Ramesh Gupta', spoke=True, early_intent='buyer', repeat=False)
    assert l.name == 'Ramesh Gupta' and l.status == 'conversed' and l.first_contact_at and l.last_touch_at
    assert l.raw['enterprise']['early_intent'] == 'buyer' and l.raw['enterprise']['repeat_caller'] is False
    crm.apply_outcome(l, source='ai_call', call_id='c2', name='Someone Else', spoke=True, early_intent='unknown', repeat=True)
    assert l.name == 'Ramesh Gupta'  # AI never overwrites an existing name
    assert l.raw['enterprise']['repeat_caller'] is True and l.raw['enterprise']['calls_seen'] == 2
    crm.apply_outcome(l, source='human_review', call_id='c2', name='Ramesh G', overwrite_name=True, visit_booked=True)
    assert l.name == 'Ramesh G' and l.status == 'visit_booked'
    d = L(status='dead'); crm.apply_outcome(d, source='x', call_id='c', spoke=True, visit_booked=True)
    assert d.status == 'dead'
    assert crm.clean_name('unknown') is None


def test_bot_wiring_is_present():
    import pathlib, re
    src = (pathlib.Path(__file__).resolve().parents[1] / 'bot.py') if (pathlib.Path(__file__).resolve().parents[1] / 'bot.py').exists() else pathlib.Path('bot.py')
    t = src.read_text(encoding='utf-8')
    assert t.count('enterprise_callback_close(') >= 4
    assert 'run_llm=False' in t.split('async def handoff_to_human')[1][:1600]
    assert "'early_intent'" in t
