"""Durable single-host ledger. BEGIN IMMEDIATE protects concurrent processes.
Not a distributed store: readiness requires an explicit persistent single-host deployment.
Never hold a database transaction while contacting a provider.
"""
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


@contextmanager
def transaction():
    path = Path(os.getenv('DATA_DIR','outputs')).resolve() / 'enterprise.db'
    path.parent.mkdir(parents=True,exist_ok=True)
    db = sqlite3.connect(path,timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA busy_timeout=10000')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS quota(tenant TEXT,month TEXT,calls INTEGER NOT NULL DEFAULT 0,
      seconds INTEGER NOT NULL DEFAULT 0,reserved INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(tenant,month));
    CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,tenant TEXT,month TEXT,phone TEXT,
      lead_id TEXT,started REAL,reserved INTEGER,state TEXT,mode TEXT,ended REAL,reason TEXT);
    CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,tenant TEXT,session_id TEXT,kind TEXT,
      payload TEXT,created REAL,dedupe TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS work(id TEXT PRIMARY KEY,dedupe TEXT UNIQUE,kind TEXT,tenant TEXT,
      session_id TEXT,payload TEXT,state TEXT,lease_until REAL,attempts INTEGER DEFAULT 0,
      provider_id TEXT,last_error TEXT,created REAL,expires REAL);
    CREATE TABLE IF NOT EXISTS callbacks(id TEXT PRIMARY KEY,tenant TEXT,session_id TEXT UNIQUE,phone TEXT,
      lead_id TEXT,reason TEXT,status TEXT NOT NULL DEFAULT 'open',assignee TEXT,due_at REAL,created REAL,
      assigned_at REAL,completed_at REAL,outcome TEXT,notes TEXT,attempts INTEGER NOT NULL DEFAULT 0);
    ''')
    try:
        db.execute('BEGIN IMMEDIATE')
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally: db.close()


def event(tenant,session_id,kind,payload,dedupe=None):
    with transaction() as db:
        db.execute('INSERT OR IGNORE INTO events(tenant,session_id,kind,payload,created,dedupe) VALUES(?,?,?,?,?,?)',
          (tenant,session_id,kind,json.dumps(payload),time.time(),dedupe))


def _alert(db,tenant,session_id,now,level,scope,period,payload):
    db.execute('INSERT OR IGNORE INTO events(tenant,session_id,kind,payload,created,dedupe) VALUES(?,?,?,?,?,?)',
      (tenant,session_id,'quota_alert',json.dumps({**payload,'threshold':level,'scope':scope,'delivery':'pending_operator_ack'}),now,
       f'{tenant}:{period}:{scope}:{level}' if scope=='daily' else f'{tenant}:{period}:quota:{level}'))


def reserve(session_id,tenant,phone,quota,seconds=240,mode='ai',lead_id=None):
    now=time.time()
    zone=ZoneInfo(quota['timezone'])
    local=datetime.fromtimestamp(now,zone)
    month=local.strftime('%Y-%m')
    day=local.strftime('%Y-%m-%d')
    day_start=local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    daily_cap=quota.get('daily_calls')
    with transaction() as db:
        existing=db.execute('SELECT * FROM sessions WHERE id=?',(session_id,)).fetchone()
        if existing: return {'allowed':existing['state']=='active','replay':True,'seconds':existing['reserved']}
        db.execute('INSERT OR IGNORE INTO quota(tenant,month) VALUES(?,?)',(tenant,month))
        row=db.execute('SELECT * FROM quota WHERE tenant=? AND month=?',(tenant,month)).fetchone()
        remaining=quota['monthly_minutes']*60-row['seconds']-row['reserved']
        allotted=min(seconds,max(0,remaining))
        monthly_ok=row['calls']<quota['monthly_calls'] and allotted>0
        # Daily cap counts every admitted call today (blocked attempts do not consume it).
        daily_used=db.execute("SELECT count(*) FROM sessions WHERE tenant=? AND started>=? AND state!='blocked'",(tenant,day_start)).fetchone()[0] if daily_cap else 0
        daily_ok=(not daily_cap) or daily_used<daily_cap
        allowed=monthly_ok and daily_ok
        if allowed:
            db.execute('UPDATE quota SET calls=calls+1,reserved=reserved+? WHERE tenant=? AND month=?',(allotted,tenant,month))
        db.execute('INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)',
          (session_id,tenant,month,phone,lead_id,now,allotted if allowed else 0,'active' if allowed else 'blocked',mode,None,None))
        db.execute('INSERT OR IGNORE INTO events(tenant,session_id,kind,payload,created,dedupe) VALUES(?,?,?,?,?,?)',
          (tenant,session_id,'quota_limits',json.dumps({'seconds_limit':quota['monthly_minutes']*60}),now,session_id+':quota_limits'))
        for level in (70,85,100):
            call_pct=100*(row['calls']+int(allowed))/quota['monthly_calls']
            minute_pct=100*(row['seconds']+row['reserved']+(allotted if allowed else 0))/(quota['monthly_minutes']*60)
            if max(call_pct,minute_pct)>=level:
                _alert(db,tenant,session_id,now,level,'monthly',month,{'calls_percent':call_pct,'minutes_committed_percent':minute_pct})
            if daily_cap:
                daily_pct=100*(daily_used+int(allowed))/daily_cap
                if daily_pct>=level:
                    _alert(db,tenant,session_id,now,level,'daily',day,{'daily_calls_percent':daily_pct,'daily_calls_used':daily_used+int(allowed),'daily_cap':daily_cap})
        reason=None if allowed else ('monthly_cap_reached' if not monthly_ok else 'daily_cap_reached')
        return {'allowed':allowed,'seconds':allotted if allowed else 0,'reason':reason}


def finish(session_id,reason='completed',duration=None):
    with transaction() as db:
        row=db.execute('SELECT * FROM sessions WHERE id=?',(session_id,)).fetchone()
        if not row or row['state']!='active': return
        used=max(0,int(duration if duration is not None else time.time()-row['started'])+1)
        # Account observed elapsed even if telephony exceeded our limit: never hide overrun.
        db.execute('UPDATE quota SET reserved=max(0,reserved-?),seconds=seconds+? WHERE tenant=? AND month=?',
          (row['reserved'],used,row['tenant'],row['month']))
        db.execute("UPDATE sessions SET state='ended',ended=?,reason=? WHERE id=?",(time.time(),reason,session_id))


def session(session_id):
    with transaction() as db:
        row=db.execute('SELECT * FROM sessions WHERE id=?',(session_id,)).fetchone()
        return dict(row) if row else None


def enqueue(kind,tenant,session_id,payload,dedupe,expires=None):
    with transaction() as db:
        work_id=str(uuid.uuid4())
        db.execute('INSERT OR IGNORE INTO work(id,dedupe,kind,tenant,session_id,payload,state,created,expires) VALUES(?,?,?,?,?,?,?,?,?)',
          (work_id,dedupe,kind,tenant,session_id,json.dumps(payload),'pending',time.time(),expires))
        return dict(db.execute('SELECT * FROM work WHERE dedupe=?',(dedupe,)).fetchone())


def claim(kind,tenant=None):
    now=time.time()
    with transaction() as db:
        # A crashed send may have reached the provider. Never automatically replay it.
        db.execute("UPDATE work SET state='uncertain',last_error='Worker lease expired; reconcile provider outcome before retry' WHERE kind=? AND state='processing' AND lease_until<?",(kind,now))
        row=db.execute("SELECT * FROM work WHERE kind=? AND state='pending' AND (expires IS NULL OR expires>?) AND (? IS NULL OR tenant=?) ORDER BY created LIMIT 1",(kind,now,tenant,tenant)).fetchone()
        if not row:return None
        db.execute("UPDATE work SET state='processing',lease_until=?,attempts=attempts+1 WHERE id=?",(now+900,row['id']))
        result=dict(row);result['payload']=json.loads(result['payload']);return result


def complete(work_id,state,provider_id=None,error=None,payload=None):
    with transaction() as db:
        db.execute("UPDATE work SET state=?,provider_id=coalesce(?,provider_id),last_error=?,payload=coalesce(?,payload),lease_until=CASE WHEN ?='processing' THEN lease_until ELSE NULL END WHERE id=?",
          (state,provider_id,error,json.dumps(payload) if payload is not None else None,state,work_id))


def message_status(provider_id,status):
    # Failed never downgrades delivered/read and out-of-order sent never downgrades delivery.
    rank={'accepted':1,'sent':2,'failed':3,'delivered':4,'read':5}
    with transaction() as db:
        row=db.execute('SELECT id,state FROM work WHERE provider_id=?',(provider_id,)).fetchone()
        if row and rank.get(status,0)>rank.get(row['state'],0):db.execute('UPDATE work SET state=? WHERE id=?',(status,row['id']))


def summary(tenant,since,until):
    with transaction() as db:
        rows=[dict(r) for r in db.execute('SELECT mode,state,count(*) count,sum(coalesce(ended,started)-started) seconds FROM sessions WHERE tenant=? AND started>=? AND started<? GROUP BY mode,state',(tenant,since,until))]
        alerts=[dict(r) for r in db.execute("SELECT id,kind,payload,created FROM events WHERE tenant=? AND kind IN ('quota_alert','callback_required','missed_call') AND created>=? AND created<?",(tenant,since,until))]
        return {'calls':rows,'events':alerts,'since':since,'until':until,'accounting':'application observed seconds, not provider invoice','delivery':'private report; not sent'}


def extend_for_transfer(session_id,seconds):
    """Reserve additional call seconds atomically before B-leg creation."""
    with transaction() as db:
        row=db.execute('SELECT * FROM sessions WHERE id=?',(session_id,)).fetchone()
        if not row or row['state']!='active':return False
        quota=db.execute('SELECT * FROM quota WHERE tenant=? AND month=?',(row['tenant'],row['month'])).fetchone()
        limit_event=db.execute("SELECT payload FROM events WHERE dedupe=?",(session_id+':quota_limits',)).fetchone()
        if not limit_event:return False
        limit=json.loads(limit_event['payload'])['seconds_limit']
        if quota['seconds']+quota['reserved']+seconds>limit:return False
        db.execute('UPDATE quota SET reserved=reserved+? WHERE tenant=? AND month=?',(seconds,row['tenant'],row['month']))
        db.execute('UPDATE sessions SET reserved=reserved+? WHERE id=?',(seconds,session_id))
        return True


def reconcile_session(session_id,reason,duration):
    """Operator confirmed external end state only; never infer a crashed call ended."""
    finish(session_id,reason,duration)


def bind_provider(local_id,provider_id):
    """Atomically rename pre-dial reservation to actual provider call UUID."""
    with transaction() as db:
        row=db.execute('SELECT * FROM sessions WHERE id=?',(local_id,)).fetchone()
        if not row:raise ValueError('Admission missing')
        if local_id==provider_id:return
        existing=db.execute('SELECT id FROM sessions WHERE id=?',(provider_id,)).fetchone()
        if existing:raise ValueError('Provider call already bound')
        db.execute('UPDATE sessions SET id=? WHERE id=?',(provider_id,local_id))
        db.execute('UPDATE events SET session_id=? WHERE session_id=?',(provider_id,local_id))
        limits=db.execute('SELECT payload FROM events WHERE dedupe=?',(local_id+':quota_limits',)).fetchone()
        if limits:db.execute('UPDATE events SET dedupe=? WHERE dedupe=?',(provider_id+':quota_limits',local_id+':quota_limits'))


# ---- Callback inbox: a callback is a work item with an owner, a due time and a done state. ----
CALLBACK_OUTCOMES={'reached','no_answer','wrong_number','not_interested','visit_scheduled','other'}


def callback_create(tenant,session_id,phone,lead_id,reason,due_minutes=30):
    """Idempotent per session: a call that triggers several fallbacks still yields one inbox item."""
    now=time.time()
    with transaction() as db:
        db.execute('INSERT OR IGNORE INTO callbacks(id,tenant,session_id,phone,lead_id,reason,status,due_at,created) VALUES(?,?,?,?,?,?,?,?,?)',
          (str(uuid.uuid4()),tenant,session_id,phone,lead_id,reason,'open',now+max(1,int(due_minutes))*60,now))
        return dict(db.execute('SELECT * FROM callbacks WHERE session_id=?',(session_id,)).fetchone())


def _callback_view(row,now):
    d=dict(row)
    d['overdue']=d['status'] in {'open','assigned'} and bool(d['due_at']) and d['due_at']<now
    return d


def callback_list(tenant,statuses=('open','assigned'),limit=200):
    now=time.time()
    marks=','.join('?'*len(statuses))
    with transaction() as db:
        rows=db.execute(f'SELECT * FROM callbacks WHERE tenant=? AND status IN ({marks}) ORDER BY due_at LIMIT ?',(tenant,*statuses,limit)).fetchall()
    return [_callback_view(r,now) for r in rows]


def callback_get(tenant,callback_id):
    with transaction() as db:
        row=db.execute('SELECT * FROM callbacks WHERE id=? AND tenant=?',(callback_id,tenant)).fetchone()
    return _callback_view(row,time.time()) if row else None


def callback_assign(tenant,callback_id,assignee,due_minutes=None):
    assignee=str(assignee or '').strip()[:80]
    if not assignee:raise ValueError('assignee required')
    now=time.time()
    with transaction() as db:
        row=db.execute('SELECT status FROM callbacks WHERE id=? AND tenant=?',(callback_id,tenant)).fetchone()
        if not row:return None
        if row['status'] in {'done','cancelled'}:raise ValueError('callback already closed')
        if due_minutes is not None:
            db.execute("UPDATE callbacks SET status='assigned',assignee=?,assigned_at=?,due_at=? WHERE id=?",(assignee,now,now+max(1,int(due_minutes))*60,callback_id))
        else:
            db.execute("UPDATE callbacks SET status='assigned',assignee=?,assigned_at=? WHERE id=?",(assignee,now,callback_id))
    return callback_get(tenant,callback_id)


def callback_complete(tenant,callback_id,outcome,notes='',retry_minutes=60):
    """no_answer keeps the item open with a new due time; every other outcome closes it."""
    if outcome not in CALLBACK_OUTCOMES:raise ValueError('invalid outcome')
    now=time.time();notes=str(notes or '')[:1000]
    with transaction() as db:
        row=db.execute('SELECT status FROM callbacks WHERE id=? AND tenant=?',(callback_id,tenant)).fetchone()
        if not row:return None
        if row['status'] in {'done','cancelled'}:return callback_get_in(db,tenant,callback_id,now)
        if outcome=='no_answer':
            db.execute("UPDATE callbacks SET attempts=attempts+1,outcome=?,notes=?,due_at=? WHERE id=?",(outcome,notes,now+retry_minutes*60,callback_id))
        else:
            db.execute("UPDATE callbacks SET status='done',completed_at=?,outcome=?,notes=?,attempts=attempts+1 WHERE id=?",(now,outcome,notes,callback_id))
    return callback_get(tenant,callback_id)


def callback_get_in(db,tenant,callback_id,now):
    row=db.execute('SELECT * FROM callbacks WHERE id=? AND tenant=?',(callback_id,tenant)).fetchone()
    return _callback_view(row,now) if row else None
