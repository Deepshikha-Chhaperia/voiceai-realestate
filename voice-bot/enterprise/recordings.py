"""Durable recording jobs; Sarvam only, bounded HTTPS fetch, operator-visible failures."""
import asyncio
import json
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
import httpx
from enterprise import store
from enterprise.config import policy, readiness


def queue(session_id,data,config):
    row=store.session(session_id)
    if not row or not readiness(config)['human_mode']['ready']:return {'status':'not_ready'}
    if data.get('Event') in {'RecordStart','Record'}:return {'status':'awaiting_recording_stop'}
    recording_id=data.get('RecordingID');url=data.get('RecordUrl') or data.get('RecordFile')
    parsed=urlparse(str(url or ''));r=policy(config)['recording']
    hosts=r.get('allowed_hosts',[])
    # Hostnames must be the exact provider storage hosts approved in deployment; no redirects.
    if not recording_id or parsed.scheme!='https' or parsed.hostname not in hosts or parsed.username or parsed.password:
        store.event(row['tenant'],session_id,'recording_blocked',{'reason':'Unapproved recording URL or ID'},session_id+':recording_blocked')
        return {'status':'blocked'}
    item=store.enqueue('recording',row['tenant'],session_id,{'url':url,'recording_id':recording_id,
      'lead_id':row['lead_id'],'duration':data.get('RecordingDuration'),'stage':'download'},
      row['tenant']+':recording:'+recording_id,expires=time.time()+r['retention_days']*86400)
    return {'status':item['state'],'id':item['id']}


async def drain(config):
    if not readiness(config)['human_mode']['ready']:return
    item=store.claim('recording',policy(config).get('tenant_id'))
    if not item:return
    r=policy(config)['recording'];payload=item['payload']
    try:
        from sarvamai import AsyncSarvamAI
        with TemporaryDirectory(prefix='human-recording-') as tmp:
            audio=Path(tmp)/'call.mp3';size=0
            from enterprise.network import assert_public_url
            await assert_public_url(payload['url'],r['allowed_hosts'])
            async with httpx.AsyncClient(timeout=30,follow_redirects=False) as client:
                async with client.stream('GET',payload['url']) as response:
                    response.raise_for_status()
                    with audio.open('wb') as out:
                        async for chunk in response.aiter_bytes():
                            size+=len(chunk)
                            if size>64*1024*1024:raise ValueError('recording exceeds64MB')
                            out.write(chunk)
            client=AsyncSarvamAI(api_subscription_key=os.environ['SARVAM_API_KEY'])
            # Model must be explicitly selected. Never swap the live voice STT model.
            job=await client.speech_to_text_job.create_job(model=r['sarvam_model'],mode='transcribe',
              language_code=r['language_code'],with_diarization=True,num_speakers=2,with_timestamps=True)
            payload.update(stage='sarvam_created',job_id=job.job_id)
            store.complete(item['id'],'processing',payload=payload)
            await job.upload_files(file_paths=[str(audio)]);await job.start()
            await asyncio.wait_for(job.wait_until_complete(),timeout=600)
            await job.download_outputs(output_dir=tmp)
            files=list(Path(tmp).glob('*.json'))
            if not files:raise ValueError('Sarvam returned no transcript JSON')
            documents=[json.loads(f.read_text()) for f in files]
            transcript='\n'.join(d.get('transcript','') for d in documents)
            if not transcript.strip():raise ValueError('Empty human call transcript')
            await save_transcript(item,transcript,config)
            store.complete(item['id'],'completed',payload={'recording_id':payload['recording_id'],
              'lead_id':payload['lead_id'],'job_id':payload.get('job_id'),'transcript':transcript})
    except Exception as exc:
        # Paid job might exist. No automatic re-upload/re-create after uncertain failure.
        store.complete(item['id'],'needs_operator',error=type(exc).__name__,payload=payload)


async def save_transcript(item,transcript,config):
    import uuid
    from sqlalchemy import select
    from leads.db import get_session
    from leads.models import Lead,Touchpoint
    from leads.scoring import score
    # Diarization speaker indices do not identify caller versus rep. Never score rep's claims
    # as caller facts; raw transcript remains reviewable and automated tier is pending evidence.
    async with get_session() as db:
        lead=await db.get(Lead,uuid.UUID(item['payload']['lead_id']))
        if not lead:raise ValueError('Lead missing')
        existing=(await db.execute(select(Touchpoint).where(Touchpoint.call_id==item['session_id'],Touchpoint.kind=='human_transcript'))).scalars().first()
        if not existing:db.add(Touchpoint(lead_id=lead.id,kind='human_transcript',call_id=item['session_id'],
          summary='Human call transcribed; caller/rep attribution requires review',
          payload={'transcript':transcript,'recording_id':item['payload']['recording_id'],
                   'expires_at':item['expires'],'speaker_attribution':'unverified','recording_status':'transcribed'}))
        if lead.tier in {'pending','cold'}:
            lead.score=0;lead.tier='pending';lead.score_reason='Human transcript available; assign caller speaker before qualification scoring'


async def prune(config,client_factory=None):
    """Retention: remove local transcript content AND delete provider media (verified by GET 404)."""
    import json as _json
    from sqlalchemy import select
    from leads.db import get_session
    from leads.models import Touchpoint
    from enterprise import provider_media
    now=time.time()
    async with get_session() as db:
        records=(await db.execute(select(Touchpoint).where(Touchpoint.kind=='human_transcript'))).scalars().all()
        for row in records:
            p=row.payload or {}
            if p.get('expires_at') and p['expires_at']<now:row.payload={'recording_status':'retention_expired'}
    with store.transaction() as db:
        due=[dict(r) for r in db.execute("SELECT id,tenant,session_id,payload FROM work WHERE kind='recording' AND expires<? AND state NOT IN ('retention_expired')",(now,))]
    for row in due:
        payload=_json.loads(row['payload'] or '{}')
        rid=payload.get('recording_id')
        if rid and payload.get('provider_next_try',0)<=now:
            res=await provider_media.delete_recording(rid,client_factory)
            if res['state']=='deleted_verified':
                store.event(row['tenant'],row['session_id'],'recording_provider_deleted',{'recording_id':rid},row['id']+':provider_deleted')
                with store.transaction() as db:
                    db.execute("UPDATE work SET payload='{}',state='retention_expired',last_error=NULL WHERE id=?",(row['id'],))
            else:
                store.event(row['tenant'],row['session_id'],'recording_provider_delete_pending',
                  {'recording_id':rid,'result':res['state'],'note':res.get('note') or res.get('reason')},row['id']+':provider_pending:'+str(int(now//86400)))
                with store.transaction() as db:  # keep ONLY the id so the provider copy stays traceable; drop url/transcript
                    db.execute("UPDATE work SET payload=?,state='provider_delete_pending',last_error=? WHERE id=?",
                      (_json.dumps({'recording_id':rid,'provider_next_try':now+3600}),res['state'],row['id']))
        elif not rid:
            with store.transaction() as db:
                db.execute("UPDATE work SET payload='{}',state='retention_expired',last_error=NULL WHERE id=?",(row['id'],))
    with store.transaction() as db:
        db.execute("UPDATE work SET payload='{}',state='retention_expired',last_error=NULL WHERE kind='analytics' AND expires<? AND state!='retention_expired'",(now,))
