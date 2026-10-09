import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from contextlib import asynccontextmanager
import pytest
import bot
import lead_state
import google_sheets_export as sheets
from call_repairs import explicit_caller_name, pure_farewell, unambiguous_visit_time, queue_goodbye
from pipecat.frames.frames import LLMContextFrame, TTSSpeakFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

@pytest.mark.parametrize('text', ['Actually, you tell me first its amenities what all is there.', 'its amenities', 'This is a good project', 'I am interested', 'I am looking for 3 BHK', 'My name is Amenities'])
def test_no_fake_name(text):
    assert explicit_caller_name(text) is None
    assert 'spoken_name' not in bot._extract_lead_preferences(text, 'Thank you, Amenities.')

@pytest.mark.parametrize('text,name', [('My name is Alex.', 'Alex'), ('Call me Raj', 'Raj'), ('Alex speaking.', 'Alex'), ('Mera naam Riya hai', 'Riya')])
def test_explicit_name(text, name):
    assert explicit_caller_name(text) == name

def test_registered_identity_not_overwritten():
    mem = {'registered_name':'Alex','client':'Alex'}
    bot._sync_working_memory([{'role':'user','content':'My name is Raj.'}], mem)
    assert mem['client'] == 'Alex'
    assert mem['spoken_name'] == 'Raj'
    assert mem['identity_discrepancy']

@pytest.mark.parametrize('text,yes', [('Okay, okay, thank you, bye-bye.',True), ('Bye!',True), ('Before I say bye tell me the price',False), ('Bye, can I visit tomorrow?',False), ('No',False), ('Thank you',False)])
def test_farewell_scope(text,yes):
    assert pure_farewell(text) == yes

async def test_goodbye_single_owner_before_groq(monkeypatch):
    monkeypatch.setattr(FrameProcessor, 'process_frame', AsyncMock())
    coord = SimpleNamespace(request_ending=MagicMock())
    router = bot._FastPathRouter(call_end_coordinator=coord)
    router.push_frame = AsyncMock()
    f = LLMContextFrame(LLMContext([{'role':'user','content':'Okay, okay, thank you, bye-bye.'}]))
    await router.process_frame(f, FrameDirection.DOWNSTREAM)
    await router.process_frame(f, FrameDirection.DOWNSTREAM)
    coord.request_ending.assert_called_once()
    frames=[c.args[0] for c in router.push_frame.call_args_list]
    assert len(frames) == 1 and isinstance(frames[0], TTSSpeakFrame)
    assert frames[0].text == 'Thank you. Goodbye!'

async def test_goodbye_tool_does_not_double_active_audio():
    coord = SimpleNamespace(request_ending=MagicMock(), _current_audio_active=True)
    task = SimpleNamespace(queue_frames=AsyncMock())
    await queue_goodbye(coord,task)
    task.queue_frames.assert_not_awaited()
    coord.request_ending.assert_called_once()

@pytest.mark.parametrize('text,yes', [('2:00',False), ('two',False), ('at 2',False), ('11:00.',False), ('2 PM',True), ('two in the afternoon',True), ('14:00',True), ('00:30',True), ('24:00',False), ('Tomorrow at 2',False)])
def test_no_guessed_meridiem(text,yes):
    assert unambiguous_visit_time(text) == yes

async def test_unparseable_booking_never_touches_db():
    params=SimpleNamespace(arguments={'date':'tomorrow','time':'sometime'},result_callback=AsyncMock())
    await bot.execute_book_site_visit(params,stream_id='web-test',lead_memory={})
    assert params.result_callback.call_args.args[0]['status'] == 'needs_confirmation'

@pytest.mark.parametrize('mem', [{'preferred_visit_date':'2026-10-09','preferred_visit_time':'2 PM'}, {'site_visit':'Confirmed tomorrow 2 PM'}, {'disposition':'SITE_VISIT_BOOKED'}])
def test_intent_not_booking(mem):
    d=lead_state.infer_deterministic_disposition(mem,[{'role':'user','content':'Yes, can I come tomorrow at 2:00?'}])
    assert d != 'SITE_VISIT_BOOKED'

def test_success_marker_is_booking():
    assert lead_state.infer_deterministic_disposition({'disposition':'SITE_VISIT_BOOKED','site_visit':'Confirmed (2026-10-09 at 2 PM)'}) == 'SITE_VISIT_BOOKED'

@pytest.mark.parametrize('text,caller,expected', [('Yes, go ahead?','Lajo one.','Sorry, could you repeat that?'),('Yes, go ahead?','Achani Chuda meko Vastu and all bata do.','Sorry, repeat kar sakte hain?'),('Sure, tomorrow at two works. I have noted that for you, Alex.','Can I come tomorrow at 2:00?','not booked')])
def test_spoken_guard(text,caller,expected):
    g=bot._SpokenTextGuard(context=LLMContext([{'role':'user','content':caller}]))
    assert expected in g._filter_unverified_claims(text)

def worksheet(monkeypatch, ids=None):
    ws=MagicMock()
    ws.row_values.side_effect=lambda n:sheets.COLUMNS if n==1 else ['call-1']
    ws.col_values.return_value=ids or ['call_id']
    ws.append_row.return_value={'updates':{'updatedRange':"Calls!A2:Q2"}}
    ws.get.return_value=[['call-1']]
    client=MagicMock();client.open_by_key.return_value.worksheet.return_value=ws
    monkeypatch.setattr(sheets,'_get_client',lambda:client)
    return ws

def test_verified_raw_append(monkeypatch):
    ws=worksheet(monkeypatch)
    r=sheets.append_call_row_verified({'call_id':'call-1','customer_name':'=IMPORTXML("x")'})
    assert r['verified']
    assert ws.append_row.call_args.kwargs['value_input_option']=='RAW'
    ws.get.assert_called_once_with('A2:Q2')

def test_retry_after_append_timeout_reconciles(monkeypatch):
    ws=worksheet(monkeypatch)
    ws.append_row.side_effect=TimeoutError('unknown')
    with pytest.raises(TimeoutError):sheets.append_call_row_verified({'call_id':'call-1'})
    ws.col_values.return_value=['call_id','call-1']
    r=sheets.append_call_row_verified({'call_id':'call-1'})
    assert r['status']=='already_present'
    assert ws.append_row.call_count == 1

def test_sheet_header_mismatch_no_append(monkeypatch):
    ws=worksheet(monkeypatch);ws.row_values.side_effect=None;ws.row_values.return_value=['wrong']
    with pytest.raises(RuntimeError,match='header'):sheets.append_call_row_verified({'call_id':'call-1'})
    ws.append_row.assert_not_called()

def test_readback_failure_is_not_success(monkeypatch):
    ws=worksheet(monkeypatch);ws.get.return_value=[['wrong-id']]
    assert not sheets.append_call_row({'call_id':'call-1'})

async def test_durable_queue_without_lead_and_retries(monkeypatch,tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from leads.models import Base,CallSheetExport
    import leads.outbox as outbox
    engine=create_async_engine('sqlite+aiosqlite:///'+str(tmp_path/'jobs.db'))
    async with engine.begin() as c:await c.run_sync(Base.metadata.create_all)
    factory=async_sessionmaker(engine,expire_on_commit=False)
    @asynccontextmanager
    async def session():
        async with factory() as s:
            yield s
            await s.commit()
    monkeypatch.setattr(outbox,'get_session',session)
    import leads.db as db
    monkeypatch.setattr(db,'ensure_call_sheet_schema',AsyncMock())
    monkeypatch.setattr(outbox,'_sheet_drain_lock',None)
    monkeypatch.setattr(lead_state,'get_call_async',AsyncMock(return_value={'call_id':'web-1'}))
    monkeypatch.setattr(sheets,'append_call_row_verified',MagicMock(side_effect=RuntimeError('unconfigured')))
    await outbox.queue_call_sheet_export('web-1');await outbox.queue_call_sheet_export('web-1')
    assert await outbox.drain_call_sheet_exports()==0
    async with session() as s:
        job=await s.get(CallSheetExport,'web-1')
        assert job.attempts==1 and job.status=='pending' and 'unconfigured' in job.last_error
        job.next_attempt_at=None
    monkeypatch.setattr(sheets,'append_call_row_verified',MagicMock(return_value={'verified':True,'call_id':'web-1'}))
    assert await outbox.drain_call_sheet_exports()==1
    async with session() as s:
        job=await s.get(CallSheetExport,'web-1')
        assert job.status=='done' and job.receipt['verified']
    assert await outbox.drain_call_sheet_exports()==0
    await engine.dispose()

async def test_fragmented_unverified_slot_response_does_not_leak(monkeypatch):
    from pipecat.frames.frames import LLMFullResponseStartFrame, LLMFullResponseEndFrame, TextFrame
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    guard=bot._SpokenTextGuard(context=LLMContext([{'role':'user','content':'Can I come tomorrow at 2:00?'}]))
    guard.push_frame=AsyncMock()
    await guard.process_frame(LLMFullResponseStartFrame(),FrameDirection.DOWNSTREAM)
    for text in ['Sure, tomorrow at two ', 'works. ', 'I have noted that for you, Alex.']:
        await guard.process_frame(TextFrame(text=text),FrameDirection.DOWNSTREAM)
    assert not any(isinstance(c.args[0],TextFrame) for c in guard.push_frame.call_args_list)
    await guard.process_frame(LLMFullResponseEndFrame(),FrameDirection.DOWNSTREAM)
    spoken=' '.join(c.args[0].text for c in guard.push_frame.call_args_list if isinstance(c.args[0],TextFrame))
    assert 'not booked' in spoken and 'works' not in spoken and 'noted' not in spoken

def test_cost_scope_is_not_whole_call():
    from metrics_collector import CallMetricsCollector
    m=CallMetricsCollector(call_id='c',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    summary=m.summary()
    assert summary['cost_scope']=='live_pipeline_only'
    assert summary['whole_call_cost_is_partial']
    assert 'llm_prewarm' in summary['cost_excludes']

def test_analysis_does_not_retime_finished_call(monkeypatch,tmp_path):
    monkeypatch.setattr(lead_state,'DB_PATH',str(tmp_path/'calls.db'))
    monkeypatch.setattr(lead_state,'_db_initialized',False)
    lead_state.init_db()
    lead_state.upsert_call('c')
    lead_state.finalize_call('c')
    before=lead_state.get_call('c')['ended_at']
    lead_state.finalize_call('c',{'summary':'ready'})
    lead_state.finalize_call('c')
    after=lead_state.get_call('c')
    assert after['ended_at']==before
    assert 'ready' in after['analysis_json']

def test_analysis_one_owner_has_actual_transcript():
    from pathlib import Path
    root=Path(bot.__file__).parent
    source=(root/'bot.py').read_text()
    assert 'asyncio.create_task(call_analytics.analyze_call' not in source
    assert 'enqueue_on_call_finished(stream_id, transcript_to_use)' in source
    worker=(root/'leads/worker.py').read_text()
    assert 'analyze_call(call_id, messages)' in worker
    assert 'await queue_outbox_item(lead.id, "sheets"' not in worker


def test_fast_path_plus_audio_is_one_cache_hit():
    from metrics_collector import CallMetricsCollector
    m=CallMetricsCollector(call_id='c',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    m.record_cached_answer('opening_intro')
    assert m.summary()['cache_hits']==0
    m.record_cached_audio_ttfa('opening_intro')
    assert m.summary()['cache_hits']==1
