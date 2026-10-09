import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from pipecat.frames.frames import (BotStartedSpeakingFrame, UserStoppedSpeakingFrame,
    MetricsFrame, TranscriptionFrame, TTSSpeakFrame, TextFrame, LLMContextFrame)
from pipecat.metrics.metrics import LLMTokenUsage, LLMUsageMetricsData, TTFAMetricsData
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.processors.aggregators.llm_context import LLMContext
import bot
from call_repairs import (SHORT_GOODBYE, FAQ_TEXTS, valid_transcript, terminal_answer,
    faq_key, brochure_payload, brochure_reply, finish_brochure, dispatch_brochure,
    queue_goodbye, TTSStallState)
from metrics_collector import CallMetricsCollector
D = FrameDirection.DOWNSTREAM

@pytest.fixture(autouse=True)
def processor_harness(monkeypatch):
    monkeypatch.setattr(FrameProcessor, 'process_frame', AsyncMock())

def test_goodbye_cache_exact_and_no_early_substring_match():
    assert bot._match_cached_phrase(SHORT_GOODBYE) == 'short_goodbye'
    assert bot._match_cached_phrase(SHORT_GOODBYE + ' Anything else?') is None

async def test_goodbye_immediately_queues_exactly_one_frame():
    coord = SimpleNamespace(request_ending=lambda: None)
    task = SimpleNamespace(queue_frames=AsyncMock())
    await queue_goodbye(coord, task)
    frames = task.queue_frames.call_args.args[0]
    assert len(frames) == 1 and frames[0].text == SHORT_GOODBYE

@pytest.mark.parametrize('text', ['11:00.', 'No', 'No.', '3', 'हाँ', 'a', 'Alex'])
def test_valid_short_answers(text):
    assert valid_transcript(text)

@pytest.mark.parametrize('text', ['', '...', '***', '!?!'])
def test_true_nonlinguistic_noise(text):
    assert not valid_transcript(text)

async def test_noise_is_empty_before_context_but_valid_no_survives():
    tap = bot._TranscriptionTap()
    tap.push_frame = AsyncMock()
    f = TranscriptionFrame(text='***', user_id='', timestamp='')
    await tap.process_frame(f, D)
    assert tap.push_frame.call_args.args[0].text == ''
    f = TranscriptionFrame(text='No', user_id='', timestamp='')
    await tap.process_frame(f, D)
    assert tap.push_frame.call_args.args[0].text == 'No'

@pytest.mark.parametrize('text,expected', [('11:00.', True), ('Large one.', True), ('uh.', False), ('I want', False)])
def test_terminal_commit_policy(text, expected):
    st = bot.DebouncedExternalUserTurnStopStrategy()
    assert terminal_answer(text, st._is_pure_filler(text), st._ends_with_continuation_connector(text)) == expected

async def test_actual_strategy_commits_terminal_after_final_without_timeout():
    st = bot.DebouncedExternalUserTurnStopStrategy()
    st._user_speaking = False
    st._turn_open = True
    st._maybe_trigger_user_turn_stopped = AsyncMock()
    await st._handle_transcription(TranscriptionFrame(text='Large one.', user_id='', timestamp=''))
    st._maybe_trigger_user_turn_stopped.assert_awaited()

@pytest.mark.parametrize('key', list(FAQ_TEXTS))
def test_faq_exact_cache(key):
    assert bot._match_cached_phrase(FAQ_TEXTS[key]) == key
    assert bot._match_cached_phrase(FAQ_TEXTS[key] + ' Different project.') is None

@pytest.mark.parametrize('q,key', [('Can you tell me Vastu?', 'faq_vastu'), ('When is possession?', 'faq_possession'), ('What amenities?', 'faq_amenities'), ('What amenities and price?', None), ('Is it not Vastu?', None), ('Tell me possession and amenities', None)])
def test_faq_narrow_routing(q, key):
    assert faq_key(q) == key

async def test_faq_disabled_passes_through_enabled_bypasses_llm():
    for enabled in (False, True):
        router = bot._FastPathRouter(config={'faq_cache_enabled': enabled})
        router.push_frame = AsyncMock()
        frame = LLMContextFrame(LLMContext([{'role':'system','content':'fixed'}, {'role':'user','content':'What amenities?'}]))
        await router.process_frame(frame, D)
        emitted = [c.args[0] for c in router.push_frame.call_args_list]
        assert any(isinstance(f, LLMContextFrame) for f in emitted) != enabled


def config():
    return {'brochure_delivery': {'brochure_url':'https://example.test/brochure.pdf',
        'floorplan_url':'https://example.test/floorplan.pdf', 'template_name':'brochure_details'}}

@pytest.mark.parametrize('consent,phone,cfg,error', [(False,'+919876543210',config(),'needs_consent'), (True,'',config(),'needs_phone'), (True,'+919876543210',{},'not_configured')])
def test_brochure_fail_closed(consent, phone, cfg, error):
    payload, actual = brochure_payload('call',phone,'Alex','2026-10-09','11 AM',cfg,consent)
    assert payload is None and actual == error

async def test_brochure_deterministic_callback_does_not_run_second_llm():
    params = SimpleNamespace(result_callback=AsyncMock())
    task = SimpleNamespace(queue_frames=AsyncMock())
    guard = SimpleNamespace()
    await finish_brochure(params, {'status':'queued','outbox_id':'q'}, task, guard)
    assert params.result_callback.call_args.kwargs['properties'].run_llm is False
    assert 'sent' not in task.queue_frames.call_args.args[0][0].text
    assert guard._whatsapp_succeeded_this_turn is False
    await finish_brochure(params, {'status':'sent','message_id':'wamid.1'}, task, guard)
    assert guard._whatsapp_succeeded_this_turn is True

@pytest.mark.parametrize('result', [{'status':'sent'}, {'status':'failed'}, {'status':'not_configured'}])
def test_brochure_never_claims_sent_without_id(result):
    assert 'have been sent' not in brochure_reply(result)

class FakeClient:
    response = None
    request = None
    def __init__(self, **kwargs): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    async def post(self, url, **kwargs):
        self.__class__.request = kwargs['json']
        return self.__class__.response

async def test_meta_acceptance_and_parameters(monkeypatch):
    monkeypatch.setenv('WHATSAPP_ACCESS_TOKEN','test-not-real')
    monkeypatch.setenv('WHATSAPP_PHONE_NUMBER_ID','number')
    payload, _ = brochure_payload('call','+919876543210','Alex','2026-10-09','11 AM',config(),True)
    FakeClient.response = SimpleNamespace(is_success=True, status_code=200, json=lambda: {'messages':[{'id':'wamid.1'}]})
    result = await dispatch_brochure(payload, FakeClient)
    assert result['message_id'] == 'wamid.1' and result['status'] == 'sent'
    values = FakeClient.request['template']['components'][0]['parameters']
    assert [x['text'] for x in values] == ['Alex','2026-10-09','11 AM','https://example.test/brochure.pdf','https://example.test/floorplan.pdf']
    FakeClient.response = SimpleNamespace(is_success=True,status_code=200,json=lambda: {})
    assert (await dispatch_brochure(payload, FakeClient))['status'] == 'failed'

async def test_template_missing_never_uses_free_form(monkeypatch):
    monkeypatch.setenv('WHATSAPP_ACCESS_TOKEN','test')
    monkeypatch.setenv('WHATSAPP_PHONE_NUMBER_ID','number')
    result = await dispatch_brochure({'consent':True})
    assert result['status'] == 'not_configured'

async def test_unknown_send_is_not_retried(monkeypatch):
    monkeypatch.setenv('WHATSAPP_ACCESS_TOKEN','test')
    monkeypatch.setenv('WHATSAPP_PHONE_NUMBER_ID','number')
    class TimeoutClient(FakeClient):
        calls = 0
        async def post(self, *args, **kwargs):
            self.__class__.calls += 1
            raise asyncio.TimeoutError()
    p,_=brochure_payload('c','+919876543210','Alex','date','time',config(),True)
    assert (await dispatch_brochure(p, TimeoutClient))['status'] == 'uncertain'
    assert TimeoutClient.calls == 1

async def test_native_usage_hook_and_metrics_frame_exactly_once():
    m = CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam',cost_rates={'llm':{'qwen':{'prompt_per_mtok':.8,'completion_per_mtok':4}}})
    usage=LLMTokenUsage(prompt_tokens=1000,completion_tokens=10,total_tokens=1010)
    m.record_llm_token_usage('groq','qwen',1000,10,usage_object=usage)
    f=MetricsFrame(data=[LLMUsageMetricsData(processor='llm',model='qwen',value=usage)])
    await m.process_frame(f,D)
    await m.process_frame(f,D)
    assert m._totals['llm_prompt_tokens']==1000
    assert m._estimate_cost()[1]['llm']==pytest.approx(.00084)

async def test_distinct_native_usage_events_identical_counts_both_counted():
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    for _ in range(2):
        u=LLMTokenUsage(prompt_tokens=100,completion_tokens=10,total_tokens=110)
        await m.process_frame(MetricsFrame(data=[LLMUsageMetricsData(processor='llm',model='qwen',value=u)]),D)
    assert m._totals['llm_prompt_tokens']==200

async def test_tts_raw_effective_not_double_subtracted():
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    m.record_tts_silence_trimmed(100,1)
    data=TTFAMetricsData(processor='tts',model='bulbul:v3',ttfa=.5,ttfb=.28,leading_silence=.22)
    await m.process_frame(MetricsFrame(data=[data]),D)
    s=m.summary()
    assert s['avg_tts_ttfa_raw_ms']==500
    assert s['avg_tts_ttfa_effective_ms']==300

async def test_cached_answer_excluded_from_all_live_anchors():
    import time
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    await m.process_frame(UserStoppedSpeakingFrame(),D)
    m._speech_stop_at=time.monotonic()-.3
    m._last_transcript_at=time.monotonic()-.2
    m.record_cached_answer('opening_intro')
    await m.process_frame(BotStartedSpeakingFrame(),D)
    s=m.summary()
    assert s['latency_transcript_count']==0 and s['cached_answer_count']==1


def test_single_system_prefix_stays_immutable():
    messages=[{'role':'system','content':'IMMUTABLE'},{'role':'user','content':'3 BHK'}]
    for _ in range(3): bot._sync_working_memory(messages,{'configuration':'3 BHK'})
    assert messages[0]['content']=='IMMUTABLE'
    assert len([m for m in messages if m['role']=='system'])==2

async def test_stall_hedge_fires_once_cancels_and_resets():
    hedge=AsyncMock()
    state=TTSStallState(hedge,delay=.01)
    state.arm();state.arm()
    await asyncio.sleep(.025)
    hedge.assert_awaited_once()
    state.arm();await asyncio.sleep(.025)
    hedge.assert_awaited_once()
    state.cancel(new_turn=True);state.arm();state.cancel()
    await asyncio.sleep(.025)
    hedge.assert_awaited_once()

async def test_audio_observer_cancels_stall_state():
    from pipecat.frames.frames import TTSAudioRawFrame
    state=TTSStallState(AsyncMock(),delay=.01)
    observer=bot._TTSStallObserver(state);observer.push_frame=AsyncMock()
    state.arm()
    await observer.process_frame(TTSAudioRawFrame(audio=b'\0\0',sample_rate=8000,num_channels=1),D)
    await asyncio.sleep(.02)
    state.hedge.assert_not_awaited()

async def test_sent_claim_hinglish_filtered_without_success():
    guard=bot._SpokenTextGuard(stream_id='t')
    assert guard._filter_unverified_claims('Brochure aur floor plans bhej diye hain.') == ''

async def test_outbox_actions_are_distinct_and_full_brochure_survives(monkeypatch, tmp_path):
    import uuid
    from sqlalchemy import select
    import leads.db as db
    import leads.outbox as outbox
    import call_repairs
    from leads.models import Lead, OutboxItem
    old_engine, old_factory = db._async_engine, db._async_session_factory
    db._async_engine = None; db._async_session_factory = None
    monkeypatch.setenv('DATABASE_URL', 'sqlite+aiosqlite:///' + str(tmp_path / 'outbox.db'))
    try:
        await db.init_models()
        async with db.get_session() as session:
            lead=Lead(phone='+919876543210',name='Alex',source='test')
            session.add(lead);await session.flush()
            payload,_=brochure_payload('c',lead.phone,lead.name,'2026-10-09','11 AM',config(),True)
            first=await outbox.queue_outbox_item(lead.id,'whatsapp',payload,session=session)
            again=await outbox.queue_outbox_item(lead.id,'whatsapp',payload,session=session)
            assert again is first
            other=await outbox.queue_outbox_item(lead.id,'whatsapp',{'call_id':'c','phone':lead.phone},session=session)
            assert other is not first
            await session.commit()
        sent=AsyncMock(return_value={'ok':True,'status':'sent','message_id':'wamid.1'})
        monkeypatch.setattr(call_repairs,'dispatch_brochure',sent)
        import services.whatsapp_sender as sender
        monkeypatch.setattr(sender,'send_whatsapp_location',AsyncMock(return_value={'ok':True,'message_id':'location.1'}))
        assert await outbox.drain_outbox()==2
        sent.assert_awaited_once()
        assert sent.call_args.args[0]['brochure_url'].endswith('brochure.pdf')
        async with db.get_session() as session:
            items=(await session.execute(select(OutboxItem))).scalars().all()
            assert all(i.status=='done' for i in items)
            assert {i.payload['message_id'] for i in items}=={'wamid.1','location.1'}
    finally:
        await db._async_engine.dispose()
        db._async_engine,db._async_session_factory=old_engine,old_factory

async def test_outbox_unknown_send_does_not_schedule_retry(monkeypatch,tmp_path):
    from sqlalchemy import select
    import leads.db as db
    import leads.outbox as outbox
    import call_repairs
    from leads.models import Lead,OutboxItem
    old_engine,old_factory=db._async_engine,db._async_session_factory
    db._async_engine=None;db._async_session_factory=None
    monkeypatch.setenv('DATABASE_URL','sqlite+aiosqlite:///'+str(tmp_path/'uncertain.db'))
    try:
        await db.init_models()
        async with db.get_session() as session:
            lead=Lead(phone='+919876543210',source='test');session.add(lead);await session.flush()
            await outbox.queue_outbox_item(lead.id,'whatsapp',{'call_id':'c','action':'send_brochure'},session=session)
        monkeypatch.setattr(call_repairs,'dispatch_brochure',AsyncMock(return_value={'status':'uncertain','ok':False,'error':'timeout'}))
        assert await outbox.drain_outbox()==0
        async with db.get_session() as session:
            item=(await session.execute(select(OutboxItem))).scalar_one()
            assert item.status=='failed' and item.next_attempt_at is None
            assert 'manual reconciliation' in item.last_error
    finally:
        await db._async_engine.dispose();db._async_engine,db._async_session_factory=old_engine,old_factory

async def test_real_groq_usage_hook_emits_native_frame_without_double_count(monkeypatch):
    import yaml
    monkeypatch.setenv('GROQ_API_KEY','fake-test-key')
    cfg=yaml.safe_load(open('config.yaml',encoding='utf8'))
    service=bot.ServiceFactory.create('llm','groq',cfg)
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    service._call_metrics=m
    service._billing_model='llama-3.3-70b-versatile'
    usage=LLMTokenUsage(prompt_tokens=1444,completion_tokens=22,total_tokens=1466)
    service.push_frame=AsyncMock()
    service._enable_usage_metrics=True
    await service.start_llm_usage_metrics(usage)
    # The real processor metric keeps the usage object and later defaults its model to primary.
    f=await service._metrics.start_llm_usage_metrics(usage)
    await m.process_frame(f,D)
    assert m._totals['llm_prompt_tokens']==1444
    assert list(m._provider_llm_tokens)==['groq:llama-3.3-70b-versatile']

async def test_cached_pcm_goodbye_does_not_pollute_live_latency():
    import time
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    await m.process_frame(UserStoppedSpeakingFrame(),D)
    m._last_transcript_at=time.monotonic()-.1
    m.record_cached_audio_ttfa('short_goodbye')
    await m.process_frame(BotStartedSpeakingFrame(),D)
    await m.process_frame(BotStartedSpeakingFrame(),D)
    assert m.summary()['latency_live_count']==0

async def test_verified_confirmation_not_replaced_by_brochure_close_cache():
    guard=bot._SpokenTextGuard(stream_id='t')
    guard.push_frame=AsyncMock()
    f=TTSSpeakFrame(text='The brochure and floor plans have been sent on WhatsApp.')
    f.is_deterministic_confirmation=True
    await guard.process_frame(f,D)
    assert guard.push_frame.call_args.args[0].text == f.text


def test_active_provider_registries_unchanged_and_no_cerebras_active():
    import yaml
    cfg=yaml.safe_load(open('config.yaml',encoding='utf8'))
    assert cfg['active_providers']=={'stt':'sarvam','llm':'groq','tts':'sarvam'}
    assert cfg['providers']['tts']['sarvam']['params']['min_buffer_size']==30
    assert cfg['providers']['tts']['sarvam']['params']['max_chunk_length']==120
    assert cfg['providers']['llm']['groq']['params']['extra']['reasoning_effort']=='none'

async def test_stall_state_is_wired_to_real_tts_request_event(monkeypatch):
    import yaml
    monkeypatch.setenv('SARVAM_API_KEY','fake-test-key')
    cfg=yaml.safe_load(open('config.yaml',encoding='utf8'))
    tts=bot.ServiceFactory.create('tts','sarvam',cfg)
    hedge=AsyncMock();state=TTSStallState(hedge,delay=.01)
    @tts.event_handler('on_tts_request')
    async def watch(service,context_id,text): state.arm()
    await tts._call_event_handler('on_tts_request','ctx','Hello')
    await asyncio.sleep(.03)
    hedge.assert_awaited_once()


def test_goodbye_brochure_and_stall_wiring_source():
    import inspect
    source=inspect.getsource(bot.run_bot)
    assert 'await queue_goodbye(call_end_coordinator, task)' in source
    assert 'await finish_brochure(params, result, task, spoken_text_guard)' in source
    assert 'tts_stall_observer,' in source
    assert 'class _TTSStallObserver' in inspect.getsource(bot)
    assert '"whatsapp": "sent"' not in source[source.index('async def send_brochure'):source.index('async def handoff_to_human')]


def test_audio_preflight_checks_pcm_not_file_size(tmp_path):
    import wave
    from generate_static_audio import validate_wav
    fake=tmp_path/'fake.wav';fake.write_bytes(b'x'*500)
    assert not validate_wav(fake)
    good=tmp_path/'good.wav'
    with wave.open(str(good),'wb') as w:
        w.setnchannels(1);w.setsampwidth(2);w.setframerate(8000);w.writeframes(b'\0\0'*800)
    assert validate_wav(good)

async def test_native_usage_unavailable_is_explicit_not_heuristic():
    m=CallMetricsCollector(call_id='m',stt_provider='sarvam',llm_provider='groq',tts_provider='sarvam')
    m.enrich_from_transcript([{'role':'assistant','content':'Many spoken words.'}])
    assert m.summary()['llm_prompt_tokens']==0
    assert m.summary()['llm_usage_source']=='unavailable'
    assert m.summary()['cost_is_partial'] is True

async def test_faq_emits_full_speak_frame_so_25char_flush_cannot_miss_cache():
    router=bot._FastPathRouter(config={'faq_cache_enabled':True})
    router.push_frame=AsyncMock()
    context=LLMContext([{'role':'system','content':'fixed'},{'role':'user','content':'What amenities?'}])
    await router.process_frame(LLMContextFrame(context),D)
    frames=[c.args[0] for c in router.push_frame.call_args_list]
    spoken=[f for f in frames if isinstance(f,TTSSpeakFrame)]
    assert len(spoken)==1
    assert spoken[0].text==FAQ_TEXTS['faq_amenities']
    assert bot._match_cached_phrase(spoken[0].text)=='faq_amenities'
