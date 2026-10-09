from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import time
import pytest
import yaml
import bot
from spoken_numbers import spoken_numbers
from call_repairs import (campaign_faq, closing_after_work, goal_complete,
    queue_goal_close, record_manual_whatsapp, whatsapp_answer, SHORT_GOODBYE)
from pipecat.frames.frames import (TextFrame, LLMFullResponseStartFrame,
    LLMFullResponseEndFrame, LLMContextFrame, TTSSpeakFrame,
    BotStartedSpeakingFrame, BotStoppedSpeakingFrame, TranscriptionFrame)
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

@pytest.mark.parametrize('raw,expected', [
    ('1580-1750 square feet', 'fifteen hundred eighty to seventeen hundred fifty square feet'),
    ('1920-2100 sq ft', 'nineteen hundred twenty to twenty one hundred square feet'),
    ('1.7-1.8 crore', 'one point seven crore to one point eight crore'),
    ('1.45 crore hai', 'ek crore paintalis lakh hai'),
    ('1.7 se 1.8 crores hai', 'ek crore sattar lakh se ek crore assi lakh hai'),
    ('95 lakh', 'ninety five lakh'), ('₹1.45 crore', 'one point four five crore'),
    ('10% booking', 'ten percent booking'), ('3 BHK, 2 balconies', 'three B H K, two balconies'),
    ('11:30 AM', 'eleven thirty a m'), ('2 PM', 'two p m'),
    ('11:00', "eleven o'clock"), ('late 2027', 'late twenty twenty seven'),
    ('Q4 2027', 'late twenty twenty seven'), ('24/7 security', 'round the clock security'),
    ('twenty-one hundred', 'twenty one hundred')])
def test_final_sentence_numeric_exact_and_idempotent(raw, expected):
    norm = bot._SpokenTextGuard._normalize(raw)
    assert norm == expected
    assert bot._SpokenTextGuard._normalize(norm) == norm

@pytest.mark.parametrize('opaque', ['https://example.test/1.7?id=2100',
    'a2100@example.test', 'lead-2100', '+91 90383 97133', '9038397133', '415-555-0123'])
def test_opaque_identifiers_never_changed(opaque):
    assert bot._SpokenTextGuard._normalize(opaque) == opaque

async def stream(monkeypatch, parts, truncated=False):
    monkeypatch.setattr(FrameProcessor, 'process_frame', AsyncMock())
    context = LLMContext([{'role':'user','content':'Price?'}])
    context._response_truncated = truncated
    guard = bot._SpokenTextGuard(context=context)
    guard.push_frame = AsyncMock()
    await guard.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)
    context._response_truncated = truncated
    for p in parts:
        await guard.process_frame(TextFrame(text=p), FrameDirection.DOWNSTREAM)
    await guard.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    return ''.join(c.args[0].text for c in guard.push_frame.call_args_list if isinstance(c.args[0], TextFrame))

async def test_decimal_dot_split_cannot_escape_as_complete_sentence(monkeypatch):
    assert bot._find_sentence_end('price 1.7 se 1.') == 0
    assert bot._find_sentence_end('price 1.7 se 1. ') == 0
    text = await stream(monkeypatch, ['Price 1.7 se 1.', '8 crore.'])
    assert 'one point eight' in text and 'one point seven' in text

async def test_length_cut_keeps_complete_sentence_not_partial_range(monkeypatch):
    text = await stream(monkeypatch, ['A study is included. Price 1.7 se 1.'], True)
    assert text.strip() == 'A study is included.'

async def test_final_real_numeric_sentence_still_spoken(monkeypatch):
    assert (await stream(monkeypatch, ['There are 2.'])).strip() == 'There are two.'

@pytest.fixture
def cfg():
    return yaml.safe_load((Path(bot.__file__).parent / 'config.yaml').read_text())

def test_short_complete_campaign_comparison(cfg):
    answer = campaign_faq('What is the difference?', {'configuration':'3 BHK'}, cfg)
    assert answer and answer[0] == 'compare_3bhk_v1'
    norm = bot._SpokenTextGuard._normalize(answer[1])
    assert len(norm) < 269
    assert 'one point eight' in norm and 'one point eight five' not in norm
    for fact in ('fifteen hundred eighty', 'seventeen hundred fifty', 'nineteen hundred twenty', 'twenty one hundred', 'study', 'extra balcony'):
        assert fact in norm
    assert campaign_faq('What is the difference?', {'configuration':'2 BHK'}, cfg) is None
    for text in ('What is the difference and price of parking?', 'Compare payment plans', 'What is the difference? Also book a visit'):
        assert campaign_faq(text, {'configuration':'3 BHK'}, cfg) is None
    changed = dict(cfg, real_estate_sales_script=cfg['real_estate_sales_script'].replace('one point eight', 'one point eight five'))
    assert campaign_faq('What is the difference?', {'configuration':'3 BHK'}, changed) is None

@pytest.fixture
def booked():
    return {'disposition':'SITE_VISIT_BOOKED', 'site_visit':'Confirmed (2026-10-10 at 2:00 PM)',
        'visit_date_iso':'2026-10-10','time_slot':'2:00 PM'}

async def test_goal_close_single_ack_goodbye_then_three_second_grace(monkeypatch, booked):
    monkeypatch.setattr(FrameProcessor, 'process_frame', AsyncMock())
    hangup = AsyncMock()
    c = bot._CallEndCoordinator(stream_id='fix9', on_hangup=hangup, grace_seconds=3)
    emit = AsyncMock()
    line = record_manual_whatsapp(booked, {}, 'location')
    await queue_goal_close(booked, c, emit, line)
    assert emit.await_count == 1
    frame = emit.call_args.args[0]
    assert frame.text == 'The location link needs team confirmation. ' + SHORT_GOODBYE
    assert c.is_ending and frame.is_deterministic_confirmation
    await c.process_frame(BotStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    assert not c._in_grace
    await c.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    assert c._in_grace and c._grace_seconds == 3
    hangup.assert_not_awaited()
    await queue_goal_close(booked, c, emit, line)
    assert emit.await_count == 1
    await c._finish()

@pytest.mark.parametrize('missing', ['site_visit','visit_date_iso','time_slot'])
async def test_unverified_goal_cannot_autoclose(booked, missing):
    booked.pop(missing)
    c = SimpleNamespace(_dedicated_goodbye_queued=False, request_ending=AsyncMock())
    emit = AsyncMock()
    await queue_goal_close(booked, c, emit, 'Okay.')
    assert emit.call_args.args[0].text == 'Okay.'
    c.request_ending.assert_not_called()

async def test_pending_second_decision_keeps_call_open(booked):
    booked['_whatsapp_consent_action'] = 'brochure'
    assert not goal_complete(booked)

async def test_decline_resolves_decision_and_closes_without_model(monkeypatch, booked):
    monkeypatch.setattr(FrameProcessor, 'process_frame', AsyncMock())
    booked['_whatsapp_consent_action']='location'
    c=SimpleNamespace(_ended=False,is_ending=False,_dedicated_goodbye_queued=False, request_ending=lambda:None)
    r=bot._FastPathRouter(lead_memory=booked, call_end_coordinator=c)
    r.push_frame=AsyncMock()
    ctx=LLMContext([{'role':'assistant','content':'Want the location on WhatsApp?'},{'role':'user','content':'No thanks'}])
    await r.process_frame(LLMContextFrame(ctx),FrameDirection.DOWNSTREAM)
    assert r.push_frame.call_args.args[0].text == "Okay, I won't send it. " + SHORT_GOODBYE
    assert '_postcall_whatsapp_actions' not in booked

@pytest.mark.parametrize('answer', ['हाँ ठीक है sure आप भेज दीजिए ना।','हाँ हाँ।','जी हाँ'])
def test_observed_hindi_consent_is_complete_not_script_erasure(answer):
    assert whatsapp_answer([{'role':'assistant','content':'Want the location on WhatsApp?'},{'role':'user','content':answer}], 'location') == 'consent'

@pytest.mark.parametrize('answer', ['हाँ लेकिन पहले कीमत बताओ', 'हाँ भेजना मत', 'sure क्या कीमत है?', 'yes but what is the price?'])
def test_mixed_question_or_negative_does_not_dispatch(answer):
    assert whatsapp_answer([{'role':'assistant','content':'Want the location on WhatsApp?'},{'role':'user','content':answer}], 'location') != 'consent'

@pytest.mark.parametrize('text', ['Cool. Thank you.', 'Cool thanks.', 'Okay, thank you.'])
def test_shared_completed_close_intent(text):
    assert closing_after_work(text, {'disposition':'SITE_VISIT_BOOKED'})
    assert not closing_after_work(text + ' What is the price?', {'disposition':'SITE_VISIT_BOOKED'})

def test_cost_latency_scope_lock():
    root=Path(bot.__file__).parent
    cfg=yaml.safe_load((root/'config.yaml').read_text())
    assert cfg.get('hangup_grace_seconds',3) == 3
    assert cfg['llm_cache_prewarm_enabled'] is False
    source=(root/'bot.py').read_text()
    assert 'await queue_goal_close(lead_memory, call_end_coordinator, emit_goal, line)' in source
    assert 'closing_after_work(latest_user_text, lead_memory)' in source
    # Close helpers have no provider call, sleep, network, dependency or env knob.
    helper=(root/'spoken_numbers.py').read_text()
    assert 'sleep(' not in helper and 'requests' not in helper

def test_observed_hindi_comparison_uses_exact_fact_route(cfg):
    text = 'Actually वो छोटा वाला और बड़े वाले में difference क्या होगा?'
    assert campaign_faq(text, {'configuration':'3 BHK'}, cfg)

async def test_missing_link_no_offer_and_confirmation_is_complete_before_close(booked):
    from call_repairs import manual_location_offer
    line = manual_location_offer(booked, {})
    assert 'WhatsApp?' not in line and 'team confirmation' in line
    assert '_whatsapp_consent_action' not in booked
    assert not booked.get('_postcall_whatsapp_actions')
    c=SimpleNamespace(_dedicated_goodbye_queued=False,request_ending=lambda:None)
    task=SimpleNamespace(queue_frames=AsyncMock())
    g=bot._SpokenTextGuard(lead_memory=booked,call_end_coordinator=c)
    g.bind_task(task)
    g.mark_tool_succeeded('book_site_visit',confirm_msg='Booked for tomorrow at two.' + line)
    import asyncio
    await asyncio.sleep(0)
    frame=task.queue_frames.call_args.args[0][0]
    assert frame.text.endswith(SHORT_GOODBYE) and frame.text.count('Booked') == 1
    assert c._dedicated_goodbye_queued and frame.is_deterministic_confirmation

async def test_valid_link_needs_specific_decision_not_auto_close(booked):
    from call_repairs import manual_location_offer
    line=manual_location_offer(booked, {'brochure_delivery':{'postcall_location_url':'https://example.test/location'}})
    assert 'WhatsApp' in line and booked['_whatsapp_consent_action']=='location'
    assert not goal_complete(booked)

async def test_grace_real_question_gets_one_reply_and_no_second_farewell(monkeypatch, booked):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=bot._CallEndCoordinator(stream_id='fix9',on_hangup=AsyncMock())
    c.push_frame=AsyncMock()
    await queue_goal_close(booked,c,AsyncMock(),'Okay.')
    await c.process_frame(BotStartedSpeakingFrame(),FrameDirection.DOWNSTREAM)
    await c.process_frame(BotStoppedSpeakingFrame(),FrameDirection.DOWNSTREAM)
    await c.process_frame(TranscriptionFrame(text='Wait, what about parking?',user_id='',timestamp=''),FrameDirection.DOWNSTREAM)
    assert c._extra_reply_count == 1 and c._awaiting_closing
    await c._finish()

def test_numbers_local_overhead_is_submillisecond_on_sample():
    # Regression budget on CPU-only normalizer, not a claim about provider TTFA.
    text='Large is 1920-2100 sq ft at 1.7-1.8 crore, two balconies.'
    start=time.perf_counter()
    for _ in range(500): spoken_numbers(text)
    assert (time.perf_counter()-start)/500 < .001

def test_pending_decision_and_unknown_campaign_never_erased(cfg, booked):
    from call_repairs import manual_location_offer
    booked['_whatsapp_consent_action'] = 'brochure'
    manual_location_offer(booked, {})
    assert booked['_whatsapp_consent_action'] == 'brochure'
    assert not goal_complete(booked)
    assert not closing_after_work('Cool thanks', booked)
    assert campaign_faq('What is the difference?', {'configuration':'3 BHK'}, {'real_estate_sales_script':'other project'}) is None

def test_year_range_is_not_an_area():
    assert spoken_numbers('2027 to 2028') == 'twenty twenty seven to twenty twenty eight'

async def test_explicit_bye_during_pending_consent_still_ends_without_dispatch(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(_ended=False,is_ending=False,_dedicated_goodbye_queued=False,request_ending=lambda:None)
    callback=AsyncMock()
    r=bot._FastPathRouter(lead_memory={'_whatsapp_consent_action':'location'},call_end_coordinator=c,on_whatsapp_consent=callback)
    r.push_frame=AsyncMock()
    ctx=LLMContext([{'role':'assistant','content':'Want the location on WhatsApp?'},{'role':'user','content':'Bye bye'}])
    await r.process_frame(LLMContextFrame(ctx),FrameDirection.DOWNSTREAM)
    assert r.push_frame.call_args.args[0].text == SHORT_GOODBYE
    callback.assert_not_awaited()
