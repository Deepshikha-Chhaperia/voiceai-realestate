import asyncio
import wave
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
import bot
from call_repairs import dispatch_brochure
from audio_provenance import PHRASES, effective_config, fresh_audio, write_provenance
from pipecat.frames.frames import LLMContextFrame, TextFrame, LLMFullResponseStartFrame, LLMFullResponseEndFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

@pytest.mark.parametrize('text',['Definitely.','Awaasona.','Bye bye.','Thank you.'])
async def test_farewell_noise_does_not_reopen(text,monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(_ended=False,is_ending=True,_dedicated_goodbye_queued=True)
    router=bot._FastPathRouter(call_end_coordinator=c)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':text}])),FrameDirection.DOWNSTREAM)
    router.push_frame.assert_not_awaited()

async def test_one_real_late_question_not_second_closing(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(_ended=False,is_ending=True,_dedicated_goodbye_queued=True)
    router=bot._FastPathRouter(call_end_coordinator=c)
    router.push_frame=AsyncMock()
    frame=LLMContextFrame(LLMContext([{'role':'user','content':'Wait what about parking?'}]))
    await router.process_frame(frame,FrameDirection.DOWNSTREAM)
    assert router.push_frame.await_count==1
    await router.process_frame(frame,FrameDirection.DOWNSTREAM)
    assert router.push_frame.await_count==1

async def test_silence_does_not_cancel_explicit_goodbye():
    c=SimpleNamespace(_dedicated_goodbye_queued=True,is_ending=True,cancel_ending=AsyncMock())
    checker=bot._SilenceChecker(stream_id='t',call_end_coordinator=c,context_aggregator_user=None)
    checker._termination_processor=SimpleNamespace(cancel_pending_termination=AsyncMock())
    checker.on_user_speech()
    c.cancel_ending.assert_not_called()
    checker._termination_processor.cancel_pending_termination.assert_not_called()
    assert not checker._running

def test_passive_day_is_not_booking_or_assistant_slot():
    fields=bot._extract_lead_preferences('Can I come tomorrow?', 'Already confirmed for tomorrow at 2 PM')
    assert fields['site_visit'].startswith('Requested')
    assert 'time_slot' not in fields
    assert 'preferred_visit_date' not in bot._extract_lead_preferences('Yes sure', 'How about tomorrow?')
    assert bot._extract_lead_preferences('11:00.', '')['time_slot'] == '11:00 AM'

@pytest.mark.parametrize('text',['We have a slot tomorrow at two in the afternoon.','I have your site visit. See you then.','Would you come in the same slot?'])
def test_booking_hallucination_fallback(text):
    ctx=LLMContext([{'role':'user','content':'Yeah yeah that works.'}])
    guard=bot._SpokenTextGuard(context=ctx,lead_memory={'site_visit':'Requested (tomorrow)'})
    clean=guard._filter_unverified_claims(text)
    assert 'not booked' in clean
    assert 'See you' not in clean and 'same slot' not in clean and 'We have a slot' not in clean

def test_cache_provenance_never_relabels_legacy(tmp_path):
    cfg=effective_config(Path(bot.__file__).parent)
    p=tmp_path/'short_goodbye.wav'
    with wave.open(str(p),'wb') as w:
        w.setnchannels(1);w.setsampwidth(2);w.setframerate(16000);w.writeframes(b'\0\0'*200)
    assert not fresh_audio(p,'short_goodbye',PHRASES['short_goodbye'],cfg)
    write_provenance(p,'short_goodbye',PHRASES['short_goodbye'],cfg)
    assert fresh_audio(p,'short_goodbye',PHRASES['short_goodbye'],cfg)
    assert not fresh_audio(p,'short_goodbye','Changed',cfg)
    with p.open('ab') as f:f.write(b'changed')
    assert not fresh_audio(p,'short_goodbye',PHRASES['short_goodbye'],cfg)

async def test_brochure_meta_locked_is_distinct(monkeypatch):
    monkeypatch.setenv('WHATSAPP_ACCESS_TOKEN','fake')
    monkeypatch.setenv('WHATSAPP_PHONE_NUMBER_ID','fake')
    class Client:
        def __init__(self,**kwargs):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def post(self,*a,**kw):
            return SimpleNamespace(is_success=False,status_code=400,json=lambda:{'error':{'code':131031,'message':'Business Account locked'}})
    p=dict(template_name='x',consent=True,phone='+919876543210',name='A',visit_date_iso='Not booked',time_slot='Not booked',brochure_url='https://example.test/b',floorplan_url='https://example.test/f')
    result=await dispatch_brochure(p,Client)
    assert result['error_code']==131031 and '131031' in result['error']
    assert result['status']=='failed'


def test_model_cannot_turn_tomorrow_into_invented_two_pm():
    msgs=[{'role':'user','content':'Can I come tomorrow?'}, {'role':'assistant','content':'We have a slot at 2 PM'}, {'role':'user','content':'Yeah that works'}]
    assert not bot.caller_slot_matches(msgs,{'date':'tomorrow','time':'2 PM'})
    msgs.append({'role':'user','content':'Tomorrow at 2 PM please'})
    assert bot.caller_slot_matches(msgs,{'date':'tomorrow','time':'2 PM'})
    assert not bot.caller_slot_matches(msgs,{'date':'tomorrow','time':'3 PM'})

async def test_date_only_visit_asks_time_without_groq(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    router=bot._FastPathRouter()
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Can I come tomorrow?'}])),FrameDirection.DOWNSTREAM)
    frames=[c.args[0] for c in router.push_frame.call_args_list]
    assert len(frames)==1 and 'What time' in frames[0].text and not isinstance(frames[0],LLMContextFrame)


def test_uncached_first_goodbye_survives_but_late_llm_closing_does_not():
    guard=bot._SpokenTextGuard(call_end_coordinator=SimpleNamespace(_dedicated_goodbye_queued=True))
    assert guard._filter_unverified_claims('Thank you. Goodbye!') == 'Thank you. Goodbye!'
    guard._in_llm_turn=True
    assert not guard._filter_unverified_claims('Thank you. Have a great day!').strip()


@pytest.mark.parametrize('user,previous,pending,expected', [
 ('Can you tell me amenities?', '',False,'not_requested'),
 ('Please send brochure','',False,'needs_consent'),
 ('Send brochure on WhatsApp','',False,'consent'),
 ('Yes please','May I send the brochure on WhatsApp?',True,'consent'),
 ('Yes','Which BHK do you want?',True,'not_requested'),
 ('No','May I send the brochure on WhatsApp?',True,'declined'),
 ('Yes','May I send the brochure on WhatsApp?',False,'consent')])
def test_specific_channel_consent(user,previous,pending,expected):
    from call_repairs import brochure_decision
    messages=[{'role':'assistant','content':previous},{'role':'user','content':user}]
    assert brochure_decision(messages,pending)==expected

def test_delivery_state_wording():
    from call_repairs import brochure_reply
    assert 'on its way' in brochure_reply({'status':'sent','message_id':'x'})
    assert "I'll WhatsApp" in brochure_reply({'status':'queued'})
    assert 'sent' not in brochure_reply({'status':'queued'})
    assert 'unavailable' in brochure_reply({'status':'failed','error_code':131031})
    assert 'May I' in brochure_reply({'status':'needs_consent'})


async def test_consent_fastpath_asks_then_does_not_swallow_yes(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    memory={}
    router=bot._FastPathRouter(lead_memory=memory)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Send brochure please'}])),FrameDirection.DOWNSTREAM)
    assert memory['_brochure_consent_pending']
    spoken=router.push_frame.call_args.args[0]
    assert 'WhatsApp?' in spoken.text
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'assistant','content':spoken.text},{'role':'user','content':'Yes please'}])),FrameDirection.DOWNSTREAM)
    assert isinstance(router.push_frame.call_args.args[0],LLMContextFrame)
