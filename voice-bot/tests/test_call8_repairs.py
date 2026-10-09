from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
import bot
from call_repairs import visit_time,pure_farewell
from pipecat.frames.frames import LLMContextFrame, TTSSpeakFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor,FrameDirection

@pytest.mark.parametrize('text,expected',[('कल दो बजे आ जाऊं','2:00 PM'),('दो बजे का तो वो confirm करते हैं फिर।','2:00 PM'),('two','2:00 PM'),('11:00.','11:00 AM'),('14:00','2:00 PM'),('सुबह दो बजे','2:00 AM'),('शाम चार बजे','4:00 PM'),('00:30','12:30 AM'),('24:00',None),('2 BHK',None),('two balconies',None),('sometime',None)])
def test_concrete_time_in_callers_words(text,expected):
    assert visit_time(text)==expected

async def test_actual_hindi_visit_sequence_no_llm(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    memory={}
    book=AsyncMock()
    router=bot._FastPathRouter(lead_memory=memory,on_visit_ready=book)
    router.push_frame=AsyncMock()
    context=LLMContext([{'role':'system','content':'facts'},{'role':'user','content':'मैं कल आ जाऊं क्या site visit करने?'}])
    await router.process_frame(LLMContextFrame(context),FrameDirection.DOWNSTREAM)
    assert router.push_frame.call_args.args[0].text == 'Kal kitne baje aana chahenge?'
    context.messages.append({'role':'user','content':'कल दो बजे आ जाऊं'})
    await router.process_frame(LLMContextFrame(context),FrameDirection.DOWNSTREAM)
    book.assert_awaited_once_with(memory['visit_date_iso'],'2:00 PM')
    assert router.push_frame.await_count==1
    assert memory.get('disposition')!='SITE_VISIT_BOOKED'

@pytest.mark.parametrize('text',['Are thank you bye.','ठीक है thank you.'])
def test_call8_farewell(text):assert pure_farewell(text)

def test_requested_slot_can_be_corrected_confirmed_cannot():
    messages=[{'role':'user','content':'Tomorrow at two'},{'role':'user','content':'Actually four PM'}]
    memory={};bot._sync_working_memory(messages,memory)
    assert visit_time(memory['time_slot'])=='4:00 PM'
    memory['disposition']='SITE_VISIT_BOOKED'
    messages.append({'role':'user','content':'five PM'})
    bot._sync_working_memory(messages,memory)
    assert visit_time(memory['time_slot'])=='4:00 PM'

def test_no_hindi_or_hinglish_booking_claim_without_receipt():
    guard=bot._SpokenTextGuard(context=LLMContext([{'role':'user','content':'Kal Kal'}]),lead_memory={'site_visit':'Requested'})
    for line in ('Theek hai kal Saturday ko do baje ke liye site visit book kar deti hoon.','कल दो बजे साइट विजिट बुक कर देती हूँ।'):
        assert 'book kar' not in guard._filter_unverified_claims(line)
        assert 'बुक' not in guard._filter_unverified_claims(line)

def test_lean_prompt_facts_and_no_extra_prewarm():
    import yaml
    from pathlib import Path
    from prompt_builder import build_system_prompt
    cfg=yaml.safe_load((Path(bot.__file__).parent/'config.yaml').read_text())
    prompt,context=build_system_prompt('outbound',cfg,{'customer_name':'Alex'})
    assert len(prompt)<1850
    assert 'including AM or PM' not in prompt
    assert 'one point eight' in prompt
    assert cfg['llm_cache_prewarm_enabled'] is False

def test_caller_slot_guard_understands_devanagari():
    assert bot.caller_slot_matches([{'role':'user','content':'कल दो बजे आ जाऊं'}],{'date':'tomorrow','time':'2:00 PM'})
    assert not bot.caller_slot_matches([{'role':'user','content':'कल आ जाऊं'}],{'date':'tomorrow','time':'2:00 PM'})

async def test_streaming_punctuation_is_not_dropped(monkeypatch):
    from pipecat.frames.frames import TextFrame,LLMFullResponseStartFrame,LLMFullResponseEndFrame
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    guard=bot._SpokenTextGuard(context=LLMContext([{'role':'user','content':'Price?'}]))
    guard.push_frame=AsyncMock()
    await guard.process_frame(LLMFullResponseStartFrame(),FrameDirection.DOWNSTREAM)
    for part in ('That is one point seven crore','.',' What else would you like to know','?'):
        await guard.process_frame(TextFrame(text=part),FrameDirection.DOWNSTREAM)
    await guard.process_frame(LLMFullResponseEndFrame(),FrameDirection.DOWNSTREAM)
    text=''.join(c.args[0].text for c in guard.push_frame.call_args_list if isinstance(c.args[0],TextFrame))
    assert '.' in text and '?' in text

async def test_visit_refusal_does_not_book_from_old_slot(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    book=AsyncMock()
    router=bot._FastPathRouter(lead_memory={'site_visit':'Requested','visit_date_iso':'2026-10-10','time_slot':'2:00 PM'},on_visit_ready=book)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':"I don't want to visit"}])),FrameDirection.DOWNSTREAM)
    book.assert_not_awaited()

async def test_pending_visit_does_not_hijack_property_question(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    book=AsyncMock()
    router=bot._FastPathRouter(lead_memory={'site_visit':'Requested','visit_date_iso':'2026-10-10','time_slot':'2:00 PM'},on_visit_ready=book)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'What about balcony orientation?'}])),FrameDirection.DOWNSTREAM)
    book.assert_not_awaited()
    assert isinstance(router.push_frame.call_args.args[0],LLMContextFrame)

def test_hinglish_midcall_reintro_is_removed():
    guard=bot._SpokenTextGuard();guard._turn_count=10
    assert guard._strip_repeated_opening('Hi Alex main Ananya hoon Meridian Group se Aapka site visit confirm karna hai').startswith('Aapka')

@pytest.mark.parametrize('answer,expected',[('हाँ हाँ।','consent'),('जी हाँ','consent'),('नहीं','declined')])
def test_hindi_location_consent(answer,expected):
    from call_repairs import whatsapp_answer
    assert whatsapp_answer([{'role':'assistant','content':'Shall I send the site visit location on WhatsApp?'},{'role':'user','content':answer}],'location')==expected

def test_explicit_midnight_not_silently_noon():
    assert visit_time('12 AM')=='12:00 AM'

async def test_visit_venue_question_not_booking(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    book=AsyncMock()
    router=bot._FastPathRouter(lead_memory={'site_visit':'Requested','visit_date_iso':'2026-10-10','time_slot':'2:00 PM'},on_visit_ready=book)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Where is the site visit?'}])),FrameDirection.DOWNSTREAM)
    book.assert_not_awaited()
