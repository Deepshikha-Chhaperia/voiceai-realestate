import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
import pytest
import bot
from call_repairs import whatsapp_answer, safe_tts_text, fragment_transcript, CALL7_TEXTS
from audio_provenance import effective_config
from pipecat.frames.frames import LLMContextFrame, TTSSpeakFrame, TranscriptionFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

@pytest.mark.parametrize('text',['Yes, sure.','Yes please','Sure','Okay'])
async def test_location_consent_bypasses_dead_groq(text,monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    callback=AsyncMock()
    router=bot._FastPathRouter(lead_memory={'_whatsapp_consent_action':'location'},on_whatsapp_consent=callback)
    router.push_frame=AsyncMock()
    frame=LLMContextFrame(LLMContext([{'role':'assistant','content':'Site visit confirmed. Shall I send the site visit location on WhatsApp?'},{'role':'user','content':text}]))
    await router.process_frame(frame,FrameDirection.DOWNSTREAM)
    callback.assert_awaited_once_with('location')
    router.push_frame.assert_not_awaited()

async def test_direct_brochure_request_dispatches_without_llm(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    callback=AsyncMock()
    router=bot._FastPathRouter(on_whatsapp_consent=callback)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Send brochure on WhatsApp'}])),FrameDirection.DOWNSTREAM)
    callback.assert_awaited_once_with('brochure')
    router.push_frame.assert_not_awaited()

@pytest.mark.parametrize('previous,pending',[('What time works?', 'location'),('WhatsApp brochure mentioned.', 'brochure'),('Shall I send location on WhatsApp?', None)])
def test_no_unrelated_yes_authority(previous,pending):
    assert whatsapp_answer([{'role':'assistant','content':previous},{'role':'user','content':'Yes sure'}],pending) is None

async def test_no_cancels_specific_location_question(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    memory={'_whatsapp_consent_action':'location'}
    callback=AsyncMock()
    router=bot._FastPathRouter(lead_memory=memory,on_whatsapp_consent=callback)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'assistant','content':'Shall I send location on WhatsApp?'},{'role':'user','content':'No thanks'}])),FrameDirection.DOWNSTREAM)
    callback.assert_not_awaited()
    assert '_whatsapp_consent_action' not in memory

@pytest.mark.parametrize('text',['"','...','!!!',' ', '" "'])
def test_nonlinguistic_tts_not_sent(text):
    assert not safe_tts_text(text)

@pytest.mark.parametrize('text',['No.','11:00.','2 PM','Ji bilkul.','हाँ ठीक है','తెలుగు'])
def test_meaningful_short_text_preserved(text):
    assert safe_tts_text(text)
    assert not fragment_transcript(text)

async def test_guard_at_actual_aggregated_sarvam_input():
    seen=[]
    async def synth(text,cid):
        seen.append(text)
        yield 'audio'
    service=SimpleNamespace(run_tts=synth)
    bot.attach_safe_tts(service)
    assert [x async for x in service.run_tts('"','c')] == []
    assert [x async for x in service.run_tts('"Hello."','c')] == ['audio']
    assert seen == ['Hello.']

@pytest.mark.parametrize('text',["C.","I'm.","I said I."])
async def test_final_fragment_not_entered_into_context(text,monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    tap=bot._TranscriptionTap()
    tap.push_frame=AsyncMock()
    frame=TranscriptionFrame(text=text,user_id='',timestamp='')
    await tap.process_frame(frame,FrameDirection.DOWNSTREAM)
    assert frame.text == ''

def test_stt_auto_detect_not_forced_english(monkeypatch):
    monkeypatch.setenv('SARVAM_API_KEY','fake')
    cfg=effective_config(Path(bot.__file__).parent)
    stt=bot.ServiceFactory.create('stt','sarvam',cfg)
    assert stt._get_language_string()=='unknown'
    assert stt._mode=='codemix'

@pytest.mark.parametrize('key',list(CALL7_TEXTS))
def test_new_cache_phrases_exact_and_versioned(key):
    assert bot._match_cached_phrase(CALL7_TEXTS[key])==key
    assert bot._match_cached_phrase(CALL7_TEXTS[key]+' Unrelated extra facts.') is None

@pytest.mark.parametrize('range_text',["Calls!A22:Q22","'Calls'!A22:Q22","'Call Logs'!A22:Q22"])
def test_sheets_worksheet_local_readback(monkeypatch,range_text):
    import google_sheets_export as sheets
    ws=MagicMock()
    ws.row_values.return_value=sheets.COLUMNS
    ws.col_values.return_value=['call_id']
    ws.append_row.return_value={'updates':{'updatedRange':range_text}}
    ws.get.return_value=[['call1']]
    client=MagicMock()
    client.open_by_key.return_value.worksheet.return_value=ws
    monkeypatch.setattr(sheets,'_get_client',lambda:client)
    receipt=sheets.append_call_row_verified({'call_id':'call1'})
    ws.get.assert_called_once_with('A22:Q22')
    assert receipt['range']==range_text and receipt['verified']

async def test_backup_404_attempted_once_not_every_turn(monkeypatch):
    import test_failover_backup as fixtures
    service,calls,cfg=fixtures._make(monkeypatch,'429','ok')
    class Missing(Exception):status_code=404
    async def fake(**params):
        calls.append(params)
        if params['model']==cfg['providers']['llm']['groq']['params']['model']:
            raise fixtures._RateLimit()
        raise Missing('model not found')
    service._client.chat.completions.create=fake
    first=await fixtures._collect(service)
    second=await fixtures._collect(service)
    third=await fixtures._collect(service)
    assert first != second and third == ''
    assert len(calls)==2
    assert service._backup_unavailable is True

def test_small_two_bhk_does_not_become_three_bhk():
    messages=[{'role':'system','content':'facts'},{'role':'system','content':'context'}, {'role':'user','content':'I want 2 BHK'},{'role':'assistant','content':'Which size?'},{'role':'user','content':'A smaller one.'}]
    memory={}
    bot._sync_working_memory(messages,memory)
    assert memory['configuration']=='2 BHK'
    assert memory['unit_size_choice']=='small'
    assert '3 BHK' not in memory['_working_memory']

async def test_location_queue_once_exact_payload_no_send(monkeypatch):
    from contextlib import asynccontextmanager
    import leads.db as db
    import leads.outbox as outbox
    monkeypatch.setattr(bot, 'whatsapp_ready', lambda: True)
    visit=SimpleNamespace(lead_id='lead',visit_date_iso='2026-10-10',time_slot='2:00 PM')
    session=SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda:SimpleNamespace(first=lambda:visit))),get=AsyncMock(return_value=SimpleNamespace(phone='+919876543210',name='Alex',id='lead')),commit=AsyncMock())
    @asynccontextmanager
    async def get_session():yield session
    queue=AsyncMock(return_value=SimpleNamespace(status='pending',payload={},last_error=None,id='out1'))
    monkeypatch.setattr(db,'get_session',get_session)
    monkeypatch.setattr(outbox,'queue_outbox_item',queue)
    result=await bot.queue_consented_location('call1')
    assert result['status']=='queued'
    args=queue.call_args.args
    assert args[1]=='whatsapp' and args[2]['call_id']=='call1'
    assert args[2]['visit_date_iso']=='2026-10-10'
    assert 'brochure_url' not in args[2]
    assert visit.whatsapp_opt_in is True
    session.commit.assert_awaited_once()

def test_working_memory_has_current_unit_not_all_historical_units():
    messages=[{'role':'system','content':'static'},{'role':'system','content':'context'},{'role':'user','content':'2 BHK'},{'role':'user','content':'Tell me about 3 BHK'},{'role':'user','content':'larger one'}]
    memory={}
    bot._sync_working_memory(messages,memory)
    assert memory['configuration'].startswith('3 BHK Large')
    state=memory['_working_memory']
    assert 'Configuration confirmed (2 BHK)' not in state
    assert 'Looking for:' not in state
    assert len(state)<500

async def test_recovery_ack_does_not_request_groq(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    router=bot._FastPathRouter()
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'assistant','content':"I'm having connection trouble. Please give me a moment."},{'role':'user','content':'Okay.'}])),FrameDirection.DOWNSTREAM)
    router.push_frame.assert_not_awaited()

async def test_actual_safe_tts_request_arms_first_audio_watch():
    from types import SimpleNamespace
    async def original(text, context_id):
        yield text
    state=SimpleNamespace(arm=Mock())
    tts=SimpleNamespace(run_tts=original)
    bot.attach_safe_tts(tts, stall_state=state)
    assert [s async for s in tts.run_tts('Valid question?', 'ctx')]==['Valid question?']
    state.arm.assert_called_once()
    assert [s async for s in tts.run_tts('"', 'empty')]==[]
    state.arm.assert_called_once()

def test_pending_location_does_not_authorize_yes_to_other_whatsapp_question():
    assert whatsapp_answer([{'role':'assistant','content':'Do you use WhatsApp?'},{'role':'user','content':'Yes sure'}],'location') is None

async def test_call7_exact_date_only_phrase_bypasses_groq(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    router=bot._FastPathRouter()
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Come visit the property tomorrow.'}])),FrameDirection.DOWNSTREAM)
    sent=router.push_frame.call_args.args[0]
    assert isinstance(sent,TTSSpeakFrame)
    assert 'time' in sent.text and 'AM or PM' not in sent.text
