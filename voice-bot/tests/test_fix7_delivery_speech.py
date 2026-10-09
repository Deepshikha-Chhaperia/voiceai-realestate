import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
import yaml
import bot
import local_test_report
from call_repairs import postcall_whatsapp_plan, manual_whatsapp_message
from pipecat.frames.frames import (TextFrame, LLMFullResponseStartFrame,
    LLMFullResponseEndFrame, UserStartedSpeakingFrame, InterruptionFrame, TranscriptionFrame)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.processors.aggregators.llm_context import LLMContext

@pytest.fixture
def config():
    return {'brochure_delivery': {'brochure_url':'https://example.test/brochure',
        'floorplan_url':'https://example.test/plans','postcall_location_url':'https://example.test/location'}}

def test_private_plan_needs_no_meta_or_template(config, monkeypatch):
    monkeypatch.delenv('WHATSAPP_ACCESS_TOKEN', raising=False)
    monkeypatch.delenv('WHATSAPP_PHONE_NUMBER_ID', raising=False)
    memory={}
    p,e=postcall_whatsapp_plan(memory,config,'brochure')
    assert p['status']=='prepared_not_sent' and e is None
    memory['_postcall_whatsapp_actions']=p['actions']
    p,e=postcall_whatsapp_plan(memory,config,'location')
    assert p['actions']==['brochure','location']
    memory.update(_postcall_whatsapp_actions=p['actions'],site_visit='Confirmed (2026-10-10 at 2 PM)',
        visit_date_iso='2026-10-10',time_slot='2 PM')
    output=manual_whatsapp_message(memory,config)
    assert output['text'].count('Brochure:')==1
    assert 'Location:' in output['text'] and '2026-10-10 at 2 PM' in output['text']
    assert not output['blockers']

def test_specific_consent_and_pending_booking_do_not_expand(config):
    memory={'_postcall_whatsapp_actions':['location'],'site_visit':'Requested (tomorrow)',
        'visit_date_iso':'2026-10-10','time_slot':'2 PM'}
    output=manual_whatsapp_message(memory,config)
    assert 'Location:' in output['text'] and 'Brochure:' not in output['text']
    assert 'booked' not in output['text']

def test_missing_links_are_blockers_not_invented_message():
    output=manual_whatsapp_message({'_postcall_whatsapp_actions':['brochure','location']},{})
    assert len(output['blockers'])==3 and output['status']=='needs_configuration'
    assert 'http' not in output['text']

def test_report_copyable_text_and_missing_link_blocker(tmp_path,monkeypatch,config):
    monkeypatch.setattr(local_test_report,'REPORTS_DIR',tmp_path)
    monkeypatch.setattr(local_test_report.lead_state,'get_call',lambda _: {})
    monkeypatch.setattr(local_test_report.lead_state,'infer_deterministic_disposition',lambda *a:'INCOMPLETE')
    memory={'_postcall_whatsapp_actions':['brochure','location']}
    memory['_manual_whatsapp_output']=manual_whatsapp_message(memory,config)
    path=local_test_report.write_report('call-123',[{'role':'user','content':'yes'}],lead_memory=memory)
    assert path and 'Not sent' in Path(path).read_text()
    assert (tmp_path/'call-123_whatsapp.txt').read_text()==memory['_manual_whatsapp_output']['text']
    memory['_manual_whatsapp_output']=manual_whatsapp_message(memory,{})
    path=local_test_report.write_report('call-456',[{'role':'user','content':'yes'}],lead_memory=memory)
    assert 'DO NOT FORWARD YET' in Path(path).read_text()
    assert not (tmp_path/'call-456_whatsapp.txt').exists()

async def stream(parts,monkeypatch,truncated=False):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    ctx=LLMContext([])
    g=bot._SpokenTextGuard(context=ctx)
    g.push_frame=AsyncMock()
    await g.process_frame(LLMFullResponseStartFrame(),FrameDirection.DOWNSTREAM)
    for part in parts:
        await g.process_frame(TextFrame(part),FrameDirection.DOWNSTREAM)
    ctx._response_truncated=truncated
    await g.process_frame(LLMFullResponseEndFrame(),FrameDirection.DOWNSTREAM)
    return ''.join(c.args[0].text for c in g.push_frame.call_args_list if isinstance(c.args[0],TextFrame))

async def test_complete_whatsapp_question_survives_token_boundaries(monkeypatch):
    out=await stream(['Would you like the brochure and floor plans on ', 'WhatsApp', '?'],monkeypatch)
    assert out=='Would you like the brochure and floor plans on WhatsApp?'

async def test_false_past_send_prefix_never_escapes(monkeypatch):
    out=await stream(["I've shared the brochure",' and floor plans on ','WhatsApp','. Check your phone.'],monkeypatch)
    assert not out.strip()

async def test_length_limited_tail_not_spoken(monkeypatch):
    out=await stream(['The homes have balconies.', ' Would you like me to share'],monkeypatch,True)
    assert out=='The homes have balconies.'

async def test_grace_speech_start_pauses_once_but_real_question_has_one_reply(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=bot._CallEndCoordinator(stream_id='t',on_hangup=AsyncMock(),grace_seconds=.03,safety_seconds=.4)
    c.push_frame=AsyncMock();c._in_grace=True
    c._grace_task=asyncio.create_task(c._run_grace_timeout())
    await c.process_frame(UserStartedSpeakingFrame(),FrameDirection.DOWNSTREAM)
    task=c._grace_task
    await c.process_frame(InterruptionFrame(),FrameDirection.DOWNSTREAM)
    assert c._grace_task is task
    await asyncio.sleep(.05)
    assert not c._ended
    await c.process_frame(TranscriptionFrame('Wait, what about parking?','caller',''),FrameDirection.DOWNSTREAM)
    assert c._extra_reply_count==1 and c._awaiting_closing
    c._cancel_task('_grace_task');c._cancel_task('_safety_task')


def test_manual_teardown_never_queues_a_whatsapp_job():
    source=Path(bot.__file__).read_text()
    assert 'queue_postcall_whatsapp' not in source and 'dispatch_call_summary' not in source
    root=Path(bot.__file__).parent
    for f in ('config.yaml','profiles/india.yaml'):
        cfg=yaml.safe_load((root/f).read_text())
        assert cfg['whatsapp_after_call_only'] is True and cfg['postcall_whatsapp_enabled'] is False
        assert cfg['hangup_grace_seconds']==3.0

async def test_length_tail_after_multiple_sentences_in_one_chunk(monkeypatch):
    out=await stream(['A complete answer. Another full sentence. A cut fragment'],monkeypatch,True)
    assert 'Another full sentence.' in out and 'cut fragment' not in out

def test_missing_links_do_not_make_spoken_send_promise():
    plan,error=postcall_whatsapp_plan({}, {}, 'brochure')
    assert plan['actions']==['brochure']
    assert 'confirm the links' in error and 'will WhatsApp' not in error
