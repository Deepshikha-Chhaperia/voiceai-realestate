import asyncio
from unittest.mock import AsyncMock
from types import SimpleNamespace
import pytest
import bot
from call_repairs import pure_farewell,faq_key
from pipecat.frames.frames import LLMContextFrame,TTSSpeakFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor,FrameDirection

@pytest.mark.parametrize('text',['No no thank you bye bye.','No, thanks, goodbye','Nahi thank you bye','Bye bye.'])
def test_refusal_signoff_is_not_objection(text):
    assert pure_farewell(text)

@pytest.mark.parametrize('text',['No bye yet, tell me price','Before I say bye, can I come?','No','Not interested'])
def test_mixed_or_simple_refusal_not_signoff(text):
    assert not pure_farewell(text)

async def test_grace_timer_must_not_cancel_itself():
    completed=asyncio.Event()
    async def hangup():
        await asyncio.sleep(.01)
        completed.set()
    c=bot._CallEndCoordinator(stream_id='c',on_hangup=hangup,grace_seconds=.01)
    c._grace_task=asyncio.create_task(c._run_grace_timeout())
    await c._grace_task
    assert completed.is_set() and c._ended

async def test_safety_timer_must_finish_after_await():
    done=asyncio.Event()
    async def hangup():
        await asyncio.sleep(.01);done.set()
    c=bot._CallEndCoordinator(stream_id='c',on_hangup=hangup,safety_seconds=.01)
    c._safety_task=asyncio.create_task(c._run_safety_timeout())
    await c._safety_task
    assert done.is_set()

async def test_repeated_ending_no_new_timers():
    c=bot._CallEndCoordinator(stream_id='c',on_hangup=AsyncMock())
    c.request_ending()
    old=c._safety_task
    c.request_ending()
    assert c._safety_task is old
    await c._finish()
    c.request_ending()
    assert c._safety_task is None

async def test_closed_router_cannot_emit_second_goodbye(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(_ended=True)
    r=bot._FastPathRouter(call_end_coordinator=c)
    r.push_frame=AsyncMock()
    await r.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'Bye bye.'}])),FrameDirection.DOWNSTREAM)
    r.push_frame.assert_not_awaited()

async def test_no_no_goodbye_never_goes_to_groq(monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(request_ending=lambda:None,_ended=False)
    r=bot._FastPathRouter(call_end_coordinator=c)
    r.push_frame=AsyncMock()
    await r.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':'No no thank you bye bye.'}])),FrameDirection.DOWNSTREAM)
    frames=[x.args[0] for x in r.push_frame.call_args_list]
    assert len(frames)==1 and isinstance(frames[0],TTSSpeakFrame)

async def test_existing_database_gets_additive_table(monkeypatch,tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    import leads.db as db
    engine=create_async_engine('sqlite+aiosqlite:///'+str(tmp_path/'existing.db'))
    monkeypatch.setattr(db,'get_engine',lambda:engine)
    async with engine.begin() as c:
        await c.execute(text('CREATE TABLE sentinel (value TEXT)'))
        await c.execute(text("INSERT INTO sentinel VALUES ('keep')"))
    await db.ensure_call_sheet_schema();await db.ensure_call_sheet_schema()
    async with engine.connect() as c:
        assert (await c.execute(text('SELECT value FROM sentinel'))).scalar()=='keep'
        assert (await c.execute(text('SELECT COUNT(*) FROM call_sheet_exports'))).scalar()==0
    await engine.dispose()

async def test_worker_schema_failure_does_not_launch_loop(monkeypatch):
    import leads.db as db
    import leads.outbox as ob
    monkeypatch.setattr(ob,'_outbox_worker_running',False)
    monkeypatch.setattr(ob,'_outbox_worker_task',None)
    monkeypatch.setattr(db,'ensure_call_sheet_schema',AsyncMock(side_effect=RuntimeError('schema blocked')))
    with pytest.raises(RuntimeError,match='schema blocked'):await ob.start_outbox_worker()
    assert not ob._outbox_worker_running and ob._outbox_worker_task is None

def test_clear_amenities_question_route():
    assert faq_key('Can you tell me about the amenities?')=='faq_amenities'
    assert faq_key('Can you tell me about the amenities and prices?') is None

def test_natural_sentences_not_word_limit():
    import yaml
    from pathlib import Path
    cfg=yaml.safe_load((Path(bot.__file__).parent/'config.yaml').read_text())
    assert 'short complete sentences' in cfg['voice_rules']
    assert 'BHK chosen:' not in cfg['voice_rules']
    assert 'about 20 words' not in cfg['voice_rules']

async def test_reciprocal_bye_does_not_reset_grace_or_end_early(monkeypatch):
    from pipecat.frames.frames import TranscriptionFrame
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    hangup=AsyncMock()
    c=bot._CallEndCoordinator(stream_id='c',on_hangup=hangup,grace_seconds=.06)
    c.push_frame=AsyncMock()
    c._in_grace=True
    c._grace_task=asyncio.create_task(c._run_grace_timeout())
    timer=c._grace_task
    await c.process_frame(TranscriptionFrame(text='Thank you, bye bye.',user_id='',timestamp=''),FrameDirection.DOWNSTREAM)
    assert c._grace_task is timer
    hangup.assert_not_awaited()
    await timer
    hangup.assert_awaited_once()

def test_startup_no_local_leads_shadowing():
    import ast
    from pathlib import Path
    tree=ast.parse((Path(bot.__file__).parent/'main.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='lifespan')
    for n in ast.walk(fn):
        if isinstance(n,ast.Import):
            assert not any(a.name.startswith('leads.') and a.asname is None for a in n.names)

@pytest.mark.parametrize('utterance,key', [('Can you tell me about the amenities?','faq_amenities_v2'),("Yeah I'm looking for a 3 BHK.",'faq_3bhk_options'),('The larger one.','faq_3bhk_large')])
def test_campaign_single_intent_fast_routes(utterance,key):
    import yaml
    from pathlib import Path
    from call_repairs import campaign_faq,CALL5_TEXTS
    cfg=yaml.safe_load((Path(bot.__file__).parent/'config.yaml').read_text())
    match=campaign_faq(utterance,{'configuration':'3 BHK'},cfg)
    assert match and match[0]==key and match[1]==CALL5_TEXTS[key]
    assert bot._match_cached_phrase(match[1])==key

@pytest.mark.parametrize('utterance',['3 BHK but do you have discounts?','The larger one and tell me possession','Is it not Vastu compliant?','Maybe visit tomorrow, tell me Vastu too'])
def test_multi_intent_or_negative_stays_smart(utterance):
    import yaml
    from pathlib import Path
    from call_repairs import campaign_faq
    cfg=yaml.safe_load((Path(bot.__file__).parent/'config.yaml').read_text())
    assert campaign_faq(utterance,{'configuration':'3 BHK'},cfg) is None

def test_different_campaign_cannot_use_meridian_facts():
    from call_repairs import campaign_faq
    assert campaign_faq('Can you tell me about the amenities?',{}, {'real_estate_sales_script':'Other property: pool'}) is None

async def test_faq_text_bypasses_groq_without_audio(monkeypatch):
    import yaml
    from pathlib import Path
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    cfg=yaml.safe_load((Path(bot.__file__).parent/'config.yaml').read_text())
    r=bot._FastPathRouter(config=cfg)
    r.push_frame=AsyncMock()
    await r.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':"Yeah I'm looking for a 3 BHK."}])),FrameDirection.DOWNSTREAM)
    assert not any(isinstance(c.args[0],LLMContextFrame) for c in r.push_frame.call_args_list)
    assert any(isinstance(c.args[0],TTSSpeakFrame) for c in r.push_frame.call_args_list)

def test_first_turn_affirmative_does_not_eat_property_request():
    assert not bot._is_name_confirmation_affirmative("Yeah I'm looking for a 3 BHK.")
    assert bot._is_name_confirmation_affirmative('Yes speaking.')

async def test_legacy_termination_defers_to_coordinator(monkeypatch):
    from pipecat.frames.frames import TextFrame,BotStoppedSpeakingFrame
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(is_ending=True)
    p=bot._TerminationProcessor(stream_id='c',on_hangup=AsyncMock(),force_hangup_fn=AsyncMock())
    p._call_end_coordinator=c
    p.push_frame=AsyncMock()
    await p.process_frame(TextFrame(text='Thank you. Goodbye!'),FrameDirection.DOWNSTREAM)
    await p.process_frame(BotStoppedSpeakingFrame(),FrameDirection.DOWNSTREAM)
    assert p._hangup_task is None and p._safety_task is None
