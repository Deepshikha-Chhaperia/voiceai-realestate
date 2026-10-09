from types import SimpleNamespace
from unittest.mock import AsyncMock
from pathlib import Path
import pytest
import bot
from call_repairs import (whatsapp_answer, brochure_decision, record_manual_whatsapp,
    closing_after_work, pure_farewell, manual_whatsapp_message)
from pipecat.frames.frames import LLMContextFrame, TTSSpeakFrame
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

@pytest.mark.parametrize('answer',['Yeah sure that works.','Yeah yeah please.','Sure, that works.','हाँ ठीक है'])
async def test_actual_yes_routes_once_and_sticks_through_pruning(answer,monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    memory={'_whatsapp_consent_action':'brochure','_brochure_consent_pending':True}
    lines=[]
    async def callback(action):
        line=record_manual_whatsapp(memory,{},action)
        if line:lines.append(line)
    router=bot._FastPathRouter(lead_memory=memory,on_whatsapp_consent=callback)
    router.push_frame=AsyncMock()
    ctx=LLMContext([{'role':'assistant','content':'May I send you the brochure and floor plans on WhatsApp?'},{'role':'user','content':answer}])
    await router.process_frame(LLMContextFrame(ctx),FrameDirection.DOWNSTREAM)
    assert memory['_postcall_whatsapp_actions']==['brochure']
    assert '_brochure_consent_pending' not in memory and '_whatsapp_consent_action' not in memory
    assert len(lines)==1 and 'need team confirmation' in lines[0]
    # A repeated stale consent turn cannot speak/prepare twice.
    await router.process_frame(LLMContextFrame(ctx),FrameDirection.DOWNSTREAM)
    assert len(lines)==1
    messages=[{'role':'system','content':'facts'},{'role':'system','content':'context'},{'role':'user','content':'What about parking?'}]
    bot._sync_working_memory(messages,memory)
    assert 'Manual WhatsApp consent recorded for brochure' in memory['_working_memory']
    assert 'pending channel consent' not in memory['_working_memory']

@pytest.mark.parametrize('text',['Okay ठीक है thank you.','Yeah okay thank you bye-bye.','No no nothing all good.'])
async def test_completed_work_signoff_bypasses_model_and_queues_one_goodbye(text,monkeypatch):
    monkeypatch.setattr(FrameProcessor,'process_frame',AsyncMock())
    c=SimpleNamespace(_ended=False,is_ending=False,_dedicated_goodbye_queued=False,request_ending=lambda:None)
    monkeypatch.setattr(bot.logger,'info',lambda *a,**k:None)
    callback=AsyncMock()
    router=bot._FastPathRouter(lead_memory={'_postcall_whatsapp_actions':['brochure']},on_whatsapp_consent=callback,call_end_coordinator=c)
    router.push_frame=AsyncMock()
    await router.process_frame(LLMContextFrame(LLMContext([{'role':'user','content':text}])),FrameDirection.DOWNSTREAM)
    callback.assert_not_awaited()
    frames=[v.args[0] for v in router.push_frame.call_args_list]
    assert len(frames)==1 and isinstance(frames[0],TTSSpeakFrame) and 'Goodbye' in frames[0].text

@pytest.mark.parametrize('text',['Thanks, what is the price?','Before bye tell me about parking','No no can you tell me the Vastu once','Okay'])
def test_no_accidental_close_of_questions(text):
    assert not closing_after_work(text,{'_postcall_whatsapp_actions':['brochure']})
    assert not pure_farewell(text)


def test_long_location_request_keeps_target_not_all_brochure():
    msgs=[{'role':'assistant','content':'Want the location on WhatsApp after this call?'},{'role':'user','content':'Ah, sure. Can you send me all the details and location and everything?'}]
    assert whatsapp_answer(msgs,'location')=='consent'
    memory={}
    record_manual_whatsapp(memory,{},'location')
    assert memory['_postcall_whatsapp_actions']==['location']
    draft=manual_whatsapp_message(memory,{})
    assert len(draft['blockers'])==1 and 'Location' in draft['blockers'][0]


def test_unrelated_or_negative_reply_does_not_gain_consent():
    for text in ['Yeah, but what is the price?','No thanks','Not now','Yeah dont send it','Before sending tell me about parking']:
        msgs=[{'role':'assistant','content':'May I send brochure on WhatsApp?'},{'role':'user','content':text}]
        assert whatsapp_answer(msgs,'brochure')!='consent'
        assert brochure_decision(msgs,True)!='consent'


def test_helper_no_api_and_existing_sentence_safety_kept():
    source=Path(bot.__file__).read_text()
    assert "{'status':'not_requested'}, properties=FunctionCallResultProperties(run_llm=False)" in source
    assert 'split_idx = _find_sentence_end(self._leading_buffer)' in source
    assert 'queue_postcall_whatsapp' not in source

def test_nonlatin_unrecognized_answer_is_not_vacuous_consent():
    assert brochure_decision([{'role':'assistant','content':'May I send brochure on WhatsApp?'},{'role':'user','content':'ਅੱਛਾ ਠੀਕ ਹੈ'}],True)=='not_requested'


def test_model_cannot_reask_recorded_consent_or_claim_sent():
    guard=bot._SpokenTextGuard(lead_memory={'_postcall_whatsapp_actions':['brochure']})
    guard._in_llm_turn=True
    assert not guard._filter_unverified_claims('May I send you brochure on WhatsApp?').strip()
    assert not guard._filter_unverified_claims("I've sent it on WhatsApp.").strip()
    assert 'parking' in guard._filter_unverified_claims('What about parking?')

def test_nonlatin_question_not_erased_into_closing():
    assert not pure_farewell('bye क्या कीमत है?')
    assert not closing_after_work('Thanks क्या कीमत है?',{'_postcall_whatsapp_actions':['brochure']})
    assert not closing_after_work('Thank you ਪਾਰਕਿੰਗ?',{'_postcall_whatsapp_actions':['brochure']})
