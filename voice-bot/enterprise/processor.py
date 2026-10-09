"""Passive intent evidence before normal user flow; unknown callers never get junk timers."""
import asyncio
import time
from pipecat.processors.frame_processor import FrameProcessor,FrameDirection
from pipecat.frames.frames import TranscriptionFrame,EndFrame,CancelFrame,TTSSpeakFrame
from enterprise.intent import classify,human_requested
from enterprise.config import policy
from enterprise import store


class IntentGate(FrameProcessor):
    def __init__(self,call_id,memory,config,hangup,handoff,ending):
        super().__init__();self.call_id=call_id;self.memory=memory;self.config=config
        self.hangup=hangup;self.handoff=handoff;self.ending=ending;self.started=time.monotonic()
        self._monitor=None;self._task=None;self._handoff_task=None;self.intent='unknown';self._window_closed=False
    def bind_task(self,task):
        self._task=task
        async def monitor():
            await asyncio.sleep(10)
            self._window_closed=True
            self.memory['_early_intent']=self.intent
            # Intent unknown at10s is an honest result, not grounds for rejecting buyer.
            if self.intent in {'broker','job','wrong_number'} and policy(self.config).get('enabled'):
                p=policy(self.config)
                if self.intent in {'broker','job'} and p.get('routing',{}).get('broker_job_policy')=='route':
                    await self.handoff(self.intent)
                    return
                if self.intent in {'broker','job'} and p.get('routing',{}).get('broker_job_policy')!='close':return
                await asyncio.sleep(max(0,60-(time.monotonic()-self.started)))
                if self.intent not in {'broker','job','wrong_number'} or self.ending():return
                if self._task:
                    await self._task.queue_frames([TTSSpeakFrame(text='Thank you for calling. Goodbye!',append_to_context=False)])
                    await asyncio.sleep(2)
                if not self.ending():await self.hangup('nonbuyer_intent_'+self.intent)
        self._monitor=asyncio.create_task(monitor())
    async def process_frame(self,frame,direction):
        await super().process_frame(frame,direction)
        if isinstance(frame,(EndFrame,CancelFrame)):
            if self._monitor:self._monitor.cancel()
            # Handoff is a bounded side effect; let its result reconcile even after stream closes.
        if direction==FrameDirection.DOWNSTREAM and isinstance(frame,TranscriptionFrame) and frame.text.strip():
            intent=classify(frame.text)
            if intent!='unknown' and (not self._window_closed or intent=='buyer'):
                self.intent=intent;self.memory['_caller_intent']=intent
            if human_requested(frame.text) and not self.ending() and not self._handoff_task:
                self._handoff_task=asyncio.create_task(self.handoff('caller_requested'))
        await self.push_frame(frame,direction)
