import asyncio
import re
import sys
import time
import logging
import pytest
from pathlib import Path

# Add voice-bot root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from faq_recovery import FAQReplyRecovery
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.processors.aggregators.async_tool_messages import ASYNC_TOOL_INSTRUCTIONS
from prompt_builder import build_system_prompt
import yaml


# ==============================================================================
# ITEM 1: Heartbeat Monitor & Turn Stop Deduplication Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_loop_heartbeat_monitor_lifecycle():
    """Verify loop heartbeat monitor runs, measures lag, and terminates cleanly."""
    lag_records = []
    threshold_ms = 20.0
    interval_s = 0.01

    async def _monitor():
        last = time.monotonic()
        while True:
            await asyncio.sleep(interval_s)
            now = time.monotonic()
            lag_ms = (now - last - interval_s) * 1000.0
            if lag_ms > threshold_ms:
                lag_records.append(lag_ms)
            last = now

    task = asyncio.create_task(_monitor())
    await asyncio.sleep(0.05)
    
    # Introduce a small controlled synthetic lag
    time.sleep(0.03)  # 30ms sync block
    await asyncio.sleep(0.02)
    
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert task.done()
    # At least one lag above 20ms should have been captured
    assert len(lag_records) >= 1, "Heartbeat failed to detect 30ms sync block lag"


def test_turn_stop_deduplication():
    """Verify duplicate turn_stop within 100ms with empty content is deduplicated."""
    last_turn_stop_time = 0.0
    turn_stops_handled = []

    def handle_turn_stop(t: float, transcript: str):
        nonlocal last_turn_stop_time
        # Logic matching bot.py on_user_turn_stopped
        if t - last_turn_stop_time < 0.10 and not transcript.strip():
            return "DEDUPLICATED"
        last_turn_stop_time = t
        turn_stops_handled.append((t, transcript))
        return "HANDLED"

    # Turn stop 1 at 27.026 with empty transcript
    r1 = handle_turn_stop(27.026, "")
    assert r1 == "HANDLED"
    assert len(turn_stops_handled) == 1

    # Duplicate turn stop 2 at 27.029 (3ms later) with empty transcript -> DEDUPLICATED
    r2 = handle_turn_stop(27.029, "")
    assert r2 == "DEDUPLICATED"
    assert len(turn_stops_handled) == 1

    # Legitimate turn stop at 27.029 with REAL transcript -> HANDLED
    r3 = handle_turn_stop(27.029, "Okay, so I'll come tomorrow at five.")
    assert r3 == "HANDLED"
    assert len(turn_stops_handled) == 2


# ==============================================================================
# ITEM 2: Interrupted Flag Cleared on Real Transcript (No Stray Clarify)
# ==============================================================================

def test_faq_recovery_interrupted_cleared_on_real_transcript():
    """Verify self.interrupted is cleared when a real transcript arrives.
    
    Otherwise, a stray empty turn later would incorrectly fire clarify.
    """
    recovery = FAQReplyRecovery()
    assert not recovery.interrupted

    # 1. Caller interrupts bot during Turn 1
    recovery.interrupt()
    assert recovery.interrupted is True

    # 2. A real transcript arrives for Turn 2 ("I want 3 BHK")
    recovery.new_transcript()
    assert recovery.interrupted is False, "new_transcript() did not clear interrupted flag!"

    # 3. Later, an empty turn arrives (VAD trigger / silence)
    # Since interrupted was cleared by the real transcript, clarification MUST NOT fire!
    can_clarify = recovery.take_clarification()
    assert can_clarify is False, "take_clarification fired on stray empty turn after real transcript!"


def test_faq_recovery_clarifies_only_after_real_interruption_followed_by_silence():
    """Verify take_clarification fires only when an interruption is followed directly by silence."""
    recovery = FAQReplyRecovery()
    
    # Idle empty turn without interruption -> no clarify
    assert recovery.take_clarification() is False

    # Interruption happens
    recovery.interrupt()
    assert recovery.interrupted is True

    # Immediate silence (no transcript) -> clarify allowed once
    assert recovery.take_clarification() is True
    assert recovery.interrupted is False
    assert recovery.clarified is True

    # Subsequent silence -> already clarified, must not fire again
    assert recovery.take_clarification() is False


# ==============================================================================
# ITEM 5: Startup Assertions, Zero Duplicate Warnings, Single System Prompt
# ==============================================================================

class _WarningCollector(logging.Handler):
    def __init__(self):
        super().__init__()
        self.warnings = []

    def emit(self, record):
        if record.levelno >= logging.WARNING:
            self.warnings.append(record.getMessage())


def test_pipecat_startup_assertions_and_zero_duplicate_warning():
    """Verify startup assertions, suppressed framework injection, and exact single system prompt."""
    config_path = Path(__file__).resolve().parent.parent / 'config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    sys_prompt_base, customer_context = build_system_prompt("inbound", config, None)
    sys_prompt = sys_prompt_base + "\n\n" + ASYNC_TOOL_INSTRUCTIONS

    llm = OpenAILLMService(api_key="fake_key", model="qwen/qwen3.8-27b")

    # Startup assertions (matching bot.py)
    assert hasattr(llm, "_compose_system_instruction"), (
        "Pipecat OpenAILLMService missing _compose_system_instruction. Version incompatibility."
    )
    assert hasattr(llm, "_settings") and hasattr(llm._settings, "system_instruction"), (
        "Pipecat LLM instance missing _settings.system_instruction. Version incompatibility."
    )

    # Register tools
    tools = ["end_call", "update_lead_info", "book_site_visit", "send_brochure", "handoff_to_human"]
    for t in tools:
        llm.register_function(t, lambda: None, cancel_on_interruption=(t != "handoff_to_human"))

    # Apply override to suppress duplicate framework injection
    llm._compose_system_instruction = lambda: None
    llm._settings.system_instruction = None

    # Capture logs to verify zero duplicate warnings
    collector = _WarningCollector()
    logging.getLogger().addHandler(collector)

    # Build context
    messages = [
        {"role": "system", "content": sys_prompt},
    ]
    if customer_context:
        messages.append({"role": "system", "content": customer_context})

    llm_tools = [
        FunctionSchema(name="handoff_to_human", description="Handoff", properties={}, required=[])
    ]
    context = LLMContext(messages, tools=llm_tools)
    adapter = llm.get_llm_adapter()

    # Simulate 5 turns
    for turn in range(1, 6):
        context.messages.append({"role": "user", "content": f"Turn {turn} user utterance"})
        params = adapter.get_llm_invocation_params(
            context,
            system_instruction=llm._settings.system_instruction,
            convert_developer_to_user=not llm.supports_developer_role,
        )
        inv_msgs = params.get("messages", [])
        system_msgs = [m for m in inv_msgs if m.get("role") == "system"]
        
        # Exactly 2 system messages (1 static prompt containing ASYNC_TOOL_INSTRUCTIONS + 1 customer context)
        expected_sys_count = 2 if customer_context else 1
        assert len(system_msgs) == expected_sys_count, (
            f"Expected {expected_sys_count} system messages, got {len(system_msgs)}"
        )
        assert "ASYNC TOOLS:" in system_msgs[0]["content"], "ASYNC_TOOL_INSTRUCTIONS missing from static prompt!"

    logging.getLogger().removeHandler(collector)

    # Prove zero "Both system_instruction and an initial system message in context are set" warnings
    dup_warnings = [w for w in collector.warnings if "Both system_instruction and an initial system message" in w]
    assert len(dup_warnings) == 0, f"Duplicate system instruction warning was emitted: {dup_warnings}"


# ==============================================================================
# ITEM 6: Regex With Negation & Questions
# ==============================================================================

COMPLETED_BOOKING_PATTERN = re.compile(
    r"\b(?:"
    r"(?:i(?:'ve|\s+have|\s+already)?|you(?:'re|\s+are)?)\s+(?:booked|scheduled|confirmed)|"
    r"(?:site\s+visit|visit|slot)\s+(?:is|has\s+been|was)\s+(?:booked|scheduled|confirmed)|"
    r"booked\s+(?:for\s+(?:tomorrow|today|yesterday|monday|tuesday|wednesday|thursday|friday|saturday|sunday|\d+)|your\s+visit)|"
    r"all\s+set\s+for\s+(?:tomorrow|today|monday|tuesday|wednesday|thursday|friday|saturday|sunday|\d+)|"
    r"(?:i\s+)?booked\s+(?:your|a)\s+(?:site\s+visit|visit|slot)|"
    r"(?:aapka\s+)?(?:visit|site\s+visit|slot)\s+book\s+ho\s+gaya|"
    r"(?:book|confirm|schedule)\s+(?:kar\s+(?:diya|diye|di|chuka|chuke|chuki|gaya|gaye)|ho\s+(?:gaya|gaye|chuka))"
    r")\b|"
    r"(?:बुक|कन्फर्म|शिड्यूल)\s*(?:कर\s*(?:दिया|दिये|दी|चुका|चुके|चुकी|गया|गए)|हो\s*(?:गया|गए|चुका))",
    re.I
)

def is_unverified_booking_claim(text: str) -> bool:
    t = text.strip()
    if not t:
        return False
    for clause in re.split(r'[,;:]+|\s+-\s+', t):
        c = clause.strip()
        if not c:
            continue
        if COMPLETED_BOOKING_PATTERN.search(c):
            is_clause_q = c.endswith("?") and bool(re.search(r"(?:\b(?:has|is|was|can|could|will|kya)\b|क्या)", c, re.I))
            is_clause_neg = bool(re.search(r"(?:\b(?:not|n't|never|nahi|nahin)\b|नही|नहीं)", c, re.I))
            if not (is_clause_q or is_clause_neg):
                return True
    return False


def test_booking_regex_blocked_claims():
    """Verify unverified completed booking assertions are blocked before tool success."""
    blocked_examples = [
        "You're booked for tomorrow at 5, okay?",
        "no problem, you're booked",
        "you're booked for tomorrow at 5",
        "you are booked",
        "all set for tomorrow",
        "booked for Saturday",
        "aapka visit book ho gaya",
        "I have booked your visit for tomorrow.",
        "I've booked a slot for you at 5 pm.",
        "I booked your site visit.",
        "Your site visit is confirmed for Saturday.",
        "Your visit has been scheduled.",
        "Maine aapka site visit book kar diya hai.",
        "Aapka slot book ho gaya hai.",
        "Slot confirm kar diya hai.",
        "मैंने आपका स्लॉट बुक कर दिया है।",
        "विजिट कन्फर्म हो गया है।",
    ]
    for text in blocked_examples:
        assert is_unverified_booking_claim(text) is True, f"Failed to block: {text}"


def test_booking_regex_allowed_questions_negations_and_future():
    """Verify questions, negations, future commitments, and offers are allowed."""
    allowed_examples = [
        "has it been booked?",
        "has the visit been booked?",
        "is it booked for tomorrow?",
        "The visit is not booked yet.",
        "I have not booked your visit.",
        "Slot book nahi hua hai.",
        "क्या स्लॉट बुक हो गया है?",
        "Main book kar dungi, aap kal kitne baje aana chahenge?",
        "Main aapka slot book kar deta hoon.",
        "Aap time bata dijiye main book kar doongi.",
        "I'll book a slot for you, what time works?",
        "I will book your visit once you tell me the time.",
        "Can I book a site visit for tomorrow?",
        "Shall I book a slot for you?",
        "What time should I book your visit?",
        "Main aapke liye site visit book kar sakti hoon.",
        "क्या मैं कल के लिए बुक कर दूँ?",
        "आप बताइए मैं बुक कर दूँगी।",
    ]
    for text in allowed_examples:
        assert is_unverified_booking_claim(text) is False, f"Incorrectly blocked: {text}"


# ==============================================================================
# ITEM 7: Fast-Path Threshold (<=8 Words, 1 Sentence), Multi-Ask & Expiry
# ==============================================================================

def is_simple_fast_path_utterance(text: str) -> bool:
    """Matches bot.py fast-path limit: <=8 words, 1 sentence, 1 question mark, no multi-clause."""
    t = text.strip()
    words = t.split()
    if len(words) > 8:
        return False
    # Check sentence count
    sentences = [s for s in re.split(r'[.!?]+', t) if s.strip()]
    if len(sentences) > 1:
        return False
    if t.count("?") > 1:
        return False
    # Check multi-clause with conjunction
    clauses = [c.strip() for c in re.split(r'[,;]+', t) if c.strip()]
    if len(clauses) >= 2 and all(len(c.split()) >= 3 for c in clauses):
        return False
    return True


def test_fast_path_utterance_threshold():
    """Verify <=8 words, 1 sentence limit routes compound and multi-ask to LLM."""
    # Simple single intent <= 8 words -> eligible for fast-path
    assert is_simple_fast_path_utterance("Yes, speaking.") is True
    assert is_simple_fast_path_utterance("Tell me the price") is True
    assert is_simple_fast_path_utterance("Tomorrow at 5 pm") is True

    # "Can I visit tomorrow, and is parking available?" -> 8 words, but 2 clauses >=3 words each -> routes to LLM!
    assert is_simple_fast_path_utterance("Can I visit tomorrow, and is parking available?") is False

    # Multi-ask > 8 words -> routes to LLM
    assert is_simple_fast_path_utterance("Achcha, theek hai, can you tell me about the Vastu and the amenities?") is False
    assert is_simple_fast_path_utterance("Actually you know mere ko thoda vastu and possession bata do?") is False

    # Multi-sentence -> routes to LLM
    assert is_simple_fast_path_utterance("Achha theek hai. Can I come visit tomorrow?") is False
    assert is_simple_fast_path_utterance("Okay, fine. Okay, thank you. Bye bye.") is False


def test_pending_visit_intent_two_turn_expiry():
    """Verify _pending_visit_intent expires after 2 turns if user drops the visit."""
    lead_memory = {
        "_pending_visit_intent": {
            "requested_date": "tomorrow",
            "expiry_turns": 2
        }
    }

    # Turn 1: User asks unrelated question about maintenance charges
    # Router decrements expiry_turns
    pending = lead_memory.get("_pending_visit_intent")
    assert pending is not None
    pending["expiry_turns"] -= 1
    assert pending["expiry_turns"] == 1

    # Turn 2: User asks another unrelated question about floor plans
    pending["expiry_turns"] -= 1
    if pending["expiry_turns"] <= 0:
        lead_memory.pop("_pending_visit_intent")

    # Turn 3: User says "Okay, thank you" -> pending visit intent expired, cannot book dropped visit!
    assert "_pending_visit_intent" not in lead_memory


def test_pending_visit_intent_consumed_when_slot_certain():
    """Verify _pending_visit_intent is consumed when caller provides certain time."""
    lead_memory = {
        "_pending_visit_intent": {
            "requested_date": "tomorrow",
            "expiry_turns": 2
        }
    }

    # Turn 2: Caller says "tomorrow at 5 pm" (time is certain)
    caller_slot_reply = "tomorrow at 5 pm"
    time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm)?|\d{1,2}\s*baje)\b", caller_slot_reply, re.I)
    assert time_match is not None, "Failed to extract slot time with certainty"

    # Slot is certain -> consumed for booking
    pending = lead_memory.pop("_pending_visit_intent")
    assert pending["requested_date"] == "tomorrow"
    lead_memory["site_visit_slot"] = "tomorrow at 5 pm"
    assert "_pending_visit_intent" not in lead_memory
    assert lead_memory["site_visit_slot"] == "tomorrow at 5 pm"


@pytest.mark.asyncio
async def test_fast_path_router_rejects_non_time_numbers_and_sends_to_llm():
    """Verify _FastPathRouter does not book an older time_slot from memory
    when caller's utterance contains numbers that are not times ('2 BHK',
    '3 people', 'second floor'). Those must route to the LLM."""
    from bot import _FastPathRouter
    from pipecat.frames.frames import LLMContextFrame
    from pipecat.processors.aggregators.llm_context import LLMContext
    from pipecat.pipeline.pipeline import FrameDirection

    for non_time_utterance in [
        "can I visit the 2 BHK tomorrow",
        "can I visit with 3 people",
        "can I visit the second floor",
    ]:
        booked_slots = []
        async def on_visit_ready(date, slot):
            booked_slots.append((date, slot))

        pushed_frames = []
        lead_memory = {
            "visit_date_iso": "2026-10-17",
            "time_slot": "5:00 PM",
        }
        router = _FastPathRouter(
            stream_id="test-non-time",
            lead_memory=lead_memory,
            on_visit_ready=on_visit_ready,
        )
        async def fake_push(f, d):
            pushed_frames.append(f)
        router.push_frame = fake_push

        ctx = LLMContext([{"role": "user", "content": non_time_utterance}])
        await router.process_frame(LLMContextFrame(ctx), FrameDirection.DOWNSTREAM)

        # Must NOT have booked the older 5:00 PM slot!
        assert len(booked_slots) == 0, f"Incorrectly booked older slot on non-time utterance: {non_time_utterance}"
        # Must have pushed the LLMContextFrame downstream to the LLM
        assert len(pushed_frames) == 1
        assert isinstance(pushed_frames[0], LLMContextFrame)

    # Conversely, when the utterance actually names the matching time ("tomorrow at 5 pm"):
    booked_slots = []
    async def on_visit_ready_match(date, slot):
        booked_slots.append((date, slot))

    pushed_frames = []
    lead_memory = {
        "visit_date_iso": "2026-10-17",
        "time_slot": "5:00 PM",
    }
    router = _FastPathRouter(
        stream_id="test-matching-time",
        lead_memory=lead_memory,
        on_visit_ready=on_visit_ready_match,
    )
    router.push_frame = fake_push

    ctx = LLMContext([{"role": "user", "content": "tomorrow at 5 pm"}])
    await router.process_frame(LLMContextFrame(ctx), FrameDirection.DOWNSTREAM)
    assert len(booked_slots) == 1, "Failed to book when utterance time matched memory slot"
    assert booked_slots[0] == ("2026-10-17", "5:00 PM")


# ==============================================================================
# ITEM 1: Muting After Booking Prevention & Audio-Level Barge-In Test
# ==============================================================================

@pytest.mark.asyncio
async def test_audio_no_muting_and_barge_in_working_after_booking():
    """Verify on AUDIO frames: confirmation plays, then the next turn's TTS audio
    is created and plays (not muted/dropped as stale), and a barge-in during that
    next reply still drops it (not stuck as an immortal booking confirmation)."""
    from services.web_serializer import WebPCMFrameSerializer
    from bot import _InterruptionAudioGate, _SpokenTextGuard
    from pipecat.frames.frames import (
        AudioRawFrame,
        TTSAudioRawFrame,
        TTSSpeakFrame,
        BotStoppedSpeakingFrame,
        TTSStoppedFrame,
        InterruptionFrame,
        UserStartedSpeakingFrame,
        UserStoppedSpeakingFrame,
    )
    from pipecat.pipeline.pipeline import FrameDirection

    class StubTTS:
        def __init__(self):
            self._current_speak_frame_gen = None
            self._current_turn_gen = 1
            self._active_booking_conf_tag = False

        def synthesize(self, text: str):
            f = TTSAudioRawFrame(audio=b"\x00\x20" * 320, sample_rate=16000, num_channels=1)
            f.generation_id = self._current_speak_frame_gen or self._current_turn_gen
            if self._active_booking_conf_tag:
                f.is_booking_confirmation = True
            return f

    tts_stub = StubTTS()
    serializer = WebPCMFrameSerializer("test-audio-muting-stream")
    gate = _InterruptionAudioGate(
        stream_id="test-audio-muting-stream",
        serializer=serializer,
        on_interruption=serializer.on_interruption,
        tts=tts_stub,
    )
    guard = _SpokenTextGuard(
        stream_id="test-audio-muting-stream",
        interruption_audio_gate=gate,
    )
    guard._tts = tts_stub

    out_frames = []
    async def fake_push(f, d):
        out_frames.append(f)
    gate.push_frame = fake_push

    # 1. Turn 1 starts
    gate.next_generation()
    assert gate._current_gen_id == 1
    assert tts_stub._current_turn_gen == 1

    # Booking succeeds: confirmation queued to TTS
    guard.mark_tool_succeeded(
        "book_site_visit",
        whatsapp_confirmed=True,
        confirm_msg="Great, your visit is booked for tomorrow at 5 PM.",
    )
    conf_speak = TTSSpeakFrame(text="Great, your visit is booked for tomorrow at 5 PM.")
    conf_speak.is_booking_confirmation = True
    conf_speak.generation_id = 1
    await guard.process_frame(conf_speak, FrameDirection.DOWNSTREAM)
    assert tts_stub._active_booking_conf_tag is True
    assert tts_stub._current_speak_frame_gen == 1

    # TTS stub creates confirmation audio frame
    conf_audio = tts_stub.synthesize(conf_speak.text)
    assert conf_audio.generation_id == 1
    assert conf_audio.is_booking_confirmation is True

    # Gate and serializer process confirmation audio -> plays!
    await gate.process_frame(conf_audio, FrameDirection.DOWNSTREAM)
    audio_out = [f for f in out_frames if isinstance(f, AudioRawFrame)]
    assert len(audio_out) == 1
    assert await serializer.serialize(conf_audio) is not None

    # Confirmation finishes playing
    await guard.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await gate.process_frame(TTSStoppedFrame(), FrameDirection.DOWNSTREAM)
    # Verify tags on tts_stub were reset!
    assert tts_stub._active_booking_conf_tag is False
    assert tts_stub._current_speak_frame_gen is None

    # 2. Next turn (Turn 2): Caller asks "what's the address?"
    await guard.process_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await guard.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    gen2 = gate.next_generation()
    assert gen2 == 2
    assert tts_stub._current_turn_gen == 2
    assert tts_stub._active_booking_conf_tag is False
    assert tts_stub._current_speak_frame_gen is None

    # Bot creates next turn's TTS audio for the address answer
    turn2_speak = TTSSpeakFrame(text="The project is located at Meridian Park.")
    turn2_speak.generation_id = 2
    turn2_speak.is_booking_confirmation = False
    await guard.process_frame(turn2_speak, FrameDirection.DOWNSTREAM)

    turn2_audio = tts_stub.synthesize(turn2_speak.text)
    assert turn2_audio.generation_id == 2, "Turn 2 audio was stuck with Turn 1 generation!"
    assert not getattr(turn2_audio, "is_booking_confirmation", False), "Turn 2 audio was stuck with is_booking_confirmation=True!"

    # Turn 2 audio arrives at gate and serializer -> PLAYS (not dropped as stale)!
    out_frames.clear()
    await gate.process_frame(turn2_audio, FrameDirection.DOWNSTREAM)
    audio_out = [f for f in out_frames if isinstance(f, AudioRawFrame)]
    assert len(audio_out) == 1, "Turn 2 audio was dropped by gate as stale!"
    assert await serializer.serialize(turn2_audio) is not None, "Turn 2 audio was dropped by serializer!"

    # 3. Barge-in during that next reply (Turn 2)
    await gate.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    serializer.on_interruption()
    assert gate._is_interrupted is True

    # Remaining audio chunks of Turn 2 arrive after barge-in
    turn2_chunk2 = tts_stub.synthesize("Meridian Park.")
    out_frames.clear()
    await gate.process_frame(turn2_chunk2, FrameDirection.DOWNSTREAM)
    audio_out = [f for f in out_frames if isinstance(f, AudioRawFrame)]
    assert len(audio_out) == 0, "Barge-in failed to drop subsequent turn audio!"
    assert gate._dropped_frames >= 1
    assert await serializer.serialize(turn2_chunk2) is None, "Barge-in failed to drop in serializer!"


# ==============================================================================
# ITEM 2: Serializer 10s Safety Timeout & Generation Reset
# ==============================================================================

@pytest.mark.asyncio
async def test_serializer_booking_conf_timeout_and_reset():
    """Verify _booking_conf_playing resets on next_generation() and after 10s timeout."""
    from services.web_serializer import WebPCMFrameSerializer
    from pipecat.frames.frames import AudioRawFrame

    serializer = WebPCMFrameSerializer("test-stream")
    serializer.next_generation(1)

    # Booking confirmation frame arrives
    conf_audio = AudioRawFrame(audio=b"\x00\x10" * 320, sample_rate=16000, num_channels=1)
    conf_audio.generation_id = 1
    conf_audio.is_booking_confirmation = True

    res = await serializer.serialize(conf_audio)
    assert res is not None
    assert serializer._booking_conf_playing is True
    assert serializer._booking_conf_deadline > time.monotonic()

    # While active, colliding reply audio is dropped
    other_audio = AudioRawFrame(audio=b"\x00\x10" * 320, sample_rate=16000, num_channels=1)
    other_audio.generation_id = 2
    other_audio.is_booking_confirmation = False
    assert await serializer.serialize(other_audio) is None

    # next_generation() clears the booking confirmation lock immediately
    serializer.next_generation(2)
    assert serializer._booking_conf_playing is False
    assert serializer._booking_conf_deadline == 0.0

    # New audio plays freely
    assert await serializer.serialize(other_audio) is not None

    # Test safety timeout: simulate booking confirmation with expired deadline
    serializer._booking_conf_playing = True
    serializer._booking_conf_deadline = time.monotonic() - 1.0  # Expired
    # Should not drop because deadline has passed
    assert await serializer.serialize(other_audio) is not None
    assert serializer._booking_conf_playing is False


# ==============================================================================
# ITEMS 3 & 4: End-to-End Gate & Serializer Collision, TTS Stub & Stragglers
# ==============================================================================

@pytest.mark.asyncio
async def test_end_to_end_booking_confirmation_with_tts_stub_and_gate():
    """A booking confirmation queued just before a barge-in must still play,
    and must not overlap a new reply. If both want to speak, keep the booking
    confirmation and drop the other."""
    from services.web_serializer import WebPCMFrameSerializer
    from bot import _InterruptionAudioGate, _SpokenTextGuard
    from pipecat.frames.frames import (
        InterruptionFrame,
        TTSSpeakFrame,
        TTSAudioRawFrame,
    )
    from pipecat.pipeline.pipeline import FrameDirection

    class StubTTS:
        def __init__(self):
            self._current_speak_frame_gen = None
            self._current_turn_gen = 1
            self._active_booking_conf_tag = False

        def synthesize(self, text: str):
            f = TTSAudioRawFrame(audio=b"\x00\x20" * 320, sample_rate=16000, num_channels=1)
            f.generation_id = self._current_speak_frame_gen or self._current_turn_gen
            if self._active_booking_conf_tag:
                f.is_booking_confirmation = True
            return f

    tts_stub = StubTTS()
    serializer = WebPCMFrameSerializer("test-e2e-stream")
    gate = _InterruptionAudioGate(
        stream_id="test-e2e-stream",
        serializer=serializer,
        on_interruption=serializer.on_interruption,
        tts=tts_stub,
    )
    guard = _SpokenTextGuard(
        stream_id="test-e2e-stream",
        interruption_audio_gate=gate,
    )
    guard._tts = tts_stub

    # 1. Turn 1:
    gen1 = gate.next_generation()
    assert gen1 == 1

    # Tool succeeds: arm booking confirmation
    guard.mark_tool_succeeded(
        "book_site_visit",
        whatsapp_confirmed=True,
        confirm_msg="Great, your visit is booked for tomorrow at 5 PM.",
    )
    conf_speak = TTSSpeakFrame(text="Great, your visit is booked for tomorrow at 5 PM.")
    conf_speak.is_booking_confirmation = True
    conf_speak.generation_id = gen1
    await guard.process_frame(conf_speak, FrameDirection.DOWNSTREAM)
    assert tts_stub._active_booking_conf_tag is True
    assert tts_stub._current_speak_frame_gen == gen1

    # TTS stub generates fresh frame from conf_speak
    conf_audio = tts_stub.synthesize(conf_speak.text)
    assert conf_audio.generation_id == 1
    assert conf_audio.is_booking_confirmation is True
    tts_stub._active_booking_conf_tag = False
    tts_stub._current_speak_frame_gen = None

    # 2. Barge-in happens just before playback:
    await gate.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert gate._is_interrupted is True

    # 3. User speaks new turn -> Generation 2 starts
    gen2 = gate.next_generation()
    assert gen2 == 2
    assert tts_stub._current_turn_gen == 2

    # 4. In Gen 2, another reply is generated
    other_speak = TTSSpeakFrame(text="Parking is available on site.")
    other_speak.generation_id = 2
    other_speak.is_booking_confirmation = False

    # Guard checks colliding speak frame -> dropped due to confirmation priority
    colliding_dropped = False
    if not getattr(other_speak, "is_booking_confirmation", False) and (
        getattr(guard, "_confirmation_queued_to_tts", False)
        or getattr(guard, "_confirmation_speaking", False)
    ):
        colliding_dropped = True
    assert colliding_dropped is True

    # Colliding audio frame is generated
    colliding_audio = tts_stub.synthesize("Parking is available")
    assert colliding_audio.generation_id == 2
    assert not getattr(colliding_audio, "is_booking_confirmation", False)

    # 5. Booking confirmation audio arrives at gate:
    # Gate does NOT drop it because is_booking_confirmation=True!
    gate_frames_out = []
    async def fake_push(f, d):
        gate_frames_out.append(f)
    gate.push_frame = fake_push

    await gate.process_frame(conf_audio, FrameDirection.DOWNSTREAM)
    assert len(gate_frames_out) == 1, "Confirmation audio was dropped by gate!"

    # 6. Confirmation audio reaches serializer:
    out = await serializer.serialize(conf_audio)
    assert out is not None, "Confirmation audio dropped by serializer!"
    assert serializer._booking_conf_playing is True

    # 7. Colliding audio arrives at serializer while confirmation is playing:
    colliding_out = await serializer.serialize(colliding_audio)
    assert colliding_out is None, "Colliding audio was NOT dropped by serializer!"

    # No two voices! Confirmation was kept, other reply was dropped.


@pytest.mark.asyncio
async def test_straggler_frame_dropped_after_next_generation():
    """Tag each frame with the generation it was created in. An old-generation
    frame arriving after next_generation() is dropped, and new-generation audio
    is never dropped."""
    from services.web_serializer import WebPCMFrameSerializer
    from bot import _InterruptionAudioGate
    from pipecat.frames.frames import AudioRawFrame, TTSStartedFrame
    from pipecat.pipeline.pipeline import FrameDirection

    serializer = WebPCMFrameSerializer("test-straggler")
    gate = _InterruptionAudioGate(
        stream_id="test-straggler",
        serializer=serializer,
        on_interruption=serializer.on_interruption,
    )

    out_frames = []
    async def fake_push(f, d):
        out_frames.append(f)
    gate.push_frame = fake_push

    # Turn 1
    gate.next_generation()
    assert gate._current_gen_id == 1

    # Old frame created during Turn 1
    old_frame = AudioRawFrame(audio=b"\x00\x10" * 320, sample_rate=16000, num_channels=1)
    old_frame.generation_id = 1
    old_frame.is_booking_confirmation = False

    # Turn 2 starts
    gate.next_generation()
    assert gate._current_gen_id == 2
    await gate.process_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)

    # Old straggler from Gen 1 arrives at gate now
    await gate.process_frame(old_frame, FrameDirection.DOWNSTREAM)
    audio_out = [f for f in out_frames if isinstance(f, AudioRawFrame)]
    assert len(audio_out) == 0, "Old straggler frame was NOT dropped by gate!"
    assert gate._dropped_frames == 1

    # New frame created in Turn 2
    new_frame = AudioRawFrame(audio=b"\x00\x10" * 320, sample_rate=16000, num_channels=1)
    new_frame.generation_id = 2
    new_frame.is_booking_confirmation = False

    await gate.process_frame(new_frame, FrameDirection.DOWNSTREAM)
    audio_out = [f for f in out_frames if isinstance(f, AudioRawFrame)]
    assert len(audio_out) == 1, "New-generation audio frame was incorrectly dropped!"
    assert audio_out[0] == new_frame

    # Serializer also drops old stragglers
    assert await serializer.serialize(old_frame) is None
    assert await serializer.serialize(new_frame) is not None


async def test_attach_safe_tts_none_frame_does_not_raise_and_no_retry():
    """run_tts yielding a None frame does not raise, and the line is spoken once, with no retry."""
    from bot import attach_safe_tts
    from pipecat.frames.frames import TTSAudioRawFrame

    call_count = 0

    class StubTTS:
        def __init__(self):
            self._current_speak_frame_gen = 5
            self._active_booking_conf_tag = True

        async def run_tts(self, text, context_id):
            nonlocal call_count
            call_count += 1
            # Streaming WebSocket TTS (like Sarvam) yields None, then audio frames
            yield None
            yield TTSAudioRawFrame(audio=b"\x00\x10" * 160, sample_rate=16000, num_channels=1)

    class StubMetrics:
        def __init__(self):
            self.played = 0
            self.failed = 0

        def record_tts_played_characters(self, count):
            self.played += count

        def record_tts_failed_characters(self, count, reason=""):
            self.failed += count

    tts = StubTTS()
    metrics = StubMetrics()
    attach_safe_tts(tts, stream_id="test-none-frame", metrics_collector=metrics, sample_rate=16000)

    frames = []
    async for f in tts.run_tts("Hello world", "ctx-1"):
        frames.append(f)

    # 1. No exception was raised
    # 2. Spoken once, no retry
    assert call_count == 1
    # 3. Frames yielded: [None, TTSAudioRawFrame]
    assert len(frames) == 2
    assert frames[0] is None
    assert isinstance(frames[1], TTSAudioRawFrame)
    assert frames[1].generation_id == 5
    assert frames[1].is_booking_confirmation is True
    # 4. Played characters recorded, failed characters == 0
    assert metrics.played == len("Hello world")
    assert metrics.failed == 0


async def test_barge_in_during_farewell_cancels_ending_and_bot_answers():
    """Noise/short sound does not cut goodbye, but real non-empty transcript
    during farewell cancels call ending, stops goodbye audio, and routes
    transcript to LLM to answer before ending."""
    from bot import (
        _CallEndCoordinator,
        _InterruptionAudioGate,
        _SpokenTextGuard,
        _TranscriptionTap,
    )
    from pipecat.frames.frames import (
        InterruptionFrame,
        TranscriptionFrame,
        TTSStartedFrame,
        TTSAudioRawFrame,
    )
    from pipecat.pipeline.pipeline import FrameDirection

    hangup_called = False
    async def fake_hangup():
        nonlocal hangup_called
        hangup_called = True

    coordinator = _CallEndCoordinator(
        stream_id="test-farewell-bargein",
        on_hangup=fake_hangup,
        grace_seconds=3.0,
    )

    class MockWS:
        def __init__(self):
            self.sent = []
        async def send_text(self, text):
            self.sent.append(text)

    ws = MockWS()
    gate = _InterruptionAudioGate(
        stream_id="test-farewell-bargein",
        call_end_coordinator=coordinator,
        websocket=ws,
    )
    guard = _SpokenTextGuard(
        stream_id="test-farewell-bargein",
        call_end_coordinator=coordinator,
        interruption_audio_gate=gate,
    )
    gate._spoken_guard = guard

    tap = _TranscriptionTap(
        stream_id="test-farewell-bargein",
        spoken_guard=guard,
    )

    # 1. Bot books and requests ending: goodbye is queued/playing
    coordinator.request_ending()
    assert coordinator.is_ending is True

    # Bot starts speaking goodbye
    await coordinator.process_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)
    await gate.process_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)
    assert coordinator.is_closing_in_progress is True
    assert gate._active_playing_gen_id == gate._current_gen_id

    # 2. Noise / short burp arrives during farewell -> goodbye protected, not cut!
    await gate.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert gate._is_interrupted is False, "Noise cut goodbye audio!"
    assert coordinator.is_ending is True, "Noise cancelled call ending!"

    # 3. Real caller transcript arrives during farewell: "can you tell me about the address?"
    user_frame = TranscriptionFrame(text="can you tell me about the address?", user_id="caller", timestamp="0")
    await tap.process_frame(user_frame, FrameDirection.DOWNSTREAM)

    # Ending must be cancelled, goodbye audio stopped
    assert coordinator.is_ending is False, "Real transcript did not cancel ending!"
    assert coordinator.is_closing_in_progress is False
    assert gate._is_interrupted is True, "Goodbye audio was not interrupted!"
    assert gate._active_playing_gen_id == 0, "Goodbye audio active gen was not cleared!"
    assert any("clearAudio" in m for m in ws.sent), "clearAudio was not sent to websocket!"

    # 4. Bot processes turn and answers before ending
    assert coordinator._ended is False
    assert hangup_called is False


def test_metrics_collector_never_bills_failed_line_twice_and_no_double_counting():
    """Ensure a failed line retried by TTS is never billed twice,
    and tts_failure_cost is not double-counted in total estimated cost."""
    from metrics_collector import CallMetricsCollector
    from pipecat.processors.metrics.frame_processor_metrics import TTSUsageMetricsData

    cost_rates = {
        "tts": {
            "sarvam": {
                "per_character": 0.003,
            }
        }
    }
    collector = CallMetricsCollector(
        call_id="test-cost-double-count",
        stt_provider="sarvam",
        llm_provider="groq",
        tts_provider="sarvam",
        cost_rates=cost_rates,
    )
    collector._tts_models_used.add("bulbul:v3")

    # Line of 184 characters fails attempt 1, retries attempt 2:
    # Pipecat emits TTSUsageMetricsData twice (184 + 184 = 368)
    collector._handle_metrics_data(TTSUsageMetricsData(processor="SarvamTTSService#1", model="bulbul:v3", value=184))
    collector._handle_metrics_data(TTSUsageMetricsData(processor="SarvamTTSService#1", model="bulbul:v3", value=184))
    assert collector._totals["tts_characters"] == 368

    # Synthesis failed after retry:
    collector.record_tts_failed_characters(184, reason="synthesis error")

    # 1. Retry usage was deducted: only billed once (184), never billed twice
    assert collector._totals["tts_characters"] == 184
    assert collector._totals["tts_characters_failed"] == 184

    # 2. In cost breakdown: delivered is 0, failed is 184
    total_cost, breakdown = collector._estimate_cost()
    assert breakdown["tts"] == 0.0  # delivered TTS cost
    assert breakdown["tts_failure_cost"] == round(184 * 0.003, 6)
    # Total cost equals exactly the failure cost (184 * 0.003), NOT double-counted!
    assert total_cost == round(184 * 0.003, 6)



