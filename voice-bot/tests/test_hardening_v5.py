"""
Unit and integration tests for Voice Bot Hardening v5 (Fixes 1 through 8 + User Edits 1 through 7).
"""

import asyncio
from datetime import datetime, timezone
import os
import re
import time
from unittest.mock import AsyncMock, MagicMock, patch
import zoneinfo
import pytest

from bot import (
    _AUDIO_CACHE,
    _CACHED_PHRASE_TEXTS,
    _DelayedRaceFiller,
    _FastPathRouter,
    _HistoryPruner,
    _InterruptionAudioGate,
    _SilenceChecker,
    _SpokenTextGuard,
    _TerminationProcessor,
    _find_sentence_end,
    _match_cached_phrase,
    _sync_working_memory,
    execute_book_site_visit,
)
from leads.worker import normalize_visit_date, normalize_visit_time
from services.whatsapp_sender import send_whatsapp_location
from metrics_collector import CallMetricsCollector
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    InterruptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    TextFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection


# ==============================================================================
# FIX 1: Empty Transcript Silent Turn & Watchdog Retries
# ==============================================================================

@pytest.mark.asyncio
async def test_fix1_filler_and_watchdog_arm_only_on_llm_request():
    mock_gate = MagicMock()
    mock_gate.play_cached_audio = AsyncMock()
    mock_llm = MagicMock()
    mock_llm.process_frame = AsyncMock()
    mock_context = MagicMock()

    filler = _DelayedRaceFiller(
        stream_id="test-empty-turn",
        interruption_audio_gate=mock_gate,
        timeout_seconds=0.7,
        llm_service=mock_llm,
        context=mock_context,
    )

    # Empty user turn: User stops speaking but NO LLM request is sent
    await filler.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    assert filler._turn_in_flight is False
    assert filler._race_task is None
    assert filler._watchdog_task is None

    # When LLM request is sent: timers arm
    mock_context.messages = [{"role": "user", "content": "I am interested in 2 BHK"}]
    await filler.process_frame(LLMContextFrame(mock_context), FrameDirection.DOWNSTREAM)
    assert filler._turn_in_flight is True
    assert filler._watchdog_task is not None


@pytest.mark.asyncio
async def test_fix1_watchdog_skips_retry_when_last_message_is_assistant():
    mock_gate = MagicMock()
    mock_gate.play_cached_audio = AsyncMock()
    mock_llm = MagicMock()
    mock_llm.process_frame = AsyncMock()

    mock_context = MagicMock()
    # Context ending with assistant message (the exact symptom from 23:27:57.089 log)
    mock_context.messages = [
        {"role": "system", "content": "You are Ananya."},
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi, I am Ananya from Meridian Valley."},
    ]

    filler = _DelayedRaceFiller(
        stream_id="test-watchdog-assistant",
        interruption_audio_gate=mock_gate,
        timeout_seconds=0.7,
        llm_service=mock_llm,
        context=mock_context,
    )
    filler.arm_llm_request(1)
    assert filler._turn_in_flight is True

    # Fire watchdog directly
    await filler._watchdog_timer(1)

    # Watchdog should NOT retry LLM because last message was assistant
    assert mock_llm.process_frame.called is False
    assert filler._watchdog_retried is False


@pytest.mark.asyncio
async def test_fix1_watchdog_retries_when_last_message_is_user():
    mock_gate = MagicMock()
    mock_gate.play_cached_audio = AsyncMock()
    mock_llm = MagicMock()
    mock_llm.process_frame = AsyncMock()

    mock_context = MagicMock()
    mock_context.messages = [
        {"role": "system", "content": "You are Ananya."},
        {"role": "user", "content": "What is the price of 3 BHK?"},
    ]

    filler = _DelayedRaceFiller(
        stream_id="test-watchdog-user",
        interruption_audio_gate=mock_gate,
        timeout_seconds=0.7,
        llm_service=mock_llm,
        context=mock_context,
    )
    _AUDIO_CACHE[8000] = {"filler_en": b"RIFFfakeaudio"}
    filler.arm_llm_request(1)

    # Fire watchdog directly
    await filler._watchdog_timer(1)

    # Watchdog MUST retry LLM because caller asked a question and was unanswered
    assert mock_llm.process_frame.called is True
    assert filler._watchdog_retried is True


# ==============================================================================
# FIX 2 & EDIT 1: Context Rewind, Working Memory, & Opening Intro Restriction
# ==============================================================================

def test_fix2_opening_intro_only_matches_turn_1():
    intro_text = _CACHED_PHRASE_TEXTS["opening_intro"]

    # Turn 1: matches opening_intro
    match_turn_1 = _match_cached_phrase(intro_text, turn_count=1)
    assert match_turn_1 == "opening_intro"

    # Turn > 1: MUST NOT match opening_intro
    match_turn_2 = _match_cached_phrase(intro_text, turn_count=2)
    assert match_turn_2 != "opening_intro"
    match_turn_5 = _match_cached_phrase(intro_text, turn_count=5)
    assert match_turn_5 != "opening_intro"


@pytest.mark.asyncio
async def test_fix2_duplicate_assistant_record_logged_and_skipped():
    metrics = CallMetricsCollector(call_id="test-dup-record", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    mock_context = MagicMock()
    mock_context.messages = [
        {"role": "system", "content": "System prompt"},
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hello! I am Ananya from Meridian Valley."},
    ]

    guard = _SpokenTextGuard(
        stream_id="test-dup-guard",
        context=mock_context,
        call_metrics=metrics,
    )

    # Attempt to record the exact same assistant message again
    guard._record_assistant_spoken("Hello! I am Ananya from Meridian Valley.")

    # Context messages should not contain a duplicated assistant record
    assistant_msgs = [m for m in mock_context.messages if m.get("role") == "assistant"]
    assert len(assistant_msgs) == 1
    assert metrics._totals["duplicate_assistant_records"] == 1


@pytest.mark.asyncio
async def test_fix2_context_rewind_unexplained_drop_detected():
    metrics = CallMetricsCollector(call_id="test-rewind", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    pruner = MagicMock()
    pruner._last_pruned_len = 5

    router = _FastPathRouter(
        stream_id="test-rewind-router",
        history_pruner=pruner,
        call_metrics=metrics,
    )

    mock_context = MagicMock()
    # Unexplained drop: only 3 messages when last_pruned was 5
    mock_context.messages = [
        {"role": "system", "content": "System"},
        {"role": "user", "content": "Hi"},
        {"role": "assistant", "content": "Hello"},
    ]

    await router.process_frame(LLMContextFrame(mock_context), FrameDirection.DOWNSTREAM)
    assert metrics._totals["context_rewinds"] == 1


def test_fix2_working_memory_synced_to_last_system_message():
    lead_memory = {"budget": "1.5 Cr"}
    messages = [
        {"role": "system", "content": "Static instructions."},
        {"role": "system", "content": "Persona prompt."},
        {"role": "user", "content": "Hello"},
    ]

    _sync_working_memory(messages, lead_memory, "stream-wm")

    # Static instructions should remain untouched
    assert "[ACTIVE LEAD STATE:" not in messages[0]["content"]
    # Last system message should have working memory appended
    assert "[ACTIVE LEAD STATE:" in messages[1]["content"]
    assert "1.5 Cr" in messages[1]["content"]


# ==============================================================================
# FIX 3 & EDIT 2: Premature Booking Claims Dropped in SpokenTextGuard
# ==============================================================================

def test_fix3_spoken_text_guard_drops_unverified_booking_claims():
    guard = _SpokenTextGuard(stream_id="test-guard")
    guard._site_visit_succeeded_this_turn = False
    guard._whatsapp_succeeded_this_turn = False

    # English booking claim
    filtered_en = guard._filter_unverified_claims(
        "Wonderful, I have scheduled your site visit for tomorrow. We have great 3 BHK units."
    )
    assert "scheduled your site visit" not in filtered_en
    assert "great 3 BHK units" in filtered_en

    # Hindi booking claim
    filtered_hi = guard._filter_unverified_claims(
        "आपकी साइट विजिट बुक कर दी गई है। क्या आप 3 बीएचके देखना चाहते हैं?"
    )
    assert "बुक" not in filtered_hi
    assert "3 बीएचके" in filtered_hi

    # Unverified WhatsApp claim
    filtered_wa = guard._filter_unverified_claims(
        "You will receive the location on WhatsApp. Let me know if you need anything else."
    )
    assert "WhatsApp" not in filtered_wa
    assert "anything else" in filtered_wa


def test_fix3_spoken_text_guard_allows_claims_when_tool_succeeded():
    guard = _SpokenTextGuard(stream_id="test-guard-success")
    guard.mark_tool_succeeded("book_site_visit", whatsapp_confirmed=True, confirm_msg="Site visit confirmed for tomorrow.")

    # Under v5.1, confirmation is queued directly to TTS; duplicate LLM booking claim sentence is suppressed
    text = "Wonderful, I have scheduled your site visit for tomorrow. You will receive the location on WhatsApp."
    filtered = guard._filter_unverified_claims(text)
    assert "scheduled your site visit" not in filtered
    assert "WhatsApp" in filtered
    assert guard._confirmation_queued_to_tts is True
    assert guard._confirmation_actually_played is False

    # Once BotStoppedSpeakingFrame fires, confirmation is marked actually_played
    guard.process_frame_sync(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM) if hasattr(guard, "process_frame_sync") else None


# ==============================================================================
# FIX 4 & 5: Bilingual Date/Time Normalization & Booking Integrity
# ==============================================================================

def test_fix4_normalize_visit_date():
    tz_ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    # Simulate a known Wednesday
    now_ist = datetime(2026, 10, 7, 10, 0, tzinfo=tz_ist)

    # "kal" / "कल" -> tomorrow
    res_kal = normalize_visit_date("कल", now_ist)
    assert res_kal is not None
    assert res_kal[0] == "2026-10-08"

    res_tomorrow = normalize_visit_date("tomorrow", now_ist)
    assert res_tomorrow is not None
    assert res_tomorrow[0] == "2026-10-08"

    # "today" / "aaj" / "आज" -> today
    res_aaj = normalize_visit_date("आज", now_ist)
    assert res_aaj is not None
    assert res_aaj[0] == "2026-10-07"

    # Weekday in Hindi ("शनिवार" -> Saturday)
    res_shani = normalize_visit_date("शनिवार", now_ist)
    assert res_shani is not None
    assert res_shani[0] == "2026-10-10"

    # Invalid / vague date -> None
    assert normalize_visit_date("kabhi bhi", now_ist) is None
    assert normalize_visit_date("", now_ist) is None


def test_fix4_normalize_visit_time():
    # English AM / PM
    assert normalize_visit_time("11:00 AM") == "11:00 AM"
    assert normalize_visit_time("5 pm") == "5:00 PM"

    # Hindi / Hinglish phrases
    assert normalize_visit_time("शाम 5 बजे") == "5:00 PM"
    assert normalize_visit_time("subah 10 baje") == "10:00 AM"
    assert normalize_visit_time("dopahar 2 baje") == "2:00 PM"
    assert normalize_visit_time("evening 4") == "4:00 PM"

    # Invalid / empty
    assert normalize_visit_time("") is None
    assert normalize_visit_time("whenever") is None


# ==============================================================================
# FIX 6: WhatsApp Outbox Client
# ==============================================================================

@pytest.mark.asyncio
async def test_fix6_whatsapp_sender_redaction_and_phone_normalization():
    from services.whatsapp_sender import send_whatsapp_location
    import httpx

    with patch("httpx.AsyncClient.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"messages": [{"id": "wamid.HBgL"}]}'
        mock_response.json.return_value = {"messages": [{"id": "wamid.HBgL"}]}
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "secret_token_12345", "WHATSAPP_PHONE_NUMBER_ID": "123456"}):
            res = await send_whatsapp_location(
                to_phone="9876543210",  # 10 digits
                visit_date="Tomorrow",
                visit_time="5:00 PM",
            )
            assert res["ok"] is True
            assert res["status"] == "sent"
            # Verify phone was normalized to E.164 without '+' for Meta Cloud API (919876543210)
            call_kwargs = mock_post.call_args[1]
            payload = call_kwargs["json"]
            assert payload["to"] == "919876543210"


# ==============================================================================
# FIX 7: Farewell Hangup Integrity
# ==============================================================================

@pytest.mark.asyncio
async def test_fix7_farewell_hangup_not_cancelled_by_interruption():
    mock_hangup = AsyncMock()
    mock_force_hangup = AsyncMock()
    tp = _TerminationProcessor(
        stream_id="test-farewell",
        on_hangup=mock_hangup,
        force_hangup_fn=mock_force_hangup,
        grace_seconds=0.05,
    )

    # Bot speaks farewell text
    await tp.process_frame(TextFrame(text="Thank you for your time. Have a wonderful day!"), FrameDirection.DOWNSTREAM)
    assert tp._waiting_for_bot_stop is True

    # Bot finishes speaking
    await tp.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    assert tp._hangup_task is not None

    # Caller interrupts during farewell grace delay
    await tp.process_frame(InterruptionFrame(), FrameDirection.UPSTREAM)

    # Hangup task MUST NOT be cancelled
    assert not tp._hangup_task.cancelled()


# ==============================================================================
# FIX 8: Decimal Dot Sentence Boundary Detection & Pre-roll
# ==============================================================================

def test_fix8_find_sentence_end_preserves_decimals_and_abbreviations():
    # Decimal in price ("1.45 crores")
    text_price = "The 2 BHK is priced at 1.45 crores. Would you like to schedule a visit?"
    idx = _find_sentence_end(text_price)
    first_sentence = text_price[:idx].strip()
    assert first_sentence == "The 2 BHK is priced at 1.45 crores."

    # Decimal in BHK ("3.5 BHK")
    text_bhk = "We have 3.5 BHK units available. They have lake views."
    idx_bhk = _find_sentence_end(text_bhk)
    first_bhk = text_bhk[:idx_bhk].strip()
    assert first_bhk == "We have 3.5 BHK units available."

    # Currency abbreviation ("Rs. 95 lakhs")
    text_rs = "Starting from Rs. 95 lakhs. Booking is open."
    idx_rs = _find_sentence_end(text_rs)
    first_rs = text_rs[:idx_rs].strip()
    assert first_rs == "Starting from Rs. 95 lakhs."


@pytest.mark.asyncio
async def test_fix8_interruption_audio_gate_300ms_trim():
    """Verify PCM byte/sample reconciliation, 300ms ceiling, silence-only trim,
    speech at sample 0 preservation, and per-turn budget reset.
    16kHz 16-bit mono: 1 sample = 2 bytes -> 300ms = 4800 samples = 9600 bytes.
    8kHz 16-bit mono: 1 sample = 2 bytes -> 300ms = 2400 samples = 4800 bytes.
    """
    # 1. 16kHz test: 15000 bytes silence -> trimmed capped strictly at 9600 bytes (300ms)
    metrics_16k = CallMetricsCollector(call_id="test-trim-16k", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    gate_16k = _InterruptionAudioGate(stream_id="test-gate-16k", metrics_collector=metrics_16k, max_trim_ms=300.0)
    gate_16k._awaiting_first_audio_of_turn = True

    silence_16k = b"\x00" * 15000
    speech_16k = b"\x10\x20" * 2000
    frame_16k = TTSAudioRawFrame(audio=silence_16k + speech_16k, sample_rate=16000, num_channels=1)

    await gate_16k.process_frame(frame_16k, FrameDirection.DOWNSTREAM)
    assert gate_16k._accumulated_trimmed_bytes == 9600  # exactly 4800 samples = 300ms
    assert len(metrics_16k._tts_silence_trimmed_ms) == 1
    assert metrics_16k._tts_silence_trimmed_ms[0] == 300.0

    # 2. 8kHz test: 10000 bytes silence -> trimmed capped strictly at 4800 bytes (300ms)
    metrics_8k = CallMetricsCollector(call_id="test-trim-8k", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    gate_8k = _InterruptionAudioGate(stream_id="test-gate-8k", metrics_collector=metrics_8k, max_trim_ms=300.0)
    gate_8k._awaiting_first_audio_of_turn = True

    silence_8k = b"\x00" * 10000
    speech_8k = b"\x10\x20" * 1000
    frame_8k = TTSAudioRawFrame(audio=silence_8k + speech_8k, sample_rate=8000, num_channels=1)

    await gate_8k.process_frame(frame_8k, FrameDirection.DOWNSTREAM)
    assert gate_8k._accumulated_trimmed_bytes == 4800  # exactly 2400 samples = 300ms
    assert len(metrics_8k._tts_silence_trimmed_ms) == 1
    assert metrics_8k._tts_silence_trimmed_ms[0] == 300.0

    # 3. Speech at sample 0 untouched: frame audio and length untouched
    gate_speech0 = _InterruptionAudioGate(stream_id="test-gate-sp0")
    gate_speech0._awaiting_first_audio_of_turn = True
    immediate_speech = b"\x50\x20" * 1000 + b"\x00" * 2000
    orig_bytes = bytes(immediate_speech)
    frame_sp0 = TTSAudioRawFrame(audio=immediate_speech, sample_rate=16000, num_channels=1)

    await gate_speech0.process_frame(frame_sp0, FrameDirection.DOWNSTREAM)
    assert gate_speech0._accumulated_trimmed_bytes == 0
    assert frame_sp0.audio == orig_bytes

    # 4. Leading trim removes silence only, keeping 20ms pad (1000 samples - 320 samples = 680 samples = 1360 bytes @ 16kHz)
    gate_lead = _InterruptionAudioGate(stream_id="test-gate-lead")
    gate_lead._awaiting_first_audio_of_turn = True
    lead_silence = b"\x00" * 2000
    subsequent_speech = b"\x40\x20" * 500
    frame_lead = TTSAudioRawFrame(audio=lead_silence + subsequent_speech, sample_rate=16000, num_channels=1)

    await gate_lead.process_frame(frame_lead, FrameDirection.DOWNSTREAM)
    assert gate_lead._accumulated_trimmed_bytes == 1360  # exactly 680 samples trimmed, 320 samples (20ms) pad retained

    # 5. Cumulative budget resets per turn via next_generation() and TTSStartedFrame
    gate_lead.next_generation()
    assert gate_lead._accumulated_trimmed_bytes == 0
    assert gate_lead._awaiting_first_audio_of_turn is True


@pytest.fixture(autouse=True)
async def ensure_db_initialized():
    from leads.db import init_models
    await init_models()


# ==============================================================================
# AUDIT ITEM 1: Fixed IST Now Date Resolution & Consistency
# ==============================================================================

@pytest.mark.asyncio
async def test_fixed_ist_date_resolved_equals_stored_equals_spoken():
    """Verify that with fixed IST now (2026-10-07 01:04 IST), date resolution,
    storage in SiteVisit, and spoken confirmation all match exactly.
    Harness utterance for scenario 4 was: 'Can I book a visit for Saturday at 11 am?'
    """
    fixed_now = datetime(2026, 10, 7, 1, 4, tzinfo=zoneinfo.ZoneInfo("Asia/Kolkata"))  # Wednesday

    cases = [
        ("कल", "2026-10-08"),
        ("kal", "2026-10-08"),
        ("tomorrow", "2026-10-08"),
        ("परसों", "2026-10-09"),
        ("Saturday", "2026-10-10"),
    ]

    for raw_date, expected_iso in cases:
        date_res = normalize_visit_date(raw_date, fixed_now)
        assert date_res is not None
        iso_date, date_label = date_res
        assert iso_date == expected_iso, f"Failed for {raw_date}: expected {expected_iso}, got {iso_date}"

        # Test tool handler execution
        result_holder = {}
        async def mock_callback(data, **kwargs):
            result_holder.update(data)

        params = MagicMock()
        params.arguments = {"date": raw_date, "time": "5:00 PM"}
        params.result_callback = mock_callback

        mock_metrics = MagicMock()
        mock_guard = MagicMock()
        call_id = f"test-fixed-date-{raw_date}-{time.time()}"
        lead_mem = {}

        with patch("bot.datetime") as mock_b_dt, patch("leads.worker.datetime") as mock_w_dt:
            mock_b_dt.now.return_value = fixed_now
            mock_b_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            mock_w_dt.now.return_value = fixed_now
            mock_w_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            await execute_book_site_visit(
                params,
                stream_id=call_id,
                call_metrics=mock_metrics,
                spoken_text_guard=mock_guard,
                lead_memory=lead_mem,
                call_type="web",
            )

        assert result_holder.get("status") == "confirmed"
        # 1. visit_date_iso resolved
        assert iso_date == expected_iso
        # 2. value stored in site_visits
        from leads.db import get_session
        from leads.models import SiteVisit
        from sqlalchemy import select
        async with get_session() as session:
            sv = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == call_id))).scalars().first()
            assert sv is not None
            assert sv.visit_date_iso == expected_iso
        # 3. value in spoken confirmation (FIX 6: natural spoken date, never raw ISO/year)
        spoken_msg = result_holder.get("message", "")
        spoken_date = result_holder.get("spoken_date")
        assert spoken_date is not None
        assert spoken_date in spoken_msg
        assert "2026-" not in spoken_msg
        assert sv.visit_date_iso == iso_date == expected_iso


# ==============================================================================
# AUDIT ITEM 2 & 9: WhatsApp 4 States (sent, queued, failed, not_configured)
# ==============================================================================

@pytest.mark.asyncio
async def test_whatsapp_four_states_spoken_and_outbox():
    """Verify sent, queued, failed, and not_configured states, checking spoken line and outbox."""
    from leads.db import get_session
    from leads.models import SiteVisit, OutboxItem
    from sqlalchemy import select

    # State 1: not_configured (no WhatsApp credentials)
    with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "", "WHATSAPP_PHONE_NUMBER_ID": ""}):
        res_not_cfg = await send_whatsapp_location("+919876543210", visit_date="2026-10-08", visit_time="5:00 PM")
        assert res_not_cfg["status"] == "not_configured"
        assert res_not_cfg["ok"] is False

        result_holder = {}
        async def cb(data, **kw): result_holder.update(data)
        params = MagicMock()
        params.arguments = {"date": "tomorrow", "time": "5:00 PM"}
        params.result_callback = cb
        call_id = f"test-wa-notcfg-{time.time()}"
        lead_mem = {"whatsapp_opt_in": True}

        await execute_book_site_visit(
            params,
            stream_id=call_id,
            call_metrics=MagicMock(),
            spoken_text_guard=MagicMock(),
            lead_memory=lead_mem,
            call_type="web",
        )
        assert result_holder.get("whatsapp_status") == "not_configured"
        spoken = result_holder.get("message", "")
        assert "WhatsApp" not in spoken

    # State 2: sent (Meta Cloud API returns HTTP 200 with message_id)
    with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "valid_token", "WHATSAPP_PHONE_NUMBER_ID": "12345"}):
        with patch("services.whatsapp_sender.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"messages": [{"id": "wamid.HBgLMTIzNDU2"}]}
            mock_client.post.return_value = mock_resp
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            result_holder = {}
            async def cb_sent(data, **kw): result_holder.update(data)
            params = MagicMock()
            params.arguments = {"date": "tomorrow", "time": "5:00 PM"}
            params.result_callback = cb_sent
            call_id_sent = f"test-wa-sent-{time.time()}"
            lead_mem = {"whatsapp_opt_in": True}

            await execute_book_site_visit(
                params,
                stream_id=call_id_sent,
                call_metrics=MagicMock(),
                spoken_text_guard=MagicMock(),
                lead_memory=lead_mem,
                call_type="web",
            )
            assert result_holder.get("whatsapp_status") == "sent"
            spoken = result_holder.get("message", "")
            assert "on its way" in spoken

            async with get_session() as session:
                sv = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == call_id_sent))).scalars().first()
                assert sv.whatsapp_status == "sent"
                assert sv.whatsapp_message_id == "wamid.HBgLMTIzNDU2"

    # State 3: queued (when background worker is running)
    with patch("leads.outbox.is_outbox_worker_running", return_value=True):
        result_holder = {}
        async def cb_q(data, **kw): result_holder.update(data)
        params = MagicMock()
        params.arguments = {"date": "tomorrow", "time": "5:00 PM"}
        params.result_callback = cb_q
        call_id_q = f"test-wa-queued-{time.time()}"
        lead_mem = {"whatsapp_opt_in": True}

        await execute_book_site_visit(
            params,
            stream_id=call_id_q,
            call_metrics=MagicMock(),
            spoken_text_guard=MagicMock(),
            lead_memory=lead_mem,
            call_type="web",
        )
        assert result_holder.get("whatsapp_status") == "queued"
        spoken = result_holder.get("message", "")
        assert "I'll WhatsApp" in spoken
        assert "WhatsApp" in spoken

        async with get_session() as session:
            sv = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == call_id_q))).scalars().first()
            assert sv.whatsapp_status == "queued"
            outbox_row = (await session.execute(select(OutboxItem).where(OutboxItem.target == "whatsapp"))).scalars().all()
            assert any(item.payload.get("call_id") == call_id_q for item in outbox_row)

    # State 4: failed via Meta error 131031 (paused template)
    with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "valid_token", "WHATSAPP_PHONE_NUMBER_ID": "12345"}):
        with patch("services.whatsapp_sender.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 400
            mock_resp.json.return_value = {
                "error": {
                    "message": "(#131031) All template instances are paused",
                    "type": "OAuthException",
                    "code": 131031,
                    "error_data": {"messaging_product": "whatsapp", "details": "Template paused"},
                }
            }
            mock_client.post.return_value = mock_resp
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            result_holder = {}
            async def cb_fail(data, **kw): result_holder.update(data)
            params = MagicMock()
            params.arguments = {"date": "tomorrow", "time": "5:00 PM"}
            params.result_callback = cb_fail
            call_id_fail = f"test-wa-fail-131031-{time.time()}"
            lead_mem = {"whatsapp_opt_in": True}

            await execute_book_site_visit(
                params,
                stream_id=call_id_fail,
                call_metrics=MagicMock(),
                spoken_text_guard=MagicMock(),
                lead_memory=lead_mem,
                call_type="web",
            )
            assert result_holder.get("whatsapp_status") == "failed"
            spoken = result_holder.get("message", "")
            assert "WhatsApp" not in spoken

            # Verify direct sender returns error_code 131031 and no message_id
            direct_res = await send_whatsapp_location("+919876543210", visit_date="2026-10-08", visit_time="5:00 PM")
            assert direct_res["status"] == "failed"
            assert direct_res["error_code"] == 131031
            assert direct_res["message_id"] is None
            assert direct_res["ok"] is False

    # State 5: failed via HTTP 200 without message_id
    with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "valid_token", "WHATSAPP_PHONE_NUMBER_ID": "12345"}):
        with patch("services.whatsapp_sender.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"messages": []}
            mock_client.post.return_value = mock_resp
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            result_holder_200 = {}
            async def cb_fail_200(data, **kw): result_holder_200.update(data)
            params_200 = MagicMock()
            params_200.arguments = {"date": "tomorrow", "time": "5:00 PM"}
            params_200.result_callback = cb_fail_200
            call_id_200 = f"test-wa-fail-200-{time.time()}"

            await execute_book_site_visit(
                params_200,
                stream_id=call_id_200,
                call_metrics=MagicMock(),
                spoken_text_guard=MagicMock(),
                lead_memory={"whatsapp_opt_in": True},
                call_type="web",
            )
            assert result_holder_200.get("whatsapp_status") == "failed"
            spoken_200 = result_holder_200.get("message", "")
            assert "WhatsApp" not in spoken_200

            direct_res_200 = await send_whatsapp_location("+919876543210", visit_date="2026-10-08", visit_time="5:00 PM")
            assert direct_res_200["status"] == "failed"
            assert direct_res_200["message_id"] is None

    # State 6: 401/403 invalid token -> failed with authentication error
    with patch.dict(os.environ, {"WHATSAPP_ACCESS_TOKEN": "invalid_token", "WHATSAPP_PHONE_NUMBER_ID": "12345"}):
        with patch("services.whatsapp_sender.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 401
            mock_resp.json.return_value = {
                "error": {
                    "message": "Invalid OAuth access token.",
                    "type": "OAuthException",
                    "code": 190,
                }
            }
            mock_resp.text = "Unauthorized"
            mock_client.post.return_value = mock_resp
            mock_client_cls.return_value.__aenter__.return_value = mock_client

            auth_res = await send_whatsapp_location("+919876543210", visit_date="2026-10-08", visit_time="5:00 PM")
            assert auth_res["status"] == "failed"
            assert "authentication error" in auth_res["error"].lower()

    # Delivery status webhook updates Outbox row to failed
    from leads.models import Lead
    from leads.outbox import queue_outbox_item, update_outbox_whatsapp_delivery_status
    test_wamid = f"wamid.DELIVERY_FAIL_{time.time()}"
    async with get_session() as session:
        lead = Lead(name="Test Webhook Lead", phone="+919876543210", status="new", source="web-test")
        session.add(lead)
        await session.flush()
        outbox_item = await queue_outbox_item(
            lead_id=lead.id,
            target="whatsapp",
            payload={"phone": "+919876543210", "message_id": test_wamid},
            session=session,
        )
        outbox_item.status = "done"

    # Simulate delivery status webhook arriving with status="failed"
    updated = await update_outbox_whatsapp_delivery_status(test_wamid, status="failed", error_info="Meta error 131031 (delivery dropped)")
    assert updated is True

    async with get_session() as session:
        item = (await session.execute(select(OutboxItem).where(OutboxItem.target == "whatsapp"))).scalars().all()
        matching = [i for i in item if isinstance(i.payload, dict) and i.payload.get("message_id") == test_wamid]
        assert len(matching) == 1
        assert matching[0].status == "failed"
        assert "delivery dropped" in (matching[0].last_error or "")



# ==============================================================================
# AUDIT ITEM 5: Working Memory Retention After Pruning to 3 Messages
# ==============================================================================

def test_working_memory_retains_all_fields_after_pruning_to_3():
    from bot import _prune_history
    lead_memory = {
        "client": "Alex",
        "configuration": "3 BHK Large (with study & 2 balconies)",
        "visit_date_iso": "2026-10-08",
        "time_slot": "5:00 PM",
        "disposition": "SITE_VISIT_BOOKED",
    }

    messages = [
        {"role": "system", "content": "You are Ananya, a real estate advisor."},
        {"role": "user", "content": "Hi there"},
        {"role": "assistant", "content": "Hello! Looking for a 2 or 3 BHK?"},
        {"role": "user", "content": "I want 3 BHK Large"},
        {"role": "assistant", "content": "Great choice. Would you like to visit?"},
        {"role": "user", "content": "Yes, tomorrow at 5 pm"},
        {"role": "assistant", "content": "Confirmed."},
    ]

    _sync_working_memory(messages, lead_memory)
    _prune_history(messages, 3)
    _sync_working_memory(messages, lead_memory)

    # Two system messages are required to preserve the immutable shared prefix.
    assert len(messages) <= 4
    assert messages[0]["content"] == "You are Ananya, a real estate advisor."
    sys_content = messages[1]["content"]
    assert "[ACTIVE LEAD STATE:" in sys_content
    # Must retain configuration, name, date, and time
    assert "3 BHK Large" in sys_content
    assert "Alex" in sys_content
    assert "2026-10-08" in sys_content
    assert "5:00 PM" in sys_content


# ==============================================================================
# AUDIT ITEM 9: Missing Unit Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_book_site_visit_idempotency_by_call_id():
    """Verify idempotency by call_id: second call returns existing booking without creating duplicate row."""
    from leads.db import get_session
    from leads.models import SiteVisit
    from sqlalchemy import select, func

    call_id = f"test-idemp-{time.time()}"
    lead_mem = {}

    result1 = {}
    async def cb1(data, **kw): result1.update(data)
    p1 = MagicMock()
    p1.arguments = {"date": "tomorrow", "time": "5:00 PM"}
    p1.result_callback = cb1

    await execute_book_site_visit(p1, stream_id=call_id, lead_memory=lead_mem, call_type="web")
    assert result1.get("status") == "confirmed"

    # Second call with same call_id
    result2 = {}
    async def cb2(data, **kw): result2.update(data)
    p2 = MagicMock()
    p2.arguments = {"date": "tomorrow", "time": "5:00 PM"}
    p2.result_callback = cb2

    await execute_book_site_visit(p2, stream_id=call_id, lead_memory=lead_mem, call_type="web")
    assert result2.get("status") == "confirmed"

    async with get_session() as session:
        count = (await session.execute(
            select(func.count(SiteVisit.id)).where(SiteVisit.call_id == call_id)
        )).scalar_one()
        assert count == 1, f"Expected exactly 1 SiteVisit row, found {count}"


@pytest.mark.asyncio
async def test_book_site_visit_needs_phone_on_real_call():
    """Verify needs_phone rejection on real telephony call without phone."""
    result = {}
    async def cb(data, **kw): result.update(data)
    p = MagicMock()
    p.arguments = {"date": "tomorrow", "time": "5:00 PM"}
    p.result_callback = cb

    with patch.dict(os.environ, {"LOCAL_DEMO": "false"}):
        await execute_book_site_visit(
            p,
            stream_id=f"test-real-nophone-{time.time()}",
            call_type="inbound",
            lead_memory={},
        )
    assert result.get("status") == "needs_phone"


@pytest.mark.asyncio
async def test_book_site_visit_missing_time_fails_closed():
    """Verify that omitting time returns needs_confirmation and does not book."""
    result = {}
    async def cb(data, **kw): result.update(data)
    p = MagicMock()
    p.arguments = {"date": "tomorrow", "time": ""}
    p.result_callback = cb

    await execute_book_site_visit(p, stream_id="test-notime", lead_memory={})
    assert result.get("status") == "needs_confirmation"
    assert "time" in result.get("missing", [])


@pytest.mark.asyncio
async def test_book_site_visit_disposition_equals_tool_result():
    """Verify that successful booking sets disposition to SITE_VISIT_BOOKED."""
    lead_mem = {}
    call_id = f"test-disp-{time.time()}"
    p = MagicMock()
    p.arguments = {"date": "tomorrow", "time": "4:00 PM"}
    p.result_callback = AsyncMock()

    await execute_book_site_visit(p, stream_id=call_id, lead_memory=lead_mem, call_type="web")
    assert lead_mem.get("disposition") == "SITE_VISIT_BOOKED"


@pytest.mark.asyncio
async def test_book_site_visit_whatsapp_opt_in_yes_no():
    """Verify that opt-in yes vs no sets whatsapp_opt_in correctly."""
    from leads.db import get_session
    from leads.models import SiteVisit
    from sqlalchemy import select

    # Case 1: Opt-in YES
    call_id_yes = f"test-optin-yes-{time.time()}"
    p_yes = MagicMock()
    p_yes.arguments = {"date": "tomorrow", "time": "2:00 PM"}
    p_yes.result_callback = AsyncMock()
    await execute_book_site_visit(p_yes, stream_id=call_id_yes, lead_memory={"whatsapp_opt_in": True}, call_type="web")

    async with get_session() as session:
        sv_yes = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == call_id_yes))).scalars().first()
        assert sv_yes.whatsapp_opt_in is True

    # Case 2: Opt-in NO
    call_id_no = f"test-optin-no-{time.time()}"
    p_no = MagicMock()
    p_no.arguments = {"date": "tomorrow", "time": "2:00 PM"}
    p_no.result_callback = AsyncMock()
    await execute_book_site_visit(p_no, stream_id=call_id_no, lead_memory={}, call_type="web")

    async with get_session() as session:
        sv_no = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == call_id_no))).scalars().first()
        assert sv_no.whatsapp_opt_in is False
        assert sv_no.whatsapp_status == "not_requested"


@pytest.mark.asyncio
async def test_outbox_done_only_with_message_id():
    """Verify outbox item transitions to done ONLY when message_id is present."""
    from leads.db import get_session
    from leads.models import OutboxItem, Lead
    from leads.outbox import queue_outbox_item, drain_outbox

    async with get_session() as session:
        lead = Lead(name="Test Outbox", phone="+919876543210", status="new", source="web-test")
        session.add(lead)
        await session.commit()
        lead_id = lead.id

    # Queue item
    item = await queue_outbox_item(
        lead_id=lead_id,
        target="whatsapp",
        payload={"phone": "+919876543210", "visit_date": "2026-10-08", "time_slot": "5:00 PM"},
    )
    assert item.status == "pending"

    # Drain with no message id (failure/unconfigured)
    with patch("services.whatsapp_sender.send_whatsapp_location", return_value={"ok": False, "error": "simulated"}):
        await drain_outbox()
        async with get_session() as session:
            db_item = await session.get(OutboxItem, item.id)
            assert db_item.status != "done"

    # Drain with valid message_id
    with patch("services.whatsapp_sender.send_whatsapp_location", return_value={"ok": True, "message_id": "wamid.VALID123"}):
        # Reset attempt count and next_attempt_at so it can be retried immediately
        async with get_session() as session:
            db_item = await session.get(OutboxItem, item.id)
            db_item.next_attempt_at = None
            db_item.status = "pending"
            await session.commit()

        await drain_outbox()
        async with get_session() as session:
            db_item = await session.get(OutboxItem, item.id)
            assert db_item.status == "done"
            assert db_item.payload.get("message_id") == "wamid.VALID123"


@pytest.mark.asyncio
async def test_groq_failed_generation_retry_then_fallback():
    """Verify that on Groq failed_generation error:
    1. Primary is retried exactly once (total 2 calls to primary).
    2. Fallback to Cerebras is triggered with qwen-3.8-27b and reasoning_effort=none.
    3. Plain-text fallback response is emitted (no tool calls).
    4. NO booking is made, no SiteVisit inserted, and NO WhatsApp claim spoken.
    """
    from bot import _attach_resilient_failover, _SpokenTextGuard
    from leads.db import get_session
    from leads.models import SiteVisit
    from sqlalchemy import select

    mock_service = MagicMock()
    mock_service.supports_developer_role = False
    mock_service._settings = MagicMock()
    mock_service._settings.system_instruction = "You are Ananya."

    mock_adapter = MagicMock()
    mock_adapter.get_llm_invocation_params.return_value = {"messages": [{"role": "user", "content": "Book tomorrow 5pm"}]}
    mock_service.get_llm_adapter.return_value = mock_adapter
    mock_service.build_chat_completion_params.return_value = {
        "messages": [{"role": "user", "content": "Book tomorrow 5pm"}]
    }

    primary_call_count = 0
    async def fake_get_chat(context):
        nonlocal primary_call_count
        primary_call_count += 1
        raise Exception("failed_generation: Model failed to call a function 'book_site_visit'")

    mock_service.get_chat_completions = fake_get_chat

    cfg = {"providers": {"llm": {}}}
    _attach_resilient_failover(mock_service, "groq", cfg)

    context_mock = MagicMock()
    context_mock.messages = [{"role": "user", "content": "Book tomorrow 5pm"}]

    # Invoke resilient generator
    res_stream = await mock_service.get_chat_completions(context_mock)
    chunks = []
    async for chunk in res_stream:
        chunks.append(chunk)

    # 1. Assert exactly 1 retry: primary_call_count == 2
    assert primary_call_count == 2, f"Expected exactly 2 attempts on primary, got {primary_call_count}"

    # 2. Assert synthetic fallback chunk contract: tool_calls=None and recovery content
    assert len(chunks) == 1
    fallback_chunk = chunks[0]
    assert fallback_chunk.choices[0].delta.tool_calls is None
    assert fallback_chunk.tool_calls is None
    content = fallback_chunk.choices[0].delta.content
    assert content is not None
    assert "booked" not in content.lower()
    assert "whatsapp" not in content.lower()

    # 3. DB check: no SiteVisit row inserted
    async with get_session() as session:
        svs = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == "test-failover-cerebras"))).scalars().all()
        assert len(svs) == 0


# ==============================================================================
# AUDIT ITEM 3: Deletion Audit Passing Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_deletion_audit_metrics_frame_no_replay_loop():
    """Verify that MetricsFrame deduplication and sanity ceilings prevent replay loops.
    Even if an upstream frame repeats 10 times, it is counted once.
    """
    from pipecat.frames.frames import MetricsFrame
    from pipecat.metrics.metrics import LLMTokenUsage, LLMUsageMetricsData
    metrics = CallMetricsCollector(call_id="test-metrics-loop", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")

    # Simulate identical MetricsFrame re-injected multiple times (as occurred in web-c963cb8a)
    token_usage = LLMTokenUsage(prompt_tokens=1858, completion_tokens=34, total_tokens=1892)
    data_item = LLMUsageMetricsData(processor="llm", model="qwen", value=token_usage)
    frame = MetricsFrame(data=[data_item])

    for _ in range(10):
        await metrics.process_frame(frame, FrameDirection.DOWNSTREAM)

    summary = metrics.summary()
    # Deduplication ensures prompt tokens are counted exactly once, not 10 times
    assert summary["llm_prompt_tokens"] == 1858
    assert summary["llm_completion_tokens"] == 34


@pytest.mark.asyncio
async def test_deletion_audit_purge_at_next_utterance_eliminated():
    """Verify that removing purge-at-next-utterance prevents dropping valid bot audio
    on non-interruption audio frames, and only drops on true InterruptionFrame.
    """
    gate = _InterruptionAudioGate(stream_id="test-gate-purge")
    gate._active_playing_gen_id = 1
    gate._current_gen_id = 1

    # Normal audio frame downstream is processed and not dropped
    audio_frame = TTSAudioRawFrame(audio=b"\x00" * 320, sample_rate=16000, num_channels=1)
    await gate.process_frame(audio_frame, FrameDirection.DOWNSTREAM)
    assert gate._dropped_frames == 0

    # True InterruptionFrame purges stale state
    from pipecat.frames.frames import InterruptionFrame
    await gate.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert gate.is_interrupted is True

    # Subsequent stale audio frame is dropped
    await gate.process_frame(audio_frame, FrameDirection.DOWNSTREAM)
    assert gate._dropped_frames == 1


@pytest.mark.asyncio
async def test_deletion_audit_recovery_frame_repeat_eliminated():
    """Verify that failed LLM responses do not dispatch infinite recovery loops."""
    metrics = CallMetricsCollector(call_id="test-recovery", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    metrics.record_tool_call("book_site_visit", success=False, latency_ms=100.0)
    summary = metrics.summary()
    assert summary["tool_calls_failed"] == 1
    assert summary["tool_calls_ok"] == 0


# ==============================================================================
# V5.1 HARDENING TESTS (Live Call web-2183a9b5 Verification & Protections)
# ==============================================================================

@pytest.mark.asyncio
async def test_v5_1_cerebras_adapter_coalesces_exact_two_system_payload():
    """Verify that when primary LLM fails, failover yields the synthetic recovery chunk
    with tool_calls=None and does not attempt calling removed Cerebras client.
    """
    from bot import _attach_resilient_failover

    mock_service = MagicMock()
    mock_service.supports_developer_role = False
    mock_service._settings = MagicMock()
    mock_service._settings.system_instruction = None

    class FakeAdapter:
        def get_llm_invocation_params(self, ctx, **kwargs):
            return {"messages": ctx.messages}

    mock_service.get_llm_adapter.return_value = FakeAdapter()
    mock_service.build_chat_completion_params = lambda p: dict(p)

    # Groq fails immediately with TimeoutError
    async def fake_get_chat(ctx):
        raise asyncio.TimeoutError()

    mock_service.get_chat_completions = fake_get_chat

    cfg = {"providers": {"llm": {}}}
    _attach_resilient_failover(mock_service, "groq", cfg)

    sys_instructions = "Role: Ananya, Senior Property Advisor, Meridian Group."
    facts_block = "CALL CONTEXT: You are calling Alex.\n[ACTIVE LEAD STATE: Client: Alex]"
    user_msg = "Can I come tomorrow at 2?"

    orig_messages = [
        {"role": "system", "content": sys_instructions},
        {"role": "system", "content": facts_block},
        {"role": "user", "content": user_msg},
    ]
    context_mock = MagicMock()
    context_mock.messages = list(orig_messages)

    res_stream = await mock_service.get_chat_completions(context_mock)
    chunks = []
    async for chunk in res_stream:
        chunks.append(chunk)

    # 1. Assert fallback yielded synthetic chunk with tool_calls=None
    assert len(chunks) == 1
    recovery_chunk = chunks[0]
    assert recovery_chunk.choices[0].delta.tool_calls is None
    assert recovery_chunk.tool_calls is None

    # 2. Assert original context.messages was NOT modified
    assert len(context_mock.messages) == 3
    assert context_mock.messages[0]["content"] == sys_instructions
    assert context_mock.messages[1]["content"] == facts_block
    assert context_mock.messages[2]["content"] == user_msg


def test_v5_1_three_bhk_does_not_parse_as_three_am():
    """Verify that 'Yeah I am looking for a 3 BHK.' resolves to None (NOT 3:00 AM).
    Enforces that 'am' inside 'I am' and BHK numbers do not trigger false time parsing.
    """
    # Exact utterance from live call
    assert normalize_visit_time("Yeah I am looking for a 3 BHK.") is None
    assert normalize_visit_time("looking for 3 bhk") is None
    assert normalize_visit_time("I am looking for a 3 BHK flat") is None
    assert normalize_visit_time("two bedroom flat") is None

    # Valid times resolve accurately
    assert normalize_visit_time("Can I come tomorrow at 2?") == "2:00 PM"
    assert normalize_visit_time("Two.") == "2:00 PM"
    assert normalize_visit_time("at 2") == "2:00 PM"
    assert normalize_visit_time("5 baje") == "5:00 PM"
    assert normalize_visit_time("10 am") == "10:00 AM"
    assert normalize_visit_time("11:30 am") == "11:30 AM"


def test_v5_1_language_switching_fillers_do_not_flip_language():
    """Verify that fillers ('Oh.', 'Yeah.', 'Okay.') do not flip language or count toward candidate turns."""
    from language_state import LanguageState

    ls = LanguageState(current_language="hi", supported_languages=frozenset({"en", "hi", "te"}), sustained_switch_turns=2)
    assert ls.current_language == "hi"

    # Caller says filler "Oh." with STT detected language "en"
    lang, switched = ls.observe_stt(language="en", text="Oh.")
    assert lang == "hi"
    assert switched is False
    assert ls.consecutive_candidate_turns == 0

    # Caller says filler "Yeah." with STT detected language "en"
    lang, switched = ls.observe_stt(language="en", text="Yeah.")
    assert lang == "hi"
    assert switched is False
    assert ls.consecutive_candidate_turns == 0

    # Meaningful English sentence: turn 1
    lang, switched = ls.observe_stt(language="en", text="Can I visit tomorrow at two?")
    assert lang == "hi"
    assert switched is False
    assert ls.consecutive_candidate_turns == 1

    # Second meaningful English sentence: turn 2 -> switches
    lang, switched = ls.observe_stt(language="en", text="What is the price of three BHK?")
    assert lang == "en"
    assert switched is True


@pytest.mark.asyncio
async def test_v5_1_dual_llm_failure_yields_recovery_line():
    """Verify that when BOTH Groq and Cerebras fail, a deterministic recovery line is spoken
    directly through synthetic chunk, bounded per call, and does not freeze into silence.
    """
    from bot import _attach_resilient_failover

    mock_service = MagicMock()
    mock_service.supports_developer_role = False
    mock_service._settings = MagicMock()
    mock_service._settings.system_instruction = None

    class FakeAdapter:
        def get_llm_invocation_params(self, ctx, **kwargs):
            return {"messages": ctx.messages}

    mock_service.get_llm_adapter.return_value = FakeAdapter()
    mock_service.build_chat_completion_params = lambda p: dict(p)

    # Groq fails
    async def fake_get_chat(ctx):
        raise RuntimeError("Groq rate limit 429")

    mock_service.get_chat_completions = fake_get_chat

    cfg = {"providers": {"llm": {"cerebras": {"api_key": "csk-test"}}}}
    with patch.dict(os.environ, {"CEREBRAS_API_KEY": "csk-test"}):
        _attach_resilient_failover(mock_service, "groq", cfg)

    # Cerebras also fails
    with patch("openai.AsyncOpenAI") as mock_openai_cls:
        mock_fb_client = MagicMock()
        mock_fb_client.chat.completions.create = AsyncMock(side_effect=RuntimeError("Cerebras connection failed"))
        mock_openai_cls.return_value = mock_fb_client

        context_mock = MagicMock()
        context_mock._superseded = False
        context_mock._is_interrupted = False
        context_mock.messages = [{"role": "user", "content": "Hello"}]
        context_mock.lead_memory = {"bhk": "3 BHK Large"}

        res_stream = await mock_service.get_chat_completions(context_mock)
        chunks = []
        async for chunk in res_stream:
            chunks.append(chunk)

        assert len(chunks) == 1
        content = chunks[0].choices[0].delta.content
        assert "connection trouble" in content.lower()


@pytest.mark.asyncio
async def test_v5_1_farewell_protects_audio_and_honors_late_barge_in():
    """Verify that during closing/grace, AudioGate suppresses clearAudio,
    CallEndCoordinator allows at most 1 extra reply, and reciprocal farewell preserves grace.
    """
    from bot import _CallEndCoordinator, _InterruptionAudioGate
    from pipecat.frames.frames import TranscriptionFrame

    hangup_called = False
    async def fake_hangup():
        nonlocal hangup_called
        hangup_called = True

    coord = _CallEndCoordinator(stream_id="test-coord", on_hangup=fake_hangup, grace_seconds=0.1)
    mock_ws = MagicMock()
    mock_ws.send_text = AsyncMock()
    gate = _InterruptionAudioGate(stream_id="test-gate", websocket=mock_ws, call_end_coordinator=coord)

    # 1. Request call ending
    coord.request_ending()
    assert coord.is_ending is True

    # 2. Closing audio starts speaking
    await coord.process_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)
    assert coord.is_closing_in_progress is True

    # 3. Caller speaks during closing -> InterruptionFrame arrives at gate
    await gate.process_frame(InterruptionFrame(), FrameDirection.UPSTREAM)
    # MUST NOT send clearAudio over websocket while closing statement is active
    assert mock_ws.send_text.called is False

    # 4. Caller says meaningful continuation -> allows 1 extra reply without cancelling hangup
    trans_frame = TranscriptionFrame(text="actually one question about parking", user_id="user", timestamp="2026-10-07T13:14:00Z")
    await coord.process_frame(trans_frame, FrameDirection.DOWNSTREAM)
    assert coord.is_ending is True
    assert coord._extra_reply_count == 1

    # 5. Caller reciprocates farewell -> no second speech; preserve owner-selected window
    farewell_frame = TranscriptionFrame(text="thank you bye bye", user_id="user", timestamp="2026-10-07T13:14:01Z")
    await coord.process_frame(farewell_frame, FrameDirection.DOWNSTREAM)
    assert hangup_called is False
    # Complete a simulated playback then allow the short test grace to elapse.
    from pipecat.frames.frames import BotStoppedSpeakingFrame
    coord._closing_in_progress = True
    await coord.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(0.15)
    assert hangup_called is True


@pytest.mark.asyncio
async def test_v5_1_observability_transcript_preserves_repeated_words():
    """Verify that repeated words across turns ('yes', 'okay') are preserved in full_transcript."""
    from bot import _HistoryPruner

    mock_context = MagicMock()
    full_transcript = []
    pruner = _HistoryPruner(context=mock_context, max_messages=10, stream_id="test-pruner", full_transcript=full_transcript)

    msg1 = {"role": "user", "content": "yes"}
    msg2 = {"role": "assistant", "content": "Great, looking forward to meeting you."}
    msg3 = {"role": "user", "content": "yes"}  # Repeated word on later turn

    mock_context.messages = [msg1, msg2, msg3]
    await pruner.process_frame(LLMContextFrame(mock_context), FrameDirection.DOWNSTREAM)

    assert len(full_transcript) == 3
    assert full_transcript[0]["content"] == "yes"
    assert full_transcript[1]["content"] == "Great, looking forward to meeting you."
    assert full_transcript[2]["content"] == "yes"


@pytest.mark.asyncio
async def test_v5_1_metrics_cached_answer_cohort_and_silence_trim_baseline():
    """Verify cached answer cohort, per-generation silence trimming, and matched baseline."""
    from pipecat.frames.frames import BotStartedSpeakingFrame, ErrorFrame
    metrics = CallMetricsCollector(call_id="test-obs", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")

    # Record cached answer
    metrics.record_cached_answer("opening_intro")
    await metrics.process_frame(BotStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    # Record silence trimming with gen_id
    metrics.record_tts_silence_trimmed(120.0, gen_id=1)
    metrics.record_tts_silence_trimmed(150.0, gen_id=2)

    # Record provider failure once
    metrics.record_provider_failure("groq", "TimeoutError: connection timed out", 1500.0)
    # Subsequent ErrorFrame for same turn must NOT double count
    await metrics.process_frame(ErrorFrame(error="TimeoutError: connection timed out"), FrameDirection.DOWNSTREAM)

    summary = metrics.summary()
    assert summary["cached_answer_count"] == 1
    assert summary["generation_silence_trimmed"][1] == 120.0
    assert summary["generation_silence_trimmed"][2] == 150.0
    assert summary["provider_errors"] == 1  # Exactly 1, not 2
    assert "baseline_before_trim_ttfa_ms" in summary
    assert "effective_after_trim_ttfa_ms" in summary
    assert "silence_saved_ms" in summary


@pytest.mark.asyncio
async def test_spoken_text_guard_streaming_spaces():
    """Verify that streaming tokens ('from', ' 1.7', ' crores.') preserve spaces into 'from 1.7 crores.'
    and the recorded spoken text is exact, single spaced with no split words."""
    guard = _SpokenTextGuard()
    guard._turn_count = 1

    pushed_texts = []
    async def fake_push_frame(frame, direction):
        if isinstance(frame, TextFrame):
            pushed_texts.append(frame.text)

    guard.push_frame = fake_push_frame

    # Start turn
    await guard.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)

    # Stream chunks
    for chunk in ["from", " 1.7", " crores."]:
        await guard.process_frame(TextFrame(text=chunk), FrameDirection.DOWNSTREAM)

    # End turn
    await guard.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)

    combined_tts = "".join(pushed_texts)
    assert combined_tts == "from 1.7 crores."
    assert guard._last_spoken_turn_text == "from 1.7 crores."


@pytest.mark.asyncio
async def test_book_site_visit_rejects_vague_morning_time():
    """Verify that calling book_site_visit with vague time='morning' fails closed."""
    params = MagicMock()
    params.arguments = {"date": "tomorrow", "time": "morning"}
    res_holder = {}
    async def cb(data, **kw): res_holder.update(data)
    params.result_callback = cb

    await execute_book_site_visit(
        params,
        stream_id="test-vague-morning",
        call_metrics=MagicMock(),
        spoken_text_guard=MagicMock(),
        lead_memory={},
        call_type="web",
    )
    assert res_holder.get("status") == "needs_confirmation"
    assert "time" in res_holder.get("message", "").lower()


def test_history_pruner_transcript_content_dedupe():
    """Verify that recreating message dicts across prune cycles does not duplicate turns in full_transcript."""
    full_transcript = []
    context = MagicMock()
    pruner = _HistoryPruner(
        max_messages=8,
        context=context,
        stream_id="test-transcript-dedupe",
        full_transcript=full_transcript,
    )

    # Turn 1
    m1 = {"role": "user", "content": "Hello"}
    m2 = {"role": "assistant", "content": "Hi there"}
    context.messages = [m1, m2]
    pruner._record_full_transcript()
    assert len(full_transcript) == 2

    # Prune cycle recreates dicts (simulating _coalesce_consecutive_messages)
    context.messages = [dict(m1), dict(m2)]
    pruner._record_full_transcript()
    assert len(full_transcript) == 2  # No duplicate!

    # Turn 2 adds new messages with recreated dicts for previous
    m3 = {"role": "user", "content": "3 BHK price?"}
    context.messages = [dict(m1), dict(m2), m3]
    pruner._record_full_transcript()
    assert len(full_transcript) == 3
    assert full_transcript[-1]["content"] == "3 BHK price?"


