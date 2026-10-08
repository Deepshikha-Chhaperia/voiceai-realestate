"""
Unit tests for root-cause hardening fixes:
1. SarvamSmartTurnStopStrategy contract and inference task cancellation.
2. Booking silence fix, deterministic confirmation tagging, WhatsApp opt-in question preservation.
3. Whitespace chunk preservation through SpokenTextGuard to TTS.
4. CallAnalytics idempotency.
5. Outbox queue idempotency & truthful sheets result handling.
6. Dual-anchor latency metric collection and reporting.
"""

import asyncio
import os
import sys
import time
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from bot import (
    SarvamSmartTurnStopStrategy,
    _SpokenTextGuard,
)
from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    TextFrame,
    TranscriptionFrame,
    TTSSpeakFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from metrics_collector import CallMetricsCollector
import call_analytics
from leads import outbox
from leads.models import OutboxItem


class DummyTurnAnalyzer:
    async def analyze_end_of_turn(self):
        return None, None


@pytest.mark.asyncio
async def test_sarvam_smart_turn_stop_strategy_contract_and_cancellation():
    analyzer = DummyTurnAnalyzer()
    strategy = SarvamSmartTurnStopStrategy(turn_analyzer=analyzer)
    
    # Check property contract
    assert strategy.resolves_proposed_turn_stop_frames is True

    # Simulate an active inference task
    async def dummy_slow_coro():
        await asyncio.sleep(10)

    task = asyncio.create_task(dummy_slow_coro())
    strategy._analysis_task = task
    assert not task.done()

    # Call cancel_inference
    strategy.cancel_inference()
    assert strategy._analysis_task is None
    assert task.cancelling() or task.cancelled()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_spoken_text_guard_deterministic_confirmation_not_dropped():
    lead_memory = {}
    guard = _SpokenTextGuard(lead_memory=lead_memory)
    mock_task = MagicMock()
    queued_frames = []

    async def mock_queue(frames):
        queued_frames.extend(frames)

    mock_task.queue_frames = mock_queue
    guard._task = mock_task

    confirm_msg = "Site visit confirmed for Sunday at 11 AM. Shall I send the details on WhatsApp?"
    guard.mark_tool_succeeded("book_site_visit", whatsapp_confirmed=False, confirm_msg=confirm_msg)

    assert guard._site_visit_succeeded_this_turn is True
    assert guard._confirmation_queued_to_tts is True

    # Allow asyncio.create_task in mark_tool_succeeded to yield
    await asyncio.sleep(0.01)
    assert len(queued_frames) == 1
    conf_frame = queued_frames[0]
    assert isinstance(conf_frame, TTSSpeakFrame)
    assert getattr(conf_frame, "is_deterministic_confirmation", False) is True

    # Pass the confirmation frame through process_frame
    pushed = []
    guard.push_frame = AsyncMock(side_effect=lambda f, d: pushed.append(f))

    await guard.process_frame(conf_frame, FrameDirection.DOWNSTREAM)
    # Ensure text is not wiped out
    assert conf_frame.text != ""
    assert "Site visit confirmed" in conf_frame.text
    assert "Shall I send the details on WhatsApp" in conf_frame.text
    assert guard._confirmation_speaking is True

    # Simulate BotStoppedSpeakingFrame
    bot_stopped = BotStoppedSpeakingFrame()
    await guard.process_frame(bot_stopped, FrameDirection.DOWNSTREAM)
    assert guard._confirmation_actually_played is True
    assert guard._confirmation_spoken is True
    assert lead_memory.get("confirmation_spoken") is True


@pytest.mark.asyncio
async def test_spoken_text_guard_interruption_resets_confirmation_speaking():
    lead_memory = {}
    guard = _SpokenTextGuard(lead_memory=lead_memory)
    guard._confirmation_queued_to_tts = True
    guard._confirmation_speaking = True
    guard._confirmation_actually_played = False

    # Caller interrupts before playback finishes
    user_started = UserStartedSpeakingFrame()
    await guard.process_frame(user_started, FrameDirection.DOWNSTREAM)

    assert guard._confirmation_queued_to_tts is False
    assert guard._confirmation_speaking is False
    assert guard._confirmation_actually_played is False
    assert lead_memory.get("confirmation_spoken") is None


def test_filter_unverified_claims_preserves_whatsapp_opt_in_question():
    guard = _SpokenTextGuard()
    guard._whatsapp_succeeded_this_turn = False

    # A statement claiming WhatsApp was already sent should be dropped
    stmt = "I have sent the location on WhatsApp."
    filtered_stmt = guard._filter_unverified_claims(stmt)
    assert filtered_stmt == ""

    # An opt-in question asking if details can be sent should NOT be dropped
    question = "Shall I send the details on WhatsApp?"
    filtered_q = guard._filter_unverified_claims(question)
    assert filtered_q == question

    question_modal = "Can I send the brochure on WhatsApp?"
    assert guard._filter_unverified_claims(question_modal) == question_modal


@pytest.mark.asyncio
async def test_spoken_text_guard_preserves_whitespace_only_text_frames():
    guard = _SpokenTextGuard()
    pushed = []
    guard.push_frame = AsyncMock(side_effect=lambda f, d: pushed.append(f))

    # Simulate streaming tokens after leading buffer is already flushed
    guard._leading_flushed = True

    f_word1 = TextFrame(text="The")
    f_space = TextFrame(text=" ")
    f_word2 = TextFrame(text="3 BHK")

    await guard.process_frame(f_word1, FrameDirection.DOWNSTREAM)
    await guard.process_frame(f_space, FrameDirection.DOWNSTREAM)
    await guard.process_frame(f_word2, FrameDirection.DOWNSTREAM)

    texts = [f.text for f in pushed if isinstance(f, TextFrame)]
    assert texts == ["The", " ", "3 BHK"]


@pytest.mark.asyncio
async def test_call_analytics_idempotency(tmp_path):
    call_id = "test-call-idemp-1"
    db_file = str(tmp_path / "calls.db")

    with patch("lead_state.DB_PATH", db_file):
        import lead_state
        lead_state.init_db()
        await lead_state.upsert_call_async(call_id)
        await lead_state.finalize_call_async(
            call_id,
            analysis={"summary": "Pre-existing analysis", "sentiment": "positive"},
        )

        with patch("call_analytics._call_llm", new_callable=AsyncMock) as mock_llm:
            result = await call_analytics.analyze_call(call_id)
            assert result is not None
            assert result.get("analysis", {}).get("summary") == "Pre-existing analysis"
            # Ensure LLM was NOT invoked because analysis already existed
            mock_llm.assert_not_called()


@pytest.mark.asyncio
async def test_outbox_queue_call_id_idempotency(tmp_path):
    call_id = "test-call-outbox-1"
    test_data_dir = str(tmp_path)

    with patch.dict(os.environ, {"DATA_DIR": test_data_dir}):
        from leads import db
        from leads.models import Lead
        db._async_engine = None
        db._async_session_factory = None
        await db.init_models()

        async with db.get_session() as sess:
            lead = Lead(phone="+919999999999", source="test")
            sess.add(lead)
            await sess.flush()
            lead_id = lead.id

        # Queue once
        item1 = await outbox.queue_outbox_item(
            lead_id=lead_id,
            target="sheets",
            payload={"call_id": call_id, "name": "Alex", "phone": "+919999999999"},
        )
        assert item1 is not None

        # Queue duplicate with same call_id and target
        item2 = await outbox.queue_outbox_item(
            lead_id=lead_id,
            target="sheets",
            payload={"call_id": call_id, "name": "Alex", "phone": "+919999999999"},
        )
        # Must return the existing item, not duplicate
        assert item2.id == item1.id


@pytest.mark.asyncio
async def test_outbox_truthful_sheets_result_does_not_mark_done(tmp_path):
    call_id = "test-call-sheets-1"
    test_data_dir = str(tmp_path)

    with patch.dict(os.environ, {"DATA_DIR": test_data_dir, "GOOGLE_SHEETS_SPREADSHEET_ID": "sheet-123"}):
        from leads import db
        from leads.models import Lead
        db._async_engine = None
        db._async_session_factory = None
        await db.init_models()

        async with db.get_session() as sess:
            lead = Lead(phone="+919999999999", source="test")
            sess.add(lead)
            await sess.flush()
            lead_id = lead.id

        mock_gs_mod = ModuleType("google_sheets_export")
        # Simulates sheets export returning False (unconfigured or failed)
        mock_gs_mod.export_call_to_sheet = MagicMock(return_value=False)

        with patch.dict(sys.modules, {"google_sheets_export": mock_gs_mod}):
            item = await outbox.queue_outbox_item(
                lead_id=lead_id,
                target="sheets",
                payload={"call_id": call_id, "name": "Test"},
            )

            # Drain outbox
            await outbox.drain_outbox()

            async with db.get_session() as sess:
                from sqlalchemy import select
                updated_item = (await sess.execute(
                    select(OutboxItem).where(OutboxItem.id == item.id)
                )).scalar_one()

                # status must NOT be 'done' since export returned False
                assert updated_item.status != "done"
                assert updated_item.attempts == 1


@pytest.mark.asyncio
async def test_metrics_collector_dual_anchor_latency():
    collector = CallMetricsCollector(
        call_id="call-dual-anchor",
        stt_provider="sarvam",
        llm_provider="groq",
        tts_provider="sarvam",
    )
    pushed = []
    collector.push_frame = AsyncMock(side_effect=lambda f, d: pushed.append(f))

    # User stops speaking at t0
    f_stop = UserStoppedSpeakingFrame()
    await collector.process_frame(f_stop, FrameDirection.DOWNSTREAM)

    # Final transcript arrives 50ms later
    await asyncio.sleep(0.05)
    f_trans = TranscriptionFrame(text="Hello", user_id="user", timestamp=time.time())
    await collector.process_frame(f_trans, FrameDirection.DOWNSTREAM)

    # Bot starts speaking 50ms later
    await asyncio.sleep(0.05)
    f_bot = BotStartedSpeakingFrame()
    await collector.process_frame(f_bot, FrameDirection.DOWNSTREAM)

    data = collector.summary()
    assert "avg_transcript_to_audio_ms" in data
    assert "avg_speech_stop_to_audio_ms" in data
    # Speech stop latency must be greater than transcript latency
    assert data["avg_speech_stop_to_audio_ms"] > data["avg_transcript_to_audio_ms"]
