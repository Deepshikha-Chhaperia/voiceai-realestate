"""
Tests for BUG A (visit date normalization) and the extractor overwrite guard.
Fixed fake now_ist = 2026-10-04 10:00 IST (Sunday).
"""
import sys
import os

import pytest

# Ensure voice-bot/ is on path so imports resolve without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime
import zoneinfo

from leads.worker import normalize_visit_date, _resolve_visit_datetime

_IST = zoneinfo.ZoneInfo("Asia/Kolkata")
# Fixed anchor: Sunday 4 Oct 2026, 10:00 IST (weekday index 6)
NOW_IST = datetime(2026, 10, 4, 10, 0, tzinfo=_IST)


# ---------------------------------------------------------------------------
# normalize_visit_date — basic English
# ---------------------------------------------------------------------------

def test_today_english():
    iso, label = normalize_visit_date("today", NOW_IST)
    assert iso == "2026-10-04"
    assert "Today" in label


def test_tomorrow_english():
    iso, label = normalize_visit_date("tomorrow", NOW_IST)
    assert iso == "2026-10-05"
    assert "Tomorrow" in label


def test_saturday_next_occurrence():
    # Now is Sunday 4 Oct; next Saturday is 10 Oct
    iso, label = normalize_visit_date("saturday", NOW_IST)
    assert iso == "2026-10-10"
    assert "Saturday" in label


def test_sunday_skips_today():
    # Now is Sunday; "sunday" must mean next Sunday (7 days ahead), not today
    iso, label = normalize_visit_date("sunday", NOW_IST)
    assert iso == "2026-10-11"


def test_monday():
    iso, label = normalize_visit_date("monday", NOW_IST)
    assert iso == "2026-10-05"  # tomorrow is Monday


# ---------------------------------------------------------------------------
# normalize_visit_date — Hindi/Hinglish
# ---------------------------------------------------------------------------

def test_kal_tomorrow():
    iso, label = normalize_visit_date("kal", NOW_IST)
    assert iso == "2026-10-05"
    assert "Tomorrow" in label


def test_hindi_kal():
    iso, label = normalize_visit_date("कल", NOW_IST)
    assert iso == "2026-10-05"


def test_aaj_today():
    iso, label = normalize_visit_date("aaj", NOW_IST)
    assert iso == "2026-10-04"


def test_hindi_aaj():
    iso, label = normalize_visit_date("आज", NOW_IST)
    assert iso == "2026-10-04"


def test_parso():
    iso, label = normalize_visit_date("parso", NOW_IST)
    assert iso == "2026-10-06"


def test_hindi_parso():
    iso, label = normalize_visit_date("परसों", NOW_IST)
    assert iso == "2026-10-06"


# ---------------------------------------------------------------------------
# normalize_visit_date — returns None on unparseable input (no fallback)
# ---------------------------------------------------------------------------

def test_gibberish_returns_none():
    assert normalize_visit_date("random gobbledygook", NOW_IST) is None


def test_bare_number_returns_none():
    # "10" must not match inside "100" — whole-word; also no day semantics
    assert normalize_visit_date("10", NOW_IST) is None


def test_bare_2_returns_none():
    # "2" must not match inside "12"
    assert normalize_visit_date("2", NOW_IST) is None


def test_empty_returns_none():
    assert normalize_visit_date("", NOW_IST) is None


def test_ambiguous_phrase_returns_none():
    # "sometime next month" has no specific day
    assert normalize_visit_date("sometime next month", NOW_IST) is None


# ---------------------------------------------------------------------------
# _resolve_visit_datetime — end-to-end with Hindi input
# ---------------------------------------------------------------------------

def test_resolve_kal_4pm():
    from datetime import datetime as _dt
    import zoneinfo as _zi
    _actual_now = _dt.now(_zi.ZoneInfo("Asia/Kolkata"))
    expected_iso = (_actual_now.date() + __import__("datetime").timedelta(days=1)).isoformat()
    dt = _resolve_visit_datetime("कल", "4 PM")
    assert dt.date().isoformat() == expected_iso
    assert dt.hour == 16


def test_resolve_saturday_11am():
    from datetime import datetime as _dt, timedelta as _td
    import zoneinfo as _zi
    _actual_now = _dt.now(_zi.ZoneInfo("Asia/Kolkata"))
    days_ahead = (5 - _actual_now.weekday()) % 7  # 5 = Saturday
    if days_ahead == 0:
        days_ahead = 7
    expected_iso = (_actual_now.date() + _td(days=days_ahead)).isoformat()
    dt = _resolve_visit_datetime("saturday", "11:00 AM")
    assert dt.date().isoformat() == expected_iso
    assert dt.hour == 11


# ---------------------------------------------------------------------------
# _extract_lead_preferences + _sync_working_memory overwrite guard
# ---------------------------------------------------------------------------

def test_extractor_does_not_overwrite_tool_set_date():
    """Once book_site_visit stores preferred_visit_date, regex must not overwrite it."""
    # Import here to avoid triggering lifespan in main
    from bot import _extract_lead_preferences, _sync_working_memory  # noqa: E402

    # Simulate tool having already stored a normalized label
    lead_memory = {
        "preferred_visit_date": "Tomorrow — 05 Oct 2026",
        "preferred_visit_time": "4 PM",
    }

    # User next turn says "kal theek hai" — extractor would see "kal" → Tomorrow
    messages = [
        {"role": "user", "content": "kal theek hai"},
        {"role": "assistant", "content": "Great, I have scheduled your visit for tomorrow."},
    ]
    _sync_working_memory(messages, lead_memory)

    # Date must be unchanged (tool value wins)
    assert lead_memory["preferred_visit_date"] == "Tomorrow — 05 Oct 2026"
    assert lead_memory["preferred_visit_time"] == "4 PM"


def test_extractor_sets_date_when_not_already_set():
    """Without a tool-set value, extractor should populate the field."""
    from bot import _sync_working_memory  # noqa: E402

    lead_memory: dict = {}
    messages = [
        {"role": "user", "content": "saturday works for me"},
        {"role": "assistant", "content": "Saturday it is!"},
    ]
    _sync_working_memory(messages, lead_memory)
    assert "preferred_visit_date" in lead_memory


# ---------------------------------------------------------------------------
# FIX 1 — StallWatchdog fake-interruption re-delivery guards
#
# Test harness: drive StallWatchdog._arm_resume / _disarm_resume / _resume_watch
# directly without a full pipeline.  We replace the asyncio pipeline task with a
# lightweight mock that records every frame queued to it.
# ---------------------------------------------------------------------------

import asyncio
import sys

import pytest


class _FakeTask:
    """Records TTSSpeakFrame texts passed via queue_frames."""
    def __init__(self):
        self.queued: list[str] = []

    async def queue_frames(self, frames):
        for f in frames:
            text = getattr(f, "text", None)
            if text is not None:
                self.queued.append(text)


def _make_watchdog(last_text="Hello, how can I help?", turn_in_flight=False):
    """Build a StallWatchdog wired to a fake task, with controllable state."""
    from stall_watchdog import StallWatchdog

    task = _FakeTask()
    wd = StallWatchdog(
        stream_id="test-stream",
        on_repeated_stall=lambda sid: asyncio.sleep(0),
        is_call_ending=lambda: False,
        is_turn_in_flight=lambda: turn_in_flight,
        get_last_interrupted_text=lambda: last_text,
    )
    wd._task = task
    return wd, task


@pytest.mark.asyncio
async def test_fix1_interruption_then_real_transcript_zero_redeliveries():
    """Interruption followed by a real transcript must produce zero re-deliveries."""
    from pipecat.frames.frames import InterruptionFrame, TranscriptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    wd, task = _make_watchdog()

    # Simulate InterruptionFrame → arms the 2.5s timer
    intr = InterruptionFrame()
    await wd.process_frame(intr, FrameDirection.DOWNSTREAM)
    assert wd._resume_task is not None and not wd._resume_task.done()

    # Simulate real transcript arriving immediately (disarms timer)
    tx = TranscriptionFrame(text="kal aa jaun kya", user_id="u1", timestamp="")
    await wd.process_frame(tx, FrameDirection.DOWNSTREAM)

    # Task must be cancelled
    assert wd._resume_task is None or wd._resume_task.cancelled()

    # Wait past the 2.5s window — nothing should be queued
    await asyncio.sleep(3.0)
    assert task.queued == [], f"Expected zero re-deliveries, got: {task.queued}"


@pytest.mark.asyncio
async def test_fix1_interruption_true_silence_exactly_one_redelivery():
    """Interruption with true silence (no transcript, no new bot turn) → exactly one re-delivery."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    LAST_TEXT = "We have 2 BHKs from 95 Lakhs."
    wd, task = _make_watchdog(last_text=LAST_TEXT, turn_in_flight=False)

    # Arm resume timer
    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert wd._resume_task is not None

    # Wait for timer to fire (2.5s + small buffer)
    await asyncio.sleep(3.0)

    assert len(task.queued) == 1, f"Expected exactly 1 re-delivery, got {len(task.queued)}: {task.queued}"
    assert task.queued[0] == LAST_TEXT


@pytest.mark.asyncio
async def test_fix1_new_bot_turn_cancels_redelivery():
    """LLMFullResponseStartFrame arriving after InterruptionFrame must cancel re-delivery."""
    from pipecat.frames.frames import InterruptionFrame, LLMFullResponseStartFrame
    from pipecat.processors.frame_processor import FrameDirection

    wd, task = _make_watchdog()

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert wd._resume_task is not None

    # New bot turn starts — must cancel resume
    await wd.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)
    assert wd._resume_task is None or wd._resume_task.cancelled()

    await asyncio.sleep(3.0)
    assert task.queued == []


@pytest.mark.asyncio
async def test_fix1_dedupe_same_text_within_15s():
    """Re-delivery must not fire a second time for the same text within 15 seconds."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    LAST_TEXT = "Great, let me tell you about the project."
    wd, task = _make_watchdog(last_text=LAST_TEXT, turn_in_flight=False)

    # First interruption → timer fires → one re-delivery
    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(3.0)
    assert len(task.queued) == 1

    # Second interruption immediately after, same text → dedupe blocks it
    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(3.0)
    assert len(task.queued) == 1, f"Dedupe failed — got {len(task.queued)} deliveries"


@pytest.mark.asyncio
async def test_fix1_turn_in_flight_skips_redelivery():
    """If a user turn is in flight when the timer fires, skip re-delivery."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    # turn_in_flight=True simulates caller speaking longer than 2.5s
    wd, task = _make_watchdog(turn_in_flight=True)

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(3.0)
    assert task.queued == [], f"Expected zero re-deliveries (turn in flight), got: {task.queued}"


# ---------------------------------------------------------------------------
# Additional FIX tests — primary bot-level callback paths (not frame-based)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fix_primary_user_turn_callback_cancels_redelivery():
    """disarm_resume('user_turn') via bot-level callback cancels re-delivery.
    This tests the primary case-1 fix path (on_user_turn_stopped), not the
    frame-based fallback that pipecat may swallow upstream."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    wd, task = _make_watchdog()

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert wd._resume_task is not None and not wd._resume_task.done()

    # Simulate on_user_turn_stopped calling the primary disarm
    wd.disarm_resume("user_turn")
    assert wd._resume_task is None or wd._resume_task.cancelled()

    await asyncio.sleep(3.0)
    assert task.queued == [], f"user_turn callback: expected zero re-deliveries, got {task.queued}"


@pytest.mark.asyncio
async def test_fix_primary_new_bot_turn_callback_cancels_redelivery():
    """disarm_resume('new_bot_turn') via SpokenTextGuard LLMFullResponseStartFrame
    callback cancels re-delivery before TTS can start."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection

    wd, task = _make_watchdog()

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    assert wd._resume_task is not None

    # Simulate _SpokenTextGuard.on_new_bot_turn callback
    wd.disarm_resume("new_bot_turn")
    assert wd._resume_task is None or wd._resume_task.cancelled()

    await asyncio.sleep(3.0)
    assert task.queued == [], f"new_bot_turn callback: expected zero re-deliveries, got {task.queued}"


@pytest.mark.asyncio
async def test_fix_case2_current_bot_turn_text_blocks_redelivery():
    """Case 2: if get_current_bot_turn_text returns the same text as last_interrupted_text,
    the watchdog must NOT re-deliver — it would double the audio the LLM just spoke."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection
    from stall_watchdog import StallWatchdog

    BOT_TEXT = "Great choice. The 3 BHK Large is available for 1.2 Crore."

    task = _FakeTask()
    wd = StallWatchdog(
        stream_id="test-case2",
        on_repeated_stall=lambda sid: asyncio.sleep(0),
        is_call_ending=lambda: False,
        is_turn_in_flight=lambda: False,
        get_last_interrupted_text=lambda: BOT_TEXT,
        # The LLM has already produced the same text naturally
        get_current_bot_turn_text=lambda: BOT_TEXT,
    )
    wd._task = task

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(3.0)

    assert task.queued == [], (
        f"Case 2: watchdog re-delivered text already spoken by LLM: {task.queued}"
    )


@pytest.mark.asyncio
async def test_fix_case2_different_text_still_redelivers():
    """Case 2 guard must NOT block re-delivery when the current bot text differs
    from the interrupted text (i.e. a different turn was interrupted)."""
    from pipecat.frames.frames import InterruptionFrame
    from pipecat.processors.frame_processor import FrameDirection
    from stall_watchdog import StallWatchdog

    INTERRUPTED_TEXT = "We have 2 BHKs starting from 95 Lakhs."
    CURRENT_BOT_TEXT = ""  # no new bot turn yet

    task = _FakeTask()
    wd = StallWatchdog(
        stream_id="test-case2b",
        on_repeated_stall=lambda sid: asyncio.sleep(0),
        is_call_ending=lambda: False,
        is_turn_in_flight=lambda: False,
        get_last_interrupted_text=lambda: INTERRUPTED_TEXT,
        get_current_bot_turn_text=lambda: CURRENT_BOT_TEXT,
    )
    wd._task = task

    await wd.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(3.0)

    assert len(task.queued) == 1, f"Expected 1 re-delivery, got {task.queued}"
    assert task.queued[0] == INTERRUPTED_TEXT
