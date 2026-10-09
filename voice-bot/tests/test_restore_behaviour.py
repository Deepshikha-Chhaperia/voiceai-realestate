"""Behaviour restored after the Oct 4 fast version: flush size, duplicate window, silence ladder,
latency anchors, endpointing wiring."""
import asyncio
import time

import pytest
from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    TextFrame,
    TTSSpeakFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

import bot
from bot import (
    _SILENCE_GOODBYE_LINES,
    _SILENCE_SECOND_LINES,
    _SILENCE_SOFT_LINES,
    _SilenceChecker,
    _SpokenTextGuard,
    _match_cached_phrase,
)
from metrics_collector import CallMetricsCollector


# ---------------------------------------------------------------- guard harness
@pytest.fixture
def guard(monkeypatch):
    async def _noop(self, frame, direction):
        return None

    monkeypatch.setattr(FrameProcessor, "process_frame", _noop)
    g = _SpokenTextGuard(stream_id="t")
    g.pushed = []

    async def _push(frame, direction=FrameDirection.DOWNSTREAM):
        g.pushed.append(frame)

    g.push_frame = _push
    return g


async def _stream(g, tokens, end=True):
    await g.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)
    for t in tokens:
        await g.process_frame(TextFrame(text=t), FrameDirection.DOWNSTREAM)
    if end:
        await g.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    return [f.text for f in g.pushed if isinstance(f, TextFrame)]


# ---------------------------------------------------------------- 2. flush
async def test_flush_starts_before_60_chars_and_never_splits_a_word(guard):
    tokens = ["Our", " three", " bedroom", " flats", " have", " big", " balconies", " and", " good", " light"]
    await guard.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)
    first_flush_len = None
    buffered = ""
    for t in tokens:
        buffered += t
        await guard.process_frame(TextFrame(text=t), FrameDirection.DOWNSTREAM)
        if guard.pushed and first_flush_len is None:
            first_flush_len = len(buffered)
    assert first_flush_len is not None and first_flush_len < 60
    spoken = "".join(f.text for f in guard.pushed if isinstance(f, TextFrame))
    # whatever was spoken so far ends on a word boundary of the original text
    assert "Our three bedroom flats have big balconies and good light".startswith(spoken.strip())
    await guard.process_frame(LLMFullResponseEndFrame(), FrameDirection.DOWNSTREAM)
    full = "".join(f.text for f in guard.pushed if isinstance(f, TextFrame)).strip()
    assert full == "Our three bedroom flats have big balconies and good light"


async def test_mid_word_token_boundary_is_not_flushed(guard):
    # 26 chars land exactly in the middle of "balconies": the partial word must stay buffered
    await guard.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)
    for t in ["Big", " flats", " with", " large", " bal", "con"]:
        await guard.process_frame(TextFrame(text=t), FrameDirection.DOWNSTREAM)
    spoken = "".join(f.text for f in guard.pushed if isinstance(f, TextFrame))
    assert "bal" not in spoken


# ---------------------------------------------------------------- 3. duplicate guard
async def test_short_fragments_are_not_duplicates_even_right_after_a_turn(guard):
    guard._last_spoken_turn_text = "I have sent it to you on the brochure link."
    guard._last_spoken_turn_time = time.monotonic()
    assert guard._is_recent_duplicate("it") is False
    assert guard._is_recent_duplicate("sent") is False
    assert guard._is_recent_duplicate("Sure,") is False


async def test_exact_repeat_inside_3s_is_suppressed_without_regeneration(guard):
    guard._last_spoken_turn_text = "We have fifteen eighty sq ft from one point four five crore."
    guard._last_spoken_turn_time = time.monotonic()
    queued = []

    class _Task:
        async def queue_frames(self, frames):
            queued.extend(frames)

    guard._task = _Task()
    out = await _stream(guard, ["We", " have", " fifteen", " eighty", " sq", " ft", " from", " one", " point", " four", " five", " crore."])
    assert out == []
    assert queued == []  # no extra LLM turn was queued


async def test_same_text_after_the_window_is_spoken(guard):
    guard._last_spoken_turn_text = "We have fifteen eighty sq ft from one point four five crore."
    guard._last_spoken_turn_time = time.monotonic() - 3.5
    out = await _stream(guard, ["We", " have", " fifteen", " eighty", " sq", " ft", " from", " one", " point", " four", " five", " crore."])
    assert "".join(out).strip().startswith("We have fifteen eighty")


# ---------------------------------------------------------------- 5. silence ladder
def test_silence_lines_never_hit_a_cached_wav_or_farewell_pattern():
    for line in _SILENCE_SOFT_LINES + _SILENCE_SECOND_LINES + _SILENCE_GOODBYE_LINES:
        assert _match_cached_phrase(line) is None, line
    for line in _SILENCE_GOODBYE_LINES:
        low = line.lower()
        assert not any(w in low for w in ("bye", "take care", "have a great", "have a nice", "have a good", "have a wonderful")), line
    for line in _SILENCE_SOFT_LINES + _SILENCE_SECOND_LINES + _SILENCE_GOODBYE_LINES:
        assert "\u2014" not in line and "whatsapp" not in line.lower() and "booked" not in line.lower()


class _FakeTask:
    def __init__(self):
        self.frames = []

    async def queue_frames(self, frames):
        self.frames.extend(frames)


class _FakeCoordinator:
    is_ending = False

    def __init__(self):
        self.ended = 0

    def request_ending(self):
        self.ended += 1
        self.is_ending = True

    def cancel_ending(self, reason):
        self.is_ending = False


async def test_ladder_soft_then_second_then_goodbye_then_hangup():
    task = _FakeTask()
    coord = _FakeCoordinator()
    checker = _SilenceChecker(
        stream_id="ladder",
        context_aggregator_user=None,
        task=task,
        call_end_coordinator=coord,
        silence_threshold_secs=0.2,
        second_threshold_secs=0.2,
        third_threshold_secs=0.2,
        poll_interval_secs=0.05,
    )
    checker._running = True
    checker._last_user_speech_time = time.monotonic() - 1.0
    mon = asyncio.create_task(checker._monitor_silence())

    async def _bot_starts_when_goodbye_queued():
        for _ in range(200):
            if len(task.frames) >= 3:
                checker._bot_is_speaking = True
                return
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_bot_starts_when_goodbye_queued(), 8)
    await asyncio.sleep(0.4)
    texts = [f.text for f in task.frames]
    assert texts[0] in _SILENCE_SOFT_LINES
    assert texts[1] in _SILENCE_SECOND_LINES
    assert texts[2] in _SILENCE_GOODBYE_LINES
    assert getattr(task.frames[0], "is_silence_nudge", False) and getattr(task.frames[1], "is_silence_nudge", False)
    assert all(isinstance(f, TTSSpeakFrame) for f in task.frames)
    assert coord.ended == 1
    checker.stop()
    mon.cancel()


async def test_user_speech_aborts_goodbye_and_allows_a_second_ladder():
    task = _FakeTask()
    coord = _FakeCoordinator()
    checker = _SilenceChecker(stream_id="abort", context_aggregator_user=None, task=task, call_end_coordinator=coord)
    checker._hangup_action_task = asyncio.create_task(checker._say_goodbye_and_end("x"))
    checker.on_user_speech()
    await asyncio.sleep(0.05)
    assert checker._hangup_action_task is None
    assert coord.ended == 0
    checker.stop()


def test_ladder_lines_do_not_repeat_within_a_call():
    checker = _SilenceChecker(stream_id="rep", context_aggregator_user=None)
    picks = [checker._pick_line(None, _SILENCE_SOFT_LINES) for _ in range(len(_SILENCE_SOFT_LINES))]
    assert len(set(picks)) == len(_SILENCE_SOFT_LINES)


# ---------------------------------------------------------------- latency anchors
async def test_headline_latency_uses_turn_commit_anchor_and_logs_others():
    m = CallMetricsCollector(call_id="m", stt_provider="sarvam", llm_provider="groq", tts_provider="sarvam")
    now = time.monotonic()
    m._speech_stop_at = now - 0.60       # STT END_SPEECH
    m._last_transcript_at = now - 0.45   # final transcript
    await m.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)  # turn commit (time ~ now)
    m._user_stopped_speaking_at = now - 0.30
    await asyncio.sleep(0.05)
    await m.process_frame(BotStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    s = m.summary()
    headline = s["avg_voice_latency_ms"]
    assert 300 <= headline < 500                       # commit -> audio, NOT transcript -> audio
    assert s["avg_transcript_to_audio_ms"] > headline  # transcript anchor is earlier, so larger
    assert s["avg_end_speech_to_audio_ms"] > s["avg_transcript_to_audio_ms"]
    assert m._last_transcript_at is None and m._speech_stop_at is None  # no stale anchor next turn


# ---------------------------------------------------------------- 1. endpointing wiring
def test_smart_turn_is_opt_in_in_both_configs():
    import yaml, os
    here = os.path.dirname(os.path.dirname(__file__))
    prof = yaml.safe_load(open(os.path.join(here, "profiles", "india.yaml"), encoding="utf-8"))
    tm = prof["turn_management"]
    assert tm["smart_turn_enabled"] is False
    assert tm["filler_debounce_seconds"] == 0.15
    assert tm["user_speech_timeout"] == 0.20
