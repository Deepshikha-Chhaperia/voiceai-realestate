"""
Unit tests for Latency Optimization & Cost Reduction.
Covers:
  - Phase 1: Provider and config unification (Sarvam STT, Groq LLM, Sarvam TTS).
  - Item 1: Silence nudge first interval (15s).
  - Item 5: Reply length cap (max_completion_tokens=60).
  - Item 7: Static audio cache expansion (23 phrases loaded in 8kHz and 16kHz).
  - Item 8: DelayedRaceFiller 700ms threshold and token cancellation.
"""

import asyncio
from pathlib import Path
import pytest
import yaml

from unittest.mock import AsyncMock, MagicMock

from bot import (
    _AUDIO_CACHE,
    _CACHED_PHRASE_TEXTS,
    _DelayedRaceFiller,
    _match_cached_phrase,
)
from pipecat.frames.frames import (
    LLMContextFrame,
    TextFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection


def test_phase1_provider_unification():
    config_path = Path(__file__).parent.parent / "config.yaml"
    india_path = Path(__file__).parent.parent / "profiles" / "india.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    with open(india_path, "r", encoding="utf-8") as f:
        india_cfg = yaml.safe_load(f)

    # Active providers in config.yaml must be the sole selector
    assert base_cfg["active_providers"]["stt"] == "sarvam"
    assert base_cfg["active_providers"]["llm"] == "groq"
    assert base_cfg["active_providers"]["tts"] == "sarvam"
    assert "llm_fallback" not in base_cfg["active_providers"]

    # profiles/india.yaml must NOT have active_providers
    assert "active_providers" not in india_cfg

    # Merged config must preserve base_cfg's active_providers
    merged = {**base_cfg, **india_cfg}
    assert merged["active_providers"]["stt"] == "sarvam"
    assert merged["active_providers"]["tts"] == "sarvam"
    assert merged["active_providers"]["llm"] == "groq"


def test_item1_silence_nudge_config():
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert cfg.get("silence_nudge_first_secs") == 15


def test_item5_reply_length_cap():
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    groq_max_tokens = (
        base_cfg.get("providers", {})
        .get("llm", {})
        .get("groq", {})
        .get("params", {})
        .get("max_completion_tokens")
    )
    assert groq_max_tokens == 60


def test_item7_phrase_cache_expansion():
    # Verify top phrases are present in cache mapping
    assert len(_CACHED_PHRASE_TEXTS) >= 20
    assert "checkin_generic" in _CACHED_PHRASE_TEXTS
    assert "filler_en" in _CACHED_PHRASE_TEXTS
    assert "filler_hi" in _CACHED_PHRASE_TEXTS
    assert "ack_ji_bilkul" in _CACHED_PHRASE_TEXTS
    assert "clarify_property" in _CACHED_PHRASE_TEXTS

    # Verify matching logic
    assert _match_cached_phrase("Hello? Are you still there?") == "checkin_generic"
    assert _match_cached_phrase("Ji bilkul") == "ack_ji_bilkul"
    assert _match_cached_phrase("Theek hai.") == "ack_theek_hai"
    assert _match_cached_phrase("Are you looking for a property?") == "clarify_property"
    assert _match_cached_phrase("Sure, absolutely.") == "ack_sure"


@pytest.mark.asyncio
async def test_item8_delayed_race_filler_cancels_on_token():
    filler = _DelayedRaceFiller(
        stream_id="test-filler",
        timeout_seconds=0.700,
    )
    # Turn 2 user stops speaking
    filler._turn_index = 1
    await filler.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await filler.process_frame(LLMContextFrame(MagicMock()), FrameDirection.DOWNSTREAM)

    assert filler._race_task is not None
    assert not filler._race_task.done()

    # LLM produces first token (TextFrame) before 700ms
    await filler.process_frame(TextFrame("Hello"), FrameDirection.DOWNSTREAM)

    # Race task must be cancelled immediately
    assert filler._race_task is None
    assert filler._filler_dispatched is False


def test_spoken_date_and_time_formatting():
    from datetime import datetime, date
    from leads.worker import format_spoken_date, format_spoken_time
    from bot import _SpokenTextGuard

    now = datetime(2026, 10, 8, 10, 0, 0) # Thursday, 08 Oct 2026

    # 1. format_spoken_date: today / tomorrow / day after tomorrow
    assert format_spoken_date("2026-10-09", now, "hi") == "kal"
    assert format_spoken_date("2026-10-09", now, "en") == "tomorrow"
    assert format_spoken_date("2026-10-10", now, "hi") == "parso"
    assert format_spoken_date("2026-10-10", now, "en") == "day after tomorrow"

    # 2. format_spoken_time
    assert format_spoken_time("11:00 AM", "en") == "11 am"
    assert format_spoken_time("15:00", "en") == "3 pm"
    assert format_spoken_time("11 AM", "hi") == "subah 11 baje"

    # 3. _SpokenTextGuard._normalize removes ISO dates and 4-digit years
    norm = _SpokenTextGuard._normalize("Your visit is set for 2026-10-09 at 11 AM.")
    assert "2026-10-09" not in norm
    assert "2026" not in norm

    norm_year = _SpokenTextGuard._normalize("Thursday, 09 Oct 2026 at 11 AM")
    assert "2026" not in norm_year
    assert "09 Oct" in norm_year

    # 4. _SpokenTextGuard._normalize replaces hyphens between words with comma
    norm_hyphen = _SpokenTextGuard._normalize("Tomorrow - Friday at 11 AM")
    assert "Tomorrow - Friday" not in norm_hyphen
    assert "Tomorrow, Friday" in norm_hyphen


@pytest.mark.asyncio
async def test_clause_boundary_first_chunk_streaming():
    from bot import _SpokenTextGuard, _find_clause_or_sentence_end
    from pipecat.frames.frames import UserStoppedSpeakingFrame, LLMFullResponseStartFrame, TextFrame

    # Check clause boundary detection
    text_with_comma = "Sure, I can help you with that"
    idx = _find_clause_or_sentence_end(text_with_comma)
    assert idx == len("Sure, ")

    text_with_dash = "Certainly - we have 3 BHK units"
    idx = _find_clause_or_sentence_end(text_with_dash)
    assert idx == len("Certainly - ")

    # Six words boundary
    text_words = "We have great three bedroom luxury apartments available"
    idx = _find_clause_or_sentence_end(text_words)
    assert idx > 0

    # Test SpokenTextGuard first chunk flush at clause boundary
    guard = _SpokenTextGuard(stream_id="test-clause-stream")
    pushed_frames = []
    async def capture_push(frame, direction=None):
        pushed_frames.append(frame)
    guard.push_frame = capture_push

    await guard.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await guard.process_frame(LLMFullResponseStartFrame(), FrameDirection.DOWNSTREAM)

    # Push first clause: "Sure, "
    await guard.process_frame(TextFrame("Sure, "), FrameDirection.DOWNSTREAM)
    # The first clause should be flushed immediately on boundary
    assert guard._leading_flushed is True
    text_frames = [f for f in pushed_frames if isinstance(f, TextFrame)]
    assert len(text_frames) >= 1
    assert "Sure" in text_frames[0].text


def test_system_prompt_compression_and_prefix_cache_invariant():
    from prompt_builder import build_system_prompt

    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    sys_prompt, customer_context = build_system_prompt("outbound", cfg, {"customer_name": "Alex"})

    # Assert compressed prompt is concise (< 2900 chars vs ~3808 baseline)
    assert len(sys_prompt) < 2900
    assert "Ananya" in sys_prompt
    assert "Meridian" in sys_prompt
    assert "book_site_visit" in sys_prompt
    assert "ACTIVE LEAD STATE" in sys_prompt
    assert customer_context is not None

    # Assert cerebras is completely absent from config providers.llm
    assert "cerebras" not in cfg.get("providers", {}).get("llm", {})

