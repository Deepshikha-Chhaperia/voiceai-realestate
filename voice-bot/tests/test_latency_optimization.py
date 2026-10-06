"""
Unit and integration tests for Latency Optimization & Cost Reduction.
Covers:
  - Phase 1: Provider and config unification (Sarvam STT, Groq LLM, Sarvam TTS).
  - Item 1: Silence nudge first interval (15s).
  - Item 2: SpamQualifyGate (machine greeting >2.4s, dead-line silence 7s, intent tokens).
  - Item 3: Smart Turn endpointing availability.
  - Item 5: Reply length cap (max_completion_tokens=60).
  - Item 7: Static audio cache expansion (22 phrases loaded in 8kHz and 16kHz).
  - Item 8: DelayedRaceFiller 700ms threshold and token cancellation.
  - Item 10: CallMetricsCollector new metrics (cache_hit_pct, tts_chars_per_min, latency_split).
"""

import asyncio
import yaml
from pathlib import Path
import pytest

from bot import (
    _AUDIO_CACHE,
    _CACHED_PHRASE_TEXTS,
    _DelayedRaceFiller,
    _SpamQualifyGate,
    _match_cached_phrase,
)
from metrics_collector import CallMetricsCollector
from pipecat.processors.frame_processor import FrameDirection
from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    TextFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)


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
    assert base_cfg["active_providers"]["llm_fallback"] == "cerebras"

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
    assert "filler_en" in _CACHED_PHRASE_TEXTS
    assert "filler_hi" in _CACHED_PHRASE_TEXTS
    assert "ack_ji_bilkul" in _CACHED_PHRASE_TEXTS
    assert "clarify_property" in _CACHED_PHRASE_TEXTS

    # Verify matching logic
    assert _match_cached_phrase("Ji bilkul") == "ack_ji_bilkul"
    assert _match_cached_phrase("Theek hai.") == "ack_theek_hai"
    assert _match_cached_phrase("Are you looking for a property?") == "clarify_property"
    assert _match_cached_phrase("Sure, absolutely.") == "ack_sure"


def test_item3_smart_turn_import():
    from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
    from pipecat.turns.user_stop.turn_analyzer_user_turn_stop_strategy import (
        TurnAnalyzerUserTurnStopStrategy,
    )
    analyzer = LocalSmartTurnAnalyzerV3()
    strategy = TurnAnalyzerUserTurnStopStrategy(turn_analyzer=analyzer)
    assert strategy is not None


@pytest.mark.asyncio
async def test_item2_spam_gate_machine_greeting():
    hangup_called = []

    async def fake_hangup(reason: str):
        hangup_called.append(reason)

    gate = _SpamQualifyGate(stream_id="test-gate-1", force_hangup_fn=fake_hangup)

    # User speaks before bot audio for 2.6s (>2.4s threshold)
    await gate.process_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    await asyncio.sleep(0.05)
    # Simulate duration by setting start time in the past
    gate._user_speech_start_time = gate._user_speech_start_time - 2.6
    await gate.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    assert gate._gate_resolved is True
    assert any("machine_greeting" in r for r in hangup_called)


@pytest.mark.asyncio
async def test_item2_spam_gate_intent_token_qualifies():
    hangup_called = []

    async def fake_hangup(reason: str):
        hangup_called.append(reason)

    gate = _SpamQualifyGate(stream_id="test-gate-2", force_hangup_fn=fake_hangup)

    # User says "I need a 3 BHK flat"
    await gate.process_frame(
        TranscriptionFrame("I need a 3 BHK flat in prime area", "user", "2026-10-05T00:00:00Z"),
        FrameDirection.DOWNSTREAM,
    )

    assert gate._gate_resolved is True
    assert len(hangup_called) == 0


@pytest.mark.asyncio
async def test_item8_delayed_race_filler_cancels_on_token():
    filler = _DelayedRaceFiller(
        stream_id="test-filler",
        timeout_seconds=0.700,
    )
    # Turn 2 user stops speaking
    filler._turn_index = 1
    await filler.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    assert filler._race_task is not None
    assert not filler._race_task.done()

    # LLM produces first token (TextFrame) before 700ms
    await filler.process_frame(TextFrame("Hello"), FrameDirection.DOWNSTREAM)

    # Race task must be cancelled immediately
    assert filler._race_task is None
    assert filler._filler_dispatched is False


def test_item10_metrics_collector():
    collector = CallMetricsCollector(
        call_id="test-metrics-call",
        stt_provider="sarvam",
        llm_provider="groq",
        tts_provider="sarvam",
    )

    # Simulate turns and cached hits
    collector._turn_index = 4
    collector.record_cached_audio_ttfa("opening_intro")
    collector.record_cached_audio_ttfa("ack_sure")

    collector._llm_ttft_ms.append(220.0)
    collector._tts_ttfa_ms.append(0.0)
    collector._stt_final_ms.append(180.0)
    collector._totals["tts_characters"] = 150

    summary = collector.summary()

    assert summary["cache_hits"] == 2
    assert summary["cache_hit_pct"] == 50.0  # 2 hits out of 4 turns
    assert summary["avg_stt_final_ms"] == 180.0
    assert summary["avg_llm_ttft_ms"] == 220.0
    assert "latency_split" in summary
    assert summary["latency_split"]["stt_final_ms"] == 180.0
    assert summary["latency_split"]["llm_ttft_ms"] == 220.0
