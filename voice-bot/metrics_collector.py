"""
Per-call observability processor for the voice pipeline.

Passively monitors pipeline frames to track voice-to-voice latency, LLM TTFT,
TTS TTFA, token/audio usage, interruptions, and cost estimation.
"""

from __future__ import annotations

import time
from typing import Any

from loguru import logger

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    ErrorFrame,
    Frame,
    InterruptionFrame,
    MetricsFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.metrics.metrics import (
    LLMUsageMetricsData,
    ProcessingMetricsData,
    STTUsageMetricsData,
    TTFAMetricsData,
    TTFATMetricsData,
    TTFBMetricsData,
    TTSUsageMetricsData,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor


class CallMetricsCollector(FrameProcessor):
    """Passive, per-call observability processor. One instance per call."""

    def __init__(
        self,
        *,
        call_id: str,
        stt_provider: str,
        llm_provider: str,
        tts_provider: str,
        language: str = "unknown",
        cost_rates: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self._call_id = call_id
        self._stt_provider = stt_provider
        self._llm_provider = llm_provider
        self._tts_provider = tts_provider
        self._language = language
        # Per-provider $/unit rates for estimating turn and call costs
        self._cost_rates = cost_rates or {}

        self._call_start = time.monotonic()
        self._turn_index = 0
        self._user_stopped_speaking_at: float | None = None
        self._voice_to_voice_logged_this_turn = False
        self._voice_latencies_ms: list[float] = []
        self._llm_ttft_ms: list[float] = []
        self._tts_ttfa_ms: list[float] = []
        self._tts_silence_trimmed_ms: list[float] = []
        self._stt_final_ms: list[float] = []
        self._cache_hits: int = 0

        # Silence nudge tracking: keep nudge response latency separate from voice-to-voice turn latency
        self._silence_nudge_pending = False
        self._silence_nudge_sent_at: float | None = None
        self._silence_nudge_latencies_ms: list[float] = []

        # Models used during the call, captured dynamically from usage events
        self._llm_models_used: set[str] = set()
        self._tts_models_used: set[str] = set()
        self._stt_models_used: set[str] = set()

        # Aggregated for the single end-of-call summary line.
        self._totals: dict[str, Any] = {
            "llm_prompt_tokens": 0,
            "llm_completion_tokens": 0,
            "tts_characters": 0,
            "stt_audio_seconds": 0.0,
            "interruptions": 0,
            "provider_errors": 0,
            "tool_calls_ok": 0,
            "tool_calls_failed": 0,
        }

    def mark_silence_nudge(self, stage: int = 1) -> None:
        """Mark that a silence nudge TTS frame was dispatched by SilenceChecker.
        Ensures BotStartedSpeakingFrame is treated as silence_nudge_latency rather
        than contaminating caller voice_to_voice_latency."""
        self._silence_nudge_pending = True
        self._silence_nudge_sent_at = time.monotonic()

    def record_tts_silence_trimmed(self, saved_ms: float) -> None:
        """Record milliseconds of leading silence trimmed from TTS output before transmission."""
        if saved_ms > 0:
            self._tts_silence_trimmed_ms.append(round(saved_ms, 1))
            self._log(
                "tts_silence_trimmed",
                turn=self._turn_index,
                saved_ms=round(saved_ms, 1),
            )

    def record_cached_audio_ttfa(self, cached_key: str) -> None:
        """Record a 0ms TTFA for cached audio turns and increment cache hits."""
        self._cache_hits += 1
        self._tts_ttfa_ms.append(0.0)
        self._log(
            "tts_cached_audio_ttfa",
            turn=self._turn_index,
            cached_key=cached_key,
            ttfa_ms=0.0,
        )

    def record_cache_hit(self, cached_key: str = "") -> None:
        """Explicitly record a static audio cache hit."""
        self._cache_hits += 1


    def _log(self, event: str, **fields: Any) -> None:
        detail = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
        logger.info(
            "METRIC call_id={} event={} stt={} llm={} tts={} {}",
            self._call_id,
            event,
            self._stt_provider,
            self._llm_provider,
            self._tts_provider,
            detail,
        )

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, MetricsFrame):
            for metrics_data in frame.data:
                self._handle_metrics_data(metrics_data)

        elif isinstance(frame, UserStartedSpeakingFrame):
            self._silence_nudge_pending = False

        elif isinstance(frame, UserStoppedSpeakingFrame):
            self._turn_index += 1
            self._user_stopped_speaking_at = time.monotonic()
            self._voice_to_voice_logged_this_turn = False
            self._silence_nudge_pending = False

        elif isinstance(frame, BotStartedSpeakingFrame):
            if self._silence_nudge_pending:
                nudge_latency_ms = (
                    (time.monotonic() - self._silence_nudge_sent_at) * 1000
                    if self._silence_nudge_sent_at is not None
                    else 0.0
                )
                self._silence_nudge_pending = False
                self._silence_nudge_latencies_ms.append(nudge_latency_ms)
                self._log(
                    "silence_nudge_latency",
                    turn=self._turn_index,
                    latency_ms=round(nudge_latency_ms, 1),
                )
            elif (
                self._user_stopped_speaking_at is not None
                and not self._voice_to_voice_logged_this_turn
            ):
                latency_ms = (
                    time.monotonic() - self._user_stopped_speaking_at
                ) * 1000
                self._voice_to_voice_logged_this_turn = True
                self._voice_latencies_ms.append(latency_ms)
                self._log(
                    "voice_to_voice_latency",
                    turn=self._turn_index,
                    latency_ms=round(latency_ms, 1),
                )

        elif isinstance(frame, InterruptionFrame):
            self._totals["interruptions"] += 1
            self._log(
                "interruption",
                turn=self._turn_index,
                total_so_far=self._totals["interruptions"],
            )

        elif isinstance(frame, ErrorFrame):
            self._totals["provider_errors"] += 1
            self._log(
                "provider_error",
                turn=self._turn_index,
                fatal=getattr(frame, "fatal", None),
                # Truncated: an error message could in principle echo back
                # user-provided content depending on the provider.
                message=str(getattr(frame, "error", frame))[:200],
            )

        elif isinstance(frame, TranscriptionFrame):
            if self._user_stopped_speaking_at is not None:
                stt_final_ms = (time.monotonic() - self._user_stopped_speaking_at) * 1000
                self._stt_final_ms.append(round(stt_final_ms, 1))

            result = getattr(frame, "result", None)
            confidence = (
                result.get("confidence")
                if isinstance(result, dict)
                else None
            )
            detected_language = getattr(frame, "language", None)
            self._log(
                "stt_final_transcript",
                turn=self._turn_index,
                language=detected_language,
                confidence=confidence,
                # Length only, never the transcript text itself.
                char_count=len(frame.text) if getattr(frame, "text", None) else 0,
            )

        await self.push_frame(frame, direction)

    def _handle_metrics_data(self, metrics_data: Any) -> None:
        processor_name = getattr(metrics_data, "processor", None)
        model_name = getattr(metrics_data, "model", None)

        if isinstance(metrics_data, TTFATMetricsData):
            # LLM: time to first token (ttfat = time to first answer token).
            ttfat_val = metrics_data.ttfat if metrics_data.ttfat is not None else metrics_data.ttfb
            if ttfat_val is not None:
                self._llm_ttft_ms.append(round(ttfat_val * 1000, 1))
            self._log(
                "llm_time_to_first_token",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                ttfat_s=metrics_data.ttfat,
                ttfb_s=metrics_data.ttfb,
                thinking_time_s=metrics_data.thinking_time,
            )

        elif isinstance(metrics_data, TTFAMetricsData):
            # TTS: time to first audio.
            ttfa_val = metrics_data.ttfa if metrics_data.ttfa is not None else metrics_data.ttfb
            if ttfa_val is not None:
                self._tts_ttfa_ms.append(round(ttfa_val * 1000, 1))
            self._log(
                "tts_time_to_first_audio",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                ttfa_s=metrics_data.ttfa,
                ttfb_s=metrics_data.ttfb,
                leading_silence_s=metrics_data.leading_silence,
            )

        elif isinstance(metrics_data, TTFBMetricsData):
            # Generic time-to-first-byte (e.g. STT's first partial result).
            self._log(
                "ttfb",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                value_s=metrics_data.value,
            )

        elif isinstance(metrics_data, ProcessingMetricsData):
            # Total processing time for the emitting processor (covers both
            # "total generation latency" for the LLM and "total synthesis
            # latency" for TTS — which one it is is identified by processor_name).
            self._log(
                "processing_time",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                value_s=metrics_data.value,
            )

        elif isinstance(metrics_data, LLMUsageMetricsData):
            usage = metrics_data.value
            prompt_tokens = getattr(usage, "prompt_tokens", None)
            completion_tokens = getattr(usage, "completion_tokens", None)
            cache_read = getattr(usage, "cache_read_input_tokens", None)
            self._totals["llm_prompt_tokens"] += prompt_tokens or 0
            self._totals["llm_completion_tokens"] += completion_tokens or 0
            if model_name:
                self._llm_models_used.add(model_name)
            self._log(
                "llm_token_usage",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                # Prompt cache read tokens if supported by provider
                cache_read_input_tokens=cache_read,
            )

        elif isinstance(metrics_data, STTUsageMetricsData):
            audio_seconds = getattr(metrics_data.value, "audio_seconds", None)
            self._totals["stt_audio_seconds"] += audio_seconds or 0
            if model_name:
                self._stt_models_used.add(model_name)
            self._log(
                "stt_usage",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                audio_seconds=audio_seconds,
            )

        elif isinstance(metrics_data, TTSUsageMetricsData):
            self._totals["tts_characters"] += metrics_data.value or 0
            if model_name:
                self._tts_models_used.add(model_name)
            self._log(
                "tts_usage",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                characters=metrics_data.value,
            )

    def record_tool_call(
        self,
        name: str,
        success: bool,
        latency_ms: float | None = None,
    ) -> None:
        """Tool-call success/failure isn't a Pipecat metric type, so the
        end_call / set_conversation_language handlers in bot.py call this
        directly rather than it being observed passively."""
        if success:
            self._totals["tool_calls_ok"] += 1
        else:
            self._totals["tool_calls_failed"] += 1
        self._log(
            "tool_call",
            name=name,
            success=success,
            latency_ms=round(latency_ms, 1) if latency_ms is not None else None,
        )

    def _rate_for(self, category_rates: dict, models_used: set[str], provider: str) -> dict | None:
        """Looks up a $/unit rate by model and provider.
        Checks specific composite keys first (e.g. 'groq:qwen/qwen3.8-27b'),
        then model variants ('qwen/qwen3.8-27b', 'qwen-3.8-27b'),
        then provider fallback (e.g. 'groq' or 'cerebras').
        """
        if not category_rates:
            return None

        # Check models seen in this call
        for model in models_used:
            if not model:
                continue
            variants = [
                f"{provider}:{model}",
                model,
                f"{provider}:{model.replace('/', '-')}",
                model.replace("/", "-"),
                f"{provider}:{model.replace('-', '/')}",
                model.replace("-", "/"),
            ]
            for candidate in variants:
                if candidate in category_rates and isinstance(category_rates[candidate], dict):
                    return category_rates[candidate]

            if isinstance(category_rates.get(provider), dict):
                provider_dict = category_rates[provider]
                if model in provider_dict and isinstance(provider_dict[model], dict):
                    return provider_dict[model]
                norm_dash = model.replace("/", "-")
                if norm_dash in provider_dict and isinstance(provider_dict[norm_dash], dict):
                    return provider_dict[norm_dash]

        # Provider-level fallback (e.g. "groq": {prompt_per_mtok: 0.80, completion_per_mtok: 4.00})
        if provider in category_rates:
            val = category_rates[provider]
            if isinstance(val, dict):
                if any(k in val for k in ("prompt_per_mtok", "per_second", "per_character")):
                    return val
                for subval in val.values():
                    if isinstance(subval, dict) and any(k in subval for k in ("prompt_per_mtok", "per_second", "per_character")):
                        return subval

        return None

    def enrich_from_transcript(self, messages: list[dict[str, Any]]) -> None:
        """Fallback enrichment: if usage metrics frames were missing for TTS or LLM,
        accurately compute character count and token count from actual utterances."""
        asst_chars = 0
        all_chars = 0
        for m in messages:
            content = m.get("content", "")
            if isinstance(content, list):
                content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
            c_len = len(str(content))
            all_chars += c_len
            if m.get("role") == "assistant":
                asst_chars += c_len

        if self._totals.get("tts_characters", 0) <= 0 and asst_chars > 0:
            self._totals["tts_characters"] = asst_chars

        if self._totals.get("llm_completion_tokens", 0) <= 0 and asst_chars > 0:
            # Heuristic: ~4 chars per token for completions
            self._totals["llm_completion_tokens"] = max(1, asst_chars // 4)

        if self._totals.get("llm_prompt_tokens", 0) <= 0 and all_chars > 0:
            # Heuristic: base prompt (~800 tokens) + turn context
            self._totals["llm_prompt_tokens"] = max(1, 800 + (all_chars // 4))

        if self._totals.get("stt_audio_seconds", 0.0) <= 0.0:
            self._totals["stt_audio_seconds"] = max(0.0, time.monotonic() - self._call_start)

    def _estimate_cost(self) -> tuple[float | None, dict[str, float] | None]:
        """Best-effort cost estimate from configured $/unit rates. Returns
        (total_usd, breakdown) or (None, None) if no rates are configured
        for the model(s)/provider(s) this call actually used -- this must
        never silently report $0 for an unconfigured rate, since that
        would look identical to a genuinely free call on a client-facing
        report."""
        rates = self._cost_rates
        breakdown: dict[str, float] = {}

        llm_rates = self._rate_for(rates.get("llm", {}), self._llm_models_used, self._llm_provider)
        if llm_rates:
            prompt_cost = (
                self._totals["llm_prompt_tokens"] / 1_000_000
            ) * llm_rates.get("prompt_per_mtok", 0)
            completion_cost = (
                self._totals["llm_completion_tokens"] / 1_000_000
            ) * llm_rates.get("completion_per_mtok", 0)
            breakdown["llm"] = round(prompt_cost + completion_cost, 6)

        stt_seconds = self._totals.get("stt_audio_seconds", 0.0)
        if stt_seconds <= 0.0:
            stt_seconds = max(0.0, time.monotonic() - self._call_start)

        stt_rates = self._rate_for(rates.get("stt", {}), self._stt_models_used, self._stt_provider)
        if stt_rates and stt_seconds > 0:
            breakdown["stt"] = round(
                stt_seconds * stt_rates.get("per_second", 0), 6
            )

        tts_chars = self._totals.get("tts_characters", 0)
        tts_rates = self._rate_for(rates.get("tts", {}), self._tts_models_used, self._tts_provider)
        if tts_rates and tts_chars > 0:
            breakdown["tts"] = round(
                tts_chars * tts_rates.get("per_character", 0), 6
            )

        if not breakdown:
            return None, None
        return round(sum(breakdown.values()), 6), breakdown

    def summary(self) -> dict[str, Any]:
        """Called once at call teardown, alongside (not instead of)
        finalize()'s log line. Returns exactly what lead_state.py's
        record_call_stats() needs -- this is the seam between live-call
        metrics and the durable per-call/per-campaign record."""
        avg_latency = None
        median_latency = None
        p90_latency = None
        if self._voice_latencies_ms:
            sorted_latencies = sorted(self._voice_latencies_ms)
            n = len(sorted_latencies)
            avg_latency = round(sum(sorted_latencies) / n, 1)
            mid = n // 2
            median_latency = round(
                sorted_latencies[mid] if n % 2 else
                (sorted_latencies[mid - 1] + sorted_latencies[mid]) / 2,
                1,
            )
            p90_idx = min(int(n * 0.9), n - 1)
            p90_latency = round(sorted_latencies[p90_idx], 1)

        avg_llm_ttft = (
            round(sum(self._llm_ttft_ms) / len(self._llm_ttft_ms), 1)
            if self._llm_ttft_ms
            else None
        )
        avg_tts_ttfa_raw = (
            round(sum(self._tts_ttfa_ms) / len(self._tts_ttfa_ms), 1)
            if self._tts_ttfa_ms
            else None
        )
        avg_saved_silence = (
            round(sum(self._tts_silence_trimmed_ms) / len(self._tts_silence_trimmed_ms), 1)
            if self._tts_silence_trimmed_ms
            else 0.0
        )
        avg_tts_ttfa_effective = (
            round(max(0.0, avg_tts_ttfa_raw - avg_saved_silence), 1)
            if avg_tts_ttfa_raw is not None
            else None
        )

        avg_stt_final = (
            round(sum(self._stt_final_ms) / len(self._stt_final_ms), 1)
            if self._stt_final_ms
            else None
        )

        cost_usd, breakdown = self._estimate_cost()
        duration_s = round(max(0.0, time.monotonic() - self._call_start), 1)
        duration_min = max(0.01, duration_s / 60.0)
        tts_chars = self._totals.get("tts_characters", 0)
        tts_chars_per_min = round(tts_chars / duration_min, 1)
        cache_hit_pct = round((self._cache_hits / max(1, self._turn_index)) * 100, 1)

        latency_split = {
            "stt_final_ms": avg_stt_final,
            "llm_ttft_ms": avg_llm_ttft,
            "tts_ttfa_ms": avg_tts_ttfa_effective if avg_tts_ttfa_effective is not None else avg_tts_ttfa_raw,
        }

        return {
            "avg_voice_latency_ms": avg_latency,
            "median_voice_latency_ms": median_latency,
            "p90_voice_latency_ms": p90_latency,
            "avg_llm_ttft_ms": avg_llm_ttft,
            "avg_tts_ttfa_ms": avg_tts_ttfa_effective if avg_tts_ttfa_effective is not None else avg_tts_ttfa_raw,
            "avg_tts_ttfa_raw_ms": avg_tts_ttfa_raw,
            "avg_tts_ttfa_effective_ms": avg_tts_ttfa_effective,
            "avg_tts_silence_saved_ms": avg_saved_silence if avg_saved_silence > 0 else None,
            "avg_stt_final_ms": avg_stt_final,
            "cache_hit_pct": cache_hit_pct,
            "cache_hits": self._cache_hits,
            "tts_chars_per_min": tts_chars_per_min,
            "latency_split": latency_split,
            "turns": self._turn_index,
            "duration_s": duration_s,
            "cost_usd": cost_usd,
            "cost_breakdown": breakdown,
            "stt_provider": self._stt_provider,
            "llm_provider": self._llm_provider,
            "tts_provider": self._tts_provider,
        }

    def finalize(self) -> None:
        """Call once at call teardown to emit the single end-of-call summary
        line. Safe to call even if the call failed before any turns happened."""
        duration_s = time.monotonic() - self._call_start
        duration_min = max(0.01, duration_s / 60.0)
        tts_chars = self._totals.get("tts_characters", 0)
        tts_chars_per_min = round(tts_chars / duration_min, 1)
        cache_hit_pct = round((self._cache_hits / max(1, self._turn_index)) * 100, 1)

        self._log(
            "call_summary",
            duration_s=round(duration_s, 2),
            turns=self._turn_index,
            cache_hit_pct=f"{cache_hit_pct}%",
            cache_hits=self._cache_hits,
            tts_chars_per_min=tts_chars_per_min,
            **self._totals,
        )
