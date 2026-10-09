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
    ProposedUserStoppedSpeakingFrame,
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

        # FIX C: _speech_stop_at is set ONLY by ProposedUserStoppedSpeakingFrame (Sarvam STT's
        # END_SPEECH signal). Never by UserStoppedSpeakingFrame -- that frame arrives AFTER the
        # TranscriptionFrame under wait_for_transcript=True, which is the exact bug this fixes.
        self._speech_stop_at: float | None = None
        self._user_stopped_speaking_at: float | None = None  # kept for voice-to-voice ref time
        self._voice_to_voice_logged_this_turn = False
        self._voice_latencies_ms: list[float] = []
        self._live_voice_latencies_ms: list[float] = []   # FIX D: excludes cached-audio turns
        self._transcript_voice_latencies_ms: list[float] = []   # Anchor A: final transcript -> audio
        self._speech_stop_voice_latencies_ms: list[float] = []  # Anchor B: turn committed -> audio (HEADLINE, = Oct 4 definition)
        self._end_speech_voice_latencies_ms: list[float] = []   # Anchor C: STT END_SPEECH -> audio (caller-felt)
        self._cached_response_pending = False
        self._cached_audio_turns: int = 0                  # FIX D: turns served by cached audio
        self._llm_ttft_ms: list[float] = []
        self._tts_raw_ttfa_ms: list[float] = []
        self._tts_ttfa_ms: list[float] = []
        self._cached_ttfa_ms: list[float] = []
        self._tts_silence_trimmed_ms: list[float] = []
        self._stt_final_ms: list[float] = []
        self._cache_hits: int = 0
        self._last_transcript_at: float | None = None

        # FIX D: turns_with_no_reply - user spoke but bot never started speaking before next action
        self._turns_with_no_reply: int = 0
        self._waiting_for_bot_after_user_stop: bool = False

        # Silence nudge tracking: keep nudge response latency separate from voice-to-voice turn latency
        self._silence_nudge_pending = False
        self._silence_nudge_sent_at: float | None = None
        self._silence_nudge_latencies_ms: list[float] = []

        # Models used during the call, captured dynamically from usage events
        self._llm_models_used: set[str] = set()
        self._tts_models_used: set[str] = set()
        self._stt_models_used: set[str] = set()

        # FIX A: Deduplication of MetricsFrame and MetricsData objects to prevent replay loop accumulation.
        # Track seen frame ids (Pipecat frame.id) and keep strong references to data objects
        # so CPython id() addresses are never recycled for different objects.
        self._seen_frame_ids: set[int] = set()
        self._seen_metrics_ids: set[int] = set()
        self._seen_metrics_objs: list[Any] = []

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
            "context_rewinds": 0,
            "duplicate_assistant_records": 0,
        }
        self._provider_failures: list[dict] = []
        self._provider_failure_logged_this_turn = False
        self._cached_answer_latencies_ms: list[float] = []
        self._cached_answer_pending = False
        self._cached_answer_sent_at: float | None = None
        self._cached_answer_key: str = ""
        self._generation_silence_trimmed: dict[int, float] = {}
        self._missing_native_usage_requests = 0
        self._billed_usage_ids = set()
        self._billed_usage_objects = []
        self._provider_llm_tokens: dict[str, dict[str, int]] = {}
        self._turn_stop_sources: list[str] = []

    def record_llm_token_usage(
        self,
        provider: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        usage_object: Any = None,
    ) -> None:
        """Record native usage once; identical metrics objects can traverse the pipeline again."""
        if usage_object is not None:
            if id(usage_object) in self._billed_usage_ids:
                return
            self._billed_usage_ids.add(id(usage_object))
            self._billed_usage_objects.append(usage_object)
        self._totals["llm_prompt_tokens"] += prompt_tokens
        self._totals["llm_completion_tokens"] += completion_tokens
        if model:
            self._llm_models_used.add(model)
        if provider:
            if provider + ":" + model not in self._provider_llm_tokens:
                self._provider_llm_tokens[provider + ":" + model] = {"prompt": 0, "completion": 0}
            self._provider_llm_tokens[provider + ":" + model]["prompt"] += prompt_tokens
            self._provider_llm_tokens[provider + ":" + model]["completion"] += completion_tokens
        self._log(
            "llm_token_usage",
            turn=self._turn_index,
            provider=provider,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

    def record_context_rewind(self) -> None:
        """Record an unexplained drop in context message count."""
        self._totals["context_rewinds"] += 1
        self._log("context_rewind", turn=self._turn_index, total_so_far=self._totals["context_rewinds"])

    def record_duplicate_assistant(self) -> None:
        """Record a duplicate assistant message dropped before appending."""
        self._totals["duplicate_assistant_records"] += 1
        self._log("duplicate_assistant_record", turn=self._turn_index, total_so_far=self._totals["duplicate_assistant_records"])

    def record_cached_answer(self, phrase_key: str = "") -> None:
        """Record that a cached caller answer (fast-path phrase) was dispatched."""
        self._cached_answer_pending = True
        self._cached_answer_key = phrase_key
        self._cached_answer_sent_at = time.monotonic()
        # Fast-path text dispatch is not an audio hit. PCM playback records it once.

    def record_provider_failure(
        self,
        provider: str,
        error: str,
        latency_ms: float | None = None,
    ) -> None:
        """Record an LLM/TTS/STT provider failure or failover trigger."""
        self._totals["provider_errors"] += 1
        self._provider_failure_logged_this_turn = True
        self._provider_failures.append({
            "provider": provider,
            "error": str(error),
            "latency_ms": round(latency_ms, 1) if latency_ms is not None else None,
            "turn": self._turn_index,
        })
        self._log(
            "provider_failure",
            provider=provider,
            error=str(error),
            latency_ms=round(latency_ms, 1) if latency_ms is not None else None,
            turn=self._turn_index,
            total_so_far=self._totals["provider_errors"],
        )

    def mark_silence_nudge(self, stage: int = 1) -> None:
        """Mark that a silence nudge TTS frame was dispatched by SilenceChecker.
        Ensures BotStartedSpeakingFrame is treated as silence_nudge_latency rather
        than contaminating caller voice_to_voice_latency."""
        self._silence_nudge_pending = True
        self._silence_nudge_sent_at = time.monotonic()

    def record_tts_silence_trimmed(self, saved_ms: float, gen_id: int | None = None) -> None:
        """Record milliseconds of leading silence trimmed from TTS output before transmission."""
        if saved_ms > 0:
            self._tts_silence_trimmed_ms.append(round(saved_ms, 1))
            if gen_id is not None:
                self._generation_silence_trimmed[gen_id] = round(saved_ms, 1)
            self._log(
                "tts_silence_trimmed",
                turn=self._turn_index,
                gen_id=gen_id,
                saved_ms=round(saved_ms, 1),
            )

    def record_cached_audio_ttfa(self, cached_key: str) -> None:
        """Record a cached audio turn and increment cache hits without polluting generated TTS TTFA."""
        self._cache_hits += 1
        self._cached_ttfa_ms.append(0.0)
        # FIX D: cached audio turns are tracked separately; do NOT add to _live_voice_latencies_ms
        self._cached_audio_turns += 1
        self._cached_response_pending = True
        self._log(
            "tts_cached_audio_ttfa",
            turn=self._turn_index,
            cached_key=cached_key,
            ttfa_ms=0.0,
        )

    def record_cache_hit(self, cached_key: str = "") -> None:
        """Explicitly record a static audio cache hit."""
        self._cache_hits += 1

    def record_turn_stop(self, source: str) -> None:
        """Record the source of a user turn stop: 'strategy' or 'timeout'."""
        self._turn_stop_sources.append(source)
        self._log("turn_stop", turn=self._turn_index, source=source)

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
            frame_id = getattr(frame, "id", None)
            if frame_id is not None:
                if frame_id in self._seen_frame_ids:
                    return
                self._seen_frame_ids.add(frame_id)
            for metrics_data in frame.data:
                self._handle_metrics_data(metrics_data)

        elif isinstance(frame, UserStartedSpeakingFrame):
            self._silence_nudge_pending = False
            self._last_transcript_at = None
            # FIX D: if user spoke again before bot replied, count it as no-reply turn
            if self._waiting_for_bot_after_user_stop:
                self._turns_with_no_reply += 1
                self._waiting_for_bot_after_user_stop = False

        elif isinstance(frame, ProposedUserStoppedSpeakingFrame):
            # FIX C: Record STT END_SPEECH timestamp. This arrives BEFORE TranscriptionFrame
            # (Sarvam STT broadcasts END_SPEECH then sends the final transcript).
            # Under wait_for_transcript=True, UserStoppedSpeakingFrame arrives AFTER
            # TranscriptionFrame, so it cannot serve as the STT latency reference.
            self._speech_stop_at = time.monotonic()

        elif isinstance(frame, UserStoppedSpeakingFrame):
            # Voice-to-voice reference: when the user stopped speaking.
            # NOTE: _speech_stop_at (ProposedUserStopped) is used for STT final latency.
            # _user_stopped_speaking_at (here) is used for voice-to-voice start reference.
            self._turn_index += 1
            self._user_stopped_speaking_at = time.monotonic()
            self._voice_to_voice_logged_this_turn = False
            self._silence_nudge_pending = False
            self._provider_failure_logged_this_turn = False
            self._waiting_for_bot_after_user_stop = True  # FIX D

        elif isinstance(frame, BotStartedSpeakingFrame):
            # FIX D: bot replied, so this was not a no-reply turn
            self._waiting_for_bot_after_user_stop = False

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
            elif self._cached_answer_pending:
                cached_latency_ms = (
                    (time.monotonic() - self._cached_answer_sent_at) * 1000
                    if self._cached_answer_sent_at is not None
                    else 0.0
                )
                self._cached_answer_pending = False
                self._cached_answer_latencies_ms.append(round(cached_latency_ms, 1))
                self._log(
                    "cached_answer_latency",
                    turn=self._turn_index,
                    key=self._cached_answer_key,
                    latency_ms=round(cached_latency_ms, 1),
                )
                self._cached_response_pending = False
                self._voice_to_voice_logged_this_turn = True
                self._last_transcript_at = None
                self._speech_stop_at = None
            elif self._cached_response_pending:
                self._cached_response_pending = False
                self._voice_to_voice_logged_this_turn = True
                self._last_transcript_at = None
                self._speech_stop_at = None
            elif (
                (self._last_transcript_at is not None or self._user_stopped_speaking_at is not None)
                and not self._voice_to_voice_logged_this_turn
            ):
                now = time.monotonic()
                # HEADLINE ANCHOR (same definition as the Oct 4 / 543ms measurement):
                # turn committed (UserStoppedSpeakingFrame) -> first bot audio.
                # Falls back to the final-transcript time only if no turn-stop was seen.
                ref_time = self._user_stopped_speaking_at if self._user_stopped_speaking_at is not None else self._last_transcript_at
                latency_ms = (now - ref_time) * 1000
                self._voice_to_voice_logged_this_turn = True

                # Extra anchors, logged for comparison but NOT used in the headline average:
                #   transcript : final STT transcript   -> first bot audio  (includes the endpoint debounce wait)
                #   end_speech : STT END_SPEECH signal  -> first bot audio  (closest to what the caller feels)
                transcript_lat_ms = (now - self._last_transcript_at) * 1000 if self._last_transcript_at is not None else None
                speech_stop_lat_ms = (now - self._user_stopped_speaking_at) * 1000 if self._user_stopped_speaking_at is not None else None
                end_speech_lat_ms = (now - self._speech_stop_at) * 1000 if self._speech_stop_at is not None else None

                if transcript_lat_ms is not None and transcript_lat_ms > 0:
                    self._transcript_voice_latencies_ms.append(transcript_lat_ms)
                if speech_stop_lat_ms is not None and speech_stop_lat_ms > 0:
                    self._speech_stop_voice_latencies_ms.append(speech_stop_lat_ms)
                if end_speech_lat_ms is not None and end_speech_lat_ms > 0:
                    self._end_speech_voice_latencies_ms.append(end_speech_lat_ms)

                if latency_ms > 0:
                    self._voice_latencies_ms.append(latency_ms)
                    # FIX D: track live (non-cached-audio) turns separately for the headline average
                    self._live_voice_latencies_ms.append(latency_ms)
                    self._log(
                        "voice_to_voice_latency",
                        turn=self._turn_index,
                        latency_ms=round(latency_ms, 1),
                        anchor="turn_commit",
                        transcript_anchor_ms=round(transcript_lat_ms, 1) if transcript_lat_ms else None,
                        speech_stop_anchor_ms=round(speech_stop_lat_ms, 1) if speech_stop_lat_ms else None,
                        end_speech_anchor_ms=round(end_speech_lat_ms, 1) if end_speech_lat_ms else None,
                    )
                # Clear per-turn anchors so a stale value can never leak into the next turn.
                self._last_transcript_at = None
                self._speech_stop_at = None

        elif isinstance(frame, InterruptionFrame):
            self._totals["interruptions"] += 1
            # FIX D: barge-in before bot replied counts as no-reply
            if self._waiting_for_bot_after_user_stop:
                self._turns_with_no_reply += 1
                self._waiting_for_bot_after_user_stop = False
            self._log(
                "interruption",
                turn=self._turn_index,
                total_so_far=self._totals["interruptions"],
            )

        elif isinstance(frame, ErrorFrame):
            # Only count if not already recorded this turn via record_provider_failure
            if not self._provider_failure_logged_this_turn:
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
            self.record_transcription_metrics(frame)

        await self.push_frame(frame, direction)

    def record_transcription_metrics(self, frame: TranscriptionFrame) -> None:
        """Record transcription metrics without requiring downstream frame pushing."""
        now = time.monotonic()
        self._last_transcript_at = now
        # FIX C: Use _speech_stop_at (set by ProposedUserStoppedSpeakingFrame / END_SPEECH)
        # as the reference clock for STT final latency. This is the moment Sarvam STT
        # detected speech end, which is the correct start time regardless of when
        # UserStoppedSpeakingFrame subsequently fires.
        if self._speech_stop_at is not None:
            stt_final_ms = (now - self._speech_stop_at) * 1000
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


    def _handle_metrics_data(self, metrics_data: Any) -> None:
        # FIX A: Deduplicate by object identity. If the same MetricsData object has already
        # been processed (e.g. due to a pipeline replay loop), skip it silently.
        metrics_id = id(metrics_data)
        if metrics_id in self._seen_metrics_ids:
            return
        self._seen_metrics_ids.add(metrics_id)
        self._seen_metrics_objs.append(metrics_data)

        # FIX A: Sanity ceilings -- detect and block accumulation from replay loops.
        elapsed_s = max(1.0, time.monotonic() - self._call_start)
        elapsed_min = elapsed_s / 60.0

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
            # FIX 2: Report post-trim time so silence padded by TTS service does not inflate perceived TTFA.
            ttfa_raw = metrics_data.ttfa if metrics_data.ttfa is not None else metrics_data.ttfb
            leading_silence = getattr(metrics_data, "leading_silence", None) or 0.0
            # Trim drops leading silence keeping ~20ms pad
            trim_reduction = max(0.0, leading_silence - 0.020)
            ttfa_post_trim = max(0.0, ttfa_raw - trim_reduction) if ttfa_raw is not None else None

            if ttfa_raw is not None:
                self._tts_raw_ttfa_ms.append(round(ttfa_raw * 1000, 1))
            if ttfa_post_trim is not None:
                self._tts_ttfa_ms.append(round(ttfa_post_trim * 1000, 1))
            self._log(
                "tts_time_to_first_audio",
                turn=self._turn_index,
                processor=processor_name,
                model=model_name,
                ttfa_s=ttfa_post_trim,
                ttfa_raw_s=ttfa_raw,
                ttfb_s=metrics_data.ttfb,
                leading_silence_s=metrics_data.leading_silence,
                trimmed_silence_s=round(trim_reduction, 3),
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
            self.record_llm_token_usage(self._llm_provider, model_name or "unknown",
                prompt_tokens or 0, completion_tokens or 0, usage_object=usage)
            if model_name:
                self._llm_models_used.add(model_name)

        elif isinstance(metrics_data, STTUsageMetricsData):
            audio_seconds = getattr(metrics_data.value, "audio_seconds", None)
            pending_stt = self._totals["stt_audio_seconds"] + (audio_seconds or 0)
            # FIX A: Sanity ceiling -- STT seconds must not exceed 2x elapsed call duration.
            if elapsed_s > 5.0 and pending_stt > 2.0 * elapsed_s:
                logger.error(
                    "METRIC_CEILING_EXCEEDED: STT audio_seconds would be {:.1f}s vs elapsed {:.1f}s "
                    "(>2x ceiling). Skipping accumulation -- likely MetricsFrame replay loop.",
                    pending_stt,
                    elapsed_s,
                )
            else:
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
            pending_chars = self._totals["tts_characters"] + (metrics_data.value or 0)
            pending_chars_per_min = pending_chars / max(0.01, elapsed_min)
            # FIX A: Sanity ceiling -- TTS chars/min must not exceed 2000 (production max is ~800).
            if elapsed_s > 5.0 and pending_chars_per_min > 2000:
                logger.error(
                    "METRIC_CEILING_EXCEEDED: TTS chars/min would be {:.0f} (>2000 ceiling). "
                    "Skipping accumulation -- likely MetricsFrame replay loop.",
                    pending_chars_per_min,
                )
            else:
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

        if self._provider_llm_tokens:
            total_llm_cost = 0.0
            for p_name, p_toks in self._provider_llm_tokens.items():
                provider, model = p_name.split(":", 1)
                p_rates = self._rate_for(rates.get("llm", {}), {model}, provider)
                if p_rates:
                    p_cost = (
                        (p_toks.get("prompt", 0) / 1_000_000) * p_rates.get("prompt_per_mtok", 0)
                        + (p_toks.get("completion", 0) / 1_000_000) * p_rates.get("completion_per_mtok", 0)
                    )
                    total_llm_cost += p_cost
            if total_llm_cost > 0:
                breakdown["llm"] = round(total_llm_cost, 6)
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

        # FIX D: headline average excludes cached-audio turns
        headline_avg_latency = None
        if self._live_voice_latencies_ms:
            sl = sorted(self._live_voice_latencies_ms)
            headline_avg_latency = round(sum(sl) / len(sl), 1)

        avg_llm_ttft = (
            round(sum(self._llm_ttft_ms) / len(self._llm_ttft_ms), 1)
            if self._llm_ttft_ms
            else None
        )
        avg_tts_ttfa_raw = (
            round(sum(self._tts_raw_ttfa_ms) / len(self._tts_raw_ttfa_ms), 1)
            if self._tts_raw_ttfa_ms
            else None
        )
        avg_saved_silence = (
            round(sum(self._tts_silence_trimmed_ms) / len(self._tts_silence_trimmed_ms), 1)
            if self._tts_silence_trimmed_ms
            else 0.0
        )
        avg_tts_ttfa_effective = (
            round(sum(self._tts_ttfa_ms) / len(self._tts_ttfa_ms), 1)
            if self._tts_ttfa_ms
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

        avg_cached_answer_latency = (
            round(sum(self._cached_answer_latencies_ms) / len(self._cached_answer_latencies_ms), 1)
            if self._cached_answer_latencies_ms
            else None
        )

        avg_transcript_to_audio = (
            round(sum(self._transcript_voice_latencies_ms) / len(self._transcript_voice_latencies_ms), 1)
            if self._transcript_voice_latencies_ms
            else None
        )
        avg_speech_stop_to_audio = (
            round(sum(self._speech_stop_voice_latencies_ms) / len(self._speech_stop_voice_latencies_ms), 1)
            if self._speech_stop_voice_latencies_ms
            else None
        )

        avg_end_speech_to_audio = (
            round(sum(self._end_speech_voice_latencies_ms) / len(self._end_speech_voice_latencies_ms), 1)
            if self._end_speech_voice_latencies_ms
            else None
        )

        return {
            # FIX D: headline = live turns only; overall = all turns
            "latency_cohort": "live_responses_only",
            "latency_live_count": len(self._live_voice_latencies_ms),
            "latency_transcript_count": len(self._transcript_voice_latencies_ms),
            "latency_end_speech_count": len(self._end_speech_voice_latencies_ms),
            "cost_scope": "live_pipeline_only",
            "cost_excludes": ["llm_prewarm", "post_call_analysis", "offline_cache_generation", "telephony", "taxes"],
            "whole_call_cost_is_partial": True,
            "llm_usage_source": "native" if self._provider_llm_tokens else "unavailable",
            "cost_is_partial": not bool(self._provider_llm_tokens) or self._missing_native_usage_requests > 0,
            "missing_native_usage_requests": self._missing_native_usage_requests,
            "avg_voice_latency_ms": headline_avg_latency if headline_avg_latency is not None else avg_latency,
            "avg_voice_latency_all_ms": avg_latency,
            "avg_transcript_to_audio_ms": avg_transcript_to_audio,
            "avg_speech_stop_to_audio_ms": avg_speech_stop_to_audio,
            "avg_end_speech_to_audio_ms": avg_end_speech_to_audio,
            "median_voice_latency_ms": median_latency,
            "p90_voice_latency_ms": p90_latency,
            "avg_llm_ttft_ms": avg_llm_ttft,
            "avg_tts_ttfa_ms": avg_tts_ttfa_effective if avg_tts_ttfa_effective is not None else avg_tts_ttfa_raw,
            "avg_tts_ttfa_raw_ms": avg_tts_ttfa_raw,
            "avg_tts_ttfa_effective_ms": avg_tts_ttfa_effective,
            "avg_tts_cached_ttfa_ms": 0.0 if self._cached_ttfa_ms else None,
            "avg_tts_silence_saved_ms": avg_saved_silence if avg_saved_silence > 0 else None,
            "avg_cached_answer_latency_ms": avg_cached_answer_latency,
            "cached_answer_count": len(self._cached_answer_latencies_ms),
            "baseline_before_trim_ttfa_ms": avg_tts_ttfa_raw,
            "effective_after_trim_ttfa_ms": avg_tts_ttfa_effective,
            "silence_saved_ms": avg_saved_silence,
            "generation_silence_trimmed": dict(self._generation_silence_trimmed),
            "avg_stt_final_ms": avg_stt_final,
            "cache_hit_pct": cache_hit_pct,
            "cache_hits": self._cache_hits,
            "cached_audio_turns": self._cached_audio_turns,       # FIX D
            "turn_stop_sources": list(self._turn_stop_sources),
            "turn_stop_strategy_count": sum(1 for s in self._turn_stop_sources if s == "strategy"),
            "turn_stop_timeout_count": sum(1 for s in self._turn_stop_sources if s == "timeout"),
            "turns_with_no_reply": self._turns_with_no_reply,     # FIX D
            "tts_chars_per_min": tts_chars_per_min,
            "latency_split": latency_split,
            "turns": self._turn_index,
            "duration_s": duration_s,
            "cost_usd": cost_usd,
            "cost_breakdown": breakdown,
            "stt_provider": self._stt_provider,
            "llm_provider": self._llm_provider,
            "tts_provider": self._tts_provider,
            "context_rewinds": self._totals.get("context_rewinds", 0),
            "duplicate_assistant_records": self._totals.get("duplicate_assistant_records", 0),
            "tool_calls_ok": self._totals.get("tool_calls_ok", 0),
            "tool_calls_failed": self._totals.get("tool_calls_failed", 0),
            "provider_errors": self._totals.get("provider_errors", 0),
            "provider_failures": getattr(self, "_provider_failures", []),
            "llm_prompt_tokens": self._totals.get("llm_prompt_tokens", 0),
            "llm_completion_tokens": self._totals.get("llm_completion_tokens", 0),
            "tts_characters": self._totals.get("tts_characters", 0),
            "stt_audio_seconds": self._totals.get("stt_audio_seconds", 0.0),
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
            turns_with_no_reply=self._turns_with_no_reply,
            cached_audio_turns=self._cached_audio_turns,
            cache_hit_pct=f"{cache_hit_pct}%",
            cache_hits=self._cache_hits,
            tts_chars_per_min=tts_chars_per_min,
            **self._totals,
        )
