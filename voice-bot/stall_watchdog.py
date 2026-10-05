"""
Provider-health safety nets and stall detection for the voice pipeline.

Includes:
- StallWatchdog: Detects unresponsiveness between caller speech and bot response.
- ProviderErrorMonitor: Monitors service errors and escalates persistent provider failures.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from typing import Awaitable, Callable

from loguru import logger

from pipecat.frames.frames import (
    AudioRawFrame,
    BotStartedSpeakingFrame,
    CancelFrame,
    EndFrame,
    EndTaskFrame,
    ErrorFrame,
    Frame,
    InterimTranscriptionFrame,
    InterruptionFrame,
    LLMFullResponseStartFrame,
    TextFrame,
    TranscriptionFrame,
    TTSStartedFrame,
    TTSSpeakFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.utils.errors import ErrorCategory

STALL_TIMEOUT_SECS = float(os.getenv("STALL_WATCHDOG_TIMEOUT_SECS", "7.5"))
RECOVERY_LINES = {
    "English": "Sorry, I didn't catch that. Could you say that again?",
    "Hindi": "Sorry, main sun nahi paayi. Kya aap repeat kar sakte hain?",
    "Telugu": "Kshaminchandi, naku vinapada ledu. Malla cheptara?",
}

PROVIDER_ERROR_THRESHOLD = int(os.getenv("PROVIDER_ERROR_THRESHOLD", "3"))
PROVIDER_ERROR_WINDOW_SECS = float(os.getenv("PROVIDER_ERROR_WINDOW_SECS", "20.0"))


class StallWatchdog(FrameProcessor):
    """Generic backstop for a hang that produces no frame at all -- see
    module docstring. Complements, does not replace, ProviderErrorMonitor
    below: this catches silent hangs; that catches actual errors."""

    def __init__(
        self,
        *,
        stream_id: str,
        on_repeated_stall: Callable[[str], Awaitable[None]],
        is_call_ending: Callable[[], bool] | None = None,
        is_turn_in_flight: Callable[[], bool] | None = None,
        lead_memory: dict | None = None,
        get_last_interrupted_text: Callable[[], str] | None = None,
        get_current_bot_turn_text: Callable[[], str] | None = None,
    ) -> None:
        super().__init__()
        self._stream_id = stream_id
        self._on_repeated_stall = on_repeated_stall
        self._is_call_ending = is_call_ending or (lambda: False)
        self._is_turn_in_flight = is_turn_in_flight or (lambda: True)
        self._lead_memory = lead_memory
        self._watch_task: asyncio.Task | None = None
        self._stall_count = 0
        self._task = None
        self._get_last_interrupted_text = get_last_interrupted_text
        # Returns the text the bot is currently speaking or just finished speaking this turn.
        # Used at fire time to avoid re-delivering text the LLM already produced naturally.
        self._get_current_bot_turn_text = get_current_bot_turn_text
        self._resume_task: asyncio.Task | None = None
        # dedupe: (normalized_text, monotonic_time) of last re-delivery
        self._last_resume_delivery: tuple[str, float] = ("", 0.0)

    def _get_recovery_text(self) -> str:
        lang = self._lead_memory.get("language") if self._lead_memory else None
        return RECOVERY_LINES.get(lang or "English", RECOVERY_LINES["English"])

    def bind_task(self, task) -> None:
        self._task = task

    def disarm(self) -> None:
        self._disarm()

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, UserStoppedSpeakingFrame):
            self._arm()
        elif isinstance(frame, (BotStartedSpeakingFrame, TTSStartedFrame, AudioRawFrame, TextFrame, LLMFullResponseStartFrame)):
            self._disarm()
            # Fallback: frame-based disarm. Primary path is bot-level callbacks via disarm_resume().
            # Note: LLMFullResponseStartFrame may not reach here reliably — see bot.py callbacks.
            self.disarm_resume("frame_new_bot_turn")
        elif isinstance(frame, TTSSpeakFrame):
            self.disarm_resume("frame_ttsspeak")
        elif isinstance(frame, (CancelFrame, EndFrame, EndTaskFrame)):
            self._disarm()
            self._disarm_resume()

        # Arm on InterruptionFrame. TranscriptionFrame disarm kept as best-effort fallback
        # (pipecat may consume it upstream before it reaches us).
        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, InterruptionFrame):
                self._arm_resume()
            elif isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame)) and getattr(frame, "text", ""):
                self.disarm_resume("frame_transcription")

        await self.push_frame(frame, direction)

    async def cleanup(self) -> None:
        self._disarm()
        self._disarm_resume()
        await super().cleanup()

    def _arm(self) -> None:
        self._disarm()
        self._watch_task = asyncio.create_task(self._watch())

    def _disarm(self) -> None:
        if self._watch_task and not self._watch_task.done():
            self._watch_task.cancel()
        self._watch_task = None

    def disarm_resume(self, reason: str = "") -> None:
        """Public entry point: called by bot-level callbacks (user_turn, new_bot_turn).
        This is the *primary* disarm path — more reliable than frame watching because
        pipecat's LLMUserAggregator consumes TranscriptionFrame upstream of the watchdog."""
        if self._resume_task and not self._resume_task.done():
            logger.info(
                "[{}] Resume timer cancelled (reason={})",
                self._stream_id,
                reason or "explicit",
            )
        self._disarm_resume()

    def _arm_resume(self) -> None:
        self._disarm_resume()
        if self._get_last_interrupted_text and self._task:
            logger.info("[{}] Resume timer armed (2.5s)", self._stream_id)
            self._resume_task = asyncio.create_task(self._resume_watch())

    def _disarm_resume(self) -> None:
        if self._resume_task and not self._resume_task.done():
            self._resume_task.cancel()
        self._resume_task = None

    async def _resume_watch(self) -> None:
        try:
            await asyncio.sleep(2.5)
        except asyncio.CancelledError:
            return  # disarmed by callback or frame — nothing to do

        if self._is_call_ending():
            return

        # Primary guard: if a user turn is in flight, caller spoke — skip
        if self._is_turn_in_flight():
            logger.info(
                "[{}] Resume timer fired — skipped (user turn in flight)",
                self._stream_id,
            )
            return

        last_text = self._get_last_interrupted_text() if self._get_last_interrupted_text else ""
        if not last_text:
            return

        # Case 2 guard: if the bot is already speaking or has just spoken the same text
        # naturally (LLM reply arrived), re-delivering it would double the audio.
        current_bot_text = self._get_current_bot_turn_text() if self._get_current_bot_turn_text else ""
        norm_last = " ".join(last_text.lower().split())
        norm_current = " ".join(current_bot_text.lower().split())
        if norm_current and (norm_current == norm_last or norm_last in norm_current or norm_current in norm_last):
            logger.info(
                "[{}] Resume timer fired — skipped (text already spoken by LLM: {!r})",
                self._stream_id,
                current_bot_text[:60],
            )
            return

        # Dedupe: never re-deliver the same text twice within 15s
        now = time.monotonic()
        last_delivered, last_time = self._last_resume_delivery
        if last_delivered == norm_last and (now - last_time) < 15.0:
            logger.info(
                "[{}] Resume timer fired — skipped (dedupe, same text within 15s)",
                self._stream_id,
            )
            return
        self._last_resume_delivery = (norm_last, now)

        logger.info(
            "[{}] Resume timer fired — re-delivering: {!r}",
            self._stream_id,
            last_text,
        )
        try:
            await self._task.queue_frames(
                [TTSSpeakFrame(text=last_text, append_to_context=False)]
            )
        except Exception as exc:
            logger.warning("[{}] Resume re-delivery failed: {}", self._stream_id, exc)

    async def _watch(self) -> None:
        try:
            await asyncio.sleep(STALL_TIMEOUT_SECS)
        except asyncio.CancelledError:
            return  # bot responded in time -- normal path, nothing to do

        # If no turn is actually in flight (e.g. empty user transcript, non-speech noise),
        # this is not a provider stall. Do not fire recovery lines or send anything to TTS.
        if not self._is_turn_in_flight():
            logger.debug(
                "[{}] Stall watchdog timer expired but no turn in flight; skipping recovery prompt",
                self._stream_id,
            )
            return

        self._stall_count += 1

        # If call is already ending, force hangup immediately instead of retrying
        if self._is_call_ending():
            logger.error(
                "[{}] Provider stall detected during call-ending phase "
                "(no bot audio within {}s of the caller finishing "
                "speaking); skipping the recovery prompt and forcing "
                "hangup immediately instead of waiting for a second "
                "stall.",
                self._stream_id,
                STALL_TIMEOUT_SECS,
            )
            await self._on_repeated_stall(self._stream_id)
            return

        logger.error(
            "[{}] Provider stall detected: no bot audio within {}s of the "
            "caller finishing speaking (occurrence {} this call). This "
            "watchdog fires on silence alone and can't tell you which layer "
            "stalled -- if ProviderErrorMonitor logged an error around the "
            "same timestamp, trust that signal over this one; it knows "
            "which service and why.",
            self._stream_id,
            STALL_TIMEOUT_SECS,
            self._stall_count,
        )

        if self._stall_count == 1:
            # Abort any hanging upstream LLM request so late tokens don't collide with recovery line
            await self.push_frame(InterruptionFrame(), FrameDirection.UPSTREAM)
            rec_text = self._get_recovery_text()
            if self._task:
                await self._task.queue_frames(
                    [TTSSpeakFrame(text=rec_text, append_to_context=False)]
                )
            else:
                await self.push_frame(
                    TTSSpeakFrame(text=rec_text),
                    FrameDirection.DOWNSTREAM,
                )
        else:
            logger.error(
                "[{}] Second stall this call; ending the call gracefully "
                "rather than leaving the caller in silence again.",
                self._stream_id,
            )
            await self._on_repeated_stall(self._stream_id)


class ProviderErrorMonitor:
    """NOT a FrameProcessor -- deliberately not placed in the Pipeline list,
    because (as the module docstring explains) an ErrorFrame travels
    upstream and no pipeline position would ever see it reliably from all
    three services at once. Instead this attaches directly to each service
    object via the base FrameProcessor "on_error" event, which fires
    identically regardless of which provider class_path is configured --
    this is the actual mechanism that makes 'model agnostic' true for
    error handling, not just for construction.

    Usage, once per call, right after stt/llm/tts are constructed:
        monitor = ProviderErrorMonitor(stream_id=stream_id, on_unrecoverable=cb)
        monitor.attach(stt)
        monitor.attach(llm)
        monitor.attach(tts)
    """

    def __init__(
        self,
        *,
        stream_id: str,
        on_unrecoverable: Callable[[str], Awaitable[None]],
    ) -> None:
        self._stream_id = stream_id
        self._on_unrecoverable = on_unrecoverable
        self._error_times: dict[str, list[float]] = defaultdict(list)
        self._escalated = False

    def attach(self, service) -> None:
        service_name = type(service).__name__

        @service.event_handler("on_error")
        async def _on_error(processor, error) -> None:  # noqa: ANN001
            await self._handle_error(service_name, error)

    async def _handle_error(self, service_name: str, error: ErrorFrame) -> None:
        if self._escalated:
            return  # call is already being torn down; don't double-fire

        category = getattr(error, "category", None)

        if category == ErrorCategory.APPLICATION:
            # "Application code failed, not the provider" per Pipecat's own
            # ErrorCategory docs -- e.g. a tool handler exception. Says
            # nothing about this service's connection health; don't count it.
            logger.warning(
                "[{}] {} reported an application-layer error (not counted "
                "toward provider health): {}",
                self._stream_id,
                service_name,
                getattr(error, "error", error),
            )
            return

        is_permanent = bool(category) and getattr(category, "is_permanent", False)
        logger.error(
            "[{}] {} error (category={}{}): {}",
            self._stream_id,
            service_name,
            category,
            ", PERMANENT" if is_permanent else "",
            getattr(error, "error", error),
        )

        if is_permanent:
            await self._escalate(service_name, reason=f"permanent {category} error")
            return

        now = time.monotonic()
        times = self._error_times[service_name]
        times.append(now)
        # Trim to the rolling window.
        cutoff = now - PROVIDER_ERROR_WINDOW_SECS
        self._error_times[service_name] = [t for t in times if t >= cutoff]

        if len(self._error_times[service_name]) >= PROVIDER_ERROR_THRESHOLD:
            await self._escalate(
                service_name,
                reason=(
                    f"{len(self._error_times[service_name])} errors within "
                    f"{PROVIDER_ERROR_WINDOW_SECS}s"
                ),
            )

    async def _escalate(self, service_name: str, *, reason: str) -> None:
        self._escalated = True
        logger.error(
            "[{}] {} judged unrecoverable ({}); ending the call gracefully "
            "rather than continuing with a service that's clearly not "
            "coming back for this call.",
            self._stream_id,
            service_name,
            reason,
        )
        # Run unrecoverable handler asynchronously to avoid blocking the sync error handler
        asyncio.create_task(self._on_unrecoverable(self._stream_id))