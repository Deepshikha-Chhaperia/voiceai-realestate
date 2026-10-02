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
    InterruptionFrame,
    LLMFullResponseStartFrame,
    TextFrame,
    TTSStartedFrame,
    TTSSpeakFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.utils.errors import ErrorCategory

STALL_TIMEOUT_SECS = float(os.getenv("STALL_WATCHDOG_TIMEOUT_SECS", "7.5"))
RECOVERY_LINE = "Sorry, I didn't catch that. Could you say that again?"
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
    ) -> None:
        super().__init__()
        self._stream_id = stream_id
        self._on_repeated_stall = on_repeated_stall
        # Optional callback indicating whether the call is already in teardown
        self._is_call_ending = is_call_ending or (lambda: False)
        # Optional callback indicating whether a user turn is actively in flight
        self._is_turn_in_flight = is_turn_in_flight or (lambda: True)
        self._lead_memory = lead_memory
        self._watch_task: asyncio.Task | None = None
        self._stall_count = 0
        self._task = None

    def _get_recovery_text(self) -> str:
        if self._lead_memory and (lang := self._lead_memory.get("language")):
            return RECOVERY_LINES.get(lang, RECOVERY_LINE)
        return RECOVERY_LINE

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
        elif isinstance(frame, (CancelFrame, EndFrame, EndTaskFrame)):
            self._disarm()

        await self.push_frame(frame, direction)

    async def cleanup(self) -> None:
        self._disarm()
        await super().cleanup()

    def _arm(self) -> None:
        self._disarm()
        self._watch_task = asyncio.create_task(self._watch())

    def _disarm(self) -> None:
        if self._watch_task and not self._watch_task.done():
            self._watch_task.cancel()
        self._watch_task = None

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