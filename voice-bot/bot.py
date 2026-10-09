"""
Production voicebot orchestration with:
- Working interruption handling (Pipecat built-in + serializer drop window)
- Low-latency deterministic outbound call start
- Global VAD reuse for fast cold-start
- Proper Vobiz µ-law <-> PCM conversion
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import inspect
import json
import os
import random
import re
import struct
import time
import uuid
from importlib import import_module
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import aiohttp
import httpx
from dotenv import load_dotenv
from fastapi import WebSocket
from loguru import logger
from openai import AsyncOpenAI, RateLimitError

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.frames.frames import (
    AudioRawFrame,
    EndTaskFrame,
    FunctionCallResultProperties,
    Frame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    MetricsFrame,
    LLMContextFrame,
    LLMRunFrame,
    FunctionCallResultFrame,
    ProposedUserStartedSpeakingFrame,
    ProposedUserStoppedSpeakingFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSUpdateSettingsFrame,
    TranscriptionFrame,
    TextFrame,
    TTSSpeakFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
    FunctionCallResultProperties,
)
from pipecat.services.llm_service import FunctionCallParams
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.turns.user_start import (
    ExternalUserTurnStartStrategy,
    TranscriptionUserTurnStartStrategy,
    VADUserTurnStartStrategy,
)
from pipecat.turns.user_stop import (
    ExternalUserTurnStopStrategy,
    SpeechTimeoutUserTurnStopStrategy,
)
from pipecat.turns.user_stop.turn_analyzer_user_turn_stop_strategy import TurnAnalyzerUserTurnStopStrategy
from pipecat.audio.turn.smart_turn.base_smart_turn import EndOfTurnState
from pipecat.turns.types import ProcessFrameResult
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

# Compatibility patch: audiolab Graph renamed rate to sample_rate in newer releases
try:
    from audiolab.av import Graph as _OrigGraph
    _orig_graph_init = _OrigGraph.__init__
    def _patched_graph_init(self, *args, **kwargs):
        if "rate" in kwargs and "sample_rate" not in kwargs:
            kwargs["sample_rate"] = kwargs.pop("rate")
        return _orig_graph_init(self, *args, **kwargs)
    _OrigGraph.__init__ = _patched_graph_init
except ImportError:
    pass  # audiolab not installed -- RNNoiseFilter import below will also fail

# Optional input-audio noise suppression. Imported defensively because it
# requires the `pyrnnoise` dependency. Falls back to None if not installed.
try:
    from pipecat.audio.filters.rnnoise_filter import RNNoiseFilter
except ImportError:  # pragma: no cover - depends on optional extra
    RNNoiseFilter = None  # type: ignore[assignment,misc]

from language_state import (
    LANGUAGE_LOCALES,
    LanguageCode,
    LanguageState,
    normalize_language,
)
from prompt_builder import build_system_prompt
from services.vobiz_serializer import VobizFrameSerializer
from services.web_serializer import WebPCMFrameSerializer
from metrics_collector import CallMetricsCollector

import lead_state
import local_test_report
from call_repairs import (SHORT_GOODBYE, FAQ_TEXTS, valid_transcript, terminal_answer, faq_key,
                          brochure_payload, finish_brochure, queue_goodbye, TTSStallState, explicit_caller_name, clarification_for, unambiguous_visit_time, pure_farewell, CALL5_TEXTS, campaign_faq, genuine_late_question, brochure_decision, safe_tts_text, fragment_transcript, whatsapp_answer, CALL7_TEXTS, visit_time, visit_intent, whatsapp_ready, postcall_whatsapp_plan, manual_whatsapp_message, record_manual_whatsapp, closing_after_work)
from stall_watchdog import StallWatchdog, ProviderErrorMonitor


load_dotenv(override=True)
os.makedirs("logs", exist_ok=True)

logger.add(
    "logs/voicebot_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="10 days",
    level="DEBUG",
)

STREAM_PROVIDER_CALL_IDS: dict[str, str] = {}

# Shared HTTP client for Vobiz API to reuse pooled TLS connections
_vobiz_http_client = httpx.AsyncClient(timeout=2.0)


# Check whether FunctionSchema supports the 'handler' parameter in the installed Pipecat version
_FUNCTION_SCHEMA_SUPPORTS_HANDLER = (
    "handler" in inspect.signature(FunctionSchema.__init__).parameters
)


def _build_audio_in_filter(
    config: dict,
    stream_id: str,
    call_type: str = "telephony",
) -> Any | None:
    """Constructs the input-side noise suppression filter.

    For browser (web) clients, modern browsers already execute hardware
    echo-cancellation and noise-suppression in WebRTC/AudioContext, so
    we skip RNNoise to prevent double-filtering artifacts and static crackle.
    """
    if call_type == "web":
        return None

    if not config.get("noise_suppression_enabled", True):
        return None

    if RNNoiseFilter is None:
        logger.warning(
            "[{}] noise_suppression_enabled is set but pipecat's RNNoise "
            "filter isn't installed; continuing without input noise "
            "suppression. Install with: pip install \"pipecat-ai[rnnoise]\"",
            stream_id,
        )
        return None

    try:
        filt = RNNoiseFilter()
        logger.info("[{}] RNNoise input noise suppression active", stream_id)
        return filt
    except Exception:
        logger.exception(
            "[{}] Failed to construct RNNoiseFilter; continuing without "
            "input noise suppression",
            stream_id,
        )
        return None


# Deterministic rules to validate end_call invocations and prevent premature hangups
_FAREWELL_WORD_RE = re.compile(
    r"\b(bye+|goodbye|good\s+bye|take\s+care|see\s+ya|talk\s+later|alvida|dhanyavaad|dhanyavad|dhanyawaad|shukriya|shubh\s*ho)\b|\bby\s*$|(बाय|अलविदा|शुक्रिया|धन्यवाद|शुभ\s*हो)", re.IGNORECASE
)
_FAREWELL_PAIR_RE = re.compile(
    r"\b(thank\s+you|thanks|ok(?:ay)?)\b", re.IGNORECASE
)

# Standalone phrases that are unambiguously terminal on their own (hard refusals).
# Soft brush-offs like "not interested" or "no thanks" are handled via objection
# handling in the prompt, so they are intentionally excluded from this hard list.
_STANDALONE_END_PATTERNS = [
    re.compile(
        r"\b("
        r"please\s+stop\s+calling|stop\s+calling|"
        r"don\s*t\s+call\s+me|do\s+not\s+call\s+me|"
        r"i\s+don\s*t\s+want\s+to\s+continue|"
        r"i\s+do\s+not\s+want\s+to\s+continue|"
        r"i\s+(have|need|got|gotta)\s+to\s+go|"
        r"let\s*s\s+end\s+(here|this|it)|"
        r"that\s*s\s+all(\s+for\s+now)?|that\s+is\s+all"
        r")\b",
        re.IGNORECASE,
    ),
    # Hindi strong-rejection patterns (unambiguous refusals).
    re.compile(
        r"\b("
        r"nahi\s+chahiye|"
        r"mujhe\s+interest\s+nahi\s+hai|"
        r"interest\s+nahi\s+hai|"
        r"baat\s+nahi\s+karni|"
        r"call\s+mat\s+karna|"
        r"dobara\s+call\s+mat\s+karna|"
        r"bas\s+itna\s+hi|"
        r"bas\s+ho\s+gaya"
        r")\b",
        re.IGNORECASE,
    ),
]


def _normalize_for_intent(text: str) -> str:
    """Normalize text for regex matching by removing punctuation and collapsing whitespace."""
    if not text:
        return ""
    # Replace punctuation with spaces so word boundaries \b work cleanly
    # e.g., "No, thank you." -> "No  thank you", "that's it" -> "that s it"
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    return " ".join(cleaned.split()).strip()


def _caller_explicitly_ended(text: str) -> bool:
    """Return True only when the caller clearly ends or declines the call.

    Strong farewell = bye/goodbye (standalone or paired), OR a standalone
    unambiguous ending phrase like 'not interested', 'stop calling', etc.
    """
    cleaned = _normalize_for_intent(text)
    if not cleaned:
        return False
    # 1. Any utterance containing bye/goodbye — standalone is strong enough
    if _FAREWELL_WORD_RE.search(cleaned):
        return True
    # 2. Standalone unambiguous ending phrases (rejections, Hindi refusals)
    if any(p.search(cleaned) for p in _STANDALONE_END_PATTERNS):
        return True
    return False


# Phrases that signal mutual conversation wrap-up when end_call is invoked
_SOFT_CLOSE_RE = re.compile(
    r"\b("
    r"take\s+care|"
    r"fine\s+thanks|fine\s+thank\s+you|"
    r"all\s+good|"
    r"that\s*ll\s+be\s+all|that\s+will\s+be\s+all|"
    r"we\s+are\s+good|we\s*re\s+good|"
    r"no\s+nothing\s+else|nothing\s+else|"
    r"no\s+thank\s+you|no\s+thanks|"
    r"never\s+mind|"
    r"that\s*(?:would|will|s)\s*be\s*it|"
    r"that\s*s\s+it|that\s+is\s+it|"
    r"that\s*s\s+all|that\s+is\s+all|"
    r"okay\s+thanks|ok\s+thanks|ok\s+thank\s+you|okay\s+thank\s+you|"
    r"i\s*m\s+good|i\s+am\s+good|"
    r"no\s+i\s*m\s+fine|no\s+i\s+am\s+fine|"
    r"think\s+about\s+it|"
    r"let\s+you\s+know|"
    r"get\s+back\s+to\s+you|"
    r"soch\s+ke\s+bata|soch\s+kar\s+bata|"
    r"bas|theek\s+hai\s+bas|"
    r"nahi\s+kuch\s+nahi|kuch\s+nahi"
    r")\b",
    re.IGNORECASE,
)


def _is_soft_close(text: str) -> bool:
    """Return True when the caller uses a soft closing phrase.

    These phrases alone don't authorize hangup — they only pass when the
    LLM has already decided to call end_call, i.e. the model with full
    conversation context believes the call objective is complete.
    """
    cleaned = _normalize_for_intent(text)
    if not cleaned:
        return False
    return bool(_SOFT_CLOSE_RE.search(cleaned))


_TRAILING_CURRENT_TIME_RE = re.compile(r"\n\nCurrent time: .*$")


def _strip_dynamic_time_suffix(prompt: str) -> str:
    return _TRAILING_CURRENT_TIME_RE.sub("", prompt)


class LanguageObserver(FrameProcessor):
    """STT-driven language observer without hardcoded wordlists."""

    _LANGUAGE_LABELS = {
        "en": "English",
        "hi": "Hindi",
        "te": "Telugu",
    }

    def __init__(
        self,
        *,
        stream_id: str,
        language_state: LanguageState,
        tts: Any,
        tts_provider: str,
        lead_memory: Optional[dict[str, str]] = None,
    ) -> None:
        super().__init__()
        self._stream_id = stream_id
        self._language_state = language_state
        self._tts = tts
        self._tts_provider = tts_provider
        self._lead_memory = lead_memory

    @staticmethod
    def _detect_explicit_language_request(text: str, supported: frozenset[str] | None = None) -> Optional[str]:
        if not text:
            return None
        
        # Default supported languages if not provided
        valid_langs = supported if supported is not None else frozenset({"en", "hi", "te"})

        # Reject descriptive, past-tense, negation, or commentary statements
        if re.search(
            r"\b(?:was\s+speaking|were\s+speaking|spoke|not\s+in|don'?t\s+speak|hard\s+for\s+me|difficult|only\s+na|nahi\s+aati|radu|ledu)\b",
            text,
            re.IGNORECASE,
        ):
            return None

        # Telugu explicit requests (if supported in active market)
        if "te" in valid_langs and (
            re.search(
                r"\b(?:please\s+)?(?:speak|say|talk|tell|switch|converse|explain|reply)\s*(?:that\s*)?(?:in|to)\s*telugu\b|"
                r"\b(?:can|could)\s*you\s*(?:please\s*)?(?:speak|talk|reply)\s*(?:in\s*)?telugu\b|"
                r"\btelugu\s*(?:mein\s+bolo|me\s+bolo|lo\s+matladandi|lo\s+cheppandi|lo\s+cheppu|please)\b|"
                r"^(?:in\s*)?telugu(?:\s*please)?[\?\.\!]*$|"
                r"(?:తెలుగులో\s*(?:మాట్లాడండి|చెప్పండి|మాట్లాడగలరా))|(?:తెలుగులో)",
                text,
                re.IGNORECASE,
            )
        ):
            return "te"

        # Hindi explicit requests (Latin & Devanagari - if supported in active market)
        if "hi" in valid_langs and (
            re.search(
                r"\b(?:please\s+)?(?:speak|say|talk|tell|switch|converse|explain|reply)\s*(?:that\s*)?(?:in|to)\s*hindi\b|"
                r"\b(?:can|could)\s*you\s*(?:please\s*)?(?:speak|talk|reply)\s*(?:in\s*)?hindi\b|"
                r"\bhindi\s*(?:mein\s+bolo|me\s+bolo|me\s+baat\s+karo|mein\s+baat\s+karo|me\s+baat|mein\s+baat|please)\b|"
                r"^(?:in\s*)?hindi(?:\s*please)?[\?\.\!]*$|"
                r"(?:हिंदी|हिन्दी)\s*(?:में\s*(?:बात|बोलिए|बोलो|बताओ|कहो))|"
                r"(?:हिंदी|हिन्दी)\s*में|"
                r"(?:बात\s*कर\s*सकते\s*हो\s*(?:क्या\s*)?(?:हिंदी|हिन्दी)\s*में)",
                text,
                re.IGNORECASE,
            )
        ):
            return "hi"


        # English explicit requests
        if "en" in valid_langs and (
            re.search(
                r"\b(?:please\s+)?(?:speak|say|talk|tell|switch|converse|explain|reply|translate\s*(?:that\s*)?)\s*(?:that\s*)?(?:in|to)\s*english\b|"
                r"\b(?:can|could)\s*you\s*(?:please\s*)?(?:speak|talk|reply|translate)\s*(?:in\s*)?english\b|"
                r"\benglish\s*(?:mein\s+bolo|me\s+bolo|please)\b|"
                r"^(?:in\s*)?english(?:\s*please)?[\?\.\!]*$",
                text,
                re.IGNORECASE,
            )
        ):
            return "en"

        return None

    _PHONETIC_CORRECTIONS = [
        (re.compile(r"\b(?:ya\s+to\s+be\s+educated|to\s+be\s+educated)\b", re.IGNORECASE), "2 BHK"),
        (re.compile(r"\b(?:to|too)\s+b\s*h\s*k\b", re.IGNORECASE), "2 BHK"),
        (re.compile(r"\b(?:three|tree)\s+b\s*h\s*k\b", re.IGNORECASE), "3 BHK"),
        (re.compile(r"\b([1-5])\s*b\s*h\s*k\b", re.IGNORECASE), r"\1 BHK"),
    ]

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        await super().process_frame(frame, direction)

        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(frame, TranscriptionFrame)
        ):
            text = (getattr(frame, "text", "") or "").strip()
            if text:
                for pattern, replacement in self._PHONETIC_CORRECTIONS:
                    text = pattern.sub(replacement, text)
                frame.text = text
            supported_langs = getattr(self._language_state, "supported_languages", None)
            explicit_lang = self._detect_explicit_language_request(text, supported_langs)

            if explicit_lang:
                old_language = self._language_state.current_language
                new_language, switched = self._language_state.set_explicit(
                    explicit_lang, "explicit_request"
                )
                locale = LANGUAGE_LOCALES.get(new_language, "en-IN")
                label = self._LANGUAGE_LABELS.get(new_language, "English")
                if self._lead_memory is not None:
                    self._lead_memory["language"] = label
                    self._lead_memory.pop("language_ambiguous", None)

                await self.push_frame(
                    TTSUpdateSettingsFrame(settings={"language": locale}),
                    FrameDirection.DOWNSTREAM,
                )

                logger.info(
                    "[{}] Explicit language request: '{}' -> switched from {} to {} ({})",
                    self._stream_id,
                    text,
                    old_language,
                    label,
                    locale,
                )
            else:
                raw_language, probability = self._extract_language(frame)
                old_language = self._language_state.current_language
                new_language, switched = self._language_state.observe_stt(
                    raw_language, probability, text=text
                )

                if self._lead_memory is not None:
                    if getattr(self._language_state, "is_ambiguous", False):
                        self._lead_memory["language_ambiguous"] = True
                    else:
                        self._lead_memory.pop("language_ambiguous", None)

                if switched:
                    locale = LANGUAGE_LOCALES.get(new_language, "en-IN")
                    label = self._LANGUAGE_LABELS.get(new_language, "English")
                    if self._lead_memory is not None:
                        self._lead_memory["language"] = label

                    await self.push_frame(
                        TTSUpdateSettingsFrame(settings={"language": locale}),
                        FrameDirection.DOWNSTREAM,
                    )

                    logger.info(
                        "[{}] Language automatically adapted: old={} new={} ({})",
                        self._stream_id,
                        old_language,
                        label,
                        locale,
                    )
                elif self._language_state.established and self._lead_memory is not None and "language" not in self._lead_memory:
                    label = self._LANGUAGE_LABELS.get(new_language, "English")
                    self._lead_memory["language"] = label

        await self.push_frame(frame, direction)

    @staticmethod
    def _extract_language(
        frame: TranscriptionFrame,
    ) -> tuple[Optional[str], Optional[float]]:
        raw_language = getattr(frame, "language", None)
        raw_probability = getattr(frame, "language_probability", None)

        if raw_language:
            val = getattr(raw_language, "value", raw_language)
            return str(val), _normalize_probability(raw_probability)

        result = getattr(frame, "result", None)
        if isinstance(result, dict):
            data = result.get("data")
            containers = [result, data if isinstance(data, dict) else {}]

            for container in containers:
                raw_lang = (
                    container.get("language_code")
                    or container.get("language")
                    or container.get("detected_language")
                )

                raw_prob = (
                    container.get("language_probability")
                    if container.get("language_probability") is not None
                    else container.get("language_confidence")
                )

                if raw_prob is None:
                    raw_prob = container.get("confidence")

                if raw_lang:
                    return str(raw_lang), _normalize_probability(raw_prob)

        return None, None


def _normalize_probability(value: object) -> Optional[float]:
    try:
        result = float(value)

        if result > 1:
            result /= 100.0

        return max(0.0, min(1.0, result))

    except (TypeError, ValueError):
        return None


def _attach_resilient_failover(service: Any, provider_name: str, config: dict) -> None:
    """Attaches an automated in-flight failover to the LLM service.

    If the primary LLM provider/model hits an API rate limit (429), overload,
    or connection/token timeout mid-call, this catches the exception immediately and falls back
     rather than dropping to 0 tokens and triggering StallWatchdog termination.
    """
    orig_get_chat = getattr(service, "get_chat_completions", None)
    if not orig_get_chat:
        return

    if hasattr(service, "_client") and hasattr(service._client, "max_retries"):
        service._client.max_retries = 0

    # BACKUP MODEL (second Groq model, same Groq key/client). Config key is optional:
    #   llm_backup_groq_model: "llama-3.3-70b-versatile"   (set to "" to disable)
    backup_model = str(
        (config or {}).get("llm_backup_groq_model", "") or ""
    ).strip()
    primary_model = str(getattr(getattr(service, "_settings", None), "model", "") or "")
    has_backup = bool(
        provider_name == "groq"
        and backup_model
        and backup_model != primary_model
        and hasattr(service, "_client")
        and hasattr(service, "build_chat_completion_params")
    )

    async def _open_backup_stream(context: Any):
        """Open a stream on the backup Groq model. Returns (iterator, first_chunk)."""
        from pipecat.utils.types import assert_given

        adapter = service.get_llm_adapter()
        params_from_context = adapter.get_llm_invocation_params(
            context,
            system_instruction=assert_given(service._settings.system_instruction),
            convert_developer_to_user=not service.supports_developer_role,
        )
        params = service.build_chat_completion_params(params_from_context)
        params["model"] = backup_model
        # reasoning_effort is a Qwen/gpt-oss option; Llama models reject it with a 400.
        params.pop("reasoning_effort", None)
        params.pop("service_tier", None)
        stream = await asyncio.wait_for(service._client.chat.completions.create(**params), timeout=3.0)
        it = stream.__aiter__()
        first = await asyncio.wait_for(it.__anext__(), timeout=3.0)
        return it, first

    def _extract_err_details(exc: Exception) -> tuple[str, str]:
        cls_name = type(exc).__name__
        detail = str(exc) if str(exc).strip() else repr(exc)
        status = getattr(exc, "status_code", None)
        resp = getattr(exc, "response", None)
        headers = getattr(resp, "headers", {}) if resp is not None else {}
        rem_toks = headers.get("x-ratelimit-remaining-tokens")
        retry_aft = headers.get("retry-after")
        header_info = ""
        if status is not None:
            header_info += f" status={status}"
        if rem_toks is not None:
            header_info += f" x-ratelimit-remaining-tokens={rem_toks}"
        if retry_aft is not None:
            header_info += f" retry-after={retry_aft}"
        full_str = f"{cls_name}: {detail}{header_info}"
        return cls_name, full_str

    async def _resilient_get_chat_completions(context: Any):
        async def _stream_wrapper():
            primary_failed = False
            first_chunk = None
            iter_stream = None

            call_metrics = getattr(context, "call_metrics", None) or getattr(service, "_call_metrics", None)
            lead_mem = getattr(context, "lead_memory", None)
            if lead_mem is None:
                lead_mem = getattr(service, "_lead_memory", None)
            msgs = getattr(context, "messages", []) or []

            if isinstance(msgs, list):
                msgs[:] = [m for m in msgs if not (m.get("role") == "assistant" and any(p in str(m.get("content", "")) for p in (
                    "I've noted your preference. Let me check the details for you.",
                    "I'm having connection trouble. Please give me a moment.",
                    "The connection is still unavailable. Please try this call again later.")))]

            # Circuit breaker: if primary failed earlier this call, skip Groq for circuit duration
            now_mono = time.monotonic()
            skip_val = getattr(service, "_skip_primary_until", 0.0)
            skip_until = float(skip_val) if isinstance(skip_val, (int, float)) else 0.0
            if now_mono < skip_until:
                logger.info(
                    "[LLM Skip] Primary provider '{}' failed earlier this call; skipping for {:.1f}s more and using backup model / recovery line directly",
                    provider_name,
                    skip_until - now_mono,
                )
                primary_failed = True
            else:
                # Diagnostics: log exact messages payload before LLM request
                msgs = getattr(context, "messages", []) or []
                msg_roles = [m.get("role") for m in msgs if isinstance(m, dict)]
                msg_lens = [len(str(m.get("content", ""))) for m in msgs if isinstance(m, dict)]
                logger.info(
                    "[LLM Request] provider={} msg_count={} roles={} char_lens={}",
                    provider_name,
                    len(msgs),
                    msg_roles,
                    msg_lens,
                )

                t0 = time.monotonic()
                try:
                    primary_stream = await asyncio.wait_for(orig_get_chat(context), timeout=1.5)
                    iter_stream = primary_stream.__aiter__()
                    try:
                        first_chunk = await asyncio.wait_for(iter_stream.__anext__(), timeout=1.5)
                        ttfc_ms = round((time.monotonic() - t0) * 1000, 1)
                        logger.info("[LLM Response] provider={} TTFC={}ms", provider_name, ttfc_ms)
                    except (asyncio.TimeoutError, Exception) as stream_err:
                        elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                        err_cls, err_str = _extract_err_details(stream_err)
                        if call_metrics and hasattr(call_metrics, "record_provider_failure"):
                            call_metrics.record_provider_failure(provider_name, err_str, elapsed_ms)

                        # Check 429 rate limit with retry-after header
                        is_429 = "429" in err_str or "ratelimit" in err_str.lower()
                        retry_val = None
                        if "retry-after=" in err_str:
                            try:
                                retry_val = float(err_str.split("retry-after=")[1].split()[0])
                            except (ValueError, IndexError):
                                pass

                        if is_429 and retry_val is not None and retry_val <= 2.0 and not has_backup:
                            logger.warning(
                                "[LLM 429] Provider '{}' hit 429 with retry-after={:.1f}s <= 2.0s; waiting and retrying once...",
                                provider_name,
                                retry_val,
                            )
                            try:
                                await asyncio.sleep(max(0.2, retry_val))
                                retry_stream = await asyncio.wait_for(orig_get_chat(context), timeout=2.0)
                                iter_stream = retry_stream.__aiter__()
                                first_chunk = await asyncio.wait_for(iter_stream.__anext__(), timeout=2.0)
                                primary_failed = False
                                service._skip_primary_until = 0.0
                            except Exception as retry_e:
                                retry_elapsed = round((time.monotonic() - t0) * 1000, 1)
                                _, retry_err_str = _extract_err_details(retry_e)
                                logger.warning(
                                    "[LLM 429] Retry after 429 also failed after {}ms ({}); skipping primary for 60s",
                                    retry_elapsed,
                                    retry_err_str,
                                )
                                service._skip_primary_until = time.monotonic() + 60.0
                                primary_failed = True
                        elif "failed_generation" in err_str.lower() or "failed to call a function" in err_str.lower():
                            logger.warning(
                                "[LLM] Primary LLM failed_generation on first token after {}ms ({}); retrying 1 time...",
                                elapsed_ms,
                                err_str,
                            )
                            try:
                                retry_stream = await asyncio.wait_for(orig_get_chat(context), timeout=2.0)
                                iter_stream = retry_stream.__aiter__()
                                first_chunk = await asyncio.wait_for(iter_stream.__anext__(), timeout=2.0)
                                primary_failed = False
                                service._skip_primary_until = 0.0
                            except Exception as retry_e:
                                retry_elapsed = round((time.monotonic() - t0) * 1000, 1)
                                _, retry_err_str = _extract_err_details(retry_e)
                                logger.warning(
                                    "[LLM Failover] Primary retry after failed_generation also failed after {}ms ({}); skipping primary...",
                                    retry_elapsed,
                                    retry_err_str,
                                )
                                service._skip_primary_until = time.monotonic() + 60.0
                                primary_failed = True
                        else:
                            skip_dur = min(60.0, max(0.25, retry_val)) if (is_429 and retry_val is not None) else 60.0
                            service._skip_primary_until = time.monotonic() + skip_dur
                            logger.warning(
                                "[LLM Failover] Primary LLM provider '{}' first token failed/timed out after {}ms ({}); skipping primary for {:.1f}s...",
                                provider_name,
                                elapsed_ms,
                                err_str,
                                skip_dur,
                            )
                            primary_failed = True
                except (asyncio.TimeoutError, Exception) as e:
                    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                    err_cls, err_str = _extract_err_details(e)
                    if call_metrics and hasattr(call_metrics, "record_provider_failure"):
                        call_metrics.record_provider_failure(provider_name, err_str, elapsed_ms)

                    is_429 = "429" in err_str or "ratelimit" in err_str.lower()
                    retry_val = None
                    prose_retry = re.search(r"try again in ([0-9.]+)s", err_str)
                    if prose_retry:
                        retry_val = float(prose_retry.group(1))
                    if "retry-after=" in err_str:
                        try:
                            retry_val = float(err_str.split("retry-after=")[1].split()[0])
                        except (ValueError, IndexError):
                            pass

                    if is_429 and retry_val is not None and retry_val <= 2.0 and not has_backup:
                        logger.warning(
                            "[LLM 429] Provider '{}' stream init hit 429 with retry-after={:.1f}s <= 2.0s; waiting and retrying once...",
                            provider_name,
                            retry_val,
                        )
                        try:
                            await asyncio.sleep(max(0.2, retry_val))
                            retry_stream = await asyncio.wait_for(orig_get_chat(context), timeout=2.0)
                            iter_stream = retry_stream.__aiter__()
                            first_chunk = await asyncio.wait_for(iter_stream.__anext__(), timeout=2.0)
                            primary_failed = False
                            service._skip_primary_until = 0.0
                        except Exception as retry_e:
                            retry_elapsed = round((time.monotonic() - t0) * 1000, 1)
                            _, retry_err_str = _extract_err_details(retry_e)
                            logger.warning(
                                "[LLM 429] Stream init retry after 429 failed after {}ms ({}); skipping primary for 60s",
                                retry_elapsed,
                                retry_err_str,
                            )
                            service._skip_primary_until = time.monotonic() + 60.0
                            primary_failed = True
                    elif "failed_generation" in err_str.lower() or "failed to call a function" in err_str.lower():
                        logger.warning(
                            "[LLM] Primary LLM failed_generation on stream init after {}ms ({}); retrying 1 time...",
                            elapsed_ms,
                            err_str,
                        )
                        try:
                            retry_stream = await asyncio.wait_for(orig_get_chat(context), timeout=2.0)
                            iter_stream = retry_stream.__aiter__()
                            first_chunk = await asyncio.wait_for(iter_stream.__anext__(), timeout=2.0)
                            primary_failed = False
                            service._skip_primary_until = 0.0
                        except Exception as retry_e:
                            retry_elapsed = round((time.monotonic() - t0) * 1000, 1)
                            _, retry_err_str = _extract_err_details(retry_e)
                            logger.warning(
                                "[LLM Failover] Primary retry on stream init failed after {}ms ({}); skipping primary...",
                                retry_elapsed,
                                retry_err_str,
                            )
                            service._skip_primary_until = time.monotonic() + 60.0
                            primary_failed = True
                    else:
                        skip_dur = min(60.0, max(0.25, retry_val)) if (is_429 and retry_val is not None) else 60.0
                        service._skip_primary_until = time.monotonic() + skip_dur
                        logger.warning(
                            "[LLM Failover] Primary LLM provider '{}' stream creation failed after {}ms ({}); skipping primary for {:.1f}s...",
                            provider_name,
                            elapsed_ms,
                            err_str,
                            skip_dur,
                        )
                        primary_failed = True

            if not primary_failed and first_chunk is not None:
                service._billing_model = primary_model
                service._recovery_count = 0
                saw_usage = bool(getattr(first_chunk, "usage", None))
                try:
                    context._response_truncated = any(getattr(c, "finish_reason", None) == "length" for c in (getattr(first_chunk, "choices", None) or []))
                    yield first_chunk
                    async for chunk in iter_stream:
                        saw_usage = saw_usage or bool(getattr(chunk, "usage", None))
                        if any(getattr(c, "finish_reason", None) == "length" for c in (getattr(chunk, "choices", None) or [])):
                            context._response_truncated = True
                        yield chunk
                except Exception as stream_error:
                    if call_metrics:
                        call_metrics.record_provider_failure(provider_name, str(stream_error))
                    # Already yielded output: never replay/retry a partial tool-bearing turn.
                    raise
                finally:
                    if call_metrics and not saw_usage:
                        call_metrics._missing_native_usage_requests += 1
                return

            # Primary provider failed: check if cancelled/superseded first.
            if (
                asyncio.current_task().cancelled()
                or getattr(context, "_superseded", None) is True
                or getattr(context, "_is_interrupted", None) is True
            ):
                return

            # Backup stage: second Groq model. Only if nothing was spoken yet (nothing yielded above).
            if has_backup and not getattr(service, "_backup_unavailable", False):
                t_bk = time.monotonic()
                bk_it = None
                bk_first = None
                try:
                    bk_it, bk_first = await _open_backup_stream(context)
                except (asyncio.TimeoutError, Exception) as bk_err:
                    if getattr(bk_err, "status_code", None) in (400, 401, 403, 404):
                        service._backup_unavailable = True
                    bk_ms = round((time.monotonic() - t_bk) * 1000, 1)
                    _, bk_err_str = _extract_err_details(bk_err)
                    logger.error("[LLM Failover] Backup Groq model '{}' failed after {}ms ({})", backup_model, bk_ms, bk_err_str)
                    if call_metrics and hasattr(call_metrics, "record_provider_failure"):
                        call_metrics.record_provider_failure(provider_name + ":" + backup_model, bk_err_str, bk_ms)
                if bk_it is not None and bk_first is not None:
                    logger.info(
                        "[LLM Failover] Serving this turn from backup Groq model '{}' (TTFC={}ms)",
                        backup_model,
                        round((time.monotonic() - t_bk) * 1000, 1),
                    )
                    service._billing_model = backup_model
                    service._recovery_count = 0
                    saw_usage = bool(getattr(bk_first, "usage", None))
                    try:
                        context._response_truncated = any(getattr(c, "finish_reason", None) == "length" for c in (getattr(bk_first, "choices", None) or []))
                        yield bk_first
                        async for chunk in bk_it:
                            saw_usage = saw_usage or bool(getattr(chunk, "usage", None))
                            if any(getattr(c, "finish_reason", None) == "length" for c in (getattr(chunk, "choices", None) or [])):
                                context._response_truncated = True
                            yield chunk
                    finally:
                        if call_metrics and not saw_usage:
                            call_metrics._missing_native_usage_requests += 1
                    return

            prior_count = getattr(service, "_recovery_count", 0)
            fail_count = (prior_count if isinstance(prior_count, int) else 0) + 1
            service._recovery_count = fail_count
            recovery_line = (
                "I'm having connection trouble. Please give me a moment."
                if fail_count == 1 else
                "The connection is still unavailable. Please try this call again later."
            )
            if fail_count > 2:
                return
            service._last_recovery_line = recovery_line

            logger.info("[LLM Fallback] Speaking deterministic recovery line (failure count={}): {!r}", fail_count, recovery_line)

            from types import SimpleNamespace
            synthetic_chunk = SimpleNamespace(
                id="recovery-fallback",
                object="chat.completion.chunk",
                created=int(time.time()),
                model="recovery-fallback",
                choices=[
                    SimpleNamespace(
                        index=0,
                        delta=SimpleNamespace(content=recovery_line, role="assistant", tool_calls=None),
                        finish_reason="stop",
                        message=SimpleNamespace(content=recovery_line, role="assistant", tool_calls=None),
                    )
                ],
                tool_calls=None,
                usage=None,
            )
            yield synthetic_chunk
            if fail_count == 2:
                on_exhausted = getattr(service, "_on_provider_exhausted", None)
                if callable(on_exhausted):
                    await on_exhausted()

        return _stream_wrapper()

    original_usage = service.start_llm_usage_metrics
    async def native_usage(usage):
        metrics = getattr(service, "_call_metrics", None)
        if metrics:
            metrics.record_llm_token_usage(provider_name,
                getattr(service, "_billing_model", primary_model),
                usage.prompt_tokens or 0, usage.completion_tokens or 0, usage_object=usage)
        await original_usage(usage)
    service.start_llm_usage_metrics = native_usage
    service.get_chat_completions = _resilient_get_chat_completions
    logger.info("Attached resilient LLM failover to {} service", provider_name)


class ServiceFactory:
    """Configures services cleanly and prevents deprecated keyword warnings."""

    _CLASS_CACHE: dict[str, Any] = {}

    @classmethod
    def _import_class(cls, class_path: str) -> Any:
        if class_path in cls._CLASS_CACHE:
            return cls._CLASS_CACHE[class_path]
        module_path, class_name = class_path.rsplit(".", 1)
        klass = getattr(import_module(module_path), class_name)
        cls._CLASS_CACHE[class_path] = klass
        return klass

    @classmethod
    def create(
        cls,
        service_type: str,
        provider_name: str,
        config: dict,
        *,
        aiohttp_session: aiohttp.ClientSession | None = None,
        **dynamic_kwargs: Any,
    ) -> Any:
        registry = config.get("providers", {}).get(service_type, {})
        provider_config = registry.get(provider_name)

        if not provider_config:
            raise ValueError(
                f"Unknown {service_type} provider: {provider_name}"
            )

        class_path = provider_config.get("class_path")

        if not class_path:
            raise ValueError(
                f"Missing class_path for {service_type}:{provider_name}"
            )

        service_class = cls._import_class(class_path)
        kwargs: dict[str, Any] = {}

        api_key_env = provider_config.get("api_key_env")

        if api_key_env:
            api_key = os.getenv(api_key_env)

            if not api_key:
                raise ValueError(
                    f"Missing environment variable: {api_key_env}"
                )

            kwargs["api_key"] = api_key

        if provider_config.get("_needs_aiohttp", False):
            if aiohttp_session is None:
                raise RuntimeError(
                    f"{service_type}:{provider_name} requires aiohttp_session"
                )

            kwargs["aiohttp_session"] = aiohttp_session

        params = dict(provider_config.get("params", {}))
        params.pop("tools", None)

        # Sarvam legacy STT provides its own server-side VAD when enabled.
        if service_type == "stt" and provider_name == "sarvam":
            params["vad_signals"] = True

        if service_type == "stt":
            stt_lang = params.get("language")

            if provider_name == "sarvam" and (not stt_lang or stt_lang == "unknown"):
                params["language"] = None  # SDK v4 resolves None to unknown, not forced English
            elif not stt_lang or stt_lang == "unknown":
                params["language"] = "en-IN"

        if "voice_id" in params and "voice" not in params:
            params["voice"] = params.pop("voice_id")

        if "MurfFalcon2TTSService" in class_path:
            kwargs.update(params)

        else:
            settings_cls = getattr(service_class, "Settings", None)

            if settings_cls:
                top_level_names = set()
                for cls_in_mro in service_class.__mro__:
                    if hasattr(cls_in_mro, "__init__"):
                        try:
                            sig = inspect.signature(cls_in_mro.__init__)
                            top_level_names.update(sig.parameters.keys())
                        except (ValueError, TypeError):
                            pass

                top_level_names.difference_update(
                    ("self", "api_key", "aiohttp_session", "settings", "params", "kwargs")
                )

                top_level_params = {
                    key: value
                    for key, value in params.items()
                    if key in top_level_names
                    and key not in ("model", "voice", "voice_id")
                }

                settings_params = {
                    key: value
                    for key, value in params.items()
                    if key not in top_level_params
                }

                kwargs.update(top_level_params)

                if settings_params:
                    if hasattr(settings_cls, "from_mapping"):
                        extra_val = settings_params.pop("extra", None)
                        settings_inst = settings_cls.from_mapping(settings_params)
                        if extra_val and hasattr(settings_inst, "extra"):
                            if isinstance(extra_val, dict) and "provider" in extra_val and "extra_body" not in extra_val:
                                extra_val = {"extra_body": extra_val}
                            if isinstance(settings_inst.extra, dict):
                                settings_inst.extra.update(extra_val)
                            else:
                                settings_inst.extra = extra_val
                        kwargs["settings"] = settings_inst
                    else:
                        kwargs["settings"] = settings_cls(
                            **settings_params
                        )

            elif service_type == "stt" and provider_name == "sarvam":
                settings_cls = getattr(service_class, "Settings", None)
                if settings_cls:
                    valid_settings_keys = {
                        "model", "language", "vad_signals", "high_vad_sensitivity",
                        "positive_speech_threshold", "negative_speech_threshold",
                        "min_speech_frames", "first_turn_min_speech_frames",
                        "negative_frames_count", "negative_frames_window",
                        "start_speech_volume_threshold", "interrupt_min_speech_frames",
                        "pre_speech_pad_frames", "num_initial_ignored_frames"
                    }
                    settings_params = {k: v for k, v in params.items() if k in valid_settings_keys}
                    kwargs["settings"] = settings_cls(**settings_params)
                    for k in settings_params:
                        params.pop(k, None)
                kwargs.update(params)
            else:
                input_params_cls = getattr(
                    service_class,
                    "InputParams",
                    None,
                )

                if input_params_cls:
                    kwargs["params"] = input_params_cls(**params)
                else:
                    kwargs.update(params)

        kwargs.update(dynamic_kwargs)

        logger.info(
            "Creating {} provider={} class={}",
            service_type,
            provider_name,
            class_path,
        )

        service = service_class(**kwargs)

        # Force disable preprocessing on Sarvam bulbul:v3 to eliminate Hindi token leakage
        # and tail hallucinations ("jai die gai"). Pipecat library hardcodes preprocessing_always_enabled=True,
        # which overwrites config.yaml settings during construction; this patch enforces False.
        if service_type == "tts" and provider_name == "sarvam":
            if hasattr(service, "_settings"):
                if hasattr(service._settings, "enable_preprocessing"):
                    service._settings.enable_preprocessing = False
                    logger.info("Enforced enable_preprocessing=False on Sarvam TTS")
                if hasattr(service._settings, "min_buffer_size") and service._settings.min_buffer_size is not None:
                    if service._settings.min_buffer_size < 30:
                        service._settings.min_buffer_size = 30
                        logger.info("Clamped Sarvam TTS min_buffer_size to 30 (API minimum)")

        # Sentence-level text transformer on TTS: ensures numbers, ranges, currencies, and units
        # are normalized at full sentence aggregation before being sent to TTS.
        if service_type == "tts" and hasattr(service, "add_text_transformer"):
            async def _tts_sentence_normalizer(text: str, agg_type: str) -> str:
                return _SpokenTextGuard._normalize(text)
            service.add_text_transformer(_tts_sentence_normalizer)

        # Enforce reasoning_effort='none' on Groq LLM to eliminate <think> tokens and cut latency
        if service_type == "llm" and provider_name == "groq":
            if hasattr(service, "_settings") and hasattr(service._settings, "extra"):
                if isinstance(service._settings.extra, dict):
                    service._settings.extra["reasoning_effort"] = "none"
                    logger.info("Enforced reasoning_effort='none' on {} LLM", provider_name)

        if service_type == "llm" and provider_name == "groq":
            _attach_resilient_failover(service, provider_name, config)

        return service


async def warmup_providers(
    config: dict,
    aiohttp_session: aiohttp.ClientSession | None = None,
) -> None:
    """
    Pre-warm providers and connection pools to reduce cold-start lag.

    CHANGED: previously imported and pinged all 10 registered provider
    classes across every service type (deepgram, openai, cerebras,
    elevenlabs, murf, etc.) regardless of which 3 are actually active for
    this call (sarvam STT, groq LLM, sarvam TTS per active_providers in
    config.yaml). Every unused provider warmed here was pure wasted time
    sitting in front of the outbound greeting. Now scoped to only the
    active providers.

    CHANGED: the health-check ping loop was sequential
    (`for url in warmup_urls: await session.get(...)`), which is up to
    4 * 2s = 8s worst case, serially, before this function could return.
    Now fired concurrently with asyncio.gather.
    """

    provider_registry = config.get("providers", {})
    active_providers = config.get("active_providers", {})

    class_paths = sorted(
        {
            provider_registry.get(service_type, {})
            .get(provider_name, {})
            .get("class_path")
            for service_type, provider_name in active_providers.items()
        }
        - {None}
    )

    for class_path in class_paths:
        try:
            await asyncio.to_thread(
                ServiceFactory._import_class,
                class_path,
            )

        except Exception:
            logger.exception(
                "Warmup: failed to import {}",
                class_path,
            )

    if aiohttp_session and not aiohttp_session.closed:
        warmup_urls = []
        if "sarvam" in (active_providers.get("stt"), active_providers.get("tts")):
            warmup_urls.append("https://api.sarvam.ai/v1")
        if active_providers.get("llm") == "groq":
            warmup_urls.append("https://api.groq.com/openai/v1")

        async def _ping(url: str) -> None:
            try:
                async with aiohttp_session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=2.0),
                ):
                    pass

            except Exception:
                pass

        await asyncio.gather(*(_ping(url) for url in warmup_urls))

    logger.info(
        "Warmup complete: {} provider classes pre-warmed",
        len(class_paths),
    )


_NUMBER_WORDS = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "ek": "1",
    "do": "2",
    "teen": "3",
    "char": "4",
    "paanch": "5",
    "eins": "1",
    "ein": "1",
    "zwei": "2",
    "drei": "3",
    "vier": "4",
    "fünf": "5",
    "funf": "5",
}


def _extract_lead_preferences(text: str, agent_text: str = "") -> dict[str, str]:
    """Extract lead preferences dynamically and robustly from dialog turns.
    
    Instead of brittle, rigid regexes with hardcoded lists, uses flexible
    semantic patterns and cross-references agent confirmations (where the
    LLM has already parsed and disambiguated the user's intent).
    """
    extracted = {}
    user_lower = text.lower().strip()
    lower = f"{text} {agent_text}".lower().strip()

    # Detect language cues from speech
    if any(re.search(r"\b" + re.escape(w) + r"\b", user_lower) for w in ("hindi", "namaste", "bhai", "shukriya", "achha", "theek", "chahiye")):
        extracted["language"] = "Hindi"
    elif any(re.search(r"\b" + re.escape(w) + r"\b", user_lower) for w in ("telugu", "namaskaram", "andi", "cheppandi", "kavali", "avunu")):
        extracted["language"] = "Telugu"

    # 1. Configuration (e.g. 2/3 BHK, bedrooms, villa, penthouse, study/large variants - strictly caller's speech)
    m_cfg = re.search(r"\b([1-5]|one|two|three|four|five|teen|do|ek|char|paanch)?\s*(?:bhk|bed(?:room)?s?|kamre)\b", user_lower)
    has_large = bool(re.search(r"\b(large|larger|big|bigger|study|bada|2100|twenty[- ]one hundred|(?:2|two|do)\s*balcon(?:y|ies)?)\b", user_lower))
    has_std = bool(re.search(r"\b(standard|regular|small|smaller|chota|1580|fifteen eighty|(?:1|one|ek)\s*balcon(?:y|ies)?)\b", user_lower))

    # If both large and standard words appear (e.g. comparing "bigger or standard?"), don't guess
    if has_large and has_std:
        has_large = False
        has_std = False

    if m_cfg:
        num = m_cfg.group(1) or ""
        digit = _NUMBER_WORDS.get(num, num)
        if digit == "3" and has_large:
            extracted["configuration"] = "3 BHK Large (with study & 2 balconies)"
        elif digit == "3" and has_std:
            extracted["configuration"] = "3 BHK Standard (1 balcony)"
        elif digit:
            extracted["configuration"] = f"{digit} BHK"
        else:
            extracted["configuration"] = "3 BHK"
    elif has_large or has_std:
        extracted["unit_size_choice"] = "large" if has_large else "small"
    elif "villa" in user_lower:
        extracted["configuration"] = "Villa"
    elif "penthouse" in user_lower:
        extracted["configuration"] = "Penthouse"

    # 2. Budget (conversational ranges or amounts with Cr/Lakh/Crores - strictly caller's speech)
    # Handles digits (1.5, 95), words (one, two, eighty), and ranges (1 to 2 Cr, 95 Lakhs)
    m_bud = re.search(
        r"\b(?:budget\s*(?:is|of|around)?|under|around|upto|approx(?:imately)?\s*)?(\d+(?:\.\d+)?|\b(?:one|two|three|eighty|ninety)\b)\s*(?:to|-)?\s*(\d+(?:\.\d+)?)?\s*(cr(?:ore)?s?|lakh?s?|lac)\b",
        user_lower,
    )
    if m_bud:
        unit = "Crores" if "cr" in m_bud.group(3).lower() else "Lakhs"
        v1 = _NUMBER_WORDS.get(m_bud.group(1), m_bud.group(1))
        if m_bud.group(2):
            v2 = _NUMBER_WORDS.get(m_bud.group(2), m_bud.group(2))
            extracted["budget"] = f"{v1} to {v2} {unit}"
        else:
            extracted["budget"] = f"{v1} {unit}"

    # 3. Location (generalized prepositional phrases on user text or known area hubs)
    m_loc_prep = re.search(
        r"\b(?:in|near|around|towards|close to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b",
        text,
    )
    _LANGUAGES = {
        "telugu", "hindi", "english", "kannada", "tamil", "malayalam",
        "marathi", "gujarati", "bengali", "punjabi", "urdu", "hinglish",
    }
    if m_loc_prep:
        candidate_loc = m_loc_prep.group(1).strip()
        cand_lower = candidate_loc.lower()
        if cand_lower in _LANGUAGES:
            extracted["language"] = candidate_loc.title()
        else:
            loc_stopwords = {
                "the", "my", "our", "a", "this", "that", "bangalore", "bengaluru",
                "meridian", "flat", "project", "unit", "site", "details", "advance",
                "full", "part", "brief", "short", "fact", "reality", "person", "addition",
                "total", "terms", "mind", "general", "view", "between",
            } | _LANGUAGES
            if cand_lower not in loc_stopwords and len(candidate_loc) > 2:
                extracted["location"] = candidate_loc.title()
    if "location" not in extracted:
        m_loc_known = re.search(
            r"\b(whitefield|sarjapur|bellandur|electronic\s+city|indiranagar|koramangala|hebbal|manyata|marathahalli|hsr|btm|yelahanka|bannerghatta|kanakapura|hennur|devanahalli|outer\s+ring\s+road)\b",
            user_lower,
        )
        if m_loc_known:
            extracted["location"] = m_loc_known.group(1).title()

    # Name evidence is only the caller's explicit self-identification, never our own words.
    candidate_name = explicit_caller_name(text)
    if candidate_name:
        extracted["spoken_name"] = candidate_name

    is_farewell = any(f in user_lower for f in ("bye", "goodbye", "cya", "never mind", "later", "thank you", "thanks"))
    user_has_refusal = any(neg in user_lower for neg in ("not interested", "dont want", "don't want", "nahi chahiye", "kuch nahi"))

    # 5. Site Visit Day & Time (must be stated or confirmed by user, not hallucinated by agent)
    if not user_has_refusal and not is_farewell:
        from leads.worker import normalize_visit_date, normalize_visit_time
        import zoneinfo as _zi
        _now_ist = datetime.now(_zi.ZoneInfo("Asia/Kolkata"))

        norm_d = normalize_visit_date(user_lower, _now_ist)
        if norm_d:
            extracted["visit_date_iso"] = norm_d[0]
            extracted["preferred_visit_date"] = norm_d[0]
        else:
            m_vdate = re.search(
                r"\b(tomorrow|today|this\s+weekend|next\s+weekend|this\s+saturday|this\s+sunday|saturday|sunday|monday|tuesday|wednesday|thursday|friday|kal|aaj|parso)\b",
                user_lower,
            )
            # Hindi Devanagari: \b unreliable — use Unicode-aware search
            if not m_vdate:
                m_vdate = re.search(r"(कल|आज|परसों)", user_lower, re.UNICODE)

            if m_vdate:
                raw_d = m_vdate.group(1).title()
                norm_d2 = normalize_visit_date(raw_d, _now_ist)
                if norm_d2:
                    extracted["visit_date_iso"] = norm_d2[0]
                    extracted["preferred_visit_date"] = norm_d2[0]
                else:
                    extracted["preferred_visit_date"] = raw_d

        norm_t = visit_time(user_lower)
        if norm_t:
            extracted["time_slot"] = norm_t
            extracted["preferred_visit_time"] = norm_t
        else:
            m_vtime = re.search(
                r"\b(?:around|at|maybe)?\s*(\d{1,2}(?::\d{2})?|\b(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b)\s*(?:o'?clock)?\s*(?:in\s+the\s+)?(morning|afternoon|evening|pm|am)\b",
                user_lower,
            )
            if m_vtime:
                hour_raw = m_vtime.group(1)
                period = m_vtime.group(2).lower()
                hour_val = _NUMBER_WORDS.get(hour_raw, hour_raw)
                time_period = "AM" if period in ("am", "morning") else "PM"
                extracted["preferred_visit_time"] = f"{hour_val} {time_period}"
                extracted["time_slot"] = f"{hour_val} {time_period}"
            elif re.search(r"\b(\d{1,2})\s*(?:pm|am)\b", user_lower):
                m_am_pm = re.search(r"\b(\d{1,2})\s*(pm|am)\b", user_lower)
                if m_am_pm:
                    extracted["preferred_visit_time"] = f"{m_am_pm.group(1)} {m_am_pm.group(2).upper()}"
                    extracted["time_slot"] = f"{m_am_pm.group(1)} {m_am_pm.group(2).upper()}"

        v_d = extracted.get("visit_date_iso") or extracted.get("preferred_visit_date")
        v_t = extracted.get("time_slot") or extracted.get("preferred_visit_time")
        if v_d or v_t:
            parts = [p for p in (v_d, v_t) if p]
            extracted["site_visit"] = f"Requested ({' at '.join(parts)})"
        elif re.search(r"\b(?:not\s*(?:available|free|possible|interested)|can'?t\s*make\s*it|busy|no\s*time)\b.*?\b(?:visit|come|weekend|today|tomorrow)\b|\b(?:visit|come)\b.*?\b(?:not\s*(?:available|free|possible)|can'?t)\b", user_lower):
            extracted["site_visit"] = "Declined/Not free right now"
        elif re.search(r"\b(?:this|next)?\s*(?:saturday|sunday|weekend|tomorrow|today)\b", user_lower) and ("visit" in user_lower or "come" in user_lower or "free" in user_lower):
            extracted["site_visit"] = "Tentatively interested"

    # 6. Timeline
    if re.search(r"\b(ready\s*(?:to|-)\s*move|immediate|completed)\b", lower):
        extracted["timeline"] = "Ready-to-move"
    elif re.search(r"\b(under\s*construction|upcoming|new\s*launch|late\s*2027|202\d)\b", lower):
        extracted["timeline"] = "Under-construction"

    # 7. Purpose
    if re.search(r"\b(invest(?:ment)?|rental\s+yield|roi)\b", lower):
        extracted["purpose"] = "Investment"
    elif re.search(r"\b(self\s*use|living|end\s*use|family|parents|son|daughter)\b", lower):
        extracted["purpose"] = "Self-use"

    # 8. Channel mention is not a consent grant or delivery result.
    if not user_has_refusal:
        if re.search(r"\b(whatsapp|brochure|video\s*tour|details\s+on\s+whatsapp|send\s+(?:the\s+)?details|send\s+(?:the\s+)?brochure)\b", user_lower) or (
            any(k in user_lower for k in ("sure", "yes", "send", "share", "theek hai", "bhejo", "chalega"))
            and any(w in lower for w in ("whatsapp", "brochure", "video", "details"))
            and not any(neg in user_lower for neg in ("no", "nahi", "mat", "don't"))
        ):
            extracted["whatsapp"] = "Mentioned/requested, not sent"

    return extracted


_OBJECTION_RE: re.Pattern = re.compile(
    r"^(?:no|nope|nah|nahi|nahin|na|nein|not\s+really|not\s+now|kein\s+interesse)[\.\!\?]*$|"
    r"\bnot\s+(?:at\s+all\s+|that\s+|really\s+|so\s+|much\s+|very\s+)?interested\b|"
    r"\bno\s+interest\b|"
    r"\bkein\s+interesse\b|"
    r"\bnicht\s+interessiert\b|"
    r"\b(?:don'?t|dont)\s+(?:feel\s+interested|want\s+any|want\s+to\s+buy|want\s+this|need)\b|"
    r"\bnot\s+(?:looking|ready|buying|for\s+me)\b|"
    r"\bnot\s+now\b|"
    r"\bno\s+need\b|"
    r"\bno\s+thanks?\b|"
    r"\b(?:nahi|nahin|na)\s+(?:chahiye|lena|interest|dekh\s+rahe)\b|"
    r"\binterest\s+nahi\b|"
    r"\bkuch\s+nahi\b|"
    r"\bdrop\s+(?:the\s+plan|it)\b",
    re.IGNORECASE,
)


def _is_objection_utterance(text: str) -> bool:
    if not text:
        return False
    return bool(_OBJECTION_RE.search(text))


def _sync_working_memory(
    messages: list[dict],
    lead_memory: dict[str, str],
    stream_id: str | None = None,
) -> None:
    """Extract caller preferences and pin a compact working memory system message."""
    newly_extracted: dict[str, str] = {}
    for i, msg in enumerate(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            agent_text = ""
            if i + 1 < len(messages) and messages[i + 1].get("role") == "assistant":
                agent_text = messages[i + 1].get("content", "")
            if isinstance(content, str):
                extracted = _extract_lead_preferences(content, agent_text if isinstance(agent_text, str) else "")
                if extracted.get("unit_size_choice"):
                    existing_bhk = str(lead_memory.get("configuration") or lead_memory.get("bhk") or "")
                    if existing_bhk.startswith("3 BHK"):
                        extracted["configuration"] = "3 BHK Large (with study & 2 balconies)" if extracted["unit_size_choice"] == "large" else "3 BHK Standard (1 balcony)"
                    elif existing_bhk.startswith("2 BHK"):
                        extracted["configuration"] = "2 BHK"
                for k, v in extracted.items():
                    # Preserve existing specific configuration if newly extracted is generic
                    if k == "configuration" and k in lead_memory:
                        existing = lead_memory[k]
                        if len(existing) > len(v) and v.lower() in existing.lower():
                            continue
                    # Tool value always wins: never overwrite a visit date/time
                    # already committed by book_site_visit; pending requests remain correctable.
                    if k in ("preferred_visit_date", "preferred_visit_time", "visit_date_iso", "time_slot") and lead_memory.get("disposition") == "SITE_VISIT_BOOKED":
                        continue
                    if k == "site_visit" and lead_memory.get("disposition") == "SITE_VISIT_BOOKED":
                        continue
                    lead_memory[k] = v
                    newly_extracted[k] = v

    # Budget consistency check: if 3 BHK Large is selected, clear mismatched standard 1.45/1.65 Cr budget
    if "3 BHK Large" in lead_memory.get("configuration", ""):
        curr_b = lead_memory.get("budget", "")
        if "1.45" in curr_b or "1.65" in curr_b:
            lead_memory.pop("budget", None)
            newly_extracted.pop("budget", None)

    # Dynamic identity discrepancy detection (never hardcoded)
    spoken_name = lead_memory.get("spoken_name")
    registered_name = lead_memory.get("registered_name") or lead_memory.get("client")
    if spoken_name and registered_name:
        if spoken_name.lower() != registered_name.lower():
            discrepancy_msg = f"Caller stated '{spoken_name}', registered lead was '{registered_name}'"
            lead_memory["identity_discrepancy"] = discrepancy_msg
            lead_memory["registered_name"] = registered_name
            # Keep the registered identity stable; a stated different name needs confirmation.
            lead_memory["client"] = registered_name
            newly_extracted["identity_discrepancy"] = discrepancy_msg
            newly_extracted["registered_name"] = registered_name
            newly_extracted["spoken_name"] = spoken_name
        else:
            lead_memory["registered_name"] = registered_name

    # Passively persist newly extracted slots via non-blocking async task (0ms audio-loop blocking)
    if stream_id and newly_extracted:
        try:
            asyncio.create_task(lead_state.record_fields_async(stream_id, newly_extracted))
        except Exception:
            pass

    slots = []
    # Client name handling
    client_name = (
        lead_memory.get("client")
        or lead_memory.get("name")
        or lead_memory.get("customer_name")
        or lead_memory.get("spoken_name")
    )
    if lead_memory.get("identity_discrepancy"):
        slots.append(
            f"Client: {registered_name}; caller-stated name (unverified): {lead_memory.get('spoken_name')} "
            f"(Discrepancy: registered as {lead_memory.get('registered_name')}; confirm on dialed number without interrogating)"
        )
    elif client_name:
        slots.append(f"Client: {client_name}")

    config_val = lead_memory.get("configuration") or lead_memory.get("bhk")
    if config_val:
        slots.append(f"Configuration: {config_val}")

    # Preference (combines Timeline and Location if both exist, e.g. "Ready-to-move in Prime Tech Corridor")
    if "preference" in lead_memory:
        slots.append(f"Preference: {lead_memory['preference']}")
    elif "timeline" in lead_memory and "location" in lead_memory:
        slots.append(f"Preference: {lead_memory['timeline']} in {lead_memory['location']}")
    elif "timeline" in lead_memory:
        slots.append(f"Preference: {lead_memory['timeline']}")
    elif "location" in lead_memory:
        slots.append(f"Location: {lead_memory['location']}")

    if lead_memory.get("unit_size_choice"):
        slots.append(f"Unit size choice: {lead_memory['unit_size_choice']}")
    if "budget" in lead_memory:
        slots.append(f"Budget: {lead_memory['budget']}")
    if "purpose" in lead_memory:
        slots.append(f"Purpose: {lead_memory['purpose']}")

    # Define user_msgs here — used in the site visit gate and objection blocks below
    user_msgs = [m for m in messages if m.get("role") == "user" and isinstance(m.get("content"), str)]

    # Site visit handling (explicit gate: day/time required before confirmation)
    v_date = lead_memory.get("visit_date_iso") or lead_memory.get("preferred_visit_date")
    v_time = lead_memory.get("time_slot") or lead_memory.get("preferred_visit_time")
    has_visit_slot = bool(v_date or v_time)
    if "site_visit" in lead_memory:
        sv_val = lead_memory["site_visit"]
        if has_visit_slot or "Confirmed" in sv_val:
            slots.append(f"Site Visit: {sv_val}" if lead_memory.get("disposition") == "SITE_VISIT_BOOKED" else f"Site Visit: {sv_val}; not booked")
            if v_date:
                slots.append(f"Visit Date ISO: {v_date}")
            if v_time:
                slots.append(f"Time Slot: {v_time}")
        elif "Declined" in sv_val:
            slots.append(f"Site Visit: {sv_val}")
        else:
            slots.append("Site Visit Requested: ask missing day/time; not booked.")
    elif has_visit_slot:
        v_parts = [p for p in (v_date, v_time) if p]
        slots.append(f"Site Visit: {' at '.join(v_parts)}")
        if v_date:
            slots.append(f"Visit Date ISO: {v_date}")
        if v_time:
            slots.append(f"Time Slot: {v_time}")
    elif any(
        re.search(r"\b(site\s*visit|visit\s*the\s*property|come\s*(?:and|to)?\s*see|schedule\s*(?:a\s*)?visit|want\s*to\s*visit)\b", m.get("content", "").lower())
        for m in user_msgs
    ):
        slots.append("Site Visit Requested: ask missing day/time; not booked.")

    if lead_memory.get('_postcall_whatsapp_actions'):
        slots.append('Manual WhatsApp consent recorded for ' + ', '.join(lead_memory['_postcall_whatsapp_actions']) + '; prepared after call, not sent. Do not ask consent again or call send_brochure for recorded actions.')
    if lead_memory.get("_brochure_consent_pending"):
        slots.append("Brochure pending channel consent: yes to the immediately preceding WhatsApp question calls send_brochure; no cancels. Never claim sent from consent.")
    if "whatsapp" in lead_memory:
        slots.append(f"WhatsApp: {lead_memory['whatsapp']} (not delivery proof)")
    if "language" in lead_memory:
        lang_val = lead_memory["language"]
        if lang_val == "Hindi":
            slots.append("Language: natural Hinglish, English property terms.")
        elif lang_val == "Telugu":
            slots.append("Language: conversational Telugu, English property terms.")
        else:
            slots.append(f"Active Language: {lang_val}; match mixed caller speech.")

    # Track topics already discussed/covered across turns to prevent repetitive questions
    # CRITICAL: Persist in lead_memory["_covered_topics"] so history pruning never erases them!
    spoken_assistant_text = " ".join(
        msg.get("content", "") for msg in messages 
        if msg.get("role") == "assistant" and isinstance(msg.get("content"), str)
    ).lower()

    spoken_dialog_text = " ".join(
        msg.get("content", "") for msg in messages 
        if msg.get("role") in ("assistant", "user") and isinstance(msg.get("content"), str)
    ).lower()

    # Retrieve or initialize persistent covered topics set
    covered_set = lead_memory.setdefault("_covered_topics", set())
    if isinstance(covered_set, list):
        covered_set = set(covered_set)
        lead_memory["_covered_topics"] = covered_set

    # Accumulate newly covered topics into the persistent set
    if "configuration" in lead_memory:
        covered_set.difference_update({v for v in covered_set if v.startswith("Configuration confirmed (")})
        covered_set.add(f"Configuration confirmed ({lead_memory['configuration']})")
    if "budget" in lead_memory:
        covered_set.add("Budget shared")
    if any(k in spoken_assistant_text for k in ("sq ft", "carpet area", "dimensions", "floor plan", "brochure", "video")) or any(k in spoken_dialog_text for k in ("floor plan", "brochure", "video")):
        covered_set.add("Floor plan/brochure discussed (not delivery proof)")
    if any(k in spoken_assistant_text for k in ("crore", "lakh", "price", "pricing")):
        covered_set.add("Pricing shared")
    if any(k in spoken_assistant_text for k in ("10 mins from", "connectivity", "metro station", "commute")):
        covered_set.add("Location details shared")
    if any(k in spoken_assistant_text for k in ("clubhouse", "swimming pool", "pool", "gym", "amenities")):
        covered_set.add("Amenities shared")
    if any(k in spoken_dialog_text for k in ("site visit", "visit scheduled", "visit confirmed")):
        covered_set.add("Site visit discussed")
    if "whatsapp" in lead_memory or any(k in spoken_dialog_text for k in ("on whatsapp", "share details on whatsapp", "send the brochure", "video tour on whatsapp", "send me those")):
        covered_set.add("WhatsApp discussed (not consent or delivery proof)")

    if covered_set:
        slots.append(f"Already Covered: {', '.join(sorted(covered_set))}; do not repeat")

    # Track caller objection/hesitation attempts deterministically across turns
    is_active_objection_turn = False
    pivot_already_spoken = bool(lead_memory.get("_objection_pivot_spoken", False))

    if user_msgs:
        latest_user_text = user_msgs[-1]["content"]
        if _is_objection_utterance(latest_user_text):
            is_active_objection_turn = True
            last_checked = lead_memory.get("_last_objection_text", "")
            if last_checked != latest_user_text.strip().lower():
                lead_memory["_last_objection_text"] = latest_user_text.strip().lower()
                # If pivot probe was already spoken, advance objection count to 2 or 3
                if pivot_already_spoken:
                    lead_memory["_objection_count"] = max(lead_memory.get("_objection_count", 1) + 1, 2)
                else:
                    lead_memory["_objection_count"] = 1

    obj_count = lead_memory.get("_objection_count", 0)

    has_confirmed_whatsapp = bool(lead_memory.get("whatsapp_opt_in"))
    has_rejected_whatsapp = False
    for i, msg in enumerate(messages):
        if msg.get("role") == "user":
            user_txt = (msg.get("content") or "").strip().lower()
            if not user_txt:
                continue
            # Explicit refusal targeting WhatsApp/brochure/messaging
            if re.search(
                r"\b(?:no|don'?t|dont|mat|stop|nahi|nicht|kein)\b.*?\b(?:send|whatsapp|share|brochure|detail|anything|broschüre)\b",
                user_txt,
            ):
                has_rejected_whatsapp = True
                break
            # Or rejection immediately following assistant pitching WhatsApp/brochure
            if i > 0 and messages[i - 1].get("role") == "assistant":
                prev_agent = (messages[i - 1].get("content") or "").lower()
                if (
                    ("whatsapp" in prev_agent or "brochure" in prev_agent)
                    and re.search(r"^(?:no|nope|nah|nahi|mat|no\s+thanks?|nahi\s+shukriya)[\.\!\?]*$", user_txt)
                ):
                    has_rejected_whatsapp = True
                    break

    whatsapp_already_pitched = bool(lead_memory.get("_whatsapp_pitched")) or any(
        "whatsapp" in m.get("content", "").lower() and "brochure" in m.get("content", "").lower()
        for m in messages if m.get("role") == "assistant"
    )
    if whatsapp_already_pitched:
        lead_memory["_whatsapp_pitched"] = True

    if lead_memory.get('_postcall_whatsapp_actions'):
        pass  # recorded manual consent above is the authoritative call state
    elif has_confirmed_whatsapp:
        slots.append("WhatsApp Brochure: mentioned, not sent. Only send_brochure result establishes queued/API acceptance. Answer caller questions.")
    elif is_active_objection_turn:
        if obj_count == 1 and not pivot_already_spoken:
            slots.append(
                'Objection Attempt: 1 of 2. Caller hesitated/objected. DO NOT CLOSE OR END CALL. Must probe: "Totally understand, is it the location, price, or just not the right time?"'
            )
        elif not has_rejected_whatsapp:
            if not whatsapp_already_pitched:
                slots.append(
                    'Objection Attempt: 2 of 2 (Consultative Step). Address concern concisely: If price concern, highlight the flexible 10% booking payment plan (construction-linked, loan-approved by SBI/HDFC/ICICI, or 2 BHK from 95L). If location, highlight Prime Tech Corridor connectivity (10 mins to Metro). Ask one question about their price or location concern. DO NOT introduce costlier comparison areas. DO NOT close yet.'
                )
            else:
                slots.append(
                    'Consultative Step: Address caller\'s concern directly (flexible 10% booking plan, loan approved by SBI/HDFC/ICICI, Prime Tech Corridor connectivity, or 2 BHK from 95L). DO NOT introduce costlier comparison areas. DO NOT re-pitch brochure. DO NOT close yet.'
                )
        else:
            slots.append(
                'Objection (Firm Refusal): Caller refused WhatsApp/brochure. Close gracefully: "Understood, thanks for your time. Have a great day!"'
            )
    elif pivot_already_spoken:
        if has_rejected_whatsapp:
            slots.append(
                'Objection (Firm Refusal): Caller refused. Close gracefully: "Understood, thanks for your time. Have a great day!"'
            )
        elif not whatsapp_already_pitched:
            slots.append(
                'Consultative Step: Address concern concisely (10% flexible booking plan, loan approved by SBI/HDFC/ICICI, or 2 BHK from 95L). Ask one question about their price or location concern. DO NOT introduce costlier comparison areas. DO NOT close.'
            )
        else:
            slots.append(
                'Consultative Dialogue: Answer caller\'s property/pricing question directly. DO NOT re-pitch brochure. DO NOT close call while caller is asking questions.'
            )

    # Strip any existing [ACTIVE LEAD STATE: ...] block cleanly across all system messages
    for msg in messages:
        if msg.get("role") == "system":
            content = msg.get("content", "")
            if isinstance(content, str) and "[ACTIVE LEAD STATE:" in content:
                parts = content.split("[ACTIVE LEAD STATE:")
                base = parts[0].rstrip()
                after_bracket = parts[1].split("]", 1)[-1].strip() if "]" in parts[1] else ""
                msg["content"] = f"{base}{(' ' + after_bracket) if after_bracket else ''}".strip()

    if not slots and not lead_memory:
        return

    memory_str = f"[ACTIVE LEAD STATE: {' | '.join(slots)}]"
    lead_memory["_working_memory"] = memory_str

    # Find the LAST system message so static prompt prefix remains 100% frozen for Groq prefix caching
    last_sys_msg = None
    for msg in messages:
        if msg.get("role") == "system":
            last_sys_msg = msg

    if last_sys_msg is not None and sum(m.get("role") == "system" for m in messages) > 1:
        content = last_sys_msg.get("content", "")
        last_sys_msg["content"] = f"{content.rstrip()}\n\n{memory_str}".strip()
    else:
        # Fallback if no system message exists
        messages.insert(1 if messages and messages[0].get("role") == "system" else 0, {"role": "system", "content": memory_str})


def _coalesce_consecutive_messages(messages: list) -> None:
    """Cleans up empty messages in context without destroying distinct turns.
    Preserves distinct caller turns, repeated words, and revisions.
    CRITICAL: Never coalesces 'system' messages (which must remain separate for Working Memory).
    """
    if len(messages) <= 1:
        return

    cleaned = []
    for msg in messages:
        if not isinstance(msg, dict):
            cleaned.append(msg)
            continue
        content = msg.get("content")
        if isinstance(content, str) and not content.strip():
            continue
        cleaned.append(dict(msg))

    messages[:] = cleaned


def _prune_history(messages: list, max_messages: int) -> None:
    # First coalesce consecutive messages so turns are clean and compact
    _coalesce_consecutive_messages(messages)

    if len(messages) <= max_messages:
        return

    system_messages = [m for m in messages if m.get("role") == "system"]
    non_system = [m for m in messages if m.get("role") != "system"]
    keep = max_messages - len(system_messages)
    if keep < 2:
        keep = 2

    trimmed_non_system = non_system[-keep:]
    # Ensure conversation history always starts with a user message (never an orphaned assistant message)
    while trimmed_non_system and trimmed_non_system[0].get("role") != "user":
        trimmed_non_system = trimmed_non_system[1:]
    if not trimmed_non_system:
        trimmed_non_system = non_system[-keep:]

    messages[:] = [
        *system_messages,
        *trimmed_non_system,
    ]


# ---------------------------------------------------------------------------
# Static TTS Audio Cache
# Pre-generated .wav files for the 4 most-common static bot phrases.
# Loaded once at module level. Missing files = graceful fallback to live TTS.
# Phrases are keyed by their NORMALIZED text (after _SpokenTextGuard._normalize).
# ---------------------------------------------------------------------------

def _load_audio_cache() -> dict[int, dict[str, bytes]]:
    """Load pre-generated WAV audio for static phrases from static_audio/india/.

    Resamples files to active pipeline sample rates (16000Hz for web,
    8000Hz for telephony) using soxr for pristine audio quality and zero runtime latency.
    """
    import wave
    from audio_provenance import effective_config, fresh_audio, PHRASES
    expected_config = effective_config(Path(__file__).parent)
    import numpy as np
    import soxr

    cache: dict[int, dict[str, bytes]] = {16000: {}, 8000: {}}
    base_dir = Path(__file__).parent / "static_audio"
    static_dir = base_dir / "india"
    if not static_dir.exists():
        static_dir = base_dir

    phrase_files = {"short_goodbye": "short_goodbye.wav", **{k: k + ".wav" for k in {**FAQ_TEXTS, **CALL5_TEXTS, **CALL7_TEXTS}},
        "greeting_alex": "greeting_alex.wav",
        "greeting_generic": "greeting_generic.wav",
        "inbound_greeting": "inbound_greeting.wav",
        "opening_intro": "opening_intro.wav",
        "brochure_close": "brochure_close.wav",
        "objection_pivot": "objection_pivot.wav",
        "final_farewell": "final_farewell.wav",
        "farewell_polite": "farewell_polite.wav",
        "farewell_hi": "farewell_hi.wav",
        "transfer_announcement": "transfer_announcement.wav",
        "ack_sure": "ack_sure.wav",
        "ack_understood": "ack_understood.wav",
        "ack_got_it": "ack_got_it.wav",
        "ack_ji_bilkul": "ack_ji_bilkul.wav",
        "ack_haanji": "ack_haanji.wav",
        "ack_theek_hai": "ack_theek_hai.wav",
        "clarify_repeat": "clarify_repeat.wav",
        "clarify_repeat_hi": "clarify_repeat_hi.wav",
        "clarify_property": "clarify_property.wav",
        "visit_confirm": "visit_confirm.wav",
        "checkin_generic": "checkin_generic.wav",
        "filler_en": "filler_en.wav",
        "filler_hi": "filler_hi.wav",
    }

    def _load_file(path: Path, key: str):
        if not path.exists():
            return
        if not fresh_audio(path, key, PHRASES.get(key, ""), expected_config):
            logger.info("Audio cache skipped {}: missing/stale voice provenance; regenerate Sarvam cache", key)
            return
        try:
            with wave.open(str(path), "rb") as wf:
                sr_in = wf.getframerate()
                raw_pcm = wf.readframes(wf.getnframes())

            audio_int16 = np.frombuffer(raw_pcm, dtype=np.int16)
            audio_float = audio_int16.astype(np.float32) / 32768.0

            for target_sr in (16000, 8000):
                if sr_in == target_sr:
                    cache[target_sr][key] = raw_pcm
                else:
                    resampled = soxr.resample(audio_float, sr_in, target_sr)
                    pcm_out = (np.clip(resampled, -1.0, 1.0) * 32767.0).astype(np.int16).tobytes()
                    cache[target_sr][key] = pcm_out

            logger.info(
                "Audio cache loaded & resampled: {} (16kHz: {} KB, 8kHz: {} KB)",
                key,
                len(cache[16000][key]) // 1024,
                len(cache[8000][key]) // 1024,
            )
        except Exception as exc:
            logger.warning("Audio cache: failed to load/resample {}: {}", path.name, exc)

    for key, filename in phrase_files.items():
        _load_file(static_dir / filename, key)

    return cache


_AUDIO_CACHE: dict[int, dict[str, bytes]] = _load_audio_cache()


def _match_cached_phrase(text: str, turn_count: int = 1) -> str | None:
    """Matches candidate text against known pre-rendered audio cache keys.
    Matches Turn 1 opening intro, objection pivots, acknowledgments, and farewells.
    """
    if not text:
        return None
    t = text.lower().strip()
    for key, phrase in {"short_goodbye": SHORT_GOODBYE, **FAQ_TEXTS, **CALL5_TEXTS, **CALL7_TEXTS}.items():
        if t == phrase.lower():
            return key

    # Outbound greetings
    if "am i speaking with alex" in t:
        return "greeting_alex"
    if ("meridian" in t or "ananya" in t) and "good time to talk" in t:
        return "greeting_generic"
    if "thank you for calling meridian group" in t and "assist you today" in t:
        return "inbound_greeting"

    # Turn 1 Opening - STRICTLY restricted to turn_count == 1 only
    if turn_count == 1 and (
        ("meridian" in t and ("bhk" in t or "2" in t or "3" in t))
        or ("meridian" in t or "ananya" in t)
    ):
        return "opening_intro"

    # Clarifications & spam check
    if "are you still there" in t or "hello? are you there" in t or "hello are you there" in t:
        return "checkin_generic"
    if "are you looking for a property" in t:
        return "clarify_property"
    if "didn't catch that" in t or "say that again" in t:
        return "clarify_repeat"
    if "sun nahi paayi" in t or "repeat kar sakte hain" in t:
        return "clarify_repeat_hi"

    # Quick Acknowledgments
    if t in ("sure, absolutely.", "sure, absolutely", "sure absolutely"):
        return "ack_sure"
    if t in ("understood.", "understood", "okay, got it.", "okay got it"):
        return "ack_understood"
    if t in ("ji bilkul.", "ji bilkul", "ji, bilkul"):
        return "ack_ji_bilkul"
    if t in ("haanji, bilkul.", "haanji, bilkul", "haanji bilkul", "haanji"):
        return "ack_haanji"
    if t in ("theek hai.", "theek hai"):
        return "ack_theek_hai"

    # Site visit confirmation (only generic preference, never override actual scheduled visit details)
    if "preference for the site visit" in t:
        return "visit_confirm"

    # Objection pivot
    if "totally understand" in t and ("location" in t or "price" in t or "right time" in t or "time" in t):
        return "objection_pivot"

    # Brochure close
    if t == "sure, our team will share the brochure and floor plans on whatsapp shortly. have a wonderful day!":
        return "brochure_close"

    # Farewells
    if "dhanyavaad" in t and "din shubh rahe" in t:
        return "farewell_hi"
    if any(k in t for k in ("thanks for your time", "thank you for your time")) and any(k in t for k in ("have a great day", "have a nice day", "have a good day", "have a wonderful day", "goodbye", "bye")):
        return "final_farewell"

    return None


_CACHED_PHRASE_TEXTS: dict[str, str] = {"short_goodbye": SHORT_GOODBYE, **FAQ_TEXTS, **CALL5_TEXTS, **CALL7_TEXTS,
    "greeting_alex": "Hi, am I speaking with Alex?",
    "greeting_generic": "Hi, this is Ananya from Meridian Group. Is this a good time to talk?",
    "inbound_greeting": "Hello, thank you for calling Meridian Group. How may I assist you today?",
    "opening_intro": "Great, this is Ananya from Meridian Group. Are you looking for a 2 or 3 BHK?",
    "brochure_close": "Sure, our team will share the brochure and floor plans on WhatsApp shortly. Have a wonderful day!",
    "objection_pivot": "Totally understand, is it the location, price, or just not the right time?",
    "final_farewell": "Understood, thanks for your time. Have a wonderful day!",
    "farewell_polite": "Thank you for your time. Have a great day!",
    "farewell_hi": "Dhanyavaad, aapka din shubh rahe!",
    "transfer_announcement": "Please hold while I connect you to a senior property advisor.",
    "ack_sure": "Sure, absolutely.",
    "ack_understood": "Understood.",
    "ack_got_it": "Okay, got it.",
    "ack_ji_bilkul": "Ji bilkul.",
    "ack_haanji": "Haanji, bilkul.",
    "ack_theek_hai": "Theek hai.",
    "checkin_generic": "Hello? Are you still there?",
    "clarify_repeat": "Sorry, I didn't catch that. Could you say that again?",
    "clarify_repeat_hi": "Sorry, main sun nahi paayi. Kya aap repeat kar sakte hain?",
    "clarify_property": "Hello? Are you looking for a property?",
    "visit_confirm": "Wonderful, I have noted your preference for the site visit.",
    "filler_en": "Sure, one moment, let me check that for you.",
    "filler_hi": "Haan, ek second, main check karti hoon.",
}


def _find_sentence_end(text: str) -> int:
    """Finds index right after a true sentence boundary ([.!?\n]), ignoring decimal dots
    (e.g. '1.45', '3.5') and common abbreviations ('Rs.', 'Cr.', 'Mr.', 'Mrs.', 'Dr.', 'BHK').
    Returns 0 if no sentence boundary is found."""
    for m in re.finditer(r'([.!?\n])(?:\s+|$)', text):
        punct = m.group(1)
        idx = m.end()
        if punct in ('!', '?', '\n'):
            return idx
        if punct == '.':
            start = m.start(1)
            # Ignore decimal dot: digit before and digit after (e.g. 1.45, 3.5)
            if start > 0 and start + 1 < len(text):
                if text[start - 1].isdigit() and text[start + 1].isdigit():
                    continue
            # Ignore common abbreviation preceding
            prefix = text[:start].strip().split()
            last_word = prefix[-1].lower() if prefix else ""
            if last_word in ("rs", "cr", "dr", "mr", "mrs", "ms", "no", "sq", "ft", "bhk"):
                continue
            return idx
    return 0


def _find_clause_or_sentence_end(text: str) -> int:
    """Finds first safe clause boundary for early TTS flushing on the first chunk of a turn.
    Returns character index to split at (inclusive of delimiter), or -1 if no boundary found.
    Checks:
    1. Full sentence end (e.g. '.', '?', '!')
    2. Clause boundary: comma or dash with space (after at least 10 characters)
    3. Word boundary after ~6 words (at least 20 chars)
    """
    if not text:
        return -1
    # 1. Full sentence end
    s_end = _find_sentence_end(text)
    if s_end > 0:
        return s_end

    # 2. Clause boundary: comma + space (e.g. "Sure, ", "Yes definitely, ")
    m_comma = re.search(r'(?<!\d),\s+', text[3:])
    if m_comma:
        return 3 + m_comma.end()

    # 3. Dash with spaces (e.g. " - ", " – ", " — ")
    m_dash = re.search(r'\s+[-–—]\s+', text[3:])
    if m_dash:
        return 3 + m_dash.end()

    # 4. Word boundary after ~6 words (at least 20 chars)
    words = text.strip().split()
    if len(words) >= 6:
        w_cnt = 0
        in_word = False
        for i, ch in enumerate(text):
            if not ch.isspace():
                if not in_word:
                    w_cnt += 1
                    in_word = True
            else:
                if in_word:
                    in_word = False
                    if w_cnt >= 6 and i >= 20:
                        return i + 1

    return -1


class _SpokenTextGuard(FrameProcessor):
    """
    Normalizes text right before it reaches TTS so Sarvam speaks natural
    language instead of literal symbols or single characters.

    FIXED: this class used to contain a full copy of _TerminationProcessor's
    process_frame body (interruption handling, farewell regex matching,
    hangup scheduling) but never defined __init__, so every attribute it
    touched (self.silent_termination_patterns, self._waiting_for_bot_stop,
    self._hangup_task, ...) never existed on the instance. Every frame that
    passed through it -- including the very first outbound greeting --
    raised AttributeError, which is exactly the crash in the Sept 1 log
    ('_SpokenTextGuard' object has no attribute 'silent_termination_
    patterns'). Termination detection already runs correctly downstream
    in _TerminationProcessor; it never belonged here, and duplicating it
    here only broke things.

    This class now does what its docstring always claimed: normalize text
    for speech. That's the direct fix for two production symptoms:
      - Literal symbols read aloud ("greater than", "less than", "and")
        when the LLM emits text like "18 < income < 25000" or "EMI & SIP".
      - Fragmented, letter-by-letter speech: with every frame erroring out
        above, TTS was getting an inconsistent, gappy stream of text
        instead of clean, complete phrases. Fixing the crash restores a
        normal flow of complete text into TTS's own chunking/buffering.

    DO NOT CHANGE the substitutions or their ordering below without testing
    real TTS output first. This exact implementation is confirmed correct
    in production. In particular: never replace the collapse-multiple-
    spaces-to-one pattern (currently a 2-or-more-whitespace regex) with
    anything that strips ALL whitespace down to zero, and never rebuild
    `text` by joining tokens/words back together without a single-space
    separator -- either of those produces run-together speech like
    "HiSimranthisisAnanyafromMeridianGroup...". Nothing here does that
    today; keep it that way.
    """

    _PROPERTY_NUMBER_MAP: dict[str, str] = {
        "1150": "eleven fifty",
        "1320": "thirteen twenty",
        "1580": "fifteen eighty",
        "1750": "seventeen fifty",
        "1920": "nineteen twenty",
        "2100": "twenty-one hundred",
        "1100": "eleven hundred",
        "1200": "twelve hundred",
        "1300": "thirteen hundred",
        "1400": "fourteen hundred",
        "1500": "fifteen hundred",
        "1600": "sixteen hundred",
        "1800": "eighteen hundred",
        "2000": "two thousand",
        "2200": "twenty-two hundred",
        "2400": "twenty-four hundred",
        "2500": "twenty-five hundred",
    }

    _REPLACEMENTS: list[tuple[re.Pattern, str]] = [
        # INR currency (India)
        (re.compile(r"[\u20B9]\s*(\d+(?:\.\d+)?)\s*(?:L|Lakhs?|lac|lacs)\b", re.IGNORECASE), r"\1 lakhs"),
        (re.compile(r"[\u20B9]\s*(\d+(?:\.\d+)?)\s*(?:Cr|Crores?)\b", re.IGNORECASE), r"\1 crores"),
        (re.compile(r"[\u20B9]\s*(\d+(?:\.\d+)?)", re.IGNORECASE), r"\1 rupees"),
        (re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:L|Lakhs?|lac|lacs)\b", re.IGNORECASE), r"\1 lakhs"),
        (re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:Cr|Crores?)\b", re.IGNORECASE), r"\1 crores"),
        (re.compile(r"\s*>=\s*"), " at least "),
        (re.compile(r"\s*<=\s*"), " at most "),
        (re.compile(r"\s*&\s*"), " and "),
        (re.compile(r"\s*%\s*"), " percent "),
        (re.compile(r"\b(\d+)\s*mins?\b", re.IGNORECASE), r"\1 minutes"),
        (re.compile(r"\b(\d+)\s*hrs?\b", re.IGNORECASE), r"\1 hours"),
        (re.compile(r"(\d),(\d)"), r"\1\2"),
        (re.compile(r"(\d+)\s*-\s*(\d+)"), r"\1 to \2"),
        (re.compile(r"\bsq\.?\s*ft\.?|\bsqft\b", re.IGNORECASE), "square feet"),
        (re.compile(r"[*_`#]+"), ""),          # markdown emphasis/headers
        (re.compile(r"[~^|=]"), " "),          # strip tildes, carats, pipes, and EQUALS so = is never spoken aloud!
        (re.compile(r"[\(\)]"), " "),          # strip parentheses so ( and ) are never read aloud
        (re.compile(r"\s{2,}"), " "),
    ]

    _META_ANNOUNCEMENT_RE: re.Pattern = re.compile(
        r"^\s*(?:(?:Sure|Okay|Alright|Yes|Certainly|Understood|Got it|हाँ|जी|ज़रूर|अच्छा)[,\.]?\s*)?"
        r"(?:(?:let me|allow me|I will|I'll|switching to|switched to|मैं)\s+"
        r"(?:switch\s+to\s+|speak\s+in\s+|में\s+बात\s+करती\s+हूँ|में\s+बोलती\s+हूँ|में\s+बताती\s+हूँ)?"
        r"(?:Hindi|Telugu|English|हिंदी|तेलुगु|अंग्रेजी)[,\.!:\u0964]*\s*)",
        re.IGNORECASE,
    )

    @classmethod
    def _strip_meta_announcements(cls, text: str) -> str:
        if not text:
            return text
        cleaned = cls._META_ANNOUNCEMENT_RE.sub("", text)
        if len(cleaned) < len(text):
            logger.info(
                "SpokenTextGuard: Stripped meta announcement '{}'",
                text[: len(text) - len(cleaned)].strip(),
            )
            cleaned = cleaned.lstrip()
        return cleaned

    @classmethod
    def _convert_raw_tool_syntax(cls, text: str) -> tuple[str, dict]:
        """Converts accidental raw LLM tool syntax (e.g. 'booksitevisit(date=tomorrow, time=4 PM)')
        into natural spoken dialogue so callers never hear code syntax or '=' over the phone."""
        if not text:
            return text, {}

        extracted: dict[str, str] = {}

        # Delete fake-booking regex: strip any leaked code syntax without claiming booking
        text = re.sub(
            r"(?:call\s+)?\b(?:book_?site_?visit|booksitevisit)\s*\([^\)]*\)",
            "",
            text,
            flags=re.IGNORECASE,
        )

        def _replace_brochure(m: re.Match) -> str:
            return ""

        text = re.sub(
            r"(?:call\s+)?\b(?:send_?brochure|sendbrochure)\s*\([^\)]*\)",
            _replace_brochure,
            text,
            flags=re.IGNORECASE,
        )

        def _replace_handoff(m: re.Match) -> str:
            extracted["handoff"] = "requested"
            return "Let me connect you with a senior property advisor right away."

        text = re.sub(
            r"(?:call\s+)?\b(?:handoff_?to_?human|handofftohuman)\s*\([^\)]*\)",
            _replace_handoff,
            text,
            flags=re.IGNORECASE,
        )

        def _replace_end_call(m: re.Match) -> str:
            extracted["end_call"] = "requested"
            return "Thank you for your time. Have a wonderful day!"

        text = re.sub(
            r"(?:call\s+)?\b(?:end_?call|endcall)\s*\([^\)]*\)",
            _replace_end_call,
            text,
            flags=re.IGNORECASE,
        )

        # Clean any remaining code-like function call syntax func(a=b)
        text = re.sub(r"\b[a-zA-Z_]\w*\s*\([^=)]*=[^)]*\)", "", text)

        return text, extracted

    @classmethod
    def _expand_quarters_and_years(cls, text: str) -> str:
        def _rep_quarter(m: re.Match) -> str:
            q, yr = m.group(1), m.group(2)
            q_map = {"1": "early", "2": "mid", "3": "mid", "4": "late"}
            yr_words = {"2026": "twenty twenty-six", "2027": "twenty twenty-seven", "2028": "twenty twenty-eight"}
            q_word = q_map.get(q, f"quarter {q}")
            yr_word = yr_words.get(yr, yr)
            return f"{q_word} {yr_word}"
        return re.sub(r"\bQ([1-4])\s*(20\d\d)\b", _rep_quarter, text, flags=re.IGNORECASE)

    @classmethod
    def _expand_numbers(cls, text: str) -> str:
        text = cls._expand_quarters_and_years(text)
        for num, words in cls._PROPERTY_NUMBER_MAP.items():
            comma_num = f"{num[0]},{num[1:]}"
            text = re.sub(rf"\b(?:{num}|{comma_num})\b", words, text)
        return text

    def _normalize(self_or_cls, text: str | None = None) -> str:
        if text is None:
            text = str(self_or_cls)
            self = None
        else:
            self = self_or_cls if not isinstance(self_or_cls, type) else None

        # Strip internal language-switching meta announcements
        text = _SpokenTextGuard._strip_meta_announcements(text)

        # Intercept and convert accidental raw tool syntax (e.g. booksitevisit(date=tomorrow, time=4 PM))
        text, tool_extracted = _SpokenTextGuard._convert_raw_tool_syntax(text)
        if tool_extracted and self is not None:
            if getattr(self, "_lead_memory", None) is not None:
                self._lead_memory.update(tool_extracted)
            if getattr(self, "_stream_id", None):
                try:
                    asyncio.create_task(
                        lead_state.record_fields_async(self._stream_id, tool_extracted)
                    )
                except Exception as exc:
                    logger.debug("SpokenTextGuard: async record_fields error: {}", exc)

        # Strip reasoning/think tags and XML-like tags (e.g. </think>, <think>)
        if "<" in text or ">" in text:
            text = re.sub(r"</?think>", "", text, flags=re.IGNORECASE)
            text = re.sub(r"<[^>]+>", "", text)
            text = text.replace("<", "").replace(">", "")

        # FIX 6: Safety net for dates and hyphens
        # 1. Convert any residual ISO date YYYY-MM-DD into natural conversational spoken date
        def _rep_iso_date(m: re.Match) -> str:
            iso_val = m.group(0)
            try:
                import zoneinfo
                _now = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata"))
            except Exception:
                _now = datetime.now()
            from leads.worker import format_spoken_date
            lang = getattr(self, "_language", "en") if self is not None else "en"
            return format_spoken_date(iso_val, _now, lang)

        text = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", _rep_iso_date, text)

        # 2. Drop 4-digit year from spoken dates (e.g. "09 Oct 2026" -> "09 Oct")
        text = re.sub(
            r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+20\d\d\b",
            r"\1 \2",
            text,
            flags=re.IGNORECASE,
        )

        # 3. Replace hyphens and dashes between words with commas so "Tomorrow - Thursday" never becomes "Tomorrow to Thursday"
        text = re.sub(r"(?<=[a-zA-Z])\s*[-–—]\s*(?=[a-zA-Z])", ", ", text)

        text = (
            text
            .replace("’", "'")
            .replace("‘", "'")
            .replace("“", '"')
            .replace("”", '"')
            .replace("—", ", ")
            .replace(": ", ". ")
            .replace("; ", ". ")
        )
        # Convert en-dash to "to" only between numbers; otherwise comma
        text = re.sub(r"(?<=\d)\s*–\s*(?=\d)", " to ", text)
        text = text.replace("–", ", ")

        # Split options/clauses joined by ', or ' or ', and ' into separate sentences
        # so TTS begins synthesizing the first option immediately instead of waiting for a 100+ character compound sentence.
        text = re.sub(r",\s+or\b", ". Or", text, flags=re.IGNORECASE)
        text = re.sub(r",\s+and\b", ". And", text, flags=re.IGNORECASE)

        # Expand property numbers and quarters into natural conversational phonetics
        text = _SpokenTextGuard._expand_numbers(text)

        for pattern, replacement in _SpokenTextGuard._REPLACEMENTS:
            text = pattern.sub(replacement, text)

        # Convert isolated leading acknowledgment with period into comma so TTS doesn't split it into a tiny 6-char frame
        text = re.sub(
            r"^(\s*(?:Great|Got it|Sure|Okay|Right|Understood|Perfect|Definitely|Certainly))\.\s+",
            r"\1, ",
            text,
            flags=re.IGNORECASE,
        )

        # NEVER call .strip() here: streaming TextFrames contain leading/trailing
        # spaces that must be preserved between words to prevent run-together speech.
        return text

    _OPENING_REPETITION_RE: re.Pattern = re.compile(
        r"^\s*(?:(?:Hello|Hi|Hey)[,\.]?\s*)?"
        r"(?:(?:this\s+is\s+Ananya)(?:\s+from\s+Meridian(?:\s+Group)?)?[,\.]?\s*)"
        r"(?:Are\s+you\s+(?:looking\s+for|exploring)\s+(?:a\s+)?(?:2[,\s]|3[,\s]|[23]\s*BHK|[23]\s*bedroom|\d+\s+(?:bedroom|BHK))[\w\s,&]*[\?\.]*\s*)?",
        re.IGNORECASE,
    )

    def _strip_repeated_opening(self, text: str) -> str:
        if not text or self._turn_count <= 1:
            return text
        if self._OPENING_REPETITION_RE.search(text):
            logger.info(
                "SpokenTextGuard: Stripped repetitive mid-call opening introduction from turn {}",
                self._turn_count,
            )
            remainder = self._OPENING_REPETITION_RE.sub("", text).strip()
            if remainder:
                return remainder
            return "Yes, I'm listening. What's on your mind?"
        if self._turn_count > 1:
            text = re.sub(r"^\s*(?:hi|hello)[, ]+(?:alex[, ]+)?(?:main|मैं)\s+Ananya\s+(?:hoon|हूँ|हूं)\s+Meridian Group\s+(?:se|से)[, .]*", "", text, flags=re.I)
        return text

    def __init__(
        self,
        call_end_coordinator=None,
        hangup_state: dict | None = None,
        shutdown_state: dict | None = None,
        sample_rate: int = 8000,
        interruption_audio_gate=None,
        termination_processor=None,
        context=None,
        full_transcript: list[dict] | None = None,
        lead_memory: dict | None = None,
        call_metrics=None,
        stream_id: str | None = None,
        on_new_bot_turn: Callable | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._call_end_coordinator = call_end_coordinator
        self._hangup_state = hangup_state
        self._shutdown_state = shutdown_state
        self._sample_rate = sample_rate
        self._interruption_audio_gate = interruption_audio_gate
        self._termination_processor = termination_processor
        self._context = context
        self._full_transcript = full_transcript
        self._lead_memory = lead_memory
        self._call_metrics = call_metrics
        self._stream_id = stream_id
        self._on_new_bot_turn = on_new_bot_turn  # called on LLMFullResponseStartFrame
        self._turn_count = 0
        self._in_llm_turn = False
        self._turn_tokens = 0
        self._turn_has_spoken_or_generated = False
        self._leading_buffer = ""
        self._leading_flushed = False
        self._is_cancelled = False
        self._cached_audio_task: asyncio.Task | None = None
        self._cached_playback_active: bool = False
        self._last_spoken_turn_text: str = ""
        self._last_spoken_turn_time: float = 0.0
        self._current_turn_spoken_text: str = ""
        self._suppressing_duplicate_turn: bool = False
        self._stale_regeneration_attempted: bool = False
        self._site_visit_succeeded_this_turn: bool = False
        self._whatsapp_succeeded_this_turn: bool = False
        self._confirmation_queued_to_tts: bool = False
        self._confirmation_actually_played: bool = False
        self._confirmation_spoken: bool = False
        self._task: Any = None

    def bind_task(self, task: Any) -> None:
        self._task = task

    def mark_tool_succeeded(
        self,
        tool_name: str,
        whatsapp_confirmed: bool = False,
        confirm_msg: str | None = None,
    ) -> None:
        if tool_name == "book_site_visit":
            self._site_visit_succeeded_this_turn = True
            self._whatsapp_succeeded_this_turn = whatsapp_confirmed
            self._confirmation_queued_to_tts = True
            self._confirmation_actually_played = False
            self._confirmation_spoken = False
            if confirm_msg and self._task:
                logger.info("SpokenTextGuard: Queueing deterministic booking confirmation directly to TTS: {!r}", confirm_msg)
                conf_frame = TTSSpeakFrame(text=confirm_msg, append_to_context=True)
                setattr(conf_frame, "is_deterministic_confirmation", True)
                asyncio.create_task(
                    self._task.queue_frames([conf_frame])
                )
        elif tool_name == "send_brochure":
            self._whatsapp_succeeded_this_turn = True

    def _filter_unverified_claims(self, text: str) -> str:
        if not text:
            return text

        parts = re.split(r'(?<=[.!?\n\u0964])\s+', text)
        kept = []
        dropped_any = False
        for p in parts:
            p_clean = p.strip()
            if not p_clean:
                continue
            lower_p = p_clean.lower()
            recorded = (self._lead_memory or {}).get('_postcall_whatsapp_actions') or []
            if self._in_llm_turn and 'brochure' in recorded and '?' in p_clean and 'whatsapp' in lower_p and re.search(r'brochure|floor plans', lower_p) and re.search(r'\b(?:may|can|shall|would|want|like|send|share)\b', lower_p):
                dropped_any = True  # model cannot re-ask already-recorded consent
                continue
            if self._in_llm_turn and self._call_end_coordinator and getattr(self._call_end_coordinator, "_dedicated_goodbye_queued", False) and re.search(r"\b(?:goodbye|bye|thank you|have a (?:great|wonderful) day)\b", lower_p):
                dropped_any = True
                continue
            if re.fullmatch(r"yes,?\s*go ahead[?.!]?", lower_p):
                caller = ""
                if self._context:
                    for msg in reversed(self._context.messages):
                        if msg.get("role") == "user":
                            caller = str(msg.get("content", ""))
                            break
                kept.append(clarification_for(caller))
                dropped_any = True
                continue

            is_booking = bool(re.search(
                r"\b(?:have\s+)?(?:booked|scheduled|confirmed)\b|\b(?:book|confirm)\s+kar|बुक\s*कर|कन्फर्म\s*कर|\b(?:site\s+visit|visit)\s+(?:is|has\s+been)\s+(?:booked|scheduled|confirmed)\b|बुक|कन्फर्म|शिड्यूल",
                lower_p,
            ))
            caller = ""
            if self._context:
                for msg in reversed(self._context.messages):
                    if msg.get("role") == "user":
                        caller = str(msg.get("content", ""))
                        break
            is_visit_acceptance = bool((re.search(r"\b(?:come|visit|tomorrow|site|slot)\b", caller, re.I) or ((self._lead_memory or {}).get("site_visit") and (self._lead_memory or {}).get("disposition") != "SITE_VISIT_BOOKED")) and re.search(r"\b(?:works|noted|see you|all set|done|reserved|aa sakte|can come|same slot|already|i have your site visit|we have a slot)\b", lower_p))
            if is_visit_acceptance and not self._site_visit_succeeded_this_turn:
                if not any("not booked" in k for k in kept):
                    kept.append("The visit is not booked yet. What time would you like to visit?")
                dropped_any = True
                continue
            if is_booking:
                dropped_any = True
                if self._site_visit_succeeded_this_turn:
                    logger.info(
                        "SpokenTextGuard: Suppressed duplicate LLM booking claim sentence: {!r} (already queued directly)",
                        p_clean,
                    )
                else:
                    logger.warning(
                        "SpokenTextGuard: Dropped unverified booking claim sentence: {!r} (tool did not succeed this turn)",
                        p_clean,
                    )
                continue

            is_send_claim = bool(re.search(r"\b(?:i(?:'ve| have)?|we(?:'ve| have)?)\s+(?:shared|sent|delivered|am sending|will send)|\b(?:shared|sent|delivered)\b.*\b(?:brochure|floor plans?|location|details)\b|\bcheck your phone\b|bhej (?:diya|diye)|भेज (?:दिया|दी)", lower_p))
            is_wa = bool(re.search(
                r"\b(?:receive|sending|sent)\s+(?:the\s+)?(?:location|details|brochure|link)\s+(?:on|via)\s+whatsapp\b|\b(?:location|details)\s+on\s+whatsapp\b|whatsapp|व्हाट्सएप",
                lower_p,
            )) or bool(re.search(r"(?:brochure|floor plans?|details).*?(?:sent|bhej diye|bhej diya)|(?:sent|bhej diye|bhej diya).*?(?:brochure|floor plans?|details)", lower_p))
            if (is_send_claim or is_wa) and not self._whatsapp_succeeded_this_turn:
                is_wa_question = not is_send_claim and bool(re.search(r"\b(?:shall|can|may|would|should)\s+(?:i|we)\b|\blike (?:me|us) to share\b|(?:\?)$", lower_p.strip()))
                if not is_wa_question:
                    dropped_any = True
                    logger.warning(
                        "SpokenTextGuard: Dropped unverified WhatsApp claim sentence: {!r} (WhatsApp not confirmed this turn)",
                        p_clean,
                    )
                    continue

            kept.append(p)

        if not dropped_any:
            return text

        leading_ws = text[:len(text) - len(text.lstrip())]
        trailing_ws = text[len(text.rstrip()):]
        return leading_ws + " ".join(kept) + trailing_ws

    _DUP_WINDOW_SECS = 3.0

    def _is_recent_duplicate(self, norm_text: str) -> bool:
        """True only for an exact / near-exact repeat of the previous bot turn spoken within ~3s.

        Short fragments ("it", "sent", "sure") never count as duplicates unless they equal the
        whole previous turn; substring matching needs >= 12 chars of overlap.
        """
        clean_norm = re.sub(r"[^\w\s]", "", (norm_text or "").lower()).strip()
        clean_last = re.sub(r"[^\w\s]", "", self._normalize(self._last_spoken_turn_text).lower()).strip()
        if not clean_norm or not clean_last:
            return False
        if time.monotonic() - self._last_spoken_turn_time >= self._DUP_WINDOW_SECS:
            return False
        if clean_norm == clean_last:
            return True
        shorter = min(len(clean_norm), len(clean_last))
        return shorter >= 12 and (clean_norm in clean_last or clean_last in clean_norm)

    def _record_assistant_spoken(self, text: str, cached_key: str | None = None) -> None:
        if not text:
            return
        clean_text = text.strip()
        self._last_spoken_turn_text = clean_text
        self._last_spoken_turn_time = time.monotonic()
        if self._context and hasattr(self._context, "messages"):
            clean_norm = " ".join(clean_text.lower().split())
            already_present = any(
                isinstance(m, dict)
                and m.get("role") == "assistant"
                and clean_norm == " ".join(str(m.get("content", "")).lower().split())
                for m in self._context.messages
            )
            if already_present:
                logger.info(
                    "DUPLICATE_ASSISTANT_RECORD text={!r} key={!r} - skipping append",
                    clean_text[:80],
                    cached_key,
                )
                if self._call_metrics and hasattr(self._call_metrics, "record_duplicate_assistant"):
                    self._call_metrics.record_duplicate_assistant()
                return

            self._context.messages.append({"role": "assistant", "content": clean_text})
            logger.info(
                "SpokenTextGuard: Recorded assistant message in context.messages: {!r}",
                clean_text[:60],
            )

        if cached_key == "objection_pivot" and self._lead_memory is not None:
            self._lead_memory["_objection_pivot_spoken"] = True
            self._lead_memory["_objection_count"] = max(self._lead_memory.get("_objection_count", 0), 1)
        elif cached_key == "final_farewell" and self._lead_memory is not None:
            self._lead_memory["_farewell_spoken"] = True

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        await super().process_frame(frame, direction)

        if frame.__class__.__name__ in ("CancelFrame", "EndFrame", "EndTaskFrame"):
            self._is_cancelled = True

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, (UserStartedSpeakingFrame, InterruptionFrame)):
                self._is_cancelled = False
                self._in_llm_turn = False
                self._leading_buffer = ""
                self._leading_flushed = False
                self._site_visit_succeeded_this_turn = False
                self._whatsapp_succeeded_this_turn = False
                if getattr(self, "_confirmation_queued_to_tts", False) and not getattr(self, "_confirmation_actually_played", False):
                    logger.info("SpokenTextGuard: Booking confirmation interrupted before completion")
                    self._confirmation_queued_to_tts = False
                    self._confirmation_speaking = False
                if self._cached_audio_task and not self._cached_audio_task.done():
                    self._cached_audio_task.cancel()
                    self._cached_audio_task = None
                self._cached_playback_active = False
                self._current_turn_spoken_text = ""
                self._suppressing_duplicate_turn = False
                self._stale_regeneration_attempted = False
            elif isinstance(frame, UserStoppedSpeakingFrame):
                self._turn_count += 1
                self._in_llm_turn = False
                self._turn_tokens = 0
                self._turn_has_spoken_or_generated = False
                self._leading_buffer = ""
                self._leading_flushed = False
                self._cached_playback_active = False
                self._current_turn_spoken_text = ""
                self._suppressing_duplicate_turn = False
                self._stale_regeneration_attempted = False
                self._site_visit_succeeded_this_turn = False
                self._whatsapp_succeeded_this_turn = False
            elif isinstance(frame, LLMFullResponseStartFrame):
                self._in_llm_turn = True
                self._turn_tokens = 0
                self._leading_buffer = ""
                self._leading_flushed = False
                self._cached_playback_active = False
                self._current_turn_spoken_text = ""
                self._suppressing_duplicate_turn = False
                if self._context:
                    self._context._response_truncated = False
                # Primary watchdog disarm: new bot turn supersedes any pending re-delivery.
                # Called here because this frame reliably passes through SpokenTextGuard.
                if self._on_new_bot_turn:
                    self._on_new_bot_turn()
            elif isinstance(frame, BotStoppedSpeakingFrame):
                if getattr(self, "_confirmation_speaking", False) or (getattr(self, "_confirmation_queued_to_tts", False) and not getattr(self, "_confirmation_spoken", False)):
                    self._confirmation_speaking = False
                    self._confirmation_actually_played = True
                    self._confirmation_spoken = True
                    self._confirmation_queued_to_tts = False
                    if self._lead_memory is not None:
                        self._lead_memory["confirmation_spoken"] = True
                    logger.info("SpokenTextGuard: Booking confirmation playback completed (actually_played=True)")
            elif isinstance(frame, TTSSpeakFrame):
                is_silence_nudge = getattr(frame, "is_silence_nudge", False) or any(
                    nudge.lower() in getattr(frame, "text", "").lower()
                    for nudge in ("Hello? Are you there?", "are you still there", "can you hear me", "Looks like you're busy")
                )
                # Drop silence nudges ONLY if the LLM is actively streaming response tokens right now
                if is_silence_nudge and self._in_llm_turn:
                    logger.warning(
                        "SpokenTextGuard: Dropped silence nudge '{}' because LLM is actively generating a response",
                        getattr(frame, "text", ""),
                    )
                    return
                # Drop check-in nudges if call has already completed or hangup is active
                is_ending = (
                    getattr(self, "_is_cancelled", False)
                    or (self._hangup_state is not None and self._hangup_state.get("done", False))
                    or (self._shutdown_state is not None and self._shutdown_state.get("active", False))
                )
                if is_silence_nudge and is_ending and "busy" not in getattr(frame, "text", ""):
                    logger.debug(
                        "SpokenTextGuard: Dropped silence nudge '{}' because call termination is active",
                        getattr(frame, "text", ""),
                    )
                    return
                original = getattr(frame, "text", None)
                if isinstance(original, str) and original:
                    cached_key = None if getattr(frame, "is_deterministic_confirmation", False) else _match_cached_phrase(original, turn_count=self._turn_count)
                    if cached_key and self._interruption_audio_gate:
                        # Never repeat objection_pivot if it has already been spoken in this call
                        if cached_key == "objection_pivot" and (self._lead_memory or {}).get("_objection_pivot_spoken"):
                            cached_key = None
                        elif (
                            cached_key == "final_farewell"
                            and self._termination_processor
                            and not self._termination_processor.is_termination_allowed()
                        ):
                            # Only pivot to objection_pivot if it has not been spoken yet
                            if not (self._lead_memory or {}).get("_objection_pivot_spoken"):
                                logger.warning(
                                    "SpokenTextGuard: Suppressed TTSSpeakFrame farewell because termination is not allowed; playing objection_pivot"
                                )
                                cached_key = "objection_pivot"
                            else:
                                cached_key = None
                        if cached_key:
                            pcm = _AUDIO_CACHE.get(self._sample_rate, {}).get(cached_key)
                            if pcm:
                                if self._cached_playback_active:
                                    logger.debug(
                                        "SpokenTextGuard: dropping TTSSpeakFrame '{}' — cached audio already active",
                                        cached_key,
                                    )
                                    return
                                logger.info(
                                    "SpokenTextGuard: TTSSpeakFrame matches cache '{}' -> streaming cached PCM",
                                    cached_key,
                                )
                                self._cached_playback_active = True
                                self._turn_has_spoken_or_generated = True
                                phrase_text = _CACHED_PHRASE_TEXTS.get(cached_key, original)
                                self._record_assistant_spoken(phrase_text, cached_key)
                                self._cached_audio_task = asyncio.create_task(
                                    self._interruption_audio_gate.play_cached_audio(pcm, self._sample_rate)
                                )
                                if self._call_metrics and hasattr(self._call_metrics, "record_cached_audio_ttfa"):
                                    self._call_metrics.record_cached_audio_ttfa(cached_key)
                                if cached_key == "final_farewell" and self._call_end_coordinator:
                                    self._call_end_coordinator.request_ending()
                                return

                    if getattr(frame, "is_deterministic_confirmation", False):
                        filtered = original
                        self._confirmation_speaking = True
                    else:
                        filtered = self._filter_unverified_claims(original)
                    frame.text = self._normalize(filtered)
            elif isinstance(frame, TextFrame):
                if self._cached_playback_active:
                    return
                if self._site_visit_succeeded_this_turn:
                    logger.info("SpokenTextGuard: Suppressed post-booking LLM TextFrame {!r} (deterministic confirmation active)", str(getattr(frame, "text", ""))[:60])
                    return
                original = getattr(frame, "text", None)
                if isinstance(original, str) and original:
                    caller = ""
                    if self._context:
                        for msg in reversed(self._context.messages):
                            if msg.get("role") == "user":
                                caller = str(msg.get("content", ""))
                                break
                    # Hold only visit-slot acceptance turns until complete: streamed fragments
                    # like "Sure, tomorrow at two" must not escape before "works" is checked.
                    if re.search(r"\b(?:can I come|book.{0,12}visit|schedule.{0,12}visit|visit tomorrow|come.*visit|come tomorrow|which slot|what slot)\b", caller, re.I) or ((self._lead_memory or {}).get("site_visit") and (self._lead_memory or {}).get("disposition") != "SITE_VISIT_BOOKED"):
                        self._leading_buffer += original
                        return
                    if not self._leading_flushed:
                        self._leading_buffer += original
                        cached_key = _match_cached_phrase(self._leading_buffer, turn_count=self._turn_count)
                        if cached_key and self._interruption_audio_gate:
                            # Never repeat objection_pivot if it has already been spoken in this call
                            if cached_key == "objection_pivot" and (self._lead_memory or {}).get("_objection_pivot_spoken"):
                                cached_key = None
                            elif (
                                cached_key == "final_farewell"
                                and self._termination_processor
                                and not self._termination_processor.is_termination_allowed()
                            ):
                                # Only pivot to objection_pivot if it has not been spoken yet
                                if not (self._lead_memory or {}).get("_objection_pivot_spoken"):
                                    logger.warning(
                                        "SpokenTextGuard: Suppressed TextFrame farewell because termination is not allowed; playing objection_pivot"
                                    )
                                    cached_key = "objection_pivot"
                                else:
                                    cached_key = None
                            if cached_key:
                                pcm = _AUDIO_CACHE.get(self._sample_rate, {}).get(cached_key)
                                if pcm:
                                    logger.info(
                                        "SpokenTextGuard: Matched cache '{}' on turn {} -> streaming cached PCM (0ms TTS TTFA)",
                                        cached_key,
                                        self._turn_count,
                                    )
                                    self._cached_playback_active = True
                                    phrase_text = _CACHED_PHRASE_TEXTS.get(cached_key, self._leading_buffer)
                                    self._record_assistant_spoken(phrase_text, cached_key)
                                    self._leading_buffer = ""
                                    self._leading_flushed = True
                                    self._turn_has_spoken_or_generated = True
                                    self._turn_tokens += 10
                                    self._cached_audio_task = asyncio.create_task(
                                        self._interruption_audio_gate.play_cached_audio(pcm, self._sample_rate)
                                    )
                                    if self._call_metrics and hasattr(self._call_metrics, "record_cached_audio_ttfa"):
                                        self._call_metrics.record_cached_audio_ttfa(cached_key)
                                    if cached_key == "final_farewell" and self._call_end_coordinator:
                                        self._call_end_coordinator.request_ending()
                                    return

                        # Speak complete sentences, never a safety decision on a lone token.
                        split_idx = _find_sentence_end(self._leading_buffer)
                        chunk_to_flush = ""
                        if split_idx > 0:
                            chunk_to_flush = self._leading_buffer[:split_idx]
                            self._leading_buffer = self._leading_buffer[split_idx:]
                        if chunk_to_flush:
                            cleaned = self._strip_meta_announcements(chunk_to_flush)
                            cleaned = self._strip_repeated_opening(cleaned)
                            cleaned = self._filter_unverified_claims(cleaned)
                            self._leading_flushed = True
                            if cleaned.strip():
                                norm = self._normalize(cleaned)
                                if norm.strip():
                                    if self._is_recent_duplicate(norm):
                                        logger.warning(
                                            "SpokenTextGuard: Suppressed exact repeat {!r} (spoken <{:.0f}s ago); no regeneration",
                                            norm.strip(),
                                            self._DUP_WINDOW_SECS,
                                        )
                                        self._suppressing_duplicate_turn = True
                                        self._leading_buffer = ""  # drop the unspoken tail of the repeated turn too
                                        return

                                    self._current_turn_spoken_text += norm
                                    self._turn_has_spoken_or_generated = True
                                    self._turn_tokens += len(norm.split())
                                    await self.push_frame(TextFrame(text=norm), direction)
                            return
                        else:
                            return
                    else:
                        if self._suppressing_duplicate_turn:
                            return
                        self._leading_buffer += original
                        split_idx = _find_sentence_end(self._leading_buffer)
                        if split_idx <= 0:
                            return
                        to_process = self._leading_buffer[:split_idx]
                        self._leading_buffer = self._leading_buffer[split_idx:]
                        filtered = self._filter_unverified_claims(to_process)
                        if filtered.strip():
                            norm = self._normalize(filtered)
                            if self._is_recent_duplicate(norm):
                                self._suppressing_duplicate_turn = True
                                self._leading_buffer = ""
                                return
                            self._current_turn_spoken_text += norm
                            self._turn_has_spoken_or_generated = True
                            await self.push_frame(TextFrame(text=norm), direction)
                        return
            elif isinstance(frame, LLMFullResponseEndFrame):
                if self._cached_playback_active:
                    self._cached_playback_active = False
                    self._leading_buffer = ""
                    self._leading_flushed = True
                    self._in_llm_turn = False
                    self._turn_tokens = 0
                    self._current_turn_spoken_text = ""
                    self._suppressing_duplicate_turn = False
                    await self.push_frame(frame, direction)
                    return

                if self._leading_buffer and getattr(self._context, "_response_truncated", False):
                    # Preserve any completed sentences in a multi-sentence residual chunk.
                    complete_end = 0
                    while True:
                        end = _find_sentence_end(self._leading_buffer[complete_end:])
                        if end <= 0:
                            break
                        complete_end += end
                    if self._leading_buffer[complete_end:].strip():
                        logger.warning("SpokenTextGuard: Suppressed unfinished length-limited tail")
                    self._leading_buffer = self._leading_buffer[:complete_end]
                if self._leading_buffer and self._suppressing_duplicate_turn:
                    self._leading_buffer = ""
                if self._leading_buffer:
                    cleaned = self._strip_meta_announcements(self._leading_buffer)
                    cleaned = self._strip_repeated_opening(cleaned)
                    cleaned = self._filter_unverified_claims(cleaned)
                    self._leading_flushed = True
                    self._leading_buffer = ""
                    if cleaned.strip():
                        norm = self._normalize(cleaned)
                        if norm.strip():
                            is_dup = self._is_recent_duplicate(norm)
                            if not is_dup:
                                self._current_turn_spoken_text += norm
                                self._turn_has_spoken_or_generated = True
                                self._turn_tokens += len(norm.split())
                                await self.push_frame(TextFrame(text=norm), direction)
                            else:
                                logger.warning(
                                    "SpokenTextGuard: Suppressed duplicate assistant end-buffer {!r} (previous turn {!r})",
                                    norm.strip(),
                                    self._last_spoken_turn_text[:60],
                                )
                                self._suppressing_duplicate_turn = True

                if not self._suppressing_duplicate_turn and self._current_turn_spoken_text.strip():
                    self._last_spoken_turn_text = " ".join(self._current_turn_spoken_text.split())
                    self._last_spoken_turn_time = time.monotonic()
                self._current_turn_spoken_text = ""
                self._suppressing_duplicate_turn = False
                self._in_llm_turn = False
                self._turn_tokens = 0
                self._leading_buffer = ""
                self._leading_flushed = False

        await self.push_frame(frame, direction)


def _trim_leading_pcm_silence(
    audio: bytes,
    sample_rate: int = 16000,
    threshold: int = 120,
    pre_roll_ms: float = 10.0,
) -> bytes:
    """Strip leading silent PCM samples from the first audio frame of an utterance.
    
    Preserves a 10ms pre-roll lookback window before threshold crossing to guarantee
    soft phoneme attacks (unvoiced fricatives like 's', plosive bursts like 't/g', 
    nasals, and vowels) are never clipped or truncated.
    """
    if len(audio) < 4:
        return audio
    num_samples = len(audio) // 2
    try:
        samples = struct.unpack(f"<{num_samples}h", audio[:num_samples * 2])
        for i, sample in enumerate(samples):
            if abs(sample) > threshold:
                pre_roll_samples = int(sample_rate * (pre_roll_ms / 1000.0))
                keep_idx = max(0, i - pre_roll_samples)
                return audio[keep_idx * 2 :]
        return b""
    except Exception:
        return audio


def _apply_soft_fade_in(audio: bytes, sample_rate: int = 16000, duration_ms: float = 5.0) -> bytes:
    """Applies a smooth fade-in ramp over duration_ms to prevent DAC pops/clicks on audio onset."""
    num_samples = len(audio) // 2
    if num_samples < 4:
        return audio
    fade_len = min(num_samples, int(sample_rate * (duration_ms / 1000.0)))
    if fade_len <= 1:
        return audio

    try:
        samples = list(struct.unpack(f"<{num_samples}h", audio[:num_samples * 2]))
        for i in range(fade_len):
            factor = i / float(fade_len)
            samples[i] = int(samples[i] * factor)
        return struct.pack(f"<{num_samples}h", *samples) + audio[num_samples * 2 :]
    except Exception:
        return audio


class _InterruptionAudioGate(FrameProcessor):
    """Drops stale audio from interrupted utterances and trims leading TTS silence."""

    def __init__(
        self,
        *,
        stream_id: str,
        on_interruption: Callable[[], None] | None = None,
        metrics_collector: Any = None,
        llm: Any = None,
        tts: Any = None,
        websocket: Any = None,
        call_end_coordinator: Any = None,
        max_trim_ms: float = 500.0,
    ) -> None:
        super().__init__()

        self._stream_id = stream_id
        self._max_trim_ms = max_trim_ms
        self._current_gen_id = 0
        self._active_playing_gen_id = 0
        self._is_interrupted = False
        self._dropped_frames = 0
        self._on_interruption = on_interruption
        self._metrics_collector = metrics_collector
        self._llm = llm
        self._tts = tts
        self._websocket = websocket
        self._call_end_coordinator = call_end_coordinator
        self._awaiting_first_audio_of_turn = False
        self._accumulated_trimmed_bytes = 0
        self._pre_roll_buffer = b""

    def next_generation(self) -> int:
        self._current_gen_id += 1
        self._is_interrupted = False
        self._awaiting_first_audio_of_turn = True
        self._accumulated_trimmed_bytes = 0
        self._pre_roll_buffer = b""
        self._active_playing_gen_id = self._current_gen_id
        return self._current_gen_id

    @property
    def is_interrupted(self) -> bool:
        return self._is_interrupted

    async def play_cached_audio(self, pcm_bytes: bytes, sample_rate: int = 16000) -> None:
        """Streams pre-rendered PCM audio frames downstream, bypassing live TTS."""
        chunk_bytes = int(sample_rate * 2 * 0.02)  # 20ms chunk (640 bytes @ 16kHz, 320 @ 8kHz)
        sleep_sec = 0.02

        self.next_generation()
        current_gen = self._current_gen_id
        # next_generation() resets _is_interrupted to False; if an InterruptionFrame
        # had already arrived before play_cached_audio was scheduled (e.g. noise fired
        # the 280ms filler race but a real interruption came in first), restore the flag
        # and abort without playing anything.
        if self._is_interrupted:
            logger.debug(
                "[{}] AudioCache: skipping playback — interruption flag was set before start",
                self._stream_id,
            )
            return

        logger.info(
            "[{}] AudioCache: Streaming cached audio ({} bytes, {} Hz, ~{:.2f}s)",
            self._stream_id,
            len(pcm_bytes),
            sample_rate,
            len(pcm_bytes) / (sample_rate * 2),
        )

        await self.push_frame(BotStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await self.push_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)

        try:
            chunks_sent = 0
            for offset in range(0, len(pcm_bytes), chunk_bytes):
                if self._is_interrupted or self._active_playing_gen_id != current_gen:
                    logger.info(
                        "[{}] AudioCache: playback interrupted early (interrupted={}, active_gen={}, current_gen={})",
                        self._stream_id,
                        self._is_interrupted,
                        self._active_playing_gen_id,
                        current_gen,
                    )
                    break
                chunk = pcm_bytes[offset : offset + chunk_bytes]
                frame = TTSAudioRawFrame(audio=chunk, sample_rate=sample_rate, num_channels=1)
                await self.push_frame(frame, FrameDirection.DOWNSTREAM)
                chunks_sent += 1
                await asyncio.sleep(sleep_sec)
            logger.info("[{}] AudioCache: streaming completed, {} chunks sent", self._stream_id, chunks_sent)
        except Exception as e:
            logger.exception("[{}] AudioCache error during playback: {}", self._stream_id, e)
        finally:
            if not self._is_interrupted and self._active_playing_gen_id == current_gen:
                await self.push_frame(TTSStoppedFrame(), FrameDirection.DOWNSTREAM)
                await self.push_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        if isinstance(frame, InterruptionFrame):
            is_closing = False
            if self._call_end_coordinator:
                is_closing = (
                    getattr(self._call_end_coordinator, "is_closing_in_progress", False)
                    or getattr(self._call_end_coordinator, "_in_grace", False)
                )

            if not is_closing:
                self._is_interrupted = True
                self._active_playing_gen_id = 0
                self._awaiting_first_audio_of_turn = False
                self._accumulated_trimmed_bytes = 0
                self._pre_roll_buffer = b""

                # Fix 2: Immediately purge and log stale frames at interruption time
                purged = self._dropped_frames
                self._dropped_frames = 0
                logger.info(
                    "[{}] Barge-in interruption: purged {} stale audio frames immediately",
                    self._stream_id,
                    purged,
                )

                # (a) Cancel in-flight LLM generation
                if self._llm and hasattr(self._llm, "_handle_interruptions"):
                    try:
                        await self._llm._handle_interruptions(frame)
                    except Exception as _e:
                        logger.debug("[{}] Error interrupting LLM: {}", self._stream_id, _e)

                # (b) Stop TTS
                if self._tts and hasattr(self._tts, "_handle_interruption"):
                    try:
                        await self._tts._handle_interruption(frame, direction)
                    except Exception as _e:
                        logger.debug("[{}] Error interrupting TTS: {}", self._stream_id, _e)

                # (c) Flush client-side playback buffer via WebSocket clearAudio
                if self._websocket:
                    try:
                        payload = json.dumps({"event": "clearAudio", "streamId": self._stream_id})
                        if hasattr(self._websocket, "send_text"):
                            await self._websocket.send_text(payload)
                        elif hasattr(self._websocket, "send_json"):
                            await self._websocket.send_json({"event": "clearAudio", "streamId": self._stream_id})
                    except Exception as _ws_err:
                        logger.debug("[{}] Error sending clearAudio to websocket: {}", self._stream_id, _ws_err)

                if self._on_interruption:
                    self._on_interruption()
            else:
                logger.info(
                    "[{}] Closing audio active; suppressed clearAudio and interruption cancellation to protect farewell playback",
                    self._stream_id,
                )

            # Forward downstream if arriving upstream so transport.output sees it
            if direction == FrameDirection.UPSTREAM:
                await self.push_frame(frame, FrameDirection.DOWNSTREAM)
                return

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TTSStartedFrame):
                self._active_playing_gen_id = self._current_gen_id
                self._is_interrupted = False
                self._awaiting_first_audio_of_turn = True
                self._accumulated_trimmed_bytes = 0
                self._pre_roll_buffer = b""
                self._dropped_frames = 0  # Ponytail: Purge-at-next-utterance logic eliminated

            elif isinstance(frame, TTSStoppedFrame):
                self._active_playing_gen_id = 0
                self._awaiting_first_audio_of_turn = False
                self._accumulated_trimmed_bytes = 0
                self._pre_roll_buffer = b""

            elif isinstance(frame, AudioRawFrame):
                if (
                    self._is_interrupted
                    or (
                        self._active_playing_gen_id
                        != self._current_gen_id
                    )
                ):
                    self._dropped_frames += 1
                    return

                # Shave leading silence from audio onset capped strictly at max_trim_ms
                if self._awaiting_first_audio_of_turn:
                    sample_rate = getattr(frame, "sample_rate", 16000)
                    max_trim_bytes = int(sample_rate * 2 * (self._max_trim_ms / 1000.0))
                    remaining_budget_bytes = max(0, max_trim_bytes - self._accumulated_trimmed_bytes)

                    audio_bytes = frame.audio
                    num_samples = len(audio_bytes) // 2
                    speech_idx = None
                    if num_samples >= 2:
                        samples = struct.unpack(f"<{num_samples}h", audio_bytes[:num_samples * 2])
                        for i, s in enumerate(samples):
                            if abs(s) > 200:  # Robust speech threshold (above noise/dither floor)
                                speech_idx = i
                                break

                    if speech_idx is None:
                        # Pure silence
                        if len(audio_bytes) <= remaining_budget_bytes:
                            self._accumulated_trimmed_bytes += len(audio_bytes)
                            return
                        else:
                            trim_now = remaining_budget_bytes
                            self._accumulated_trimmed_bytes += trim_now
                            frame.audio = _apply_soft_fade_in(audio_bytes[trim_now:], sample_rate)
                            total_trimmed = self._accumulated_trimmed_bytes
                            self._awaiting_first_audio_of_turn = False
                    else:
                        pre_roll_samples = int(sample_rate * 0.020)  # Keep ~20ms pad before speech
                        trim_samples = max(0, speech_idx - pre_roll_samples)
                        speech_bytes = trim_samples * 2
                        trim_now = min(speech_bytes, remaining_budget_bytes)
                        self._accumulated_trimmed_bytes += trim_now
                        if trim_now > 0:
                            frame.audio = _apply_soft_fade_in(audio_bytes[trim_now:], sample_rate)
                        total_trimmed = self._accumulated_trimmed_bytes
                        self._awaiting_first_audio_of_turn = False

                    saved_ms = min(self._max_trim_ms, round((total_trimmed / (sample_rate * 2)) * 1000.0, 1))
                    if saved_ms > 0:
                        logger.info(
                            "METRIC call_id={} event=tts_silence_trimmed raw_silence_bytes={} saved_ms={:.1f}",
                            self._stream_id,
                            total_trimmed,
                            saved_ms,
                        )
                        if self._metrics_collector and hasattr(self._metrics_collector, "record_tts_silence_trimmed"):
                            self._metrics_collector.record_tts_silence_trimmed(saved_ms, gen_id=self._current_gen_id)

        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


def _compute_pcm_rms(audio: bytes) -> int:
    """Computes RMS energy of 16-bit mono PCM."""
    if not audio:
        return 0
    try:
        import audioop
        return audioop.rms(audio, 2)
    except Exception:
        import numpy as np
        samples = np.frombuffer(audio, dtype=np.int16)
        if len(samples) == 0:
            return 0
        return int(np.sqrt(np.mean(samples.astype(np.float32)**2)))


class _TTSStallObserver(FrameProcessor):
    """Lives after the audio gate. Actual output cancels the pending first-audio hedge."""
    def __init__(self, state):
        super().__init__()
        self.state = state

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, (UserStartedSpeakingFrame, InterruptionFrame)):
            self.state.cancel(new_turn=True)
        elif isinstance(frame, (AudioRawFrame, BotStartedSpeakingFrame)) or frame.__class__.__name__ in ("CancelFrame", "EndFrame", "EndTaskFrame"):
            self.state.cancel()
        await self.push_frame(frame, direction)

    async def cleanup(self):
        self.state.cancel()
        await super().cleanup()


class _AudioInputGate(FrameProcessor):
    """Pre-STT Voice Activity & Energy Gate (Priority 2).

    Monitors incoming microphone PCM energy (RMS) before forwarding to Sarvam STT.
    During extended caller silence (>800ms), gates dead-air frames so the Sarvam
    STT WebSocket is not spammed with silence. Maintains a rolling 80ms pre-buffer
    so initial consonants and speech onsets are never clipped.
    """

    def __init__(
        self,
        *,
        stream_id: str = "",
        rms_threshold: int = 180,
        hangover_seconds: float = 0.8,
        pre_buffer_frames: int = 4,
        is_bot_speaking: Callable[[], bool] | None = None,
    ):
        super().__init__()
        self._stream_id = stream_id
        self._rms_threshold = rms_threshold
        self._hangover_seconds = hangover_seconds
        self._pre_buffer_frames = pre_buffer_frames
        self._is_bot_speaking = is_bot_speaking
        self._pre_buffer: list[AudioRawFrame] = []
        self._last_speech_time: float = 0.0
        self._gate_open: bool = True

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, AudioRawFrame):
            audio = getattr(frame, "audio", b"")
            if not audio:
                await self.push_frame(frame, direction)
                return

            rms = _compute_pcm_rms(audio)
            now = time.monotonic()
            bot_speaking = self._is_bot_speaking() if self._is_bot_speaking else False
            threshold = (self._rms_threshold * 1.8) if bot_speaking else self._rms_threshold

            if rms >= threshold:
                self._last_speech_time = now
                if not self._gate_open:
                    self._gate_open = True
                    for pb_frame in self._pre_buffer:
                        await self.push_frame(pb_frame, direction)
                    self._pre_buffer.clear()
                await self.push_frame(frame, direction)
                return

            if (now - self._last_speech_time) < self._hangover_seconds:
                await self.push_frame(frame, direction)
                return

            self._gate_open = False
            self._pre_buffer.append(frame)
            if len(self._pre_buffer) > self._pre_buffer_frames:
                self._pre_buffer.pop(0)
            return

        await self.push_frame(frame, direction)


_AFFIRMATIVE_CONFIRMATIONS = {
    "yes", "yeah", "yep", "yup", "speaking", "yes speaking", "yeah speaking",
    "yes this is", "haan", "haan ji", "ha", "ji", "this side", "i am",
    "correct", "right", "myself", "sure", "boliye", "haan boliye", "yes tell me",
}

def _is_name_confirmation_affirmative(user_text: str) -> bool:
    clean = re.sub(r"[^\w\s]", "", user_text.lower()).strip()
    if clean in _AFFIRMATIVE_CONFIRMATIONS:
        return True
    if any(clean.startswith(prefix) for prefix in ("yes ", "yeah ", "haan ", "speaking", "this is ")) and len(clean.split()) <= 4:
        if not any(neg in clean for neg in ("not", "no", "wrong", "busy", "later", "who", "which")):
            return True
    return False


class _FastPathRouter(FrameProcessor):
    """Deterministic Fast-Paths & FAQ Cache Router (Priority 5 & Priority 7).

    Intercepts deterministic conversation turns before the LLM is invoked:
      - Priority 5: Turn-1 affirmative name confirmation -> 0ms LLM + 0ms TTS fast-path.
      - Explicit single-intent language switch ("Speak in Telugu") -> 0ms LLM fast-path in Telugu.
    Bypasses Groq completely, saving 120-250ms of network TTFT and $0.00 LLM cost.
    Passes all multi-part sentences, long queries, and complex consultative turns to the LLM.
    """

    def __init__(
        self,
        stream_id: str = "",
        lead_memory: dict[str, str] | None = None,
        config: dict | None = None,
        history_pruner: Any = None,
        call_metrics: Any = None,
        delayed_race_filler: Any = None,
        call_end_coordinator: Any = None,
        on_whatsapp_consent=None,
        on_visit_ready=None,
    ):
        super().__init__()
        self._on_whatsapp_consent = on_whatsapp_consent
        self._on_visit_ready = on_visit_ready
        self._stream_id = stream_id
        self._lead_memory = lead_memory if lead_memory is not None else {}
        self._config = config or {}
        self._history_pruner = history_pruner
        self._call_metrics = call_metrics
        self._delayed_race_filler = delayed_race_filler
        self._turn_count = 0
        self._call_end_coordinator = call_end_coordinator

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, UserStoppedSpeakingFrame):
            self._turn_count += 1

        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, LLMContextFrame):
            context = frame.context
            messages = getattr(context, "messages", [])
            _coalesce_consecutive_messages(messages)
            _sync_working_memory(messages, self._lead_memory, self._stream_id)

            # EDIT 1: Check for unexplained context drops (CONTEXT_REWIND)
            if self._history_pruner and getattr(self._history_pruner, "_last_pruned_len", 0) > 0:
                last_pruned = self._history_pruner._last_pruned_len
                if len(messages) < last_pruned:
                    logger.error(
                        "[{}] CONTEXT_REWIND unexplained drop in context: {} < last_pruned={}",
                        self._stream_id,
                        len(messages),
                        last_pruned,
                    )
                    if self._call_metrics and hasattr(self._call_metrics, "record_context_rewind"):
                        self._call_metrics.record_context_rewind()

            latest_user_text = ""
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    c = msg.get("content", "")
                    if isinstance(c, str):
                        latest_user_text = c.strip()
                    break

            if self._call_end_coordinator and getattr(self._call_end_coordinator, "_ended", False):
                return
            if latest_user_text:
                if self._call_end_coordinator and getattr(self._call_end_coordinator, "is_ending", False):
                    if not genuine_late_question(latest_user_text):
                        return
                    if getattr(self._call_end_coordinator, "_late_reply_routed", False):
                        return
                    self._call_end_coordinator._late_reply_routed = True

                memory = self._lead_memory or {}
                if visit_intent(latest_user_text, memory):
                    date = memory.get("visit_date_iso")
                    slot = memory.get("time_slot")
                    if date and slot and self._on_visit_ready:
                        await self._on_visit_ready(date, slot)
                        return
                    if date and not slot:
                        line = "Kal kitne baje aana chahenge?" if re.search(r"[\u0900-\u097f]|\bkal\b", latest_user_text, re.I) else "What time would you like to visit?"
                        await self.push_frame(TTSSpeakFrame(text=line, append_to_context=True), direction)
                        return

                if self._call_end_coordinator and (pure_farewell(latest_user_text) or closing_after_work(latest_user_text, memory)):
                    if getattr(self._call_end_coordinator, "_dedicated_goodbye_queued", False):
                        return
                    self._call_end_coordinator._dedicated_goodbye_queued = True
                    self._call_end_coordinator.request_ending()
                    await self.push_frame(TTSSpeakFrame(text=SHORT_GOODBYE, append_to_context=True), direction)
                    return

                if fragment_transcript(latest_user_text):
                    await self.push_frame(TTSSpeakFrame(text="Sorry, could you repeat that?", append_to_context=True), direction)
                    return
                if re.fullmatch(r"(?:okay|ok|sure|yes)[.!]?", latest_user_text.lower()) and any("connection" in str(m.get("content", "")).lower() and any(w in str(m.get("content", "")).lower() for w in ("trouble", "unavailable")) for m in messages[-3:] if m.get("role") == "assistant"):
                    return
                pending_action = (self._lead_memory or {}).get("_whatsapp_consent_action")
                answer = whatsapp_answer(messages, pending_action)
                if answer == "consent" and self._on_whatsapp_consent:
                    self._lead_memory.pop('_whatsapp_consent_action', None)
                    if pending_action == 'brochure':
                        self._lead_memory.pop('_brochure_consent_pending', None)
                    await self._on_whatsapp_consent(pending_action)
                    return
                if answer == "declined":
                    self._lead_memory.pop("_whatsapp_consent_action", None)
                    self._lead_memory.pop("_brochure_consent_pending", None)
                    await self.push_frame(TTSSpeakFrame(text="Okay, I won't send it.", append_to_context=True), direction)
                    return

                decision = brochure_decision(messages, bool((self._lead_memory or {}).get("_brochure_consent_pending")))
                if decision == "needs_consent":
                    self._lead_memory["_brochure_consent_pending"] = True
                    self._lead_memory["_whatsapp_consent_action"] = "brochure"
                    await self.push_frame(TTSSpeakFrame(text="May I send you the brochure and floor plans on WhatsApp?", append_to_context=True), direction)
                    return
                if decision == "declined":
                    self._lead_memory.pop("_brochure_consent_pending", None)
                    await self.push_frame(TTSSpeakFrame(text="Okay, I won't send it. What else would you like to know?", append_to_context=True), direction)
                    return
                if decision == "consent":
                    self._lead_memory.pop('_brochure_consent_pending', None)
                    self._lead_memory.pop('_whatsapp_consent_action', None)
                    if self._on_whatsapp_consent:
                        await self._on_whatsapp_consent("brochure")
                        return
                    await self.push_frame(frame, direction)
                    return

                assistant_msgs = [m for m in messages if m.get("role") == "assistant"]

                # Priority 5: Turn-1 Affirmative confirmation
                if self._turn_count <= 1 and len(assistant_msgs) <= 1 and _is_name_confirmation_affirmative(latest_user_text):
                    fast_reply = _CACHED_PHRASE_TEXTS["opening_intro"]
                    logger.info(
                        "[{}] FastPath: Intercepted Turn-1 name affirmation ({!r}) -> 0ms LLM fast-path",
                        self._stream_id,
                        latest_user_text,
                    )
                    if self._call_metrics and hasattr(self._call_metrics, "record_cached_answer"):
                        self._call_metrics.record_cached_answer("opening_intro")
                    await self.push_frame(LLMFullResponseStartFrame(), direction)
                    await self.push_frame(TextFrame(text=fast_reply), direction)
                    await self.push_frame(LLMFullResponseEndFrame(), direction)
                    return

                matched = campaign_faq(latest_user_text, self._lead_memory, self._config)
                # Preserve old opt-in router for explicitly supplied minimal configs/test harnesses.
                key = faq_key(latest_user_text)
                if matched and self._config.get("faq_cache_enabled", False):
                    key, reply = matched
                elif key and self._config.get("faq_cache_enabled", False) and not self._config.get("real_estate_sales_script"):
                    reply = FAQ_TEXTS[key]
                else:
                    key = None
                if key:
                    if self._call_metrics:
                        self._call_metrics.record_cached_answer(key)
                    await self.push_frame(LLMFullResponseStartFrame(), direction)
                    await self.push_frame(TTSSpeakFrame(text=reply, append_to_context=True), direction)
                    await self.push_frame(LLMFullResponseEndFrame(), direction)
                    return

                # Telugu Immediate Adaptation: Single-intent Telugu request
                clean_t = latest_user_text.lower().strip()
                if (
                    re.search(
                        r"^(?:please\s*)?(?:speak|say|talk|tell|switch|converse)\s*(?:that\s*)?(?:in|to)?\s*telugu\b|^\s*telugu(?:\s*please)?[\?\.]*\s*$|\b(?:can\s*you\s*speak|do\s*you\s*speak)\s*telugu\b|\btelugu\s*(?:lo|me|mein)\s*(?:baat\s*karo|matladandi|cheppandi)\b",
                        clean_t,
                    )
                    and len(clean_t.split()) <= 8
                    and not any(neg in clean_t for neg in ("not", "nahi", "nahin", "don't", "dont"))
                ):
                    fast_reply = "Avunu, cheppandi, meeku em details kaavali?"
                    logger.info(
                        "[{}] FastPath: Intercepted Telugu language switch ({!r}) -> 0ms LLM fast-path",
                        self._stream_id,
                        latest_user_text,
                    )
                    self._lead_memory["language"] = "Telugu"
                    await self.push_frame(LLMFullResponseStartFrame(), direction)
                    await self.push_frame(TextFrame(text=fast_reply), direction)
                    await self.push_frame(LLMFullResponseEndFrame(), direction)
                    return

                # All property and consultative queries pass through to the LLM with active language prompt
                if self._delayed_race_filler is not None:
                    self._delayed_race_filler.arm_llm_request(self._turn_count)

        await self.push_frame(frame, direction)


class _SlotAwareTurnCompleter(FrameProcessor):
    """Slot-Aware End-of-Turn Processor (Early Turn Commitment with Stability Guard).
    
    Monitors incoming TranscriptionFrames (both interim and final). When high-confidence,
    closed qualification slots (e.g. BHK selection, Turn-1 Affirmation, Budget, Brochure request)
    are matched and stable, triggers an immediate UserStoppedSpeakingFrame to commit the turn
    to LLM/FastPath ~250ms earlier than waiting for VAD silence timeout + debounce hang.
    """

    _SLOT_PATTERNS = [
        # BHK / Unit selection (English, Hindi)
        re.compile(
            r"\b(?:(?:looking\s+for|want|need|prefer|interested\s+in|give\s+me)\s+)?(?:a\s+)?(2|3|4|two|three|four)\s*(?:bhk|bed|bedroom|bedrooms|kamre|apartments?)\b",
            re.IGNORECASE,
        ),
        # Turn-1 identity affirmation ("yes", "yeah", "speaking", "alex here", "haan", "avunu")
        re.compile(
            r"^(?:yes|yeah|speaking|this\s+is\s+\w+|yeah\s+speaking|yes\s+speaking|alex\s+here|yep|yup|haan|ha|haa|haanji|avunu)[\.\!\?]*$",
            re.IGNORECASE,
        ),
        # Direct brochure / payment plan request
        re.compile(
            r"\b(?:send\s+(?:the\s+)?brochure|whatsapp\s+(?:me\s+)?(?:the\s+)?(?:brochure|details)|whatsapp\s+pe\s+bhejo)\b",
            re.IGNORECASE,
        ),
    ]

    _OPEN_CONJUNCTION_RE = re.compile(
        r"\b(?:and|aur|or|ya|but|lekin|also|bhi|plus|with)\s*$",
        re.IGNORECASE,
    )

    def __init__(self, stream_id: str = ""):
        super().__init__()
        self._stream_id = stream_id
        self._early_committed_turn = False
        self._user_speaking = False

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, UserStartedSpeakingFrame):
                self._user_speaking = True
                self._early_committed_turn = False
            elif isinstance(frame, UserStoppedSpeakingFrame):
                self._user_speaking = False
                self._early_committed_turn = False
            elif isinstance(frame, TranscriptionFrame) and self._user_speaking and not self._early_committed_turn:
                text = (getattr(frame, "text", "") or "").strip()
                if text and len(text) >= 2:
                    # Stability Guard: do not commit if user ends on open conjunction
                    if not self._OPEN_CONJUNCTION_RE.search(text):
                        for pat in self._SLOT_PATTERNS:
                            if pat.search(text):
                                logger.info(
                                    "[{}] Slot-Aware Commit: Early committing turn on stable slot {!r} (cutting ~250ms VAD wait)",
                                    self._stream_id,
                                    text,
                                )
                                self._early_committed_turn = True
                                self._user_speaking = False
                                await self.push_frame(frame, direction)
                                await self.push_frame(UserStoppedSpeakingFrame(), direction)
                                return

        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


class _DelayedRaceFiller(FrameProcessor):
    """280ms Race-Matched Conversational Filler.
    
    Launches a 280ms race timer upon UserStoppedSpeakingFrame.
    - If the LLM produces a first token (TextFrame) within 280ms, the timer cancels (0ms filler overhead).
    - If the LLM wait exceeds 280ms (e.g. complex query or queue delay), plays an instant pre-rendered
      cached filler in the active language ("Sure, let me check that for you." / "Gerne, einen kurzen Moment.")
      to maintain natural conversational flow.
    """

    def __init__(
        self,
        *,
        stream_id: str,
        sample_rate: int = 8000,
        interruption_audio_gate: Any = None,
        language_state: Any = None,
        hangup_state: dict | None = None,
        timeout_seconds: float = 0.700,
        llm_service: Any = None,
        context: Any = None,
    ):
        super().__init__()
        self._stream_id = stream_id
        self._sample_rate = sample_rate
        self._interruption_audio_gate = interruption_audio_gate
        self._language_state = language_state
        self._hangup_state = hangup_state
        self._timeout_seconds = timeout_seconds
        self._llm_service = llm_service
        self._context = context
        self._turn_index = 0
        self._race_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._turn_in_flight = False
        self._filler_dispatched = False
        self._watchdog_retried = False

    def cancel_all_timers(self, reason: str = "") -> None:
        self._turn_in_flight = False
        self._filler_dispatched = False
        self._watchdog_retried = False
        self._cancel_timers(reason)

    def arm_llm_request(self, turn_id: int | None = None) -> None:
        """Arm race and watchdog timers ONLY after an LLM request was actually sent for the turn."""
        if self._hangup_state and self._hangup_state.get("done", False):
            return
        if turn_id is not None:
            self._turn_index = turn_id
        self._turn_in_flight = True
        self._filler_dispatched = False
        self._watchdog_retried = False
        self._cancel_timers("arm LLM request")
        if self._turn_index > 1:
            self._race_task = asyncio.create_task(self._race_timer(self._turn_index))
        self._watchdog_task = asyncio.create_task(self._watchdog_timer(self._turn_index))
        logger.debug("[{}] Armed race filler & 3s watchdog for turn {}", self._stream_id, self._turn_index)

    def _cancel_timers(self, reason: str = "") -> None:
        if self._race_task and not self._race_task.done():
            self._race_task.cancel()
            self._race_task = None
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            self._watchdog_task = None

    async def _play_filler(self, reason: str) -> None:
        if self._filler_dispatched or (self._hangup_state and self._hangup_state.get("done", False)):
            return
        lang = (
            self._language_state.get_language()
            if self._language_state and hasattr(self._language_state, "get_language")
            else "en"
        )
        filler_key = "filler_hi" if lang in ("hi", "Hindi") else "filler_en"
        pcm = _AUDIO_CACHE.get(self._sample_rate, {}).get(filler_key) or _AUDIO_CACHE.get(self._sample_rate, {}).get("filler_en")
        if pcm and self._interruption_audio_gate:
            logger.info(
                "[{}] Playing cached filler '{}' (reason: {})",
                self._stream_id,
                filler_key,
                reason,
            )
            self._filler_dispatched = True
            asyncio.create_task(
                self._interruption_audio_gate.play_cached_audio(pcm, self._sample_rate)
            )

    async def _race_timer(self, turn_id: int) -> None:
        try:
            await asyncio.sleep(self._timeout_seconds)
            if (
                self._turn_in_flight
                and not self._filler_dispatched
                and self._turn_index == turn_id
                and not (self._hangup_state and self._hangup_state.get("done", False))
            ):
                await self._play_filler(f"TTFT crossed {self._timeout_seconds*1000:.0f}ms on turn {turn_id}")
        except asyncio.CancelledError:
            pass

    async def _watchdog_timer(self, turn_id: int) -> None:
        try:
            await asyncio.sleep(3.0)
            if (
                self._turn_in_flight
                and self._turn_index == turn_id
                and not self._watchdog_retried
                and not (self._hangup_state and self._hangup_state.get("done", False))
            ):
                # FIX 1: The watchdog retry must never run when the last context message is an assistant message
                last_msg = None
                if self._context and hasattr(self._context, "messages") and self._context.messages:
                    last_msg = self._context.messages[-1]
                if isinstance(last_msg, dict) and last_msg.get("role") == "assistant":
                    logger.info(
                        "[{}] 3s LLM Watchdog turn {}: last context message is assistant, skipping retry",
                        self._stream_id,
                        turn_id,
                    )
                    return

                self._watchdog_retried = True
                logger.warning(
                    "[{}] 3s LLM Watchdog fired on turn {}: no LLM response within 3.0s -> playing filler and retrying LLM",
                    self._stream_id,
                    turn_id,
                )
                await self._play_filler("3s watchdog timeout")
                if self._llm_service and self._context:
                    asyncio.create_task(
                        self._llm_service.process_frame(LLMContextFrame(self._context), FrameDirection.DOWNSTREAM)
                    )
        except asyncio.CancelledError:
            pass

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, UserStartedSpeakingFrame):
                self._cancel_timers("user started speaking")
                self._turn_in_flight = False
                self._filler_dispatched = False
                self._watchdog_retried = False
            elif isinstance(frame, InterruptionFrame):
                self._cancel_timers("interruption")
                self._turn_in_flight = False
                self._filler_dispatched = False
                self._watchdog_retried = False
            elif isinstance(frame, UserStoppedSpeakingFrame):
                self._turn_index += 1
                self._turn_in_flight = False
                self._filler_dispatched = False
                self._watchdog_retried = False
                self._cancel_timers("new user turn")
                # Timers arm ONLY when LLM request is actually dispatched
            elif isinstance(frame, LLMContextFrame):
                self.arm_llm_request(self._turn_index)
            elif isinstance(frame, (TextFrame, LLMFullResponseStartFrame, TTSSpeakFrame)):
                self._cancel_timers("LLM token arrived")
                self._turn_in_flight = False

        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


# Human-style silence ladder lines. Spoken through live TTS (no LLM call).
# Rules for editing: do NOT use phrases that _match_cached_phrase() maps to a cached wav
# ("are you still there", "hello? are you there", "didn't catch that", "say that again",
# "thanks for your time" + "have a great day"/"bye"), and keep the GOODBYE lines free of
# farewell words ("bye", "take care", "have a ... day") - the call is ended by the
# call-end coordinator, not by farewell-pattern detection.
_SILENCE_SOFT_LINES = (
    "Hello? Can you hear me okay?",
    "Take your time, I'm right here whenever you're ready.",
    "Hello? Just checking you're still on the line.",
    "Hi, the line went quiet for a moment. Are you with me?",
    "Hi, I can't hear you at the moment. Is everything alright?",
)
_SILENCE_SECOND_LINES = (
    "Hello? I'm not hearing anything on my side. Could you say something if you can hear me?",
    "Just checking once more, are you able to hear me? If it's a bad time, I'm happy to call back.",
    "The line might be unclear. If now isn't a good time, just let me know and I'll call you later.",
)
_SILENCE_GOODBYE_LINES = (
    "It sounds like now isn't a good time, so I'll let you go. Thank you for your time, and we'll try you again later.",
    "Looks like you're tied up, so I'll let you get back to it. Thank you for your time. We'll reach out again soon.",
    "I'm not able to hear you, so I'll end the call here. Thank you for your time. Feel free to call us back anytime.",
)


class _SilenceChecker(FrameProcessor):
    """
    Production-grade silence monitor adhering to enterprise voice standards:
    - 10-second initial threshold to respect human thinking time.
    - Freezes completely when the bot is generating, thinking, or speaking.
    - Resets instantly upon any user speech.
    """

    def __init__(
        self,
        *,
        stream_id: str,
        task: PipelineTask | None = None,
        context_aggregator_user: Any,
        call_end_coordinator: Any = None,
        silence_threshold_secs: float = 15.0,
        second_threshold_secs: float = 14.0,
        third_threshold_secs: float = 14.0,
        check_in_message: str | None = None,
        second_check_in_message: str | None = None,
        goodbye_message: str | None = None,
        force_hangup_fn: Callable[[str], Awaitable[None]] | None = None,
        poll_interval_secs: float = 1.0,
        call_metrics: Any = None,
        lead_memory: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self._stream_id = stream_id
        self._task = task
        self._context_aggregator_user = context_aggregator_user
        self._call_end_coordinator = call_end_coordinator
        self._silence_threshold_secs = silence_threshold_secs
        self._second_threshold_secs = second_threshold_secs
        self._third_threshold_secs = third_threshold_secs
        self._check_in_message = check_in_message
        self._second_check_in_message = second_check_in_message
        self._goodbye_message = goodbye_message
        self._force_hangup_fn = force_hangup_fn
        self._poll_interval_secs = poll_interval_secs
        self._call_metrics = call_metrics
        self._lead_memory = lead_memory
        self._shutdown_state: dict[str, bool] | None = None
        self._termination_processor: Any = None
        self._rng = random.Random()
        self._used_lines: set[str] = set()

        self._last_user_speech_time: float | None = None
        self._bot_speaking_finish_time: float | None = None
        self._stage = 0  # 0: normal, 1: 1st nudge sent, 2: 2nd nudge sent, 3: hangup pending
        self._check_task: asyncio.Task | None = None
        self._hangup_action_task: asyncio.Task | None = None
        self._running = False
        self._bot_is_speaking = False
        self._user_is_speaking = False
        self._turn_in_flight = False
        self._bot_owes_response = False

    def bind_state(
        self,
        *,
        shutdown_state: dict[str, bool] | None = None,
        termination_processor: Any = None,
    ) -> None:
        self._shutdown_state = shutdown_state
        self._termination_processor = termination_processor

    def set_task(self, task: PipelineTask) -> None:
        self._task = task

    def _pick_line(self, fixed: str | None, pool: tuple[str, ...]) -> str:
        """An explicit constructor message wins; otherwise a random unused line from the pool."""
        if fixed:
            return fixed
        fresh = [l for l in pool if l not in self._used_lines] or list(pool)
        line = self._rng.choice(fresh)
        self._used_lines.add(line)
        return line

    async def _say_goodbye_and_end(self, line: str) -> None:
        """Speak the goodbye through TTS, then end the call after it has actually played."""
        try:
            if line and self._task and not self._is_call_ending:
                await self._task.queue_frames([TTSSpeakFrame(text=line, append_to_context=False)])
                # Wait (max 4s) for goodbye audio to start, so the coordinator's 0.8s
                # "no closing line yet" fallback cannot speak a second goodbye.
                t0 = time.monotonic()
                while not self._bot_is_speaking and time.monotonic() - t0 < 4.0:
                    await asyncio.sleep(0.1)
            if self._call_end_coordinator is not None:
                self._call_end_coordinator.request_ending()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("[{}] SilenceChecker goodbye failed: {}", self._stream_id, e)

    @property
    def _is_call_ending(self) -> bool:
        if self._call_end_coordinator and getattr(self._call_end_coordinator, "is_ending", False):
            return True
        if self._shutdown_state and self._shutdown_state.get("active", False):
            return True
        if self._termination_processor and (
            getattr(self._termination_processor, "_termination_requested", False)
            or getattr(self._termination_processor, "_waiting_for_bot_stop", False)
        ):
            return True
        return False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._last_user_speech_time = time.monotonic()
        self._check_task = asyncio.create_task(self._monitor_silence())

    def stop(self) -> None:
        self._running = False
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
            self._check_task = None
        if self._hangup_action_task and not self._hangup_action_task.done():
            self._hangup_action_task.cancel()
            self._hangup_action_task = None

    def on_user_speech(self) -> None:
        self._user_is_speaking = True
        self._last_user_speech_time = time.monotonic()
        self._turn_in_flight = False
        self._stage = 0
        
        if self._hangup_action_task and not self._hangup_action_task.done():
            self._hangup_action_task.cancel()
            logger.info("[{}] SilenceChecker: user spoke — aborted pending auto-termination", self._stream_id)
        self._hangup_action_task = None

        if self._call_end_coordinator and getattr(self._call_end_coordinator, "_dedicated_goodbye_queued", False):
            # Deliberate farewell is not an idle warning. Preserve quiet barge-in window.
            return

        if self._call_end_coordinator and getattr(self._call_end_coordinator, "is_ending", False):
            self._call_end_coordinator.cancel_ending("user spoke after silence warning")
            logger.info("[{}] SilenceChecker: user spoke — cancelled call_end_coordinator pending termination", self._stream_id)

        if self._termination_processor:
            self._termination_processor.cancel_pending_termination("user spoke after silence warning")

        if not self._running or self._check_task is None or self._check_task.done():
            self.start()

    async def _monitor_silence(self) -> None:
        while self._running:
            await asyncio.sleep(self._poll_interval_secs)

            if self._is_call_ending:
                logger.info("[{}] SilenceChecker: call ending in progress -> stopping silence monitor", self._stream_id)
                self.stop()
                break

            # Safety timeout: if a turn remains in flight for > 20s without any bot response, release it
            if (
                self._turn_in_flight
                and self._last_user_speech_time is not None
                and (time.monotonic() - self._last_user_speech_time) > 20.0
            ):
                self._turn_in_flight = False

            # Freeze the timer if the bot owes a response, is active, turning, user is talking, or call ending is requested
            if (
                self._is_call_ending
                or self._bot_owes_response
                or self._bot_is_speaking
                or self._user_is_speaking
                or self._turn_in_flight
                or self._last_user_speech_time is None
            ):
                continue

            reference_time = max(
                self._last_user_speech_time,
                self._bot_speaking_finish_time or 0.0
            )
            elapsed = time.monotonic() - reference_time

            # Stage 1: Send first soft check-in nudge
            if self._stage == 0 and elapsed >= self._silence_threshold_secs and not self._is_call_ending:
                if self._bot_owes_response or self._turn_in_flight or self._bot_is_speaking or self._user_is_speaking:
                    continue
                self._stage = 1
                now = time.monotonic()
                self._last_user_speech_time = now
                self._bot_speaking_finish_time = now
                self._turn_in_flight = False
                if not self._is_call_ending:
                    logger.info("[{}] SilenceChecker: {}s user silence -> sending first soft nudge", self._stream_id, self._silence_threshold_secs)
                    try:
                        if self._call_metrics and hasattr(self._call_metrics, "mark_silence_nudge"):
                            self._call_metrics.mark_silence_nudge(stage=1)
                        if self._task and not self._is_call_ending:
                            _nudge = TTSSpeakFrame(text=self._pick_line(self._check_in_message, _SILENCE_SOFT_LINES), append_to_context=False)
                            _nudge.is_silence_nudge = True
                            await self._task.queue_frames([_nudge])
                    except Exception as e:
                        logger.error("[{}] SilenceChecker failed to queue first nudge: {}", self._stream_id, e)

            # Stage 2: Send second soft check-in nudge if still silent
            elif self._stage == 1 and elapsed >= self._second_threshold_secs and not self._is_call_ending:
                if self._bot_owes_response or self._turn_in_flight or self._bot_is_speaking or self._user_is_speaking:
                    continue
                self._stage = 2
                now = time.monotonic()
                self._last_user_speech_time = now
                self._bot_speaking_finish_time = now
                self._turn_in_flight = False
                if not self._is_call_ending:
                    logger.info("[{}] SilenceChecker: {}s user silence -> sending second soft nudge", self._stream_id, self._second_threshold_secs)
                    try:
                        if self._call_metrics and hasattr(self._call_metrics, "mark_silence_nudge"):
                            self._call_metrics.mark_silence_nudge(stage=2)
                        if self._task and not self._is_call_ending:
                            _nudge = TTSSpeakFrame(text=self._pick_line(self._second_check_in_message, _SILENCE_SECOND_LINES), append_to_context=False)
                            _nudge.is_silence_nudge = True
                            await self._task.queue_frames([_nudge])
                    except Exception as e:
                        logger.error("[{}] SilenceChecker failed to queue second nudge: {}", self._stream_id, e)

            # Stage 3: Final termination if completely ignored after 2 soft nudges
            elif self._stage == 2 and elapsed >= self._third_threshold_secs and not self._hangup_action_task and not self._is_call_ending:
                self._stage = 3
                logger.info("[{}] SilenceChecker: Silence threshold reached after 2 soft nudges -> scheduling auto-termination", self._stream_id)

                if self._lead_memory is not None:
                    self._lead_memory["disposition"] = "NO_RESPONSE"
                try:
                    import lead_state
                    lead_state.set_disposition(self._stream_id, "NO_RESPONSE")
                except Exception:
                    pass

                goodbye_line = self._pick_line(self._goodbye_message, _SILENCE_GOODBYE_LINES)
                if self._call_end_coordinator is not None:
                    self._hangup_action_task = asyncio.create_task(self._say_goodbye_and_end(goodbye_line))
                else:
                    async def _execute_hangup():
                        try:
                            if goodbye_line and self._task and not self._is_call_ending:
                                await self._task.queue_frames([TTSSpeakFrame(text=goodbye_line, append_to_context=False)])
                                await asyncio.sleep(2.0)
                            if self._force_hangup_fn and not self._is_call_ending:
                                await self._force_hangup_fn("silence_timeout_unanswered")
                            if self._task:
                                await self._task.cancel()
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.error("[{}] SilenceChecker failed during auto-hangup: {}", self._stream_id, e)

                    self._hangup_action_task = asyncio.create_task(_execute_hangup())

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if frame.__class__.__name__ == "StartFrame":
            self.start()

        frame_name = frame.__class__.__name__
        
        if frame_name in ("UserStartedSpeakingFrame", "InterruptionFrame"):
            self.on_user_speech()
            self._bot_owes_response = True
        elif frame_name == "UserStoppedSpeakingFrame":
            self._user_is_speaking = False
            self._last_user_speech_time = time.monotonic()
            self._bot_owes_response = True
        elif isinstance(frame, TranscriptionFrame) and frame.text.strip():
            self._last_user_speech_time = time.monotonic()
            self._stage = 0
            self._turn_in_flight = True
            self._bot_owes_response = True
        elif frame_name in ("LLMFullResponseStartFrame", "TTSSpeakFrame"):
            self._turn_in_flight = True
            self._bot_owes_response = False
        elif frame_name == "BotStartedSpeakingFrame":
            self._bot_is_speaking = True
            self._turn_in_flight = False
            self._bot_owes_response = False
            # NOTE: DO NOT reset self._stage here!
            # Stage represents caller silence progression and must only be reset to 0 by caller speech.
        elif frame_name == "BotStoppedSpeakingFrame":
            self._bot_is_speaking = False
            self._turn_in_flight = False
            self._bot_owes_response = False
            self._bot_speaking_finish_time = time.monotonic()
        elif frame_name == "LLMFullResponseEndFrame":
            if not self._bot_is_speaking:
                self._turn_in_flight = False
        elif frame_name in ("EndTaskFrame", "CancelFrame"):
            self.stop()

        await self.push_frame(frame, direction)


class _SpamQualifyGate(FrameProcessor):
    """20-second Qualify / Spam Gate (Item 2).

    Decides in the first ~20 seconds of a call whether caller is a real person
    or dead/spam/machine call, using deterministic thresholds only:
      1. First ~5-8s VAD pattern:
         - Continuous speech burst > 2.4s before any bot audio played -> likely machine greeting/recording.
         - Zero speech at all for 7.0s from connection -> dead line.
         Action: short polite goodbye, immediate hangup, log verdict + features.
      2. At ~10-20s:
         - Checks committed transcripts for real-estate intent tokens or valid greeting responses.
         - If neither: sends one clarifying nudge ("Hello? Are you looking for a property?").
         - If next response is still empty or nonsense, ends call politely.
      3. Logs every gate verdict + features per call for tuning.
    """

    INTENT_TOKENS = frozenset({
        "bhk", "flat", "apartment", "villa", "plot", "unit", "studio", "house", "ghar", "makan", "kamra",
        "price", "cost", "rate", "daam", "keemat", "budget", "lakh", "lakhs", "lac", "lacs", "crore", "crores", "cr",
        "visit", "site", "dekhna", "location", "area", "possession", "vastu", "balcony", "floor", "parking",
        "loan", "bank", "payment", "builder", "meridian", "brochure", "sqft", "sq ft", "square",
        # Sensible answers to opening questions ("Are you looking for a 2 or 3 BHK?" / "Is this a good time?")
        "2", "3", "two", "three", "both", "yes", "no", "yeah", "yep", "sure", "haan", "nahi", "nahin", "theek",
        "speaking", "bolo", "batao", "suno", "tell", "interested", "looking", "buy", "investment",
    })

    def __init__(
        self,
        *,
        stream_id: str,
        force_hangup_fn: Callable[[str], Awaitable[None]] | None = None,
        is_call_ending: Callable[[], bool] | None = None,
        goodbye_message: str = "Looks like you're busy right now. I'll call you later, have a great day!",
        clarifying_message: str = "Hello? Are you looking for a property?",
    ) -> None:
        super().__init__()
        self._stream_id = stream_id
        self._force_hangup_fn = force_hangup_fn
        self._is_call_ending = is_call_ending or (lambda: False)
        self._goodbye_message = goodbye_message
        self._clarifying_message = clarifying_message

        self._task = None
        self._connected_at = time.monotonic()
        self._bot_audio_started = False
        self._user_speech_start_time: float | None = None
        self._user_speech_burst_max: float = 0.0
        self._user_ever_spoke = False
        self._user_turn_count = 0
        self._gate_resolved = False
        self._clarifying_nudge_sent = False
        self._monitor_task: asyncio.Task | None = None

    def bind_task(self, task) -> None:
        self._task = task
        if self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def _hangup_with_verdict(self, verdict: str, reason: str, **features: Any) -> None:
        if self._gate_resolved or self._is_call_ending():
            return
        self._gate_resolved = True
        elapsed = round(time.monotonic() - self._connected_at, 2)
        features_str = " ".join(f"{k}={v}" for k, v in features.items())
        logger.info(
            "[{}] SpamQualifyGate VERDICT={} reason={} elapsed={}s {}",
            self._stream_id,
            verdict,
            reason,
            elapsed,
            features_str,
        )
        try:
            if self._task and not self._is_call_ending():
                await self._task.queue_frames(
                    [TTSSpeakFrame(text=self._goodbye_message, append_to_context=False)]
                )
                await asyncio.sleep(2.0)
        except Exception:
            pass
        if self._force_hangup_fn:
            await self._force_hangup_fn(f"spam_gate:{verdict}")

    async def _monitor_loop(self) -> None:
        """Background checker for 15s intent check."""
        try:
            # Check at 15.0s if turn 1 occurred but had zero intent
            await asyncio.sleep(15.0)
            if not self._gate_resolved and not self._is_call_ending():
                if self._user_turn_count >= 1 and not self._clarifying_nudge_sent:
                    logger.info(
                        "[{}] SpamQualifyGate: 15s intent check; sending clarifying nudge",
                        self._stream_id,
                    )
                    self._clarifying_nudge_sent = True
                    if self._task and not self._is_call_ending():
                        await self._task.queue_frames(
                            [TTSSpeakFrame(text=self._clarifying_message, append_to_context=False)]
                        )
        except asyncio.CancelledError:
            pass

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if self._gate_resolved or self._is_call_ending():
            await self.push_frame(frame, direction)
            return

        frame_type = type(frame).__name__

        if frame_type == "BotStartedSpeakingFrame":
            self._bot_audio_started = True

        elif frame_type == "UserStartedSpeakingFrame":
            self._user_ever_spoke = True
            self._user_speech_start_time = time.monotonic()

        elif frame_type == "UserStoppedSpeakingFrame":
            if self._user_speech_start_time is not None:
                duration = time.monotonic() - self._user_speech_start_time
                self._user_speech_burst_max = max(self._user_speech_burst_max, duration)
                # Signal A: continuous speech burst > 2.4s BEFORE bot audio started
                if not self._bot_audio_started and duration > 2.4:
                    await self._hangup_with_verdict(
                        "machine_greeting",
                        "continuous_burst_before_bot_audio",
                        burst_duration=round(duration, 2),
                    )
                    await self.push_frame(frame, direction)
                    return
                self._user_speech_start_time = None

        elif frame_type == "TranscriptionFrame":
            self._user_turn_count += 1
            text = (getattr(frame, "text", "") or "").lower()
            tokens = set(re.findall(r"\w+", text))

            # Match against intent tokens
            matched_intent = tokens & self.INTENT_TOKENS
            if matched_intent:
                self._gate_resolved = True
                elapsed = round(time.monotonic() - self._connected_at, 2)
                logger.info(
                    "[{}] SpamQualifyGate VERDICT=qualified_real_caller elapsed={}s matched_tokens={}",
                    self._stream_id,
                    elapsed,
                    list(matched_intent)[:5],
                )
            elif self._clarifying_nudge_sent:
                # User had a chance after clarifying nudge, but still gave no intent/empty response
                await self._hangup_with_verdict(
                    "unqualified_no_intent",
                    "no_intent_after_clarifying_nudge",
                    transcript_length=len(text),
                )
                await self.push_frame(frame, direction)
                return

        elif frame_type in ("CancelFrame", "EndFrame", "EndTaskFrame"):
            if self._monitor_task and not self._monitor_task.done():
                self._monitor_task.cancel()

        await self.push_frame(frame, direction)

    async def cleanup(self) -> None:
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        await super().cleanup()


class _TranscriptionTap(FrameProcessor):
    """Passively taps TranscriptionFrame and ProposedUserStoppedSpeakingFrame for call_metrics.

    Sits immediately downstream of STT (upstream of context_aggregator.user() which
    absorbs TranscriptionFrame). Notifies call_metrics directly without pushing any
    new frames or forwarding MetricsFrame, ensuring zero chance of a replay loop.
    """

    def __init__(self, collector: Any = None) -> None:
        super().__init__()
        self._collector = collector

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, TranscriptionFrame) and frame.text.strip() and (not valid_transcript(frame.text) or fragment_transcript(frame.text)):
            logger.info("Dropping nonlinguistic STT noise before aggregation: {!r}", frame.text[:60])
            frame.text = ""  # preserve the empty-turn lifecycle, never add noise to LLM context
        if self._collector and direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, ProposedUserStoppedSpeakingFrame):
                self._collector._speech_stop_at = time.monotonic()
            elif isinstance(frame, TranscriptionFrame):
                if hasattr(self._collector, "record_transcription_metrics"):
                    self._collector.record_transcription_metrics(frame)
        await self.push_frame(frame, direction)



class SarvamSmartTurnStopStrategy(TurnAnalyzerUserTurnStopStrategy):
    """
    Bridges Sarvam STT's ProposedUserStarted/StoppedSpeakingFrame into TurnAnalyzerUserTurnStopStrategy.
    Sarvam STT emits ProposedUserStarted/StoppedSpeakingFrame without Silero VAD.
    Resolves turns immediately when (Smart Turn COMPLETE) AND (final transcript received), in either order.
    Fallback timeout (250ms) fires if transcript never arrives.
    Cancels in-flight inference and fallback timer on renewed speech or barge-in.
    """

    def __init__(self, *args, fallback_timeout: float = 0.25, stream_id: str = "", **kwargs):
        super().__init__(*args, **kwargs)
        self._analysis_task: asyncio.Task | None = None
        self._fallback_timeout_task: asyncio.Task | None = None
        self._fallback_timeout_seconds: float = fallback_timeout
        self._has_transcript: bool = False
        self._turn_complete: bool = False
        self._turn_stopped: bool = False
        self._stream_id: str = stream_id

    @property
    def resolves_proposed_turn_stop_frames(self) -> bool:
        return True

    def cancel_inference(self) -> None:
        """Cancels any in-flight Smart Turn inference task and fallback timeout."""
        if self._analysis_task and not self._analysis_task.done():
            self._analysis_task.cancel()
            self._analysis_task = None
        if self._fallback_timeout_task and not self._fallback_timeout_task.done():
            self._fallback_timeout_task.cancel()
            self._fallback_timeout_task = None

    async def _discard_pending_end_of_turn(self):
        self.cancel_inference()
        self._has_transcript = False
        self._turn_complete = False
        self._turn_stopped = False
        await super()._discard_pending_end_of_turn()

    async def _check_and_trigger(self, source: str = "strategy"):
        if self._turn_stopped:
            return
        if self._turn_complete and self._has_transcript:
            self._turn_stopped = True
            if self._fallback_timeout_task and not self._fallback_timeout_task.done():
                self._fallback_timeout_task.cancel()
                self._fallback_timeout_task = None
            logger.info("[{}] turn_stop source={}", self._stream_id, source)
            await self.trigger_user_turn_stopped()

    async def _handle_vad_user_stopped_speaking(self, frame: VADUserStoppedSpeakingFrame):
        self._vad_user_speaking = False
        self._stop_secs = frame.stop_secs
        self._vad_stopped = True

        async def _run_analysis():
            try:
                state, prediction = await self._turn_analyzer.analyze_end_of_turn()
                await self._handle_prediction_result(prediction)
                self._turn_complete = (state == EndOfTurnState.COMPLETE)
                await self._check_and_trigger("strategy")
            except asyncio.CancelledError:
                pass
            except Exception as _e:
                logger.debug(f"{self}: Error during turn analysis: {_e}")

        self.cancel_inference()
        self._analysis_task = self.task_manager.create_task(
            _run_analysis(), f"{self}::_run_analysis"
        )

        async def _fallback_timeout_handler():
            try:
                await asyncio.sleep(self._fallback_timeout_seconds)
                if not self._turn_stopped:
                    self._turn_stopped = True
                    logger.info("[{}] turn_stop source=timeout", self._stream_id)
                    await self.trigger_user_turn_stopped()
            except asyncio.CancelledError:
                pass

        self._fallback_timeout_task = self.task_manager.create_task(
            _fallback_timeout_handler(), f"{self}::_fallback_timeout_handler"
        )

    async def process_frame(self, frame: Frame) -> ProcessFrameResult:
        if isinstance(frame, ProposedUserStartedSpeakingFrame):
            self.cancel_inference()
            self._turn_stopped = False
            self._has_transcript = False
            self._turn_complete = False
            await self._handle_vad_user_started_speaking(
                VADUserStartedSpeakingFrame(start_secs=0.0, timestamp=time.time())
            )
        elif isinstance(frame, ProposedUserStoppedSpeakingFrame):
            await self._handle_vad_user_stopped_speaking(
                VADUserStoppedSpeakingFrame(stop_secs=0.0, timestamp=time.time())
            )
        elif isinstance(frame, (UserStartedSpeakingFrame, InterruptionFrame)):
            self.cancel_inference()
        elif isinstance(frame, TranscriptionFrame):
            if frame.text and frame.text.strip():
                self._has_transcript = True
                self._text = frame.text
                await self._check_and_trigger("strategy")
        return await super().process_frame(frame)



class DebouncedExternalUserTurnStopStrategy(ExternalUserTurnStopStrategy):
    """
    Subclasses ExternalUserTurnStopStrategy to prevent premature turn cuts on filler/hesitation words.

    If an utterance consists purely of filler/hesitation words (e.g. 'uh', 'um', 'hmm', 'er', 'acha toh'),
    an extra debounce buffer (250ms) is applied allowing the user to complete their thought
    without the bot barging in.

    CRITICAL: NEVER use character count (<= 3). 'Yes' (3 chars) and 'No' (2 chars)
    are real answers and MUST fire immediately with 0ms delay. We check against explicit sets.
    """

    FILLER_WORDS = frozenset({
        "uh", "um", "ah", "hmm", "hm", "er", "erm", "uhh", "umm", "ahh", "hmmm", "err",
        "arre", "arrey", "acha", "accha", "toh", "aur", "matlab", "yani", "yaani",
    })

    IMMEDIATE_ANSWERS = frozenset({
        "yes", "no", "ok", "okay", "sure", "yep", "yeah", "ya", "right", "fine", "cool",
        "haan", "ha", "haa", "nahi", "nahin", "na", "theek", "suno", "batao", "bolo",
        "kya", "kyu", "kyun", "why", "what", "how", "who", "when", "where",
    })

    TRAILING_CONNECTORS = frozenset({
        # English articles, prepositions & conjunctions
        "the", "a", "an", "and", "or", "so", "but", "because", "like", "actually",
        "to", "of", "in", "with", "that", "for", "as", "at", "by", "from",
        "about", "into", "through", "before", "after", "above", "below", "between", "under",
        "since", "without", "within", "though", "if", "then", "also", "just", "well",
        # English verbs & auxiliaries
        "are", "is", "was", "were", "am", "be", "been", "being",
        "have", "has", "had", "do", "does", "did",
        "will", "would", "shall", "should", "can", "could", "may", "might", "must",
        # English pronouns & contractions
        "i", "me", "my", "we", "us", "our", "you", "your", "he", "him", "his", "she", "her",
        "it", "its", "they", "them", "their", "which", "who", "whom", "whose", "what", "where", "when", "how",
        "i'm", "im", "i've", "ive", "i'll", "ill", "i'd", "id", "we're", "you're", "they're",
        # Hindi / Hinglish connectors & pronouns
        "ki", "aur", "ya", "toh", "ke", "ka", "ko", "se", "mein", "me",
        "lekin", "par", "bhi", "matlab", "jaise", "agar", "magar", "phir", "fir",
        "mai", "main", "hum", "aap", "tum", "woh", "yeh", "wait", "ruko",
    })

    _PUNCT_STRIP = ",.!?\"' \t\r\n\u0964\u0965-:;"

    def __init__(
        self,
        *,
        timeout: float = 0.20,
        filler_debounce_seconds: float = 0.25,
        wait_for_transcript: bool = True,
        **kwargs,
    ):
        super().__init__(timeout=timeout, wait_for_transcript=wait_for_transcript, **kwargs)
        self._filler_debounce_seconds = filler_debounce_seconds
        self._filler_debounce_task: asyncio.Task | None = None

    def _is_pure_filler(self, text: str) -> bool:
        tokens = [t.strip(self._PUNCT_STRIP).lower() for t in text.split()]
        tokens = [t for t in tokens if t]
        if not tokens:
            return False
        if any(t in self.IMMEDIATE_ANSWERS for t in tokens):
            return False
        return all(t in self.FILLER_WORDS for t in tokens)

    def _ends_with_continuation_connector(self, text: str) -> bool:
        if not text:
            return False
        # If the transcript ends with terminal punctuation (period, exclamation, question mark, danda),
        # the speaker completed their sentence. It is never a trailing connector.
        if text.rstrip().endswith((".", "!", "?", "\u0964", "\u0965")):
            return False
        tokens = [t.strip(self._PUNCT_STRIP).lower() for t in text.split()]
        tokens = [t for t in tokens if t]
        if not tokens:
            return False
        if len(tokens) == 1 and tokens[0] in self.IMMEDIATE_ANSWERS:
            return False
        return tokens[-1] in self.TRAILING_CONNECTORS

    def _is_immediate_answer(self, text: str) -> bool:
        tokens = [t.strip(self._PUNCT_STRIP).lower() for t in text.split()]
        tokens = [t for t in tokens if t]
        return len(tokens) <= 2 and all(t in self.IMMEDIATE_ANSWERS for t in tokens)

    async def _handle_user_started_speaking(self, *, announced_elsewhere: bool):
        if self._filler_debounce_task and not self._filler_debounce_task.done():
            self._filler_debounce_task.cancel()
            self._filler_debounce_task = None
        await super()._handle_user_started_speaking(announced_elsewhere=announced_elsewhere)

    async def _handle_interim_transcription(self, frame: InterimTranscriptionFrame):
        if self._filler_debounce_task and not self._filler_debounce_task.done():
            self._filler_debounce_task.cancel()
            self._filler_debounce_task = None
        await super()._handle_interim_transcription(frame)

    async def _handle_transcription(self, frame: TranscriptionFrame):
        await super()._handle_transcription(frame)
        if self._filler_debounce_task and not self._filler_debounce_task.done():
            self._filler_debounce_task.cancel()
            self._filler_debounce_task = None
        # Fast-path for short definitive single-word answers (e.g. "yes", "no", "speaking", "alex"):
        # If user gave a definitive 1-2 word answer and VAD has cleared, trigger immediately.
        # For longer sentences, let _task_handler wait for the debounced timeout so the user
        # is never cut off mid-sentence while pausing to think or formulate their thoughts.
        if not self._user_speaking and self._turn_open and (self._is_immediate_answer(self._text) or terminal_answer(self._text, self._is_pure_filler(self._text), self._ends_with_continuation_connector(self._text))):
            await self._maybe_trigger_user_turn_stopped()

    async def _maybe_trigger_user_turn_stopped(self):
        if self._user_speaking:
            return
        if not self._wait_for_transcript:
            await self._trigger_user_turn_stopped()
            return
        if not self._seen_interim_results and self._text:
            if self._is_pure_filler(self._text) or self._ends_with_continuation_connector(self._text):
                if self._filler_debounce_task and not self._filler_debounce_task.done():
                    return

                async def _debounced_trigger():
                    try:
                        await asyncio.sleep(self._filler_debounce_seconds)
                        if not self._user_speaking and self._turn_open:
                            await self._trigger_user_turn_stopped()
                    except asyncio.CancelledError:
                        pass

                self._filler_debounce_task = asyncio.create_task(_debounced_trigger())
                return
            await self._trigger_user_turn_stopped()

    async def _reset(self):
        if self._filler_debounce_task and not self._filler_debounce_task.done():
            self._filler_debounce_task.cancel()
            self._filler_debounce_task = None
        await super()._reset()


_PUNCT_RE = re.compile(r"[\.,!?;:\"'`~@#$%^&*()_+=\-\[\]{}<>/\\|]")


def _is_farewell_or_acknowledgment(text: str) -> bool:
    """
    Distinguishes reciprocal farewells / backchannels from genuine conversational interruptions.
    Returns True if the caller is saying goodbye or acknowledging a sign-off (e.g. 'bye', 'thank you bye', 'theek hai').
    Returns False if the caller is asking a question or trying to continue the call (e.g. 'actually listen', 'wait').
    """
    if not text:
        return True
    t = text.strip().lower()
    # Explicit question mark indicates an active inquiry / continuation
    if "?" in t:
        return False

    clean = _PUNCT_RE.sub(" ", t).strip()
    words = set(clean.split())
    if not words:
        return True

    continuation_keywords = {
        "listen", "wait", "suno", "ruko", "boliye", "batao", "question",
        "tell", "what", "why", "how", "when", "where", "which", "who", "price",
        "cost", "bhk", "location", "amenit", "project", "metro", "visit",
        "actually", "hello", "ananya", "kya", "kyu", "kyun", "kaun", "kab",
        "kahan", "kitna", "kaise", "enti", "ela", "eppudu", "ekada", "cheppandi", "aagandi"
    }
    if words & continuation_keywords:
        return False

    farewell_words = {
        "bye", "byebye", "goodbye", "tata", "alvida",
        "बाय", "अलविदा", "टाटा", "బాయ్",
    }
    if words & farewell_words:
        return True

    closing_phrases = [
        "thank you", "thanks", "take care", "have a good day",
        "have a nice day", "no worries", "no worry", "all good",
        "theek hai", "chalo", "chalega", "okay then", "ok then",
        "shubh ho", "din shubh", "achha rahe", "din achha",
        "शुक्रिया", "धन्यवाद", "शुभ हो", "दिन शुभ", "ధన్యవాదాలు", "సరే",
    ]
    if any(p in clean for p in closing_phrases):
        return True

    backchannels = {
        "uh", "um", "hmm", "hm", "yeah", "yep", "yup",
        "haan", "ha", "ok", "okay", "ah", "oh", "mm",
        "aha", "huh", "right", "sure", "acha", "accha",
        "fine", "cool", "theek", "hai", "sare", "chalo",
    }
    if len(words) <= 3 and words.issubset(backchannels):
        return True

    return False


class _TerminationProcessor(FrameProcessor):
    """
    Detect farewell text without buffering or rewriting normal LLM output.

    This processor handles:
    - Explicit termination leaks from the LLM.
    - Farewell text generated by the LLM.
    - Cancellation when the caller interrupts a pending goodbye.
    """

    def __init__(
        self,
        *,
        stream_id: str,
        on_hangup: Callable[[], Awaitable[None]],
        force_hangup_fn: Callable[[str], Awaitable[None]],
        grace_seconds: float = 0.8,
        safety_seconds: float = 15.0,
    ) -> None:
        super().__init__()

        self._stream_id = stream_id
        self._on_hangup = on_hangup
        self._force_hangup_fn = force_hangup_fn
        self._grace_seconds = grace_seconds
        self._safety_seconds = safety_seconds

        self._termination_requested = False
        self._provider_hangup_sent = False
        self._waiting_for_bot_stop = False
        self._current_turn_text = ""
        self._hangup_task: asyncio.Task | None = None
        # Fallback timeout task if BotStoppedSpeakingFrame never arrives
        self._safety_task: asyncio.Task | None = None

        self._shutdown_state: dict[str, bool] = {}
        self._end_call_pending: dict[str, bool] = {}
        self._get_latest_user_utterance: Callable[[], str] | None = None
        self._call_end_coordinator: Any = None
        self._lead_memory: dict[str, str] | None = None
        self._rejection_attempts: dict[str, int] | None = None
        self._post_farewell_replies: int = 0

        self.silent_termination_patterns = [
            (
                "end_call_leak",
                re.compile(
                    r"\bend[_\s-]?call\b\.?",
                    re.IGNORECASE,
                ),
            ),
            (
                "tool_narration",
                re.compile(
                    r"\b(call|dial|invok|trigger|activat)\w*\s+"
                    r"(the\s+)?(tool|function|api)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "explicit_command",
                re.compile(
                    r"\b(end|clos|terminat|finish|hang)\w*\s+"
                    r"(the\s+)?(call|conversation|session|up|tool)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "disposition_leak",
                re.compile(
                    r"\b(site\s+visit|inventory|lead)\s+"
                    r"(is\s+)?(booked|sent|partial)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "explicit_tag",
                re.compile(
                    r"\[hangup\]|\[end\]",
                    re.IGNORECASE,
                ),
            ),
        ]

        self.spoken_termination_patterns = [
            re.compile(r"\bsee\s+you\s+(then|tomorrow|later|soon)\b", re.IGNORECASE),
            re.compile(r"\bhave\s+a\s+(good|great|nice|wonderful)\s+(day|evening|night|time)\b", re.IGNORECASE),
            re.compile(r"\b(goodbye|bye\s+bye|bye|take\s+care|talk\s+soon)\b", re.IGNORECASE),
            re.compile(r"\b(thanks?|thank\s+you)\s+(so\s+much\s+)?for\s+(your\s+)?time\b", re.IGNORECASE),
            re.compile(r"\b(alvida|dhanyavaad|dhanyavad|dhanyawaad|shukriya|phir\s+milenge|shubh\s+ho|din\s+shubh|din\s+achha|achha\s+rahe)\b", re.IGNORECASE),
            re.compile(r"\b(dhanyavadalu|dhanyavadamulu|selavu|untanu|untanandi|malli\s+kaluddam)\b", re.IGNORECASE),
            re.compile(r"(शुभ\s*हो|दिन\s*शुभ|अलविदा|धन्यवाद|शुक्रिया|फिर\s*मिलेंगे|शुभ\s*दिन|ధన్యవాదాలు)", re.IGNORECASE),
        ]

    def bind_state(
        self,
        *,
        shutdown_state: dict[str, bool],
        end_call_pending: dict[str, bool],
        get_latest_user_utterance: Callable[[], str] | None = None,
        call_end_coordinator: Any = None,
        lead_memory: dict[str, str] | None = None,
        rejection_attempts: dict[str, int] | None = None,
    ) -> None:
        self._shutdown_state = shutdown_state
        self._end_call_pending = end_call_pending
        if get_latest_user_utterance is not None:
            self._get_latest_user_utterance = get_latest_user_utterance
        if call_end_coordinator is not None:
            self._call_end_coordinator = call_end_coordinator
        if lead_memory is not None:
            self._lead_memory = lead_memory
        if rejection_attempts is not None:
            self._rejection_attempts = rejection_attempts

    def is_termination_allowed(self) -> bool:
        """
        Deterministic Call Termination Gate.
        A farewell from the LLM is permitted to end the call if:
        1. Call end coordinator is already closing (e.g. deliberate shutdown or silence timeout).
        2. Site visit is scheduled/booked/confirmed in lead_memory.
        3. Caller explicitly ended or used a soft-close phrase (and not an un-rebutted objection).
        4. Inverted guardrail: Allow LLM closure UNLESS the caller is actively engaged
           (asked an active question, specified property criteria, greeted, or agreed to listen).
        """
        # 1. Deliberate shutdown / silence timeout / coordinator closing
        if self._call_end_coordinator is not None and getattr(self._call_end_coordinator, "is_closing_in_progress", False):
            return True

        # 2. Objective achieved
        if self._lead_memory is not None:
            site_visit = str(self._lead_memory.get("site_visit", "")).strip().capitalize()
            if site_visit in ("Booked", "Confirmed", "Scheduled", "Interested", "Yes"):
                return True

        # 3. Caller text evaluation
        if self._get_latest_user_utterance is not None:
            latest_user = self._get_latest_user_utterance()
            if latest_user:
                lower_user = latest_user.lower()

                # Explicit caller sign-off ("bye", "goodbye") overrides objections
                if _FAREWELL_WORD_RE.search(lower_user):
                    return True

                # Veto condition 0: Caller expressed objection / hesitation (requires 2 full attempts)
                # Ensure the consultative sales agent attempts to probe/understand what's holding them back.
                has_objection = _is_objection_utterance(lower_user)
                if has_objection:
                    attempts = self._lead_memory.get("_objection_count", 0) if self._lead_memory else 0
                    if getattr(self, "_rejection_attempts", None) is not None:
                        attempts = max(attempts, self._rejection_attempts.get("count", 0))
                    has_rejected_whatsapp = bool(re.search(
                        r"\b(?:no|don'?t|dont|mat|stop|nahi)\b.*?\b(?:send|whatsapp|share|brochure|detail|anything)\b|\b(?:no|nahi|mat)\s*(?:thanks?|shukriya)?$",
                        lower_user.strip(),
                    ))
                    if attempts <= 2 and not has_rejected_whatsapp:
                        if getattr(self, "_rejection_attempts", None) is not None:
                            self._rejection_attempts["count"] = max(attempts + 1, self._rejection_attempts.get("count", 0) + 1)
                        return False
                    else:
                        # 2 objection convincing attempts completed or caller explicitly refused WhatsApp; allow graceful termination
                        return True

                # Direct match for explicit farewell or soft-close
                if _caller_explicitly_ended(latest_user) or _is_soft_close(latest_user):
                    return True

                # Veto condition A: Caller asked an active question (ignore conversational 'you know what')
                clean_for_q = re.sub(r"\byou\s+know\s+what\b", "", lower_user)
                has_active_question = "?" in lower_user or bool(re.search(
                    r"\b(amenit\w*|price|cost|budget|bhk|where|location|what|how|tell\s+me|kya|kitna|floor\s+plan|sqft|square\s+feet)\b", clean_for_q
                ))
                if has_active_question:
                    return False

                # Veto condition B: Caller stated property preferences / criteria
                has_active_criteria = bool(re.search(
                    r"\b(flexible|looking\s+for|want\s+to|interested\s+in|invest\w*|[1-4]\s*bhk|crores?|lakhs?|ready\s+to\s+move|under\s+construction)\b", lower_user
                ))
                if has_active_criteria:
                    return False

                # Veto condition C: Caller greeted or agreed to listen
                has_greeting_or_agreement = bool(re.search(
                    r"\b(hello|hi|hey|namaste|vanakkam|suno|listen|batao|go\s+ahead)\b", lower_user
                ))
                if has_greeting_or_agreement:
                    return False

                # Veto condition D: Caller used a continuation word or trailing conjunction
                has_continuation = bool(re.search(
                    r"\b(and|aur|also|matlab|par|lekin|but|suno|listen|aur\s*bhi|wait|ruko)\b[\s.?!]*$", lower_user
                )) or lower_user.strip().endswith(("and.", "and", "aur", "aur."))
                if has_continuation:
                    return False

                # If none of the active vetoes triggered and the LLM produced a farewell,
                # trust the LLM's conversational closure decision.
                return True

        return True

    def _strip_spoken_farewell(self, text: str) -> str:
        cleaned = text
        for pat in self.spoken_termination_patterns:
            cleaned = pat.sub("", cleaned)
        cleaned = re.sub(r"[\s,;:!?.-]+$", "", cleaned).strip()
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        return cleaned

    async def _hangup_after_bot_stop(self) -> None:
        try:
            await asyncio.sleep(self._grace_seconds)

            if not self._provider_hangup_sent:
                logger.info(
                    "[{}] Hangup completed reason=farewell",
                    self._stream_id,
                )
                await self._force_hangup_fn(
                    "farewell"
                )
                self._provider_hangup_sent = True

            await self.push_frame(
                EndTaskFrame(),
                FrameDirection.UPSTREAM,
            )

        except asyncio.CancelledError:
            return

    async def _schedule_hangup(
        self,
        delay: float,
    ) -> None:
        try:
            await asyncio.sleep(delay)

            if not self._provider_hangup_sent:
                logger.info(
                    "[{}] Hangup completed reason=farewell",
                    self._stream_id,
                )
                await self._force_hangup_fn(
                    "farewell"
                )
                self._provider_hangup_sent = True

            await self.push_frame(
                EndTaskFrame(),
                FrameDirection.UPSTREAM,
            )

        except asyncio.CancelledError:
            return

    async def _run_safety_timeout(self) -> None:
        """Fallback hangup timer if BotStoppedSpeakingFrame is not received after a farewell."""
        try:
            await asyncio.sleep(self._safety_seconds)
        except asyncio.CancelledError:
            return

        if self._provider_hangup_sent:
            return

        logger.warning(
            "[{}] Farewell safety timeout ({}s) -- BotStoppedSpeakingFrame "
            "never arrived; forcing hangup anyway",
            self._stream_id,
            self._safety_seconds,
        )

        logger.info(
            "[{}] Hangup completed reason=farewell",
            self._stream_id,
        )
        await self._force_hangup_fn("farewell")
        self._provider_hangup_sent = True

        await self.push_frame(
            EndTaskFrame(),
            FrameDirection.UPSTREAM,
        )

    def cancel_pending_termination(self, reason: str = "interrupted") -> None:
        was_pending = (
            self._waiting_for_bot_stop
            or self._termination_requested
            or (self._hangup_task is not None and not self._hangup_task.done())
            or (self._safety_task is not None and not self._safety_task.done())
            or (isinstance(self._shutdown_state, dict) and self._shutdown_state.get("active"))
            or (isinstance(self._end_call_pending, dict) and self._end_call_pending.get("active"))
        )
        if not was_pending:
            return

        self._waiting_for_bot_stop = False

        if self._hangup_task and not self._hangup_task.done():
            self._hangup_task.cancel()
        self._hangup_task = None

        if self._safety_task and not self._safety_task.done():
            self._safety_task.cancel()
        self._safety_task = None

        self._termination_requested = False
        self._provider_hangup_sent = False

        if isinstance(self._shutdown_state, dict):
            self._shutdown_state["active"] = False
        if isinstance(self._end_call_pending, dict):
            self._end_call_pending["active"] = False
        if self._lead_memory is not None:
            self._lead_memory.pop("_farewell_spoken", None)

        logger.info(
            "[{}] Farewell cancelled: {}",
            self._stream_id,
            reason,
        )

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        await super().process_frame(frame, direction)

        # Exactly one timer owner. Dedicated farewell/coordinator owns grace and late replies.
        if self._call_end_coordinator and self._call_end_coordinator.is_ending:
            for attr in ("_hangup_task", "_safety_task"):
                pending = getattr(self, attr, None)
                if pending and pending is not asyncio.current_task() and not pending.done():
                    pending.cancel()
                setattr(self, attr, None)
            await self.push_frame(frame, direction)
            return

        # FIX 7: In _TerminationProcessor, do not cancel hangup on caller interruption; allow max 1 post-farewell reply, then hang up logging "hangup completed reason=farewell"
        if isinstance(frame, (InterruptionFrame, UserStartedSpeakingFrame)):
            if self._termination_requested:
                if self._post_farewell_replies >= 1:
                    logger.info("[{}] Hangup completed reason=farewell", self._stream_id)
                    if not self._provider_hangup_sent:
                        await self._force_hangup_fn("farewell")
                        self._provider_hangup_sent = True
                    await self.push_frame(EndTaskFrame(), FrameDirection.UPSTREAM)
                    return
                else:
                    self._post_farewell_replies += 1
                    logger.info(
                        "[{}] Caller interrupted after farewell; allowing max 1 post-farewell reply (reply #{})",
                        self._stream_id,
                        self._post_farewell_replies,
                    )
            await self.push_frame(frame, direction)
            return

        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(frame, LLMFullResponseStartFrame)
        ):
            self._current_turn_text = ""
            if (self._hangup_task or self._waiting_for_bot_stop) and not self._termination_requested:
                self.cancel_pending_termination("new LLM response started")

        # Inspect bot speech for explicit termination.
        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(
                frame,
                (TextFrame, TTSSpeakFrame),
            )
        ):
            text = getattr(frame, "text", "") or ""
            self._current_turn_text += text

            silent_match = any(
                pattern.search(text)
                for _, pattern in self.silent_termination_patterns
            ) or any(
                pattern.search(self._current_turn_text)
                for _, pattern in self.silent_termination_patterns
            )

            spoken_match = any(
                pattern.search(text)
                for pattern in self.spoken_termination_patterns
            ) or any(
                pattern.search(self._current_turn_text)
                for pattern in self.spoken_termination_patterns
            )

            if silent_match:
                if not self._termination_requested:
                    self._termination_requested = True
                    self._shutdown_state["active"] = True

                    if (
                        self._hangup_task
                        and not self._hangup_task.done()
                    ):
                        self._hangup_task.cancel()

                    self._hangup_task = asyncio.create_task(
                        self._schedule_hangup(0.5)
                    )

                    logger.info(
                        "[{}] Silent termination detected; forcing hangup",
                        self._stream_id,
                    )

            elif spoken_match:
                if self._waiting_for_bot_stop:
                    # Already detected and armed for this farewell turn; do not duplicate processing or log
                    await self.push_frame(frame, direction)
                    return

                if not self.is_termination_allowed():
                    latest_user = self._get_latest_user_utterance() if self._get_latest_user_utterance else ""
                    logger.warning(
                        "[{}] Suppressed premature spoken farewell hangup: caller actively engaged (utterance={!r}, spoken={!r})",
                        self._stream_id,
                        latest_user,
                        text,
                    )
                    cleaned = self._strip_spoken_farewell(text)
                    # Reset accumulated text so subsequent streaming tokens don't
                    # re-match the already-stripped farewell via _current_turn_text
                    self._current_turn_text = self._strip_spoken_farewell(self._current_turn_text)
                    if not cleaned or not re.search(r"\w", cleaned):
                        has_obj = any(p in latest_user.lower() for p in ("not interested", "dont want", "don't want", "nahi chahiye", "kuch nahi", "not now", "no need", "no thanks"))
                        if has_obj:
                            pivot_text = "Totally understand, is it the location, price, or just not the right time?"
                            await self.push_frame(TTSSpeakFrame(text=pivot_text), direction)
                        return
                    if hasattr(frame, "text"):
                        frame.text = cleaned
                    await self.push_frame(frame, direction)
                    return

                self._termination_requested = True
                self._waiting_for_bot_stop = True
                self._shutdown_state["active"] = True

                # Arm safety timeout backstop while waiting for bot speech to finish
                if (
                    self._safety_task
                    and not self._safety_task.done()
                ):
                    self._safety_task.cancel()

                self._safety_task = asyncio.create_task(
                    self._run_safety_timeout()
                )

                logger.info(
                    "[{}] Farewell detected; will hang up after bot stops speaking",
                    self._stream_id,
                )
            else:
                if self._hangup_task or self._waiting_for_bot_stop or self._termination_requested:
                    self.cancel_pending_termination("new assistant speech generated")

            await self.push_frame(
                frame,
                direction,
            )
            return

        if isinstance(frame, BotStoppedSpeakingFrame):
            if (
                self._waiting_for_bot_stop
                and not self._hangup_task
            ):
                logger.info(
                    "[{}] Farewell finished; hanging up in {}s",
                    self._stream_id,
                    self._grace_seconds,
                )

                if (
                    self._safety_task
                    and not self._safety_task.done()
                ):
                    self._safety_task.cancel()

                self._safety_task = None

                self._hangup_task = asyncio.create_task(
                    self._hangup_after_bot_stop()
                )

                self._waiting_for_bot_stop = False

            await self.push_frame(frame, direction)
            return

        await self.push_frame(frame, direction)


class _CallEndCoordinator(FrameProcessor):
    """Graceful call termination handler with grace window support."""

    def __init__(
        self,
        *,
        stream_id: str,
        on_hangup: Callable[[], Awaitable[None]],
        grace_seconds: float = 1.4,
        safety_seconds: float = 20.0,
        fallback_closing_text: str = "Thank you for your time today. Have a great day!",
        fallback_seconds: float = 0.8,
    ) -> None:
        super().__init__()

        self._stream_id = stream_id
        self._on_hangup = on_hangup
        self._grace_seconds = grace_seconds
        self._safety_seconds = safety_seconds
        self._fallback_closing_text = fallback_closing_text
        self._fallback_seconds = fallback_seconds

        self._requested = False
        self._dedicated_goodbye_queued = False
        self._awaiting_closing = False
        self._closing_in_progress = False
        self._in_grace = False
        self._ended = False
        self._current_audio_active = False
        self._extra_reply_count = 0

        self._grace_task: asyncio.Task | None = None
        self._safety_task: asyncio.Task | None = None
        self._closing_fallback_task: asyncio.Task | None = None
        self._shutdown_state: dict[str, bool] | None = None
        self._end_call_pending: dict[str, bool] | None = None
        self._task: Any | None = None

    def bind_state(
        self,
        shutdown_state: dict[str, bool],
        end_call_pending: dict[str, bool] | None = None,
    ) -> None:
        self._shutdown_state = shutdown_state
        self._end_call_pending = end_call_pending

    def bind_shutdown_state(self, shutdown_state: dict[str, bool]) -> None:
        self._shutdown_state = shutdown_state

    def bind_task(self, task: Any) -> None:
        self._task = task

    @property
    def is_ending(self) -> bool:
        return self._requested

    @property
    def is_closing_in_progress(self) -> bool:
        return self._closing_in_progress

    def request_ending(self) -> None:
        if self._ended or self._requested:
            return
        self._requested = True
        self._cancel_task("_safety_task")
        self._cancel_task("_closing_fallback_task")

        # If the bot is already streaming/speaking the closing line for this turn,
        # transition immediately to closing_in_progress so we wait for it to finish naturally.
        if self._current_audio_active:
            self._awaiting_closing = False
            self._closing_in_progress = True
            logger.info(
                "[{}] Call ending requested (closing line already actively playing)",
                self._stream_id,
            )
        else:
            self._awaiting_closing = True
            self._closing_in_progress = False
            self._closing_fallback_task = asyncio.create_task(
                self._run_closing_fallback_timeout()
            )
            logger.info(
                "[{}] Call ending requested; waiting for closing line",
                self._stream_id,
            )

        self._in_grace = False
        self._safety_task = asyncio.create_task(
            self._run_safety_timeout()
        )

    def cancel_ending(self, reason: str) -> None:
        if not self._requested:
            return

        logger.info(
            "[{}] Call ending cancelled ({})",
            self._stream_id,
            reason,
        )

        self._requested = False
        self._dedicated_goodbye_queued = False
        self._awaiting_closing = False
        self._closing_in_progress = False
        self._in_grace = False

        if self._shutdown_state is not None:
            self._shutdown_state["active"] = False
        if self._end_call_pending is not None:
            self._end_call_pending["active"] = False

        self._cancel_task("_grace_task")
        self._cancel_task("_safety_task")
        self._cancel_task("_closing_fallback_task")

    def _cancel_task(self, attr: str) -> None:
        task = getattr(self, attr)

        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()

        setattr(self, attr, None)

    async def _run_safety_timeout(self) -> None:
        try:
            await asyncio.sleep(self._safety_seconds)

        except asyncio.CancelledError:
            return

        logger.warning(
            "[{}] Closing statement timed out ({}s); forcing hangup",
            self._stream_id,
            self._safety_seconds,
        )

        await self._finish()

    async def _run_closing_fallback_timeout(self) -> None:
        """NEW: guarantees the caller hears *something* before the call
        ends, even on a completion that invoked end_call with no
        accompanying spoken text (see __init__'s comment for the exact
        production case this fixes).

        Deliberately checks _awaiting_closing, not just _ended: if the
        model's own closing line already started playing (TTSStartedFrame
        already flipped _awaiting_closing to False -- see process_frame
        below), this is a silent no-op, so the normal/working case never
        hears an extra line. Also checks _ended in case the safety timeout
        somehow won the race and already tore things down.
        """
        try:
            await asyncio.sleep(self._fallback_seconds)
        except asyncio.CancelledError:
            return

        if self._ended or not self._awaiting_closing:
            return

        logger.warning(
            "[{}] No closing line started within {}s of end_call; "
            "speaking a fallback closing line instead of ending in "
            "silence",
            self._stream_id,
            self._fallback_seconds,
        )

        if self._task and not self._ended and self._awaiting_closing:
            try:
                await self._task.queue_frames([
                    TTSSpeakFrame(
                        text=self._fallback_closing_text,
                        append_to_context=False,
                    )
                ])
            except Exception as e:
                logger.error("[{}] Failed to queue fallback closing line: {}", self._stream_id, e)

    async def _wait_late_transcript(self) -> None:
        try:
            await asyncio.sleep(min(8.0, self._safety_seconds))
            await self._finish()
        except asyncio.CancelledError:
            return

    async def _run_grace_timeout(self) -> None:
        try:
            await asyncio.sleep(self._grace_seconds)

        except asyncio.CancelledError:
            return

        await self._finish()

    async def _finish(self) -> None:
        if self._ended:
            return

        self._ended = True

        self._cancel_task("_grace_task")
        self._cancel_task("_safety_task")
        self._cancel_task("_closing_fallback_task")

        await self._on_hangup()

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        await super().process_frame(
            frame,
            direction,
        )

        if (
            isinstance(frame, (InterruptionFrame, UserStartedSpeakingFrame))
            and (
                self._awaiting_closing
                or self._closing_in_progress
                or self._in_grace
            )
        ):
            # Speech that starts inside grace gets bounded time for final STT.
            # Only start once: noise/repeated VAD must not extend ending indefinitely.
            if self._in_grace and self._extra_reply_count < 1 and not getattr(self, '_late_speech_started', False):
                self._late_speech_started = True
                self._cancel_task('_grace_task')
                self._grace_task = asyncio.create_task(self._wait_late_transcript())

        if isinstance(frame, (TTSStartedFrame, TTSAudioRawFrame, BotStartedSpeakingFrame)):
            self._current_audio_active = True
            if self._awaiting_closing:
                self._awaiting_closing = False
                self._closing_in_progress = True
                self._cancel_task("_closing_fallback_task")

                logger.debug(
                    "[{}] Closing statement started speaking",
                    self._stream_id,
                )

        if isinstance(frame, BotStoppedSpeakingFrame):
            self._current_audio_active = False
            if self._closing_in_progress:
                self._closing_in_progress = False
                self._in_grace = True

                logger.info(
                    "[{}] Closing statement finished; {}s grace window open",
                    self._stream_id,
                    self._grace_seconds,
                )

                self._cancel_task("_grace_task")

                self._grace_task = asyncio.create_task(
                    self._run_grace_timeout()
                )

        # During grace window or closing, inspect transcription text.
        # If caller reciprocates farewell/signs off, complete the call cleanly without speaking again.
        # If caller says something meaningful/question/continuation, allow at most ONE extra reply without cancelling hangup.
        if (
            isinstance(frame, TranscriptionFrame)
            and (self._in_grace or self._closing_in_progress or self._awaiting_closing)
        ):
            text = (getattr(frame, "text", "") or "").strip().lower()
            if _is_farewell_or_acknowledgment(text):
                logger.info(
                    "[{}] Caller reciprocated farewell ({!r}); completing call ending without bot reply",
                    self._stream_id,
                    text,
                )
                frame.text = ""
                # No extra speech. Bound a VAD-started transcript allowance after an acknowledgment.
                if getattr(self, '_late_speech_started', False):
                    self._cancel_task('_grace_task')
                    self._grace_task = asyncio.create_task(self._run_grace_timeout())
                return
            elif genuine_late_question(text):
                # Late-barge-in contract: does NOT cancel the final hangup; allows at most ONE extra reply!
                if getattr(self, "_extra_reply_count", 0) < 1:
                    self._extra_reply_count = getattr(self, "_extra_reply_count", 0) + 1
                    logger.info(
                        "[{}] Caller spoke meaningfully during closing/grace ({!r}); allowing at most 1 extra reply (count={}) without cancelling hangup",
                        self._stream_id,
                        text,
                        self._extra_reply_count,
                    )
                    self._closing_in_progress = False
                    self._in_grace = False
                    self._awaiting_closing = True
                    self._cancel_task("_grace_task")
                    # Do NOT cancel self._safety_task - hard safety timer remains running as backstop!
                else:
                    logger.info(
                        "[{}] Caller spoke during closing/grace ({!r}) but extra reply budget exhausted; proceeding to hangup",
                        self._stream_id,
                        text,
                    )
            elif not text:
                if not self._grace_task or self._grace_task.done():
                    self._grace_task = asyncio.create_task(self._run_grace_timeout())

        await self.push_frame(
            frame,
            direction,
        )


class _HistoryPruner(FrameProcessor):
    """Keeps the LLM context bounded across long calls. Also optionally
    maintains a full, never-pruned transcript for post-call analysis --
    see full_transcript param."""

    def __init__(
        self,
        context: LLMContext,
        max_messages: int,
        stream_id: str,
        full_transcript: list[dict] | None = None,
        call_end_coordinator: _CallEndCoordinator | None = None,
        lead_memory: dict[str, str] | None = None,
    ) -> None:
        super().__init__()

        self._context = context
        self._max_messages = max_messages
        self._stream_id = stream_id
        self._full_transcript = full_transcript
        self._call_end_coordinator = call_end_coordinator
        self._lead_memory = lead_memory if lead_memory is not None else {}
        # Track recorded messages by object identity + strong reference to allow repeated words across turns
        self._recorded_ids: set[int] = set()
        self._recorded_refs: list[Any] = []
        self._last_pruned_len: int = 0

    def _record_full_transcript(self) -> None:
        if self._full_transcript is None:
            return
        msgs = getattr(self._context, "messages", None)
        if not isinstance(msgs, list):
            if hasattr(self._context, "get_messages") and callable(self._context.get_messages):
                res = self._context.get_messages()
                msgs = res if isinstance(res, list) else []
            else:
                msgs = []
        dialogue: list[dict[str, str]] = []
        for msg in msgs:
            if not isinstance(msg, dict):
                continue
            role = msg.get("role")
            content = msg.get("content")
            if isinstance(content, list):
                content = " ".join(part.get("text", "") for part in content if isinstance(part, dict))
            if not isinstance(content, str) or not content.strip() or role not in ("user", "assistant"):
                continue  # skip tool-call/tool-result/developer bookkeeping messages
            dialogue.append({"role": role, "content": content.strip()})

        if not dialogue:
            return

        if not self._full_transcript:
            self._full_transcript.extend(dialogue)
            logger.debug("[{}] Initialized full transcript ({} messages)", self._stream_id, len(self._full_transcript))
            return

        # Content-based deduplication: align dialogue slice to suffix of full_transcript
        # by matching (role, normalized_content) to prevent duplicate recordings across prune cycles
        norm = lambda item: (item.get("role"), " ".join(str(item.get("content", "")).lower().split()))
        m = min(len(self._full_transcript), len(dialogue))
        match_len = 0
        for k in range(m, 0, -1):
            if all(norm(self._full_transcript[-(k - i)]) == norm(dialogue[i]) for i in range(k)):
                match_len = k
                break

        new_items = dialogue[match_len:]
        if new_items:
            self._full_transcript.extend(new_items)
            logger.debug(
                "[{}] Appended {} new messages to full transcript (total {})",
                self._stream_id,
                len(new_items),
                len(self._full_transcript),
            )

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
    ) -> None:
        await super().process_frame(
            frame,
            direction,
        )

        if direction == FrameDirection.DOWNSTREAM:
            # Observability & CPU fix: only run pruning & transcript recording on turn boundary / LLM trigger frames!
            # 20ms AudioRawFrames (~50 per second) must NEVER trigger history pruning.
            if not isinstance(
                frame,
                (
                    LLMFullResponseEndFrame,
                    UserStoppedSpeakingFrame,
                    TranscriptionFrame,
                    LLMContextFrame,
                    FunctionCallResultFrame,
                    LLMRunFrame,
                ),
            ):
                await self.push_frame(frame, direction)
                return

            before = len(self._context.messages)

            # Capture BEFORE pruning -- this is the only point where
            # context.messages is guaranteed complete for everything added
            # since the last prune cycle.
            self._record_full_transcript()

            # Ensure in-call working memory reflects any captured preferences before pruning
            _sync_working_memory(self._context.messages, self._lead_memory, self._stream_id)

            # Late-call pruning: if call ending is requested, prune more aggressively (e.g. 10)
            # to minimize tokens and tail latency.
            target_max = self._max_messages
            if self._call_end_coordinator and self._call_end_coordinator.is_ending:
                target_max = min(target_max, 10)

            _prune_history(
                self._context.messages,
                target_max,
            )

            after = len(self._context.messages)
            self._last_pruned_len = after

            # Re-sync working memory after pruning so [ACTIVE LEAD STATE: ...] is always present
            _sync_working_memory(self._context.messages, self._lead_memory, self._stream_id)

            if after < before:
                logger.debug(
                    "[{}] Pruned conversation history {} -> {} messages",
                    self._stream_id,
                    before,
                    after,
                )

        await self.push_frame(
            frame,
            direction,
        )


async def _create_provider_services_in_parallel(
    config: dict,
    active: dict,
    *,
    sample_rate: int,
    aiohttp_session: aiohttp.ClientSession,
) -> tuple[Any, Any, Any, str]:
    """Instantiate STT, LLM, and TTS services in parallel with LLM startup fallback support.

    Returns (stt, llm, tts, llm_provider_actually_used).
    """
    stt_task = asyncio.to_thread(
        ServiceFactory.create,
        "stt",
        active["stt"],
        config,
        sample_rate=sample_rate,
    )

    async def _build_llm() -> tuple[Any, str]:
        primary = active["llm"]
        fallback = active.get("llm_fallback")
        try:
            llm = await asyncio.to_thread(
                ServiceFactory.create, "llm", primary, config
            )
            return llm, primary
        except Exception as e:
            if not fallback or fallback == primary:
                raise
            logger.error(
                "Primary LLM provider '{}' failed to initialize ({}); "
                "falling back to '{}' for this call. This call's cost "
                "profile differs from normal -- see cost_rates in "
                "config.yaml.",
                primary,
                e,
                fallback,
            )
            llm = await asyncio.to_thread(
                ServiceFactory.create, "llm", fallback, config
            )
            return llm, fallback

    tts_task = asyncio.to_thread(
        ServiceFactory.create,
        "tts",
        active["tts"],
        config,
        sample_rate=sample_rate,
        aiohttp_session=aiohttp_session,
    )

    stt, (llm, llm_provider_used), tts = await asyncio.gather(
        stt_task,
        _build_llm(),
        tts_task,
    )

    return stt, llm, tts, llm_provider_used


# Standby provider pool: holds a pre-instantiated (stt, llm, tts, provider_name)
# tuple ready for immediate acquisition on call connect (0ms setup latency).
_STANDBY_PROVIDERS: dict[int, asyncio.Queue] = {}


async def _replenish_standby(
    config: dict,
    active: dict,
    sample_rate: int,
    aiohttp_session: aiohttp.ClientSession | None = None,
) -> None:
    try:
        stt, llm, tts, llm_provider_used = await _create_provider_services_in_parallel(
            config,
            active,
            sample_rate=sample_rate,
            aiohttp_session=aiohttp_session,
        )
        queue = _STANDBY_PROVIDERS.setdefault(sample_rate, asyncio.Queue())
        await queue.put((stt, llm, tts, llm_provider_used))
        logger.debug("[{}] Standby provider services ready in pool", sample_rate)
    except Exception as e:
        logger.warning("Failed to replenish standby providers: {}", e)


async def get_provider_services(
    config: dict,
    active: dict,
    *,
    sample_rate: int,
    aiohttp_session: aiohttp.ClientSession | None = None,
) -> tuple[Any, Any, Any, str]:
    queue = _STANDBY_PROVIDERS.setdefault(sample_rate, asyncio.Queue())
    if not queue.empty():
        stt, llm, tts, llm_provider_used = queue.get_nowait()
        logger.info("[{}] Acquired pre-warmed standby providers (0ms latency)", sample_rate)
        asyncio.create_task(_replenish_standby(config, active, sample_rate, aiohttp_session))
        return stt, llm, tts, llm_provider_used

    return await _create_provider_services_in_parallel(
        config,
        active,
        sample_rate=sample_rate,
        aiohttp_session=aiohttp_session,
    )


async def execute_book_site_visit(
    params: Any,
    *,
    stream_id: str,
    call_metrics: Any = None,
    spoken_text_guard: Any = None,
    lead_memory: dict | None = None,
    call_type: str = "web",
    config: dict | None = None,
) -> None:
    """Execute site visit booking with strict date normalization, idempotency, and WhatsApp coordination."""
    tool_call_started_at = time.monotonic()
    args = getattr(params, "arguments", None) or {}
    date_raw = str(args.get("date", "")).strip()
    time_raw = str(args.get("time", "")).strip()
    logger.info(f"[{stream_id}] Tool book_site_visit called: date={date_raw!r}, time={time_raw!r}")

    # 1. Require explicit date AND time — no default fallbacks
    if not date_raw or not time_raw:
        missing = []
        if not date_raw:
            missing.append("date")
        if not time_raw:
            missing.append("time")
        logger.warning(
            f"[{stream_id}] book_site_visit missing required argument(s): {missing}"
        )
        if call_metrics and hasattr(call_metrics, "record_tool_call"):
            call_metrics.record_tool_call(
                "book_site_visit",
                success=False,
                latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
            )
        msg = (
            "Please confirm what time works best for the site visit."
            if "time" in missing and "date" not in missing
            else "Please confirm which day and time work best for the site visit."
        )
        await params.result_callback({
            "status": "needs_confirmation",
            "missing": missing,
            "message": msg,
        })
        return

    # 2. Normalize date and time before storage — fail closed on unparseable input
    try:
        import zoneinfo as _zi
        from leads.worker import normalize_visit_date, normalize_visit_time, _resolve_visit_datetime, format_spoken_date, format_spoken_time
        _now_ist = datetime.now(_zi.ZoneInfo("Asia/Kolkata"))
        date_result = normalize_visit_date(date_raw, _now_ist)
        norm_time = visit_time(time_raw)
        vague_times = {"morning", "afternoon", "evening", "night", "subah", "dopahar", "shaam", "raat", "anytime", "any time"}
        if time_raw.lower().strip() in vague_times:
            norm_time = None
    except Exception as _norm_exc:
        logger.warning(f"[{stream_id}] Normalization error: {_norm_exc}; treating as unparseable")
        date_result = None
        norm_time = None

    if date_result is None or norm_time is None:
        missing_part = "date" if date_result is None else "time"
        logger.warning(
            f"[{stream_id}] book_site_visit: unparseable {missing_part} (date={date_raw!r}, time={time_raw!r})"
        )
        if call_metrics and hasattr(call_metrics, "record_tool_call"):
            call_metrics.record_tool_call(
                "book_site_visit",
                success=False,
                latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
            )
        err_msg = (
            "Date is unclear — please confirm a specific day (e.g. Saturday, tomorrow, Monday)."
            if date_result is None
            else "Time is unclear — please confirm a specific time (e.g. 11:00 AM, 4:00 PM)."
        )
        await params.result_callback({
            "status": "needs_confirmation",
            "message": err_msg,
        })
        return

    iso_date, date_label = date_result
    active_lang = "en"
    if spoken_text_guard and hasattr(spoken_text_guard, "_language"):
        active_lang = getattr(spoken_text_guard, "_language", "en") or "en"
    spoken_date = format_spoken_date(iso_date, _now_ist, active_lang)
    spoken_time = format_spoken_time(norm_time, active_lang)
    logger.info(f"[{stream_id}] book_site_visit normalized: date={date_label!r} ({iso_date} -> {spoken_date!r}), time={norm_time!r} ({spoken_time!r})")

    try:
        from sqlalchemy import select
        from leads.db import get_session
        from leads.models import Lead, SiteVisit, Touchpoint
        from services.whatsapp_sender import send_whatsapp_location

        slot_dt = _resolve_visit_datetime(date_label, norm_time)

        async with get_session() as session:
            # 3. Enforce idempotency by call_id only
            stmt_idemp = select(SiteVisit).where(SiteVisit.call_id == stream_id)
            existing_sv = (await session.execute(stmt_idemp)).scalars().first()
            if existing_sv:
                logger.info(f"[{stream_id}] book_site_visit: idempotent replay detected for site_visit_id={existing_sv.id}")
                if call_metrics and hasattr(call_metrics, "record_tool_call"):
                    call_metrics.record_tool_call(
                        "book_site_visit",
                        success=True,
                        latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                    )
                stored_date = existing_sv.visit_date_iso or iso_date
                stored_time = existing_sv.time_slot or norm_time
                idemp_spoken_date = format_spoken_date(stored_date, _now_ist, active_lang)
                idemp_spoken_time = format_spoken_time(stored_time, active_lang)
                confirm_msg = f"Site visit confirmed for {idemp_spoken_date} at {idemp_spoken_time}."
                if (config or {}).get("whatsapp_after_call_only", False):
                    if lead_memory and 'location' not in (lead_memory.get('_postcall_whatsapp_actions') or []):
                        confirm_msg += " Want the location on WhatsApp after this call?"
                        lead_memory['_whatsapp_consent_action'] = 'location'
                elif existing_sv.whatsapp_status == "sent":
                    confirm_msg += " The location is on its way."
                elif existing_sv.whatsapp_status == "queued":
                    confirm_msg += " I'll WhatsApp you the location."
                elif not existing_sv.whatsapp_opt_in:
                    confirm_msg += " Want the location on WhatsApp after this call?"
                    if lead_memory is not None:
                        lead_memory["_whatsapp_consent_action"] = "location"

                wa_confirmed = existing_sv.whatsapp_status in ("sent", "queued")
                if spoken_text_guard and hasattr(spoken_text_guard, "mark_tool_succeeded"):
                    spoken_text_guard.mark_tool_succeeded("book_site_visit", whatsapp_confirmed=wa_confirmed, confirm_msg=confirm_msg)
                await params.result_callback(
                    {
                        "status": "confirmed",
                        "date": stored_date,
                        "spoken_date": idemp_spoken_date,
                        "time": stored_time,
                        "spoken_time": idemp_spoken_time,
                        "whatsapp_status": existing_sv.whatsapp_status,
                        "message": confirm_msg,
                    },
                    properties=FunctionCallResultProperties(run_llm=False),
                )
                return

            # 4. Check for lead ID or phone
            call_rec = (await lead_state.get_call_async(stream_id)) or {}
            lead_id_val = call_rec.get("lead_id") or (lead_memory.get("lead_id") if lead_memory else None)
            phone = call_rec.get("customer_phone") or call_rec.get("phone") or (lead_memory.get("phone") if lead_memory else None)
            lead_obj = None

            if lead_id_val:
                try:
                    lead_obj = await session.get(Lead, uuid.UUID(str(lead_id_val)))
                except Exception:
                    lead_obj = None

            if not lead_obj and phone:
                stmt_lead = select(Lead).where(Lead.phone == phone)
                lead_obj = (await session.execute(stmt_lead)).scalars().first()

            if not lead_obj:
                is_web_or_test = (
                    call_type == "web"
                    or call_rec.get("source") == "web-test"
                    or os.getenv("LOCAL_DEMO", "").lower() == "true"
                )
                if is_web_or_test:
                    test_phone = phone or "+919999999999"
                    lead_obj = Lead(
                        name=call_rec.get("customer_name") or (config.get("test_customer_name", "Alex") if config else "Alex"),
                        phone=test_phone,
                        status="visit_booked",
                        visit_genuine=True,
                        source="web-test",
                    )
                    session.add(lead_obj)
                    await session.flush()
                    logger.info(f"[{stream_id}] Created web-test Lead {lead_obj.id}")
                else:
                    logger.error(f"[{stream_id}] book_site_visit failed: missing phone on real call")
                    if call_metrics and hasattr(call_metrics, "record_tool_call"):
                        call_metrics.record_tool_call(
                            "book_site_visit",
                            success=False,
                            latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                        )
                    await params.result_callback({
                        "status": "needs_phone",
                        "message": "Please provide your phone number so we can confirm the booking.",
                    })
                    return

            # 5. WhatsApp opt-in and dispatch
            whatsapp_opt_in = not (config or {}).get("whatsapp_after_call_only", False) and bool(
                (lead_memory and lead_memory.get("whatsapp_opt_in"))
                or call_rec.get("whatsapp_opt_in")
            )
            wa_status = "not_requested"
            wa_msg_id = None

            if whatsapp_opt_in and lead_obj.phone:
                from leads.outbox import is_outbox_worker_running, queue_outbox_item
                if is_outbox_worker_running():
                    await queue_outbox_item(
                        lead_id=lead_obj.id,
                        target="whatsapp",
                        payload={
                            "phone": lead_obj.phone,
                            "visit_date_iso": iso_date,
                            "time_slot": norm_time,
                            "name": lead_obj.name,
                            "call_id": stream_id,
                        },
                        session=session,
                    )
                    wa_status = "queued"
                else:
                    try:
                        wa_res = await send_whatsapp_location(
                            to_phone=lead_obj.phone,
                            visit_date=iso_date,
                            visit_time=norm_time,
                            client_name=lead_obj.name or "Valued Customer",
                        )
                        wa_status = wa_res.get("status", "failed")
                        wa_msg_id = wa_res.get("message_id")
                    except Exception as wa_err:
                        logger.warning(f"[{stream_id}] WhatsApp dispatch exception: {wa_err}")
                        wa_status = "failed"

            # 6. Commit SiteVisit and Touchpoint to DB
            lead_obj.status = "visit_booked"
            lead_obj.visit_genuine = True

            sv = SiteVisit(
                lead_id=lead_obj.id,
                call_id=stream_id,
                visit_date_iso=iso_date,
                visit_date_original=date_raw,
                time_slot=norm_time,
                configuration=lead_memory.get("configuration") if lead_memory else None,
                slot_start=slot_dt,
                status="booked",
                whatsapp_opt_in=whatsapp_opt_in,
                whatsapp_opt_in_at=datetime.now(timezone.utc) if whatsapp_opt_in else None,
                whatsapp_status=wa_status,
                whatsapp_message_id=wa_msg_id,
                created_at=datetime.now(timezone.utc),
            )
            session.add(sv)

            tp = Touchpoint(
                lead_id=lead_obj.id,
                kind="site_visit",
                call_id=stream_id,
                summary=f"Site visit booked for {iso_date} at {norm_time}",
                occurred_at=datetime.now(timezone.utc),
            )
            session.add(tp)
            await session.commit()

            # 7. SELECT read-back verification
            read_stmt = select(SiteVisit).where(SiteVisit.id == sv.id)
            verified_sv = (await session.execute(read_stmt)).scalars().first()
            if not verified_sv:
                raise RuntimeError(f"Database read-back verification failed for SiteVisit id={sv.id}")
            logger.info(
                f"[{stream_id}] Verified SiteVisit in DB: id={verified_sv.id} call_id={verified_sv.call_id} date={verified_sv.visit_date_iso} slot={verified_sv.time_slot}"
            )

        # 8. Persist lead_state and lead_memory only after DB verification
        await lead_state.record_fields_async(
            stream_id,
            {
                "preferred_visit_date": iso_date,
                "preferred_visit_time": norm_time,
                "visit_date_iso": iso_date,
                "time_slot": norm_time,
                "site_visit": f"Confirmed ({iso_date} at {norm_time})",
                "disposition": "SITE_VISIT_BOOKED",
                "lead_id": str(lead_obj.id),
            },
        )
        if lead_memory is not None:
            lead_memory["preferred_visit_date"] = iso_date
            lead_memory["preferred_visit_time"] = norm_time
            lead_memory["visit_date_iso"] = iso_date
            lead_memory["time_slot"] = norm_time
            lead_memory["site_visit"] = f"Confirmed ({iso_date} at {norm_time})"
            lead_memory["disposition"] = "SITE_VISIT_BOOKED"
            lead_memory["lead_id"] = str(lead_obj.id)

        confirm_msg = f"Perfect, you're booked for {spoken_date} at {spoken_time}."
        if wa_status == "sent":
            confirm_msg += " The location is on its way."
        elif wa_status == "queued":
            confirm_msg += " I'll WhatsApp you the location."
        elif not whatsapp_opt_in:
            confirm_msg += " Want the location on WhatsApp after this call?"
            if lead_memory is not None:
                lead_memory["_whatsapp_consent_action"] = "location"

        wa_confirmed = wa_status in ("sent", "queued")
        if spoken_text_guard and hasattr(spoken_text_guard, "mark_tool_succeeded"):
            spoken_text_guard.mark_tool_succeeded("book_site_visit", whatsapp_confirmed=wa_confirmed, confirm_msg=confirm_msg)

        if call_metrics and hasattr(call_metrics, "record_tool_call"):
            call_metrics.record_tool_call(
                "book_site_visit",
                success=True,
                latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
            )

        await params.result_callback(
            {
                "status": "confirmed",
                "date": iso_date,
                "spoken_date": spoken_date,
                "time": norm_time,
                "spoken_time": spoken_time,
                "whatsapp_status": wa_status,
                "message": confirm_msg,
            },
            properties=FunctionCallResultProperties(run_llm=False),
        )
    except Exception as exc:
        logger.error(f"[{stream_id}] book_site_visit DB commit error: {exc}")
        if call_metrics and hasattr(call_metrics, "record_tool_call"):
            call_metrics.record_tool_call(
                "book_site_visit",
                success=False,
                latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
            )
        await params.result_callback({"status": "error", "message": "Failed to book slot."})


async def run_bot(
    websocket: WebSocket,
    call_type: str,
    config: dict,
    stream_id: str | None = None,
    campaign_data: dict | None = None,
    call_id: str | None = None,
):
    provider_call_id = (
        campaign_data.get("provider_call_id")
        if campaign_data
        else None
    )
    campaign_id = (
        campaign_data.get("campaign_id")
        if campaign_data
        else None
    )

    aiohttp_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            total=60,
            connect=10,
        ),
        connector=aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
        ),
    )

    active = config["active_providers"]

    sample_rate = (
        16000
        if call_type == "web"
        else config["audio"]["sample_rate"]
    )

    provider_construction_task = asyncio.create_task(
        get_provider_services(
            config,
            active,
            sample_rate=sample_rate,
            aiohttp_session=aiohttp_session,
        )
    )

    # Vobiz handshake.
    if not stream_id:
        try:
            for _ in range(5):
                message = await websocket.receive_text()
                data = json.loads(message)

                candidate_stream_id = (
                    data.get("streamId")
                    or data.get("start", {}).get("streamId")
                )

                candidate_call_id = (
                    data.get("callId")
                    or data.get("start", {}).get("callId")
                )

                if candidate_call_id:
                    provider_call_id = candidate_call_id

                if candidate_stream_id:
                    stream_id = candidate_stream_id
                    break

            if not stream_id:
                stream_id = "unknown"

        except Exception:
            logger.exception(
                "Failed to parse initial Vobiz messages"
            )

            stream_id = "unknown_error"

    call_label = call_id or stream_id or "no-call-id"

    if stream_id and provider_call_id:
        STREAM_PROVIDER_CALL_IDS[stream_id] = provider_call_id

    elif stream_id:
        provider_call_id = STREAM_PROVIDER_CALL_IDS.get(
            stream_id
        )

    conversation_call_type = (
        "outbound"
        if call_type == "web"
        else call_type
    )

    logger.info(
        "[{}] Starting transport={} conversation_type={} call_id={} providers={}",
        stream_id,
        call_type,
        conversation_call_type,
        call_label,
        config.get("active_providers", {}),
    )

    async def force_provider_hangup(
        trigger: str,
    ) -> None:

        if not provider_call_id:
            logger.warning(
                "[{}] Missing Vobiz call ID ({})",
                stream_id,
                trigger,
            )
            return

        auth_id = os.getenv("VOBIZ_AUTH_ID")
        auth_token = os.getenv("VOBIZ_AUTH_TOKEN")

        if not auth_id or not auth_token:
            logger.warning(
                "[{}] Missing Vobiz auth credentials ({})",
                stream_id,
                trigger,
            )
            return

        url = (
            f"https://api.vobiz.ai/api/v1/Account/"
            f"{auth_id}/Call/{provider_call_id}/"
        )

        max_retries = 2
        base_delay = 0.3

        for attempt in range(max_retries):
            try:
                response = await _vobiz_http_client.delete(
                    url,
                    headers={
                        "X-Auth-ID": auth_id,
                        "X-Auth-Token": auth_token,
                        "Content-Type": "application/json",
                    },
                )

                if response.status_code in {
                    200,
                    201,
                    202,
                    204,
                }:
                    logger.info(
                        "[{}] Vobiz hangup succeeded on attempt {} ({})",
                        stream_id,
                        attempt + 1,
                        trigger,
                    )
                    return

                logger.warning(
                    "[{}] Vobiz hangup failed status={} on attempt {} ({})",
                    stream_id,
                    response.status_code,
                    attempt + 1,
                    trigger,
                )

            except Exception as e:
                logger.warning(
                    "[{}] Vobiz hangup request error on attempt {} ({}): {}",
                    stream_id,
                    attempt + 1,
                    trigger,
                    e,
                )

            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)

                logger.info(
                    "[{}] Retrying Vobiz hangup in {}s...",
                    stream_id,
                    delay,
                )

                await asyncio.sleep(delay)

        logger.error(
            "[{}] Vobiz hangup FAILED after {} attempts ({}). "
            "PSTN call may still be connected and billing!",
            stream_id,
            max_retries,
            trigger,
        )

    # NEW: hoisted above the try block (was previously defined deep inside
    # it) so it's guaranteed to exist for the finally-block safety net
    # below, even if setup fails before reaching that point.
    hangup_state = {
        "done": False,
    }

    async def _force_hangup_and_mark_done(trigger: str) -> None:
        """
        force_provider_hangup wrapper that also marks hangup_state["done"].

        _TerminationProcessor and hard_timeout() both call the hangup
        function directly rather than through _perform_end_of_call_hangup,
        so hangup_state never got set on those paths -- meaning a
        perfectly normal, successful call end still tripped the
        finally-block safety net's warning and fired a redundant second
        hangup attempt. This wrapper closes that gap without touching
        either class's working cancellation flow (EndTaskFrame push /
        task.cancel() timing stays exactly as it was).

        FIXED: this used to only call force_provider_hangup, which hangs up
        the PSTN leg via Vobiz's API and correctly no-ops when there's no
        provider_call_id (e.g. every local web-test call) -- but it never
        cancelled the pipeline task itself. Confirmed against a real log:
        StallWatchdog/ProviderErrorMonitor called this, logged "judged
        unrecoverable", and the call kept running (and kept failing) for
        another ~16s until the caller manually disconnected, because
        nothing here ever stopped the PipelineTask. _perform_end_of_call_hangup
        below -- the proven-working path the normal end_call flow already
        uses -- always does force_provider_hangup THEN task.cancel(); this
        now matches that same two-step pattern instead of only doing half
        of it.

        NEW: guard against firing at all once a hangup has ALREADY been
        performed via a *different* path. This function is the shared exit
        point for five independent callers -- ProviderErrorMonitor,
        _TerminationProcessor's own safety timer, _SilenceChecker,
        StallWatchdog, and hard_timeout() -- and in production more than
        one of them can legitimately observe "this call needs to end" for
        the same call within seconds of each other (e.g. a stall detected
        by StallWatchdog right as _CallEndCoordinator's own closing-
        statement safety timer is about to expire -- this is exactly what
        the "Provider stall detected" followed ~3.5s later by "Closing
        statement timed out ... forcing hangup" log pair was: two
        independent safety nets, neither aware the other had already
        fired). Each of those callers already guards against re-firing
        ITSELF, but none of them knew about a hangup that happened
        through one of the *other* four -- so a second, fully independent
        force_provider_hangup() call (with its own up-to-three-attempt
        retry/backoff loop) and a second task.cancel() would run for a
        call that was already over, producing exactly the "forcing
        hangup" log line firing twice and the multi-second tail delay
        reported in production. Checking the single shared hangup_state
        flag here -- once, centrally -- makes every caller safe without
        duplicating the check at each call site.
        """
        if hangup_state["done"]:
            logger.debug(
                "[{}] Hangup already completed via another path; "
                "ignoring redundant '{}' hangup request",
                stream_id,
                trigger,
            )
            return

        hangup_state["done"] = True
        if call_type != "web":
            await force_provider_hangup(trigger)
        elif websocket:
            try:
                await websocket.close(code=1000, reason="Call completed")
            except Exception as exc:
                logger.debug("[{}] Web close notice: {}", stream_id, exc)
        # Cancel pipeline task upon call completion
        await task.cancel()

    log_handler = logger.add(
        f"logs/call_{call_label}_{stream_id}.log",
        level="DEBUG",
    )

    call_metrics = None

    try:
        # Optional input-side noise suppression filter
        audio_in_filter = _build_audio_in_filter(config, stream_id, call_type=call_type)

        serializer = (
            WebPCMFrameSerializer(
                stream_id=stream_id,
            )
            if call_type == "web"
            else VobizFrameSerializer(
                stream_id=stream_id,
                sample_rate=8000,
            )
        )

        # VAD strategy: Sarvam STT uses server-side VAD; others use Silero VAD Processor
        is_sarvam_stt = (active.get("stt") == "sarvam")
        vad_config = config.get("vad", {})
        fallback_vad_processor = None
        if not is_sarvam_stt and vad_config.get("enabled", True):
            try:
                from pipecat.audio.vad.silero import SileroVADAnalyzer
                from pipecat.audio.vad.vad_analyzer import VADParams
                silero_vad = SileroVADAnalyzer(
                    params=VADParams(
                        confidence=float(vad_config.get("start_threshold", 0.55)),
                        start_secs=float(vad_config.get("min_speech_duration", 0.20)),
                        stop_secs=float(vad_config.get("silence_timeout", 0.20)),
                    )
                )
                fallback_vad_processor = VADProcessor(vad_analyzer=silero_vad)
                logger.info("[{}] Fallback Silero VADProcessor configured for non-Sarvam provider ({})", stream_id, active.get("stt"))
            except Exception as e:
                logger.warning("[{}] Could not instantiate fallback Silero VADProcessor: {}", stream_id, e)

        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_passthrough=True,
                add_wav_header=False,
                audio_in_filter=audio_in_filter,
                serializer=serializer,
            ),
        )

        # Wait for provider construction (warmup runs in background, not blocking).
        stt, llm, tts, llm_provider_used = await provider_construction_task

        call_metrics = CallMetricsCollector(
            call_id=stream_id,
            stt_provider=active.get("stt", "sarvam"),
            llm_provider=llm_provider_used,
            tts_provider=active.get("tts", "sarvam"),
            language=config.get("language", {}).get("initial", "en"),
            cost_rates=config.get("cost_rates", {}),
        )
        setattr(llm, "_call_metrics", call_metrics)

        # Attach error monitors to services to detect repeated unrecoverable failures
        provider_error_monitor = ProviderErrorMonitor(
            stream_id=stream_id,
            on_unrecoverable=lambda sid: _force_hangup_and_mark_done(
                f"provider_error:{sid}"
            ),
        )
        provider_error_monitor.attach(stt)
        provider_error_monitor.attach(llm)
        provider_error_monitor.attach(tts)

        # Build system prompt early to allow cache pre-warming during greeting
        system_prompt, customer_context = build_system_prompt(
            conversation_call_type,
            config,
            campaign_data,
        )

        # Strip dynamic timestamp suffix to preserve prefix cache eligibility across turns
        system_prompt = _strip_dynamic_time_suffix(system_prompt)

        # Retrieve LLM tool definitions from provider config
        llm_tools_raw = (
            config.get("providers", {})
            .get("llm", {})
            .get(llm_provider_used, {})
            .get("params", {})
            .get("tools", [])
        )

        groq_rate_limited = {"active": False}

        async def _warm_llm_context_cache() -> None:
            """
            Ultra-fast in-service pre-warm of the active LLM provider.
            Calls `llm.run_inference()` out-of-band directly on the active service instance:
            1. Pre-warms the persistent TCP/TLS keep-alive connection pool of `llm._client`,
               eliminating 450-600ms of cold network handshakes on Turn 1.
            2. Primes the provider's server-side prefix cache (Cerebras / Groq / OpenAI).
            3. Universal & provider-agnostic: works cleanly across Cerebras, Groq, OpenRouter,
               and OpenAI without requiring provider-specific branching or throwaway clients.
            4. Bounded by a strict 2.5s timeout; runs safely in background during greeting playback.
            """
            try:
                # Pre-warm TLS connection and prime provider's prefix cache with system prompt
                warm_ctx = LLMContext(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": "Hello"},
                    ]
                )
                prewarm_timeout = float(config.get("llm_prewarm_timeout", 4.0))
                await asyncio.wait_for(
                    llm.run_inference(warm_ctx, max_tokens=1),
                    timeout=prewarm_timeout,
                )
                logger.info(
                    "[{}] Active LLM service connection and prompt pre-warmed for provider={}",
                    stream_id,
                    llm_provider_used,
                )
            except asyncio.TimeoutError:
                logger.debug(
                    "[{}] LLM context pre-warm timed out for {} (non-blocking, proceeding)",
                    stream_id,
                    llm_provider_used,
                )
            except Exception as e:
                logger.debug(
                    "[{}] LLM context pre-warm skipped for {}: {}",
                    stream_id,
                    llm_provider_used,
                    e,
                )

        # Fire and forget -- runs concurrently with greeting playback,
        # so the socket and prefix cache are hot before caller finishes their first turn.
        if config.get("llm_cache_prewarm_enabled", False):
            asyncio.create_task(_warm_llm_context_cache())

        async def _fast_side_channel_reply(
            *,
            instruction: str,
            max_tokens: int,
            timeout_seconds: float,
            fallback_text: str,
        ) -> str:
            """Generate a short reply directly against the active LLM's
            chat-completions endpoint, bypassing the pipeline entirely,
            bounded by timeout_seconds.

            WHY THIS EXISTS: two turns in this file used to be routed
            through the exact same context_aggregator -> llm -> tts path as
            every ordinary turn: speaking the closing line once end_call is
            accepted, and replying after a rejected end_call. Both inherited
            that path's full worst-case latency with no ceiling on it --
            production logs show 8-11.67s completions on exactly these
            turns, once totalling 25s+ of dead air after "See you tomorrow
            at three" before the stall watchdog force-ended the call. This
            talks to whichever provider is currently active_providers.llm
            (groq, sarvam, or cerebras -- all OpenAI-compatible, read
            generically from config, so this keeps working unchanged across
            the planned Sarvam A/B test) directly, with its own short
            timeout. A slow completion degrades to fallback_text instead of
            silence; it never blocks on the model.

            This still asks the model for a real, context-aware line (the
            recent turns + an instruction) rather than only ever speaking a
            fixed string -- fallback_text is a last-resort safety net for
            when even a bounded call fails, not the normal path.
            """
            provider_name = active["llm"]
            provider_cfg = (
                config.get("providers", {})
                .get("llm", {})
                .get(provider_name, {})
            )
            provider_params = provider_cfg.get("params", {})
            model = provider_params.get("model")
            api_key = os.getenv(provider_cfg.get("api_key_env", ""))
            # chat_base_url/chat_api_key_header are read only here, never by
            # ServiceFactory -- see the comments next to them in config.yaml
            # for why they're siblings of `params`, not inside it.
            base_url = provider_cfg.get("chat_base_url") or provider_params.get(
                "base_url"
            )

            if not (model and api_key and base_url):
                logger.warning(
                    "[{}] Fast side-channel reply: missing model/api_key/"
                    "base_url for provider={}; using fallback text",
                    stream_id,
                    provider_name,
                )
                return fallback_text

            # Circuit breaker: skip request if provider was already rate-limited
            if provider_name == "groq" and groq_rate_limited["active"]:
                logger.info(
                    "[{}] Fast side-channel reply: skipping Groq request "
                    "(rate limit already seen this call); using fallback "
                    "text",
                    stream_id,
                )
                return fallback_text

            recent_messages = [
                m for m in messages
                if m.get("role") in ("user", "assistant")
                and not m.get("tool_calls")  # exclude tool-call messages
            ][-6:]

            request_kwargs: dict[str, Any] = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": f"{system_prompt}\n\n{instruction}",
                    },
                    *recent_messages,
                ],
                "max_tokens": max_tokens,
                "temperature": provider_params.get("temperature", 0.4),
            }

            # Non-reasoning configuration for Groq chat completions
            if provider_name == "groq":
                request_kwargs["reasoning_effort"] = "none"

            default_headers = None
            header_name = provider_cfg.get("chat_api_key_header")

            if header_name:
                default_headers = {header_name: api_key}

            try:
                # max_retries=0 prevents internal sleep loops from consuming turn timeout budget
                async with AsyncOpenAI(
                    api_key=api_key,
                    base_url=base_url,
                    default_headers=default_headers,
                    max_retries=0,
                ) as client:
                    response = await asyncio.wait_for(
                        client.chat.completions.create(**request_kwargs),
                        timeout=timeout_seconds,
                    )

                text = (response.choices[0].message.content or "").strip()
                return text or fallback_text

            # Trip circuit breaker on rate limit errors
            except RateLimitError:
                if provider_name == "groq":
                    groq_rate_limited["active"] = True

                logger.warning(
                    "[{}] Fast side-channel reply hit a rate limit "
                    "(429) from provider={}; using fallback text",
                    stream_id,
                    provider_name,
                )
                return fallback_text

            except Exception:
                logger.opt(exception=True).warning(
                    "[{}] Fast side-channel reply timed out or failed "
                    "(provider={}); using fallback text",
                    stream_id,
                    provider_name,
                )
                return fallback_text

        # Record initial call record in persistent store
        await lead_state.upsert_call_async(stream_id, campaign_id=campaign_id)

        supported_langs = config.get("language", {}).get("supported", ["en", "hi", "te"])
        language_state = LanguageState(
            sustained_switch_turns=config.get(
                "language",
                {},
            ).get(
                "sustained_switch_turns",
                2,
            ),
            supported_languages=frozenset(supported_langs),
        )
        logger.info(
            "[{}] Effective language config: supported={} sustained_switch_turns={}",
            stream_id,
            list(supported_langs),
            language_state.sustained_switch_turns,
        )

        shutdown_state = {
            "active": False,
        }

        end_call_pending = {
            "active": False,
        }

        lead_memory: dict[str, str] = {}
        setattr(llm, "_lead_memory", lead_memory)
        if campaign_data and campaign_data.get("customer_name"):
            cust_name = str(campaign_data["customer_name"]).strip()
            lead_memory["client"] = cust_name
            lead_memory["registered_name"] = cust_name

        async def _perform_end_of_call_hangup() -> None:
            if hangup_state["done"]:
                return

            hangup_state["done"] = True
            shutdown_state["active"] = True

            logger.info(
                "[{}] Ending call now (closing statement completed)",
                stream_id,
            )

            silence_checker.stop()
            if call_type != "web":
                await force_provider_hangup("call_end_coordinator")

            # Farewell drain order on web calls: allow browser audio buffer to drain (0.8s) before closing websocket
            if call_type == "web" and websocket:
                try:
                    await asyncio.sleep(0.8)
                    if hasattr(websocket, "client_state") and websocket.client_state.name == "CONNECTED":
                        await websocket.close(code=1000, reason="Call completed normally")
                except Exception as _ws_e:
                    logger.debug("[{}] Web socket close notice: {}", stream_id, _ws_e)

            await task.cancel()

        rejection_attempts = {"count": 0}

        grace_seconds = float(config.get("hangup_grace_seconds", 3.0))
        logger.info("[{}] Resolved hangup grace_seconds={:.2f}s", stream_id, grace_seconds)

        termination_processor = _TerminationProcessor(
            stream_id=stream_id,
            on_hangup=_perform_end_of_call_hangup,
            # Mark hangup_state before provider hangup to prevent redundant triggers
            force_hangup_fn=_force_hangup_and_mark_done,
            grace_seconds=grace_seconds,
            safety_seconds=15.0,
        )

        termination_processor.bind_state(
            shutdown_state=shutdown_state,
            end_call_pending=end_call_pending,
            rejection_attempts=rejection_attempts,
        )

        call_end_coordinator = _CallEndCoordinator(
            stream_id=stream_id,
            on_hangup=_perform_end_of_call_hangup,
            grace_seconds=grace_seconds,
            safety_seconds=15.0,
            fallback_seconds=2.5,
            fallback_closing_text=config.get(
                "fallback_closing_text",
                "Thank you for your time today. Have a great day!",
            ),
        )
        call_end_coordinator.bind_state(
            shutdown_state=shutdown_state,
            end_call_pending=end_call_pending,
        )

        interruption_audio_gate = _InterruptionAudioGate(
            stream_id=stream_id,
            on_interruption=(
                serializer.on_interruption
                if hasattr(
                    serializer,
                    "on_interruption",
                )
                else None
            ),
            metrics_collector=call_metrics,
            llm=llm,
            tts=tts,
            websocket=websocket,
            call_end_coordinator=call_end_coordinator,
        )

        def _latest_user_utterance() -> str:
            for message in reversed(messages):
                if message.get("role") == "user":
                    content = message.get(
                        "content",
                        "",
                    )

                    return (
                        content.strip()
                        if isinstance(content, str)
                        else ""
                    )

            return ""

        termination_processor.bind_state(
            shutdown_state=shutdown_state,
            end_call_pending=end_call_pending,
            get_latest_user_utterance=_latest_user_utterance,
            call_end_coordinator=call_end_coordinator,
            lead_memory=lead_memory,
            rejection_attempts=rejection_attempts,
        )

        async def end_call(params: FunctionCallParams):
            tool_call_started_at = time.monotonic()
            latest_user_text = _latest_user_utterance()
            reason = params.arguments.get("reason", "conversation_ended")

            # Deterministic intent checks:
            lower_text = latest_user_text.lower()
            is_explicit_end = _caller_explicitly_ended(latest_user_text)
            is_soft_close_phrase = _is_soft_close(latest_user_text)

            # True objection/refusal (e.g. "not interested", "nahi chahiye", "don't want", "busy", "call later", "ai", "robot")
            has_objection_phrase = _is_objection_utterance(lower_text) or any(w in lower_text for w in [
                "busy", "call later", "no time", "ai", "robot"
            ]) or any(w in str(reason).lower() for w in ["declin", "not_interested", "reject"])

            # If caller expressed an objection, it takes precedence over "bye" on first 2 objection attempts
            is_objection = has_objection_phrase and not pure_farewell(latest_user_text) and rejection_attempts["count"] <= 2
            is_clean_close = not is_objection and (is_explicit_end or is_soft_close_phrase)

            has_question = any(q in lower_text for q in [
                "amenit", "price", "cost", "budget", "bhk", "where", "location", "what", "how", "tell me", "kya", "kitna", "?"
            ])
            has_agreement = any(a in lower_text for a in [
                "yeah", "yes", "sure", "tell", "listen", "batao", "ha", "haa", "haan", "suno", "okay", "ok"
            ])

            # 1. Objection handling: convince slightly on true objections before concluding
            if is_objection:
                rejection_attempts["count"] += 1
                if rejection_attempts["count"] <= 2:
                    termination_processor.cancel_pending_termination("objection convincing in progress")
                    logger.warning(
                        "[{}] Objection handling active (attempt {}/2): vetoing end_call on reason={} utterance={!r}",
                        stream_id,
                        rejection_attempts["count"],
                        reason,
                        latest_user_text,
                    )
                    await params.result_callback(
                        {
                            "status": "ignored",
                            "message": (
                                f"Do NOT hang up yet! (Convincing attempt {rejection_attempts['count']}/2). "
                                "Ask one natural question about their stated concern; do not reintroduce yourself. "
                                "Probe if Prime Tech Corridor location or price is their concern. "
                                "Keep them engaged!"
                            ),
                        },
                        properties=FunctionCallResultProperties(run_llm=True),
                    )
                    return

            # 2. Premature end_call veto: NEVER end if caller asked a question or agreed to listen, UNLESS it's a clean close
            if not is_clean_close and (has_question or has_agreement) and not call_end_coordinator.is_closing_in_progress:
                termination_processor.cancel_pending_termination("caller asked question / expressed interest")
                logger.warning(
                    "[{}] Premature end_call vetoed (caller did not end call or expressed interest): reason={} utterance={!r}",
                    stream_id,
                    reason,
                    latest_user_text,
                )
                await params.result_callback(
                    {
                        "status": "ignored",
                        "message": (
                            "Do NOT end the call! The caller is engaged and has NOT ended the call. "
                            "Pitch the property details or answer naturally. Real sales reps do not hang up on interested leads."
                        ),
                    },
                    properties=FunctionCallResultProperties(run_llm=True),
                )
                return

            logger.info(
                "[{}] end_call executed cleanly: reason={} utterance={!r}",
                stream_id,
                reason,
                latest_user_text,
            )

            # Request call ending through coordinator: ensures closing speech finishes before hangup
            await queue_goodbye(call_end_coordinator, task)

            # Return success to Pipecat with run_llm=False so the model DOES NOT generate again!
            await params.result_callback(
                {"status": "ok", "reason": reason},
                properties=FunctionCallResultProperties(run_llm=False),
            )

            call_metrics.record_tool_call(
                "end_call",
                success=True,
                latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
            )

        async def update_lead_info(params) -> None:
            """Tool handler for recording lead fields into persistent storage."""
            tool_call_started_at = time.monotonic()
            try:
                field = params.arguments.get("field")
                value = params.arguments.get("value")
                merged = await lead_state.record_field_async(stream_id, field, value)
                call_metrics.record_tool_call(
                    "update_lead_info",
                    success=bool(merged),
                    latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                )
                await params.result_callback(
                    {"status": "recorded" if merged else "ignored_unknown_field"}
                )
            except Exception:
                call_metrics.record_tool_call(
                    "update_lead_info",
                    success=False,
                    latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                )
                await params.result_callback({"status": "error"})

        async def book_site_visit(params) -> None:
            """Model arguments alone do not establish a caller-selected slot."""
            if not caller_slot_matches(context.messages, params.arguments or {}):
                await params.result_callback({"status": "needs_confirmation", "message": "Ask only for the missing day or time. Do not invent a slot."}, properties=FunctionCallResultProperties(run_llm=False))
                return
            await execute_book_site_visit(
                params,
                stream_id=stream_id,
                call_metrics=call_metrics,
                spoken_text_guard=spoken_text_guard,
                lead_memory=lead_memory,
                call_type=call_type,
                config=config,
            )

        async def send_brochure(params) -> None:
            if config.get('whatsapp_after_call_only', False) and 'brochure' in (lead_memory.get('_postcall_whatsapp_actions') or []):
                await params.result_callback({'status':'prepared_not_sent'}, properties=FunctionCallResultProperties(run_llm=False))
                return
            decision = whatsapp_answer(context.messages, "brochure" if lead_memory.get("_brochure_consent_pending") else None) or brochure_decision(context.messages, bool(lead_memory.get("_brochure_consent_pending")))
            if decision == "not_requested":
                # An unsolicited tool call must not turn goodbye/unrelated text into another pitch.
                await params.result_callback({'status':'not_requested'}, properties=FunctionCallResultProperties(run_llm=False))
                return
            if decision == "declined":
                lead_memory.pop("_brochure_consent_pending", None)
                await params.result_callback({"status": "declined", "message": "No send. Continue with the caller's question."}, properties=FunctionCallResultProperties(run_llm=True))
                return
            if decision == "needs_consent":
                lead_memory["_brochure_consent_pending"] = True
                lead_memory["_whatsapp_consent_action"] = "brochure"
                await finish_brochure(params, {"status": "needs_consent"}, task, spoken_text_guard)
                return
            lead_memory.pop("_brochure_consent_pending", None)
            lead_memory.pop("_whatsapp_consent_action", None)
            lead_memory["whatsapp_opt_in"] = True
            if config.get("whatsapp_after_call_only", False):
                line = record_manual_whatsapp(lead_memory, config, 'brochure')
                await params.result_callback({'status':'prepared_not_sent'}, properties=FunctionCallResultProperties(run_llm=False))
                if line:
                    frame = TTSSpeakFrame(text=line, append_to_context=True)
                    frame.is_deterministic_confirmation = True
                    await task.queue_frames([frame])
                return
            if not whatsapp_ready():
                await finish_brochure(params, {"status":"not_configured"}, task, spoken_text_guard)
                return
            started = time.monotonic()
            result = {"status": "failed", "error": "Unable to queue brochure"}
            try:
                from sqlalchemy import select
                from leads.db import get_session
                from leads.models import Lead, SiteVisit
                from leads.outbox import queue_outbox_item
                rec = (await lead_state.get_call_async(stream_id)) or {}
                fields = json.loads(rec.get("lead_fields") or "{}")
                consent = bool(lead_memory.get("whatsapp_opt_in") or fields.get("whatsapp_opt_in") or rec.get("whatsapp_opt_in"))
                async with get_session() as session:
                    lid = rec.get("lead_id") or fields.get("lead_id")
                    lead = await session.get(Lead, uuid.UUID(lid)) if lid else None
                    phone = (rec.get("customer_phone") or rec.get("phone") or lead_memory.get("phone") or (lead.phone if lead else ""))
                    if call_type == "web":
                        phone = os.getenv("WHATSAPP_TEST_RECIPIENT", "")
                    payload, error = brochure_payload(stream_id, phone,
                        lead.name if lead else rec.get("customer_name"),
                        lead_memory.get("visit_date_iso") or fields.get("visit_date_iso"),
                        lead_memory.get("time_slot") or fields.get("time_slot"), config, consent)
                    if error:
                        result = {"status": error}
                    elif not lead:
                        result = {"status": "needs_lead"}
                    else:
                        item = await queue_outbox_item(lead.id, "whatsapp", payload, session=session)
                        await session.flush()
                        result = {"status": "queued", "outbox_id": str(item.id)}
                        if item.status == "done" and (item.payload or {}).get("message_id"):
                            result = {"status": "sent", "message_id": item.payload["message_id"]}
                        elif item.status == "failed":
                            result = {"status": "failed", "error": item.last_error, "error_code": 131031 if "131031" in (item.last_error or "") else None}
                        if consent:
                            lead.consent_at = lead.consent_at or datetime.now(timezone.utc)
                        await session.commit()
                await lead_state.record_fields_async(stream_id, {"whatsapp": result["status"]})
            except Exception as exc:
                logger.warning("[{}] Brochure queue failed: {}", stream_id, exc)
            call_metrics.record_tool_call("send_brochure", success=result["status"] in ("queued", "sent"),
                latency_ms=(time.monotonic() - started) * 1000)
            await finish_brochure(params, result, task, spoken_text_guard)

        async def handoff_to_human(params) -> None:
            """Tool handler for human advisor escalation."""
            tool_call_started_at = time.monotonic()
            args = params.arguments or {}
            reason = str(args.get("reason", "Caller requested senior human agent"))
            logger.info(f"[{stream_id}] Tool handoff_to_human called: reason={reason}")

            try:
                await lead_state.record_fields_async(
                    stream_id,
                    {
                        "disposition": "ESCALATED_TO_HUMAN",
                    },
                )

                call_rec = (await lead_state.get_call_async(stream_id)) or {}
                lead_id_val = call_rec.get("lead_id")

                if lead_id_val:
                    try:
                        from leads.db import get_session
                        from leads.models import Lead, Touchpoint
                        from leads.notify import send_telegram_alert
                        async with get_session() as session:
                            lead_obj = await session.get(Lead, uuid.UUID(lead_id_val))
                            if lead_obj:
                                lead_obj.tier = "hot"
                                lead_obj.score = max(lead_obj.score, 80)
                                lead_obj.score_reason = f"Human handoff: {reason}"
                                tp = Touchpoint(
                                    lead_id=lead_obj.id,
                                    kind="human_handoff",
                                    call_id=stream_id,
                                    summary=f"Escalated to human advisor: {reason}",
                                    occurred_at=datetime.now(timezone.utc),
                                )
                                session.add(tp)
                                await send_telegram_alert(
                                    lead_id=str(lead_obj.id),
                                    name=lead_obj.name,
                                    phone=lead_obj.phone,
                                    score_reason=f"Human handoff ({reason})",
                                    visit_info="N/A",
                                    summary=f"Caller requested human escalation. Reason: {reason}",
                                )
                    except Exception as exc:
                        logger.warning(f"[{stream_id}] handoff_to_human DB sync warning: {exc}")

                call_metrics.record_tool_call(
                    "handoff_to_human",
                    success=True,
                    latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                )
                await params.result_callback(
                    {"status": "escalated", "message": "Lead flagged for immediate priority human callback."}
                )
            except Exception as exc:
                logger.error(f"[{stream_id}] handoff_to_human error: {exc}")
                call_metrics.record_tool_call(
                    "handoff_to_human",
                    success=False,
                    latency_ms=(time.monotonic() - tool_call_started_at) * 1000,
                )
                await params.result_callback({"status": "error", "message": "Handoff registered."})

        # Map handlers for FunctionSchema definitions
        _tool_handlers = {
            "end_call": end_call,
            "update_lead_info": update_lead_info,
            "book_site_visit": book_site_visit,
            "send_brochure": send_brochure,
            "handoff_to_human": handoff_to_human,
        }

        # Register functions on LLM service directly
        for tool_name, handler_fn in _tool_handlers.items():
            llm.register_function(
                tool_name,
                handler_fn,
                cancel_on_interruption=True,
            )

        # Item 6: Prompt ordering for Groq prefix caching.
        # Static system prompt and project facts are strictly FIRST in the message array.
        # Any per-call / dynamic context (customer name, IST date) is appended SECOND (last).
        # This guarantees exact-prefix caching (~50% token cost reduction & lower TTFT).
        messages = [
            {
                "role": "system",
                "content": system_prompt,
            }
        ]

        if customer_context:
            messages.append(
                {
                    "role": "system",
                    "content": customer_context,
                }
            )

        llm_tools = [
            FunctionSchema(
                name=raw["function"]["name"],
                description=raw["function"].get(
                    "description",
                    "",
                ),
                properties=(
                    raw["function"]
                    .get("parameters", {})
                    .get("properties", {})
                ),
                required=(
                    raw["function"]
                    .get("parameters", {})
                    .get("required", [])
                ),
            )
            for raw in llm_tools_raw
        ]

        context = LLMContext(
            messages,
            tools=llm_tools,
        )
        context.call_metrics = call_metrics
        context.lead_memory = lead_memory

        turn_config = config.get(
            "turn_management",
            {},
        )

        user_turn_start_strategies = [
            TranscriptionUserTurnStartStrategy(),
            ExternalUserTurnStartStrategy(),
        ]
        if fallback_vad_processor is not None:
            user_turn_start_strategies.insert(0, VADUserTurnStartStrategy())

        user_turn_stop_strategies = []
        # Default (restores the Oct 4 behaviour): transcript-aware 200ms debounce.
        # Smart Turn (local ONNX model) is now OPT-IN: set turn_management.smart_turn_enabled: true.
        use_smart_turn = bool(turn_config.get("smart_turn_enabled", False))
        if use_smart_turn:
            try:
                from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
                smart_analyzer = LocalSmartTurnAnalyzerV3()
                user_turn_stop_strategies.append(
                    SarvamSmartTurnStopStrategy(turn_analyzer=smart_analyzer, stream_id=stream_id)
                )
                logger.info("[{}] Smart Turn endpointing enabled (SarvamSmartTurnStopStrategy with LocalSmartTurnAnalyzerV3)", stream_id)
            except Exception as _st_err:
                logger.info(
                    "[{}] Smart Turn not available ({}); using debounced stop strategy",
                    stream_id,
                    _st_err,
                )
                use_smart_turn = False
        if not use_smart_turn:
            user_turn_stop_strategies.append(
                DebouncedExternalUserTurnStopStrategy(
                    timeout=float(
                        turn_config.get(
                            "user_speech_timeout",
                            0.20,
                        )
                    ),
                    filler_debounce_seconds=float(
                        turn_config.get(
                            "filler_debounce_seconds",
                            0.15,
                        )
                    ),
                    wait_for_transcript=True,
                )
            )

        context_aggregator = LLMContextAggregatorPair(
            context,
            user_params=LLMUserAggregatorParams(
                user_turn_strategies=UserTurnStrategies(
                    start=user_turn_start_strategies,
                    stop=user_turn_stop_strategies,
                ),
                user_turn_stop_timeout=float(
                    turn_config.get(
                        "user_turn_stop_timeout",
                        5.0,
                    )
                ),
            ),
        )

        language_observer = LanguageObserver(
            stream_id=stream_id,
            language_state=language_state,
            tts=tts,
            tts_provider=active["tts"],
            lead_memory=lead_memory,
        )

        # Unpruned transcript preserved for post-call reporting and analytics
        full_transcript: list[dict] = []

        history_pruner = _HistoryPruner(
            context=context,
            max_messages=config.get(
                "max_conversation_messages",
                12,
            ),
            stream_id=stream_id,
            full_transcript=full_transcript,
            call_end_coordinator=call_end_coordinator,
            lead_memory=lead_memory,
        )

        spoken_text_guard = _SpokenTextGuard(
            call_end_coordinator=call_end_coordinator,
            hangup_state=hangup_state,
            shutdown_state=shutdown_state,
            sample_rate=sample_rate,
            interruption_audio_gate=interruption_audio_gate,
            termination_processor=termination_processor,
            context=context,
            full_transcript=full_transcript,
            lead_memory=lead_memory,
            call_metrics=call_metrics,
            stream_id=stream_id,
            # Primary watchdog disarm: called when LLM starts generating.
            # Lambda closes over stall_watchdog (assigned below); safe because it's
            # only invoked during pipeline frame processing, after both exist.
            on_new_bot_turn=lambda: stall_watchdog.disarm_resume("new_bot_turn"),
        )

        async def _on_repeated_provider_stall(sid: str) -> None:
            # Force hangup immediately on repeated provider stall
            await _force_hangup_and_mark_done("stall_watchdog_repeated")

        stall_watchdog = StallWatchdog(
            stream_id=stream_id,
            on_repeated_stall=_on_repeated_provider_stall,
            is_call_ending=lambda: call_end_coordinator.is_ending,
            is_turn_in_flight=lambda: getattr(silence_checker, "_turn_in_flight", False),
            lead_memory=lead_memory,
            get_last_interrupted_text=lambda: getattr(spoken_text_guard, "_last_spoken_turn_text", ""),
            # Used at fire time to detect case 2: watchdog re-delivering text the LLM just spoke.
            get_current_bot_turn_text=lambda: getattr(spoken_text_guard, "_current_turn_spoken_text", "")
                or getattr(spoken_text_guard, "_last_spoken_turn_text", ""),
        )

        silence_checker = _SilenceChecker(
            stream_id=stream_id,
            task=None,
            context_aggregator_user=context_aggregator.user(),
            call_end_coordinator=call_end_coordinator,
            # TUNED: 15s first interval, 15s second, 15s third -> ~45s total to goodbye
            silence_threshold_secs=float(config.get("silence_nudge_first_secs", 15.0)),
            second_threshold_secs=15.0,
            third_threshold_secs=15.0,
            force_hangup_fn=_force_hangup_and_mark_done,
            call_metrics=call_metrics,
            lead_memory=lead_memory,
        )
        silence_checker.bind_state(
            shutdown_state=shutdown_state,
            termination_processor=termination_processor,
        )



        delayed_race_filler = _DelayedRaceFiller(
            stream_id=stream_id,
            sample_rate=sample_rate,
            interruption_audio_gate=interruption_audio_gate,
            language_state=language_state,
            hangup_state=hangup_state,
            timeout_seconds=float(config.get("filler_timeout_ms", 700)) / 1000.0,
            llm_service=llm,
            context=context,
        )

        fast_path_config = dict(config)
        # An API-supplied campaign may override the bundled script. Never speak bundled facts
        # unless they actually occur in the effective model prompt selected by prompt_builder.
        fast_path_config["real_estate_sales_script"] = system_prompt
        async def run_consented_whatsapp(action):
            if lead_memory.get("_whatsapp_dispatching"):
                return
            lead_memory["_whatsapp_dispatching"] = True
            consent_started = time.monotonic()
            try:
                if config.get('whatsapp_after_call_only', False):
                    line = record_manual_whatsapp(lead_memory, config, action)
                    if line:
                        frame = TTSSpeakFrame(text=line, append_to_context=True)
                        frame.is_deterministic_confirmation = True
                        await task.queue_frames([frame])
                    return
                if action == "brochure":
                    from types import SimpleNamespace
                    async def record_result(result, **kwargs):
                        logger.info("[{}] Deterministic brochure result: {}", stream_id, result.get("status"))
                    await send_brochure(SimpleNamespace(arguments={}, result_callback=record_result))
                elif action == "location":
                    result = await queue_consented_location(stream_id)
                    lead_memory.pop("_whatsapp_consent_action", None)
                    lead_memory["whatsapp_opt_in"] = True
                    lead_memory["whatsapp"] = result["status"]
                    call_metrics.record_tool_call("send_visit_location", success=result["status"] in ("queued", "accepted"), latency_ms=(time.monotonic() - consent_started) * 1000)
                    line = "I'll WhatsApp you the location." if result["status"] == "queued" else "The location is on its way." if result["status"] == "accepted" else "Our WhatsApp account is unavailable right now." if "131031" in (result.get("error") or "") else "I can't WhatsApp it right now."
                    f = TTSSpeakFrame(text=line, append_to_context=True)
                    f.is_deterministic_confirmation = True
                    await task.queue_frames([f])
            except Exception:
                logger.exception("[{}] Deterministic WhatsApp queue failed", stream_id)
                await task.queue_frames([TTSSpeakFrame(text="I can't send it right now. Please try again later.", append_to_context=True)])
            finally:
                lead_memory["_whatsapp_dispatching"] = False

        async def finish_provider_failure():
            await queue_goodbye(call_end_coordinator, task)
        llm._on_provider_exhausted = finish_provider_failure

        async def run_visit_ready(date, slot):
            if lead_memory.get("_visit_dispatching") or lead_memory.get("disposition") == "SITE_VISIT_BOOKED":
                return
            lead_memory["_visit_dispatching"] = True
            try:
                from types import SimpleNamespace
                async def record_booking(result, **kwargs):
                    if result.get("status") != "confirmed":
                        await task.queue_frames([TTSSpeakFrame(text="I couldn't book that visit yet. Please try again later.", append_to_context=True)])
                await execute_book_site_visit(SimpleNamespace(arguments={"date":date,"time":slot}, result_callback=record_booking), stream_id=stream_id, call_metrics=call_metrics, spoken_text_guard=spoken_text_guard, lead_memory=lead_memory, call_type=call_type, config=config)
            finally:
                lead_memory["_visit_dispatching"] = False

        fast_path_router = _FastPathRouter(
            stream_id=stream_id,
            lead_memory=lead_memory,
            config=fast_path_config,
            on_whatsapp_consent=run_consented_whatsapp,
            on_visit_ready=run_visit_ready,
            history_pruner=history_pruner,
            call_metrics=call_metrics,
            delayed_race_filler=delayed_race_filler,
            call_end_coordinator=call_end_coordinator,
        )

        spam_qualify_gate = _SpamQualifyGate(
            stream_id=stream_id,
            force_hangup_fn=_force_hangup_and_mark_done,
            is_call_ending=lambda: call_end_coordinator.is_ending,
        )

        async def hedge_tts_stall():
            if call_end_coordinator.is_ending or hangup_state["done"]:
                return
            pcm = _AUDIO_CACHE.get(sample_rate, {}).get("clarify_repeat")
            if not pcm:
                logger.error("[{}] TTS stall; cached hedge unavailable", stream_id)
                return
            logger.warning("[{}] TTS first audio stalled >2s; cancel stale synthesis and ask repeat once", stream_id)
            await tts.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
            spoken_text_guard._record_assistant_spoken(_CACHED_PHRASE_TEXTS["clarify_repeat"], "clarify_repeat")
            tts_stall_state.used = True
            await interruption_audio_gate.play_cached_audio(pcm, sample_rate)

        tts_stall_state = TTSStallState(hedge_tts_stall)
        attach_safe_tts(tts, stream_id, tts_stall_state)
        tts_stall_observer = _TTSStallObserver(tts_stall_state)
        @tts.event_handler("on_tts_request")
        async def watch_tts_request(service, context_id, text):
            if not call_end_coordinator.is_ending:
                tts_stall_state.arm()

        pipeline_elements = [transport.input()]
        if fallback_vad_processor is not None:
            pipeline_elements.append(fallback_vad_processor)
        pipeline_elements.extend([
            stt,
            _TranscriptionTap(call_metrics),
            spam_qualify_gate,
            language_observer,
            context_aggregator.user(),
            fast_path_router,
            llm,
            delayed_race_filler,
            spoken_text_guard,
            tts,
            interruption_audio_gate,
            tts_stall_observer,
            call_end_coordinator,
            termination_processor,
            silence_checker,
            stall_watchdog,
            transport.output(),
            context_aggregator.assistant(),
            history_pruner,
            call_metrics,
        ])

        pipeline = Pipeline(pipeline_elements)

        max_duration = config.get(
            "max_call_duration_seconds",
            900,
        )

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                audio_in_sample_rate=(
                    16000
                    if call_type == "web"
                    else config["audio"]["sample_rate"]
                ),
                audio_out_sample_rate=(
                    16000
                    if call_type == "web"
                    else config["audio"]["sample_rate"]
                ),
                allow_interruptions=True,
                enable_metrics=True,
                enable_usage_metrics=True,
            ),
        )

        silence_checker.set_task(task)
        call_end_coordinator.bind_task(task)
        stall_watchdog.bind_task(task)
        spam_qualify_gate.bind_task(task)
        spoken_text_guard.bind_task(task)

        # Outbound greeting initialization
        _greeting_sent = {
            "done": False,
        }

        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport,client):
            if _greeting_sent["done"]:
                logger.debug(
                    "[{}] Greeting already sent, skipping",
                    stream_id,
                )
                return

            logger.info(
                "[{}] Client connected, sending outbound greeting",
                stream_id,
            )

            customer_name = None

            if campaign_data:
                customer_name = campaign_data.get(
                    "customer_name"
                )

            if (
                not customer_name
                and call_type == "web"
            ):
                customer_name = config.get(
                    "test_customer_name"
                )

            if conversation_call_type == "outbound":
                if customer_name:
                    greeting = f"Hi, am I speaking with {customer_name}?"
                else:
                    greeting = (
                        "Hi, this is Ananya from Meridian Group. "
                        "Is this a good time to talk?"
                    )
            else:
                greeting = str(
                    config.get(
                        f"greeting_{conversation_call_type}",
                        config.get(
                            "greeting_inbound",
                            "Hi, how can I help you?",
                        ),
                    )
                )

            # Append greeting directly to context and speak via TTS without triggering LLM completion
            messages.append(
                {
                    "role": "assistant",
                    "content": greeting,
                }
            )

            interruption_audio_gate.next_generation()

            if hasattr(
                serializer,
                "next_generation",
            ):
                serializer.next_generation()

            _greeting_sent["done"] = True

            await task.queue_frames(
                [
                    TTSSpeakFrame(
                        text=greeting,
                        append_to_context=False,
                    ),
                ]
            )

            async def hard_timeout():
                await asyncio.sleep(
                    max_duration
                )

                # Guard against firing hard timeout if call already ended
                if hangup_state["done"]:
                    return

                logger.warning(
                    "[{}] Hard call timeout",
                    stream_id,
                )

                await _force_hangup_and_mark_done(
                    "hard_timeout"
                )

                await task.cancel()

            asyncio.create_task(
                hard_timeout()
            )

        @context_aggregator.user().event_handler(
            "on_user_turn_stopped"
        )
        async def on_user_turn_stopped(
            processor,
            strategy,
            *args,
            **kwargs,
        ):
            user_msg = args[0] if args else None
            user_content = getattr(user_msg, "content", "") or ""
            was_interrupted = getattr(interruption_audio_gate, "is_interrupted", False)

            source_name = "timeout" if strategy is None else "strategy"
            logger.info("[{}] turn_stop source={}", stream_id, source_name)
            if call_metrics and hasattr(call_metrics, "record_turn_stop"):
                call_metrics.record_turn_stop(source_name)

            if not str(user_content).strip():
                silence_checker._turn_in_flight = False
                silence_checker._user_is_speaking = False
                now = time.monotonic()
                silence_checker._last_user_speech_time = now
                stall_watchdog.disarm()
                if delayed_race_filler is not None:
                    delayed_race_filler.cancel_all_timers("empty transcript")
                logger.info(
                    "[{}] User turn ended with empty transcript; disarmed stall watchdog, silence monitor active (stage={})",
                    stream_id,
                    silence_checker._stage,
                )
                return
            else:
                # FIX 4: Drop STT garbage produced by music/noise (e.g. 'Iyamya.', 'लाळवान.').
                # A valid user utterance must have at least 3 alphabetic characters total.
                _alpha_count = sum(1 for c in str(user_content) if c.isalpha())
                if not valid_transcript(str(user_content)):
                    silence_checker._turn_in_flight = False
                    silence_checker._user_is_speaking = False
                    stall_watchdog.disarm()
                    logger.warning(
                        "[{}] Dropped garbage STT transcript {!r} ({} alpha chars) — not sending to LLM",
                        stream_id,
                        str(user_content)[:60],
                        _alpha_count,
                    )
                    return

                silence_checker._turn_in_flight = True
                silence_checker._user_is_speaking = False
                silence_checker._stage = 0
                # Primary watchdog disarm: a committed user turn means no re-delivery needed.
                stall_watchdog.disarm_resume("user_turn")

                # Bug 2 echo guard: browser echo cancellation sometimes fails under heavy VAD
                # activity. If the bot is currently speaking and the transcript's words
                # substantially overlap the bot's current utterance, it is almost certainly
                # acoustic echo — drop it without sending to the LLM.
                _bot_current = getattr(spoken_text_guard, "_current_turn_spoken_text", "") or \
                               getattr(spoken_text_guard, "_last_spoken_turn_text", "")
                _gate_active = (
                    interruption_audio_gate is not None
                    and getattr(interruption_audio_gate, "_active_playing_gen_id", 0) > 0
                    and not getattr(interruption_audio_gate, "_is_interrupted", True)
                )
                if _gate_active and _bot_current:
                    _strip = lambda s: set(re.sub(r"[^\w\s]", "", s.lower()).split())
                    _user_words = _strip(str(user_content))
                    _bot_words = _strip(_bot_current)
                    if _user_words and _bot_words:
                        _overlap = len(_user_words & _bot_words) / min(len(_user_words), len(_bot_words))
                        if _overlap >= 0.5:
                            silence_checker._turn_in_flight = False
                            logger.warning(
                                "[{}] Dropped probable echo transcript {!r} (overlap={:.0%} with bot utterance {!r})",
                                stream_id,
                                str(user_content)[:60],
                                _overlap,
                                _bot_current[:60],
                            )
                            return

                # If the preceding assistant message was a farewell, check whether caller reciprocated
                # or barged in with a continuation question/statement.
                last_assistant_content = ""
                for m in reversed(context.messages):
                    if isinstance(m, dict) and m.get("role") == "assistant":
                        last_assistant_content = m.get("content", "")
                        break

                is_prev_farewell = False
                if last_assistant_content:
                    is_prev_farewell = any(
                        pat.search(last_assistant_content)
                        for pat in termination_processor.spoken_termination_patterns
                    )

                if (is_prev_farewell or getattr(call_end_coordinator, "_dedicated_goodbye_queued", False)) and str(user_content).strip():
                    is_reciprocated = _is_farewell_or_acknowledgment(str(user_content))
                    clean_u = _PUNCT_RE.sub(" ", str(user_content).lower()).strip()
                    u_words = set(clean_u.split())
                    continuation_kw = {
                        "listen", "wait", "suno", "ruko", "boliye", "batao", "question",
                        "tell", "what", "why", "how", "when", "where", "which", "who", "price",
                        "cost", "bhk", "location", "amenit", "project", "metro", "visit",
                        "actually", "hello", "ananya", "kya", "kyu", "kyun", "kaun", "kab",
                        "kahan", "kitna", "kaise", "enti", "ela", "eppudu", "ekada", "cheppandi", "aagandi"
                    }
                    is_genuine_question = ("?" in str(user_content)) or bool(u_words & continuation_kw)
                    if is_reciprocated or not is_genuine_question:
                        logger.info(
                            "[{}] Caller reciprocated or concluded farewell ({!r}); completing hangup without bot reply",
                            stream_id,
                            user_content,
                        )
                        # Coordinator owns the eight-second quiet window; do not end early or re-speak.
                        return

            _coalesce_consecutive_messages(context.messages)
            _sync_working_memory(context.messages, lead_memory, stream_id)

            gen_id = (
                interruption_audio_gate.next_generation()
            )

            if hasattr(
                serializer,
                "next_generation",
            ):
                serializer.next_generation()

            logger.debug(
                "[{}] Advanced audio generation context to id={}",
                stream_id,
                gen_id,
            )

        @transport.event_handler(
            "on_client_disconnected"
        )
        async def on_client_disconnected(
            transport,
            client,
        ):
            snapshot = language_state.snapshot()

            logger.info(
                "[{}] Disconnected language={} turns={} switches={}",
                stream_id,
                snapshot["current_language"],
                snapshot["turn_index"],
                snapshot["switch_count"],
            )

            silence_checker.stop()

            await task.cancel()

        runner = PipelineRunner(
            handle_sigint=False
        )

        try:
            await runner.run(task)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "[{}] Pipeline error",
                stream_id,
            )

    except Exception:
        logger.exception(
            "[{}] Call initialization/runtime failure",
            stream_id,
        )

    finally:
        STREAM_PROVIDER_CALL_IDS.pop(
            stream_id,
            None,
        )

        # Sync any un-pruned messages from context into full_transcript
        if "context" in locals() and context is not None:
            ctx_msgs = context.get_messages() if hasattr(context, "get_messages") else getattr(context, "messages", [])
            for msg in ctx_msgs:
                role = msg.get("role")
                content = msg.get("content")
                if isinstance(content, list):
                    content = " ".join(part.get("text", "") for part in content if isinstance(part, dict))
                if isinstance(content, str) and content.strip() and role in ("user", "assistant"):
                    item = {"role": role, "content": content.strip()}
                    if "full_transcript" in locals() and full_transcript is not None:
                        if not any(t.get("role") == role and t.get("content") == content.strip() for t in full_transcript):
                            full_transcript.append(item)

        transcript_to_use = (
            full_transcript if ("full_transcript" in locals() and full_transcript)
            else (list(messages) if ("messages" in locals() and messages is not None) else [])
        )
        logger.debug("[{}] Transcript length for call report: {} messages", stream_id, len(transcript_to_use))
        current_lead_mem = lead_memory if "lead_memory" in locals() else None

        metrics_sum = None
        if call_metrics is not None:
            if transcript_to_use:
                call_metrics.enrich_from_transcript(transcript_to_use)
            call_metrics.finalize()
            metrics_sum = call_metrics.summary()
            await lead_state.record_call_stats_async(stream_id, **metrics_sum)

        # 1. Deterministic disposition inference (<0.1ms, zero-cost, instant)
        try:
            disposition = await lead_state.infer_deterministic_disposition_async(
                lead_memory=current_lead_mem,
                messages=transcript_to_use,
            )
            await lead_state.set_disposition_async(stream_id, disposition)
        except Exception as e:
            logger.warning("[{}] Failed inferring deterministic disposition: {}", stream_id, e)

        # 2. Finalize call in SQLite so ended_at and disposition are persisted BEFORE report generation
        await lead_state.finalize_call_async(stream_id)
        # Manual delivery only: export one copyable message in the report below.
        # Meta is restricted. No queue/network send, even when credentials exist.

        # Persist Sheets work BEFORE launching analysis/scoring. A missing lead must not lose a call.
        try:
            from leads.outbox import queue_call_sheet_export
            await queue_call_sheet_export(stream_id)
        except Exception:
            logger.exception("[{}] SHEETS_QUEUE_FAILED: call saved but export not queued; retry required", stream_id)

        if current_lead_mem is not None:
            current_lead_mem['_manual_whatsapp_output'] = manual_whatsapp_message(current_lead_mem, config)

        # 3. Immediate, deterministic local report generation: takes <1ms, $0 cost,
        # never waits on external LLMs, guaranteed to write on every call completion.
        if transcript_to_use:
            try:
                local_test_report.write_report(
                    stream_id,
                    transcript_to_use,
                    lead_memory=current_lead_mem,
                    metrics_summary=metrics_sum,
                )
            except BaseException as e:
                logger.warning("[{}] Failed writing local test report: {}", stream_id, e)

            # One analytics owner only: the scoring job receives the real transcript.
            # Background arq lead worker scoring, touchpoint logging, and CRM dispatch
            try:
                from leads.worker import enqueue_on_call_finished
                asyncio.create_task(enqueue_on_call_finished(stream_id, transcript_to_use))
            except Exception as exc:
                logger.debug(f"[{stream_id}] Enqueue on_call_finished warning: {exc}")

        # Last-resort hangup backstop to guarantee line termination
        if not hangup_state["done"]:
            logger.warning(
                "[{}] Finally-block safety net: hangup was never "
                "confirmed done through the normal paths; forcing one "
                "last attempt now.",
                stream_id,
            )

            try:
                await asyncio.shield(force_provider_hangup("finally_safety_net"))

            except BaseException:
                logger.exception(
                    "[{}] Finally-block safety-net hangup attempt failed",
                    stream_id,
                )

            hangup_state["done"] = True

        if not aiohttp_session.closed:
            try:
                await asyncio.shield(aiohttp_session.close())
            except BaseException:
                pass

        logger.remove(
            log_handler
        )

def caller_slot_matches(messages, arguments):
    from leads.worker import normalize_visit_date, normalize_visit_time
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    selected = {}
    for message in messages:
        if message.get("role") == "user" and isinstance(message.get("content"), str):
            fields = _extract_lead_preferences(message["content"])
            for key in ("visit_date_iso", "time_slot"):
                if fields.get(key):
                    selected[key] = fields[key]
    requested_date = normalize_visit_date(str(arguments.get("date", "")), now)
    raw_time = str(arguments.get("time", ""))
    requested_time = visit_time(raw_time)
    return bool(requested_date and requested_time and selected.get("visit_date_iso") == requested_date[0]
                and selected.get("time_slot") == requested_time)


def attach_safe_tts(tts, stream_id="", stall_state=None):
    original = tts.run_tts
    async def run_safe(text, context_id):
        clean = safe_tts_text(text)
        if not clean:
            logger.info("[{}] Skipped nonlinguistic aggregated TTS segment", stream_id)
            return
        if stall_state is not None:
            stall_state.arm()
        async for output in original(clean, context_id):
            yield output
    tts.run_tts = run_safe


async def queue_consented_location(stream_id):
    if not whatsapp_ready():
        return {"status":"not_configured", "error":"WhatsApp is not set up"}
    from sqlalchemy import select
    from leads.db import get_session
    from leads.models import SiteVisit, Lead
    from leads.outbox import queue_outbox_item
    async with get_session() as session:
        visit = (await session.execute(select(SiteVisit).where(SiteVisit.call_id == stream_id))).scalars().first()
        if not visit:
            return {"status": "needs_visit"}
        lead = await session.get(Lead, visit.lead_id)
        if not lead or not lead.phone:
            return {"status": "needs_phone"}
        item = await queue_outbox_item(lead.id, "whatsapp", {
            "call_id": stream_id, "phone": lead.phone, "name": lead.name,
            "visit_date_iso": visit.visit_date_iso, "time_slot": visit.time_slot}, session=session)
        visit.whatsapp_opt_in = True
        visit.whatsapp_opt_in_at = datetime.now(timezone.utc)
        if item.status == "done" and (item.payload or {}).get("message_id"):
            status = "accepted"
            visit.whatsapp_message_id = item.payload["message_id"]
        else:
            status = "failed" if item.status == "failed" else "queued"
        visit.whatsapp_status = "sent" if status == "accepted" else status
        await session.commit()
        return {"status": status, "error": item.last_error, "outbox_id": str(item.id)}
