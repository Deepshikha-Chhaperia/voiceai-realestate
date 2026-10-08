"""Per-call conversational language state.

There is intentionally no language dictionary or regex classifier here.

Normal language detection comes from the multilingual STT provider.
The LLM can explicitly request a language change through the
`set_conversation_language` tool.

The application only owns:
- current language
- confidence
- switch history

This module is created once per call and must never be shared between calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

LanguageCode = Literal["en", "hi", "te"]

SUPPORTED_LANGUAGES: frozenset[LanguageCode] = frozenset(
    {"en", "hi", "te"}
)

LANGUAGE_LOCALES: dict[LanguageCode, str] = {
    "en": "en-IN",
    "hi": "hi-IN",
    "te": "te-IN",
}

MIN_STT_CONFIDENCE = 0.75
DEFAULT_SUSTAINED_SWITCH_TURNS = 2



@dataclass(frozen=True)
class LanguageSwitchEvent:
    from_language: LanguageCode
    to_language: LanguageCode
    reason: Literal[
        "initial_detection",
        "explicit_request",
        "sustained_change",
    ]
    confidence: float
    turn_index: int


@dataclass
class LanguageState:
    current_language: LanguageCode = "en"
    established: bool = False
    candidate_language: Optional[LanguageCode] = None
    candidate_confidence: float = 0.0
    consecutive_candidate_turns: int = 0
    switch_reason: str = "initial"
    turn_index: int = 0
    switch_history: list[LanguageSwitchEvent] = field(default_factory=list)
    sustained_switch_turns: int = DEFAULT_SUSTAINED_SWITCH_TURNS
    is_ambiguous: bool = False
    supported_languages: frozenset[str] = field(default_factory=lambda: frozenset({"en", "hi", "te"}))

    def observe_stt(
        self,
        language: Optional[str],
        confidence: Optional[float] = None,
        text: Optional[str] = None,
    ) -> tuple[LanguageCode, bool]:
        """Consume one finalized STT language observation.

        - If language is unsupported or confidence < MIN_STT_CONFIDENCE (0.75), flags is_ambiguous=True.
        - High-confidence Indian languages (Hindi, Telugu) adapt immediately on that turn.
        - Switching from Hindi/Telugu back to English requires sustained English turns to prevent flip-flopping.
        """
        self.turn_index += 1

        if text:
            import re
            clean = re.sub(r"[^\w\s]", "", str(text).lower()).strip()
            if clean in {"oh", "yeah", "okay", "ok", "hmm", "haan", "accha", "acha", "uh", "um"}:
                return self.current_language, False

        detected = normalize_language(language)
        confidence_value = (
            normalize_probability(confidence)
            if confidence is not None
            else 0.90
        )

        # Unsupported language or low confidence -> flag ambiguity and do not switch
        if detected is None or detected not in self.supported_languages or confidence_value is None or confidence_value < MIN_STT_CONFIDENCE:
            self.is_ambiguous = bool(detected and detected not in self.supported_languages)
            return self.current_language, False

        self.is_ambiguous = False

        if detected == self.current_language:
            self.established = True
            self._clear_candidate()
            return self.current_language, False

        old = self.current_language

        # High confidence Hindi/Telugu or other supported non-English language adapts immediately
        if detected in {"hi", "te"}:
            new = self._switch(
                detected,
                "sustained_change" if self.established else "initial_detection",
                confidence_value,
            )
            return new, new != old

        # Switching back to pure English requires sustained turns (e.g. 2) to avoid flapping
        if detected == "en" and self.current_language in {"hi", "te"}:
            if self.candidate_language == detected:
                self.consecutive_candidate_turns += 1
            else:
                self.candidate_language = detected
                self.candidate_confidence = confidence_value
                self.consecutive_candidate_turns = 1

            if self.consecutive_candidate_turns >= self.sustained_switch_turns:
                new = self._switch(
                    detected,
                    "sustained_change",
                    confidence_value,
                )
                return new, new != old

            return old, False

        if not self.established:
            new = self._switch(
                detected,
                "initial_detection",
                confidence_value,
            )
            return new, new != old

        if self.candidate_language == detected:
            self.consecutive_candidate_turns += 1
        else:
            self.candidate_language = detected
            self.candidate_confidence = confidence_value
            self.consecutive_candidate_turns = 1

        if self.consecutive_candidate_turns >= self.sustained_switch_turns:
            new = self._switch(
                detected,
                "sustained_change",
                confidence_value,
            )
            return new, new != old

        return old, False

    def set_explicit(
        self,
        language: str,
        reason: str,
    ) -> tuple[LanguageCode, bool]:
        """Apply an explicit language decision requested by the agent."""
        normalized = normalize_language(language)

        if normalized is None:
            raise ValueError(
                f"Unsupported language: {language!r}"
            )

        old = self.current_language

        if old == normalized:
            self.established = True
            self.switch_reason = "explicit_request"
            self._clear_candidate()
            return old, False

        new = self._switch(
            normalized,
            "explicit_request",
            1.0,
        )
        return new, new != old

    def snapshot(self) -> dict:
        return {
            "current_language": self.current_language,
            "established": self.established,
            "candidate_language": self.candidate_language,
            "candidate_confidence": round(
                self.candidate_confidence,
                3,
            ),
            "consecutive_candidate_turns": (
                self.consecutive_candidate_turns
            ),
            "switch_reason": self.switch_reason,
            "turn_index": self.turn_index,
            "switch_count": len(self.switch_history),
        }

    def _switch(
        self,
        language: LanguageCode,
        reason: Literal[
            "initial_detection",
            "explicit_request",
            "sustained_change",
        ],
        confidence: float,
    ) -> LanguageCode:
        if language not in SUPPORTED_LANGUAGES:
            return self.current_language

        event = LanguageSwitchEvent(
            from_language=self.current_language,
            to_language=language,
            reason=reason,
            confidence=round(confidence, 3),
            turn_index=self.turn_index,
        )

        self.switch_history.append(event)
        self.current_language = language
        self.switch_reason = reason
        self.established = True
        self._clear_candidate()

        return language

    def _clear_candidate(self) -> None:
        self.candidate_language = None
        self.candidate_confidence = 0.0
        self.consecutive_candidate_turns = 0


def normalize_language(
    value: Optional[str],
) -> Optional[LanguageCode]:
    if not value:
        return None

    value = str(value).strip().lower().replace("_", "-")
    base = value.split("-", 1)[0]

    if base in {"en", "eng", "english"}:
        return "en"
    if base in {"hi", "hin", "hindi", "hinglish"}:
        return "hi"
    if base in {"te", "tel", "telugu"}:
        return "te"

    return None


def normalize_probability(
    value: object,
) -> Optional[float]:
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return None

    if probability > 1:
        probability /= 100.0

    return max(0.0, min(1.0, probability))
