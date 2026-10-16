"""
trajectory.py — Call trajectory logger (item 8).

Every call logs its full event sequence: STT segments, LLM turns (input + output),
tool calls with args and results, TTS handoffs, and timestamps.

The trajectory is written per call to:
    <DATA_DIR>/trajectories/<YYYY-MM-DD>/<call_id>.jsonl

Each line is a single JSON event. The file is streamed (one line per event)
so a crash mid-call still yields a partial trajectory — never an empty file.

Call path impact: ZERO. All writes are synchronous appends to an already-open
file handle. No async I/O, no network, no blocking on the call path.

Usage (from bot.py or wherever events fire):
    from trajectory import Trajectory
    traj = Trajectory(call_id, stream_id, call_type)
    traj.stt("caller said something")
    traj.llm_turn(messages_in, response_text, latency_ms=120)
    traj.tool_call("book_visit", {"date": "tomorrow"}, result={"ok": True})
    traj.tts("Playing: 'Great, visit booked for tomorrow!'", latency_ms=80)
    traj.close(disposition="booked", duration_s=185)

Querying by call_id:
    records = Trajectory.load(call_id)   # returns list[dict]
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def _data_dir() -> str:
    return os.getenv("DATA_DIR", "outputs")


def _traj_dir() -> Path:
    return Path(_data_dir()) / "trajectories" / date.today().isoformat()


def _ts() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class Trajectory:
    """Streams per-event JSONL into <DATA_DIR>/trajectories/<date>/<call_id>.jsonl."""

    def __init__(self, call_id: str, stream_id: str = "", call_type: str = "telephony") -> None:
        self.call_id = call_id
        self.stream_id = stream_id
        self.call_type = call_type
        self._start = _ts()
        self._closed = False

        traj_dir = _traj_dir()
        traj_dir.mkdir(parents=True, exist_ok=True)
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in call_id)
        self._path = traj_dir / f"{safe_id}.jsonl"
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)  # line-buffered

        self._write({
            "event": "call_start",
            "call_id": call_id,
            "stream_id": stream_id,
            "call_type": call_type,
        })

    # ------------------------------------------------------------------
    # Public event methods — all non-blocking synchronous writes
    # ------------------------------------------------------------------

    def stt(self, text: str, final: bool = True, language: str | None = None) -> None:
        """An STT segment was transcribed from the caller."""
        self._write({
            "event": "stt",
            "final": final,
            "text": text,
            "language": language,
        })

    def llm_turn(
        self,
        messages_in: list[dict],
        response_text: str,
        latency_ms: float | None = None,
        model: str | None = None,
        tokens_in: int | None = None,
        tokens_out: int | None = None,
    ) -> None:
        """One full LLM request-response cycle."""
        self._write({
            "event": "llm_turn",
            "model": model,
            "messages_in_count": len(messages_in),
            # Store only the last 2 user/assistant turns to keep files small,
            # not the full system prompt which is static.
            "messages_tail": messages_in[-2:] if len(messages_in) >= 2 else messages_in,
            "response_text": response_text[:2000],  # cap at 2 KB per turn
            "latency_ms": latency_ms,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
        })

    def tool_call(
        self,
        name: str,
        args: dict[str, Any],
        result: Any = None,
        error: str | None = None,
        latency_ms: float | None = None,
    ) -> None:
        """A tool/function call made by the LLM, with its result or error."""
        self._write({
            "event": "tool_call",
            "name": name,
            "args": args,
            "result": result,
            "error": error,
            "latency_ms": latency_ms,
        })

    def tts(
        self,
        text: str,
        latency_ms: float | None = None,
        provider: str | None = None,
    ) -> None:
        """TTS playback started (first-audio latency recorded)."""
        self._write({
            "event": "tts",
            "text": text[:500],
            "latency_ms": latency_ms,
            "provider": provider,
        })

    def stage_change(self, from_stage: str, to_stage: str, trigger: str = "") -> None:
        """Conversation stage transition (e.g. greeting → faq → booking)."""
        self._write({
            "event": "stage_change",
            "from": from_stage,
            "to": to_stage,
            "trigger": trigger,
        })

    def interruption(self, interrupted_text: str, duration_s: float | None = None) -> None:
        """User interrupted/barged in while bot was speaking (Bolna/SuperBryn pattern)."""
        self._write({
            "event": "interruption",
            "interrupted_text": interrupted_text[:500],
            "duration_s": duration_s,
        })

    def note(self, key: str, value: Any) -> None:
        """Arbitrary structured annotation (e.g. language detected, repair applied)."""
        self._write({"event": "note", "key": key, "value": value})

    def close(
        self,
        disposition: str = "unknown",
        duration_s: float | None = None,
        lead_fields: dict | None = None,
    ) -> None:
        """Finalize the trajectory with outcome metadata."""
        if self._closed:
            return
        self._write({
            "event": "call_end",
            "disposition": disposition,
            "duration_s": duration_s or (_ts() - self._start),
            "lead_fields": lead_fields or {},
        })
        self._closed = True
        try:
            self._fh.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Class-level query helpers
    # ------------------------------------------------------------------

    @classmethod
    def find_path(cls, call_id: str) -> Path | None:
        """Search today's and recent trajectory dirs for a call_id file."""
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in call_id)
        base = Path(_data_dir()) / "trajectories"
        if not base.exists():
            return None
        for day_dir in sorted(base.iterdir(), reverse=True):
            candidate = day_dir / f"{safe_id}.jsonl"
            if candidate.exists():
                return candidate
        return None

    @classmethod
    def load(cls, call_id: str) -> list[dict]:
        """Return all trajectory events for a call_id. Returns [] if not found."""
        path = cls.find_path(call_id)
        if not path:
            return []
        events = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return events

    @classmethod
    def latency_summary(cls, events: list[dict]) -> dict[str, Any]:
        """
        Extract per-event-type latency stats (ms) from a trajectory event list.

        Returns a dict with keys: stt_ms, llm_ms, tts_ms (each a list of floats),
        plus p50/p95 for each. Used by the eval harness for statistical latency checks.
        """
        stt: list[float] = []
        llm: list[float] = []
        tts: list[float] = []
        for ev in events:
            ms = ev.get("latency_ms")
            if ms is None:
                continue
            kind = ev.get("event", "")
            if kind == "stt":
                stt.append(float(ms))
            elif kind == "llm_turn":
                llm.append(float(ms))
            elif kind == "tts":
                tts.append(float(ms))

        def _pct(values: list[float], p: int) -> float | None:
            if not values:
                return None
            idx = max(0, int(len(values) * p / 100) - 1)
            return sorted(values)[idx]

        return {
            "stt_ms": stt,
            "llm_ms": llm,
            "tts_ms": tts,
            "stt_p95": _pct(stt, 95),
            "llm_p95": _pct(llm, 95),
            "tts_p95": _pct(tts, 95),
            "stt_p50": _pct(stt, 50),
            "llm_p50": _pct(llm, 50),
            "tts_p50": _pct(tts, 50),
        }

    def otel_export(self, events: list[dict]) -> None:
        """
        Item 5 — OTEL span export stub.

        Emits one OTEL span per trajectory event when OTEL_EXPORTER_OTLP_ENDPOINT
        is set. When the env var is unset this method returns immediately — zero
        overhead on the call path. The export itself is done in a background thread
        so it cannot block the event loop.

        This makes the whole trajectory queryable in Langfuse, Jaeger, or any
        OTLP-compatible backend without any code change — just set the env var.
        """
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
        if not endpoint:
            return  # no-op: env var not set

        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            import threading

            def _export():
                try:
                    headers_raw = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
                    headers: dict[str, str] = {}
                    for part in headers_raw.split(","):
                        if "=" in part:
                            k, _, v = part.strip().partition("=")
                            headers[k.strip()] = v.strip()

                    provider = TracerProvider()
                    exporter = OTLPSpanExporter(endpoint=endpoint, headers=headers)
                    provider.add_span_processor(BatchSpanProcessor(exporter))
                    tracer = provider.get_tracer("voicebot.trajectory")

                    with tracer.start_as_current_span(
                        "call",
                        attributes={
                            "call.id": self.call_id,
                            "call.type": self.call_type,
                            "call.stream_id": self.stream_id,
                        },
                    ) as call_span:
                        for ev in events:
                            ev_name = ev.get("event", "event")
                            with tracer.start_as_current_span(
                                ev_name,
                                attributes={
                                    k: str(v)
                                    for k, v in ev.items()
                                    if isinstance(v, (str, int, float, bool)) and k != "event"
                                },
                            ):
                                pass

                    provider.force_flush(timeout_millis=5000)
                    provider.shutdown()
                except Exception as exc:
                    pass  # never propagate — this is a best-effort export

            threading.Thread(target=_export, daemon=True, name="otel-export").start()
        except ImportError:
            pass  # opentelemetry-sdk not installed — silent no-op

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write(self, payload: dict) -> None:
        if self._closed:
            return
        payload["ts"] = _iso(_ts())
        payload["elapsed_s"] = round(_ts() - self._start, 3)
        try:
            self._fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass  # never raise on the call path
