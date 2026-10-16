"""
evals/harness.py — Eval harness (item 9, refined in item 5).

Takes a call trajectory JSONL path or a messages list and scores it using:

  A) Deterministic checks (no LLM cost):
     - visit_date_confirmed: bot read back a date after caller provided one
     - phone_readback: bot read back caller's phone number
     - brochure_order: brochure was only offered after a booking attempt
     - call_ended_cleanly: a goodbye/dhanyavaad line was present

  B) Statistical latency checks (from real trajectory span data — no LLM cost):
     - llm_latency: LLM p95 < 800 ms
     - tts_latency: TTS first-byte p95 < 250 ms

  C) LLM judge (Groq, uses existing GROQ_API_KEY):
     - hallucination_check
     - script_adherence
     - answer_quality
     Judge prompts are loaded from evals/eval_judge_prompts.yaml — edit that
     file to change any judge without touching Python.
     Judge output schema: {score: 0.0–1.0, reasoning: str, passed: bool, confidence: float}

  D) Fixture capture (Roark-style closed loop):
     When any deterministic or statistical check fails on a real call, the
     transcript is snapshotted to evals/fixtures/<check_name>/ so regression.py
     replays it forever. Prevents silent regressions.

Usage:
    python -m evals.harness path/to/call_id.jsonl
    python -m evals.harness --transcript-json '{\"call_id\":\"x\",\"messages\":[...]}'

Output: a JSON report written to <DATA_DIR>/evals/<call_id>.json and printed.

No live calls, no new API providers beyond the existing GROQ_API_KEY.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import yaml

_HERE = Path(__file__).parent
_PROMPTS_PATH = _HERE / "eval_judge_prompts.yaml"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "outputs"))


def _evals_dir() -> Path:
    d = _data_dir() / "evals"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _fixtures_dir() -> Path:
    d = _HERE / "fixtures"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_prompts() -> dict[str, str]:
    with _PROMPTS_PATH.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _transcript_from_trajectory(events: list[dict]) -> list[dict]:
    """Convert trajectory events to a simple messages list for the judge."""
    messages: list[dict] = []
    for ev in events:
        if ev.get("event") == "stt":
            messages.append({"role": "user", "content": ev.get("text", "")})
        elif ev.get("event") == "tts":
            messages.append({"role": "assistant", "content": ev.get("text", "")})
        elif ev.get("event") == "llm_turn":
            messages.append({"role": "assistant", "content": ev.get("response_text", "")})
    return messages


def _format_transcript(messages: list[dict]) -> str:
    lines = []
    for m in messages:
        role = m.get("role", "?")
        label = "Caller" if role == "user" else "Pooja"
        content = m.get("content", "").strip()
        if content:
            lines.append(f"{label}: {content}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# A — Deterministic checks
# ---------------------------------------------------------------------------

def _check_visit_date_confirmed(messages: list[dict]) -> tuple[bool, str]:
    """Bot mentioned a day/date string after the caller gave a date."""
    caller_gave_date = False
    date_pattern = re.compile(
        r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday"
        r"|kal|parso|aaj|tomorrow|today|parson"
        r"|\d{1,2}[/-]\d{1,2}|\d{4}-\d{2}-\d{2})\b",
        re.IGNORECASE,
    )
    for m in messages:
        role = m.get("role", "")
        text = m.get("content", "")
        if role == "user" and date_pattern.search(text):
            caller_gave_date = True
        if caller_gave_date and role == "assistant" and date_pattern.search(text):
            return True, "Bot confirmed visit date"
    if not caller_gave_date:
        return True, "n/a (caller gave no date)"
    return False, "Caller gave a date but bot never confirmed it"


def _check_phone_readback(messages: list[dict]) -> tuple[bool, str]:
    """Bot read back a phone number (or 'n/a' if caller never provided one)."""
    phone_re = re.compile(r"\b\d[\d\s-]{7,}\d\b")
    caller_gave_phone = False
    for m in messages:
        if m.get("role") == "user" and phone_re.search(m.get("content", "")):
            caller_gave_phone = True
        if caller_gave_phone and m.get("role") == "assistant" and phone_re.search(m.get("content", "")):
            return True, "Bot read back phone number"
    if not caller_gave_phone:
        return True, "n/a (caller gave no phone)"
    return False, "Caller gave phone but bot never read it back"


def _check_brochure_order(messages: list[dict]) -> tuple[bool, str]:
    """Brochure was only offered after a booking/visit-date was mentioned."""
    brochure_re = re.compile(r"\b(brochure|pdf|document|catalogue)\b", re.IGNORECASE)
    booking_re = re.compile(
        r"\b(visit|booking|confirm|slot|appointment|date|time|kal|tomorrow|monday"
        r"|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        re.IGNORECASE,
    )
    booking_mentioned = False
    for m in messages:
        text = m.get("content", "")
        if booking_re.search(text):
            booking_mentioned = True
        if m.get("role") == "assistant" and brochure_re.search(text):
            if not booking_mentioned:
                return False, "Bot offered brochure before any booking context"
            return True, "Brochure offered after booking (correct order)"
    return True, "n/a (brochure not mentioned)"


def _check_clean_close(messages: list[dict]) -> tuple[bool, str]:
    """Last few bot turns contain a goodbye."""
    goodbye_re = re.compile(
        r"\b(goodbye|bye|dhanyavaad|shukriya|thank you for calling|have a good|alvida)\b",
        re.IGNORECASE,
    )
    bot_turns = [m for m in messages if m.get("role") == "assistant"]
    for m in bot_turns[-3:]:
        if goodbye_re.search(m.get("content", "")):
            return True, "Goodbye/dhanyavaad found in final bot turns"
    return False, "No goodbye found in last 3 bot turns"


def run_deterministic_checks(
    messages: list[dict],
    events: list[dict],
) -> dict[str, dict]:
    checks: dict[str, dict] = {}
    for name, fn in [
        ("visit_date_confirmed", lambda: _check_visit_date_confirmed(messages)),
        ("phone_readback", lambda: _check_phone_readback(messages)),
        ("brochure_order", lambda: _check_brochure_order(messages)),
        ("call_ended_cleanly", lambda: _check_clean_close(messages)),
    ]:
        try:
            passed, detail = fn()
        except Exception as exc:
            passed, detail = False, f"check error: {exc}"
        checks[name] = {"pass": passed, "detail": detail}
    return checks


# ---------------------------------------------------------------------------
# B — Statistical latency checks (Coval-style: real span data, not regex)
# ---------------------------------------------------------------------------

# Thresholds from industry benchmarks for real-time voice AI:
_LLM_P95_THRESHOLD_MS: float = 800.0   # p95 LLM first-token ≤ 800 ms
_TTS_P95_THRESHOLD_MS: float = 250.0   # p95 TTS first-byte ≤ 250 ms


def _check_llm_latency(events: list[dict]) -> tuple[bool, str]:
    """LLM p95 latency ≤ 800 ms. Returns n/a if no latency data present."""
    lats = [float(ev["latency_ms"]) for ev in events
            if ev.get("event") == "llm_turn" and ev.get("latency_ms") is not None]
    if not lats:
        return True, "n/a (no LLM latency data in trajectory)"
    lats_sorted = sorted(lats)
    p95 = lats_sorted[max(0, int(len(lats_sorted) * 0.95) - 1)]
    avg = sum(lats) / len(lats)
    if p95 > _LLM_P95_THRESHOLD_MS:
        return False, f"LLM p95={p95:.0f}ms exceeds {_LLM_P95_THRESHOLD_MS:.0f}ms threshold (avg={avg:.0f}ms, n={len(lats)})"
    return True, f"LLM p95={p95:.0f}ms ok (avg={avg:.0f}ms, n={len(lats)})"


def _check_tts_latency(events: list[dict]) -> tuple[bool, str]:
    """TTS first-byte p95 latency ≤ 250 ms. Returns n/a if no data."""
    lats = [float(ev["latency_ms"]) for ev in events
            if ev.get("event") == "tts" and ev.get("latency_ms") is not None]
    if not lats:
        return True, "n/a (no TTS latency data in trajectory)"
    lats_sorted = sorted(lats)
    p95 = lats_sorted[max(0, int(len(lats_sorted) * 0.95) - 1)]
    avg = sum(lats) / len(lats)
    if p95 > _TTS_P95_THRESHOLD_MS:
        return False, f"TTS p95={p95:.0f}ms exceeds {_TTS_P95_THRESHOLD_MS:.0f}ms threshold (avg={avg:.0f}ms, n={len(lats)})"
    return True, f"TTS p95={p95:.0f}ms ok (avg={avg:.0f}ms, n={len(lats)})"


def run_statistical_checks(events: list[dict]) -> dict[str, dict]:
    checks: dict[str, dict] = {}
    for name, fn in [
        ("llm_latency_p95", lambda: _check_llm_latency(events)),
        ("tts_latency_p95", lambda: _check_tts_latency(events)),
    ]:
        try:
            passed, detail = fn()
        except Exception as exc:
            passed, detail = False, f"check error: {exc}"
        checks[name] = {"pass": passed, "detail": detail}
    return checks


# ---------------------------------------------------------------------------
# C — LLM judge (standardized output schema)
# ---------------------------------------------------------------------------

def _call_groq(prompt: str, model: str = "llama-3.1-8b-instant") -> dict:
    """Call Groq synchronously via httpx (already in requirements)."""
    import httpx

    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return {"error": "GROQ_API_KEY not set; judge skipped"}

    try:
        resp = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 512,
                "response_format": {"type": "json_object"},
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        raw = json.loads(content)

        # Normalize to standard schema: {score, reasoning, passed, confidence}
        # The judge prompts must emit JSON with at least "score" (0.0–1.0).
        score = float(raw.get("score", 0.0))
        reasoning = str(raw.get("reasoning", raw.get("explanation", raw.get("detail", ""))))
        passed = bool(raw.get("passed", score >= 0.7))
        confidence = float(raw.get("confidence", 1.0))
        return {
            "score": round(score, 3),
            "reasoning": reasoning,
            "passed": passed,
            "confidence": round(confidence, 3),
        }
    except Exception as exc:
        return {"error": str(exc), "score": None, "passed": None}


def run_llm_judge(messages: list[dict]) -> dict[str, dict]:
    prompts = _load_prompts()
    transcript_text = _format_transcript(messages)
    results: dict[str, dict] = {}
    for check_name, prompt_template in prompts.items():
        prompt = prompt_template.replace("{transcript}", transcript_text)
        results[check_name] = _call_groq(prompt)
    return results


# ---------------------------------------------------------------------------
# D — Fixture capture (Roark-style closed loop)
# ---------------------------------------------------------------------------

def save_fixture(
    check_name: str,
    call_id: str,
    messages: list[dict],
    events: list[dict],
    detail: str,
) -> Path:
    """
    Snapshot a failing call to evals/fixtures/<check_name>/YYYY-MM-DD-<call_id>.json
    so regression.py replays it on every future run.

    This is the Roark AutoImprove pattern: real-call failures become permanent
    regression fixtures, preventing the same failure from going undetected again.
    """
    fixture_dir = _fixtures_dir() / check_name
    fixture_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    out = fixture_dir / f"{today}-{call_id}.json"
    out.write_text(
        json.dumps(
            {
                "call_id": call_id,
                "check_name": check_name,
                "failure_detail": detail,
                "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "messages": messages,
                "events": events,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return out


# ---------------------------------------------------------------------------
# E — SuperBryn-style Root Cause Attribution (STT vs LLM vs TTS vs CALLER)
# ---------------------------------------------------------------------------

def attribute_root_cause(
    deterministic_checks: dict[str, dict],
    statistical_checks: dict[str, dict],
    llm_judge: dict[str, dict],
    events: list[dict],
) -> dict[str, Any]:
    """
    SuperBryn-style Root Cause Analysis:
    Pinpoints which layer of the voice pipeline broke:
      - 'STT': ASR failed or transcribed empty/corrupted user input
      - 'LLM': Reasoning failure, missed entity capture, hallucination, or prompt drift
      - 'TTS': Synthesis latency SLA breach or audio generation failure
      - 'CALLER': Early disconnect or caller interruption
    """
    failures: list[dict] = []

    # Statistical timing checks
    if not statistical_checks.get("tts_latency_p95", {}).get("pass", True):
        failures.append({"layer": "TTS", "check": "tts_latency_p95", "detail": statistical_checks["tts_latency_p95"]["detail"]})
    if not statistical_checks.get("llm_latency_p95", {}).get("pass", True):
        failures.append({"layer": "LLM", "check": "llm_latency_p95", "detail": statistical_checks["llm_latency_p95"]["detail"]})

    # Deterministic checks
    for check in ("visit_date_confirmed", "phone_readback", "brochure_order"):
        res = deterministic_checks.get(check, {})
        if not res.get("pass", True):
            failures.append({"layer": "LLM", "check": check, "detail": res.get("detail", "")})

    # Call ending check: distinguish caller hangup from bot abruptness
    if not deterministic_checks.get("call_ended_cleanly", {}).get("pass", True):
        if any(ev.get("event") == "interruption" for ev in events):
            failures.append({"layer": "CALLER", "check": "call_ended_cleanly", "detail": "Caller interrupted or hung up before polite close"})
        else:
            failures.append({"layer": "LLM", "check": "call_ended_cleanly", "detail": deterministic_checks["call_ended_cleanly"].get("detail", "")})

    # LLM judge checks
    for check, res in llm_judge.items():
        if res.get("passed") is False or (res.get("score") is not None and res.get("score") < 0.7):
            failures.append({"layer": "LLM", "check": check, "detail": res.get("reasoning", "")})

    if not failures:
        return {"primary_layer": "NONE", "status": "HEALTHY", "failures": []}

    layers = [f["layer"] for f in failures]
    primary = max(set(layers), key=layers.count)
    return {
        "primary_layer": primary,
        "status": "DEGRADED",
        "failure_count": len(failures),
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------

def score_transcript(
    call_id: str,
    messages: list[dict],
    events: list[dict] | None = None,
    run_judge: bool = True,
    capture_fixtures: bool = True,
) -> dict:
    """
    Score one transcript. Returns a full report dict.

    capture_fixtures=True (default): any failing deterministic/statistical check
    automatically snapshots this call to evals/fixtures/ for permanent regression.
    """
    events = events or []
    det = run_deterministic_checks(messages, events)
    stat = run_statistical_checks(events)
    judge = run_llm_judge(messages) if run_judge else {}

    # Capture fixtures for any failing checks (closed loop)
    if capture_fixtures:
        all_checks = {**det, **stat}
        for check_name, result in all_checks.items():
            if not result.get("pass", True):
                try:
                    fixture_path = save_fixture(
                        check_name=check_name,
                        call_id=call_id,
                        messages=messages,
                        events=events,
                        detail=result.get("detail", ""),
                    )
                except Exception:
                    pass  # fixture save errors must never affect the report

    all_checks_combined = {**det, **stat}
    det_pass = sum(1 for v in all_checks_combined.values() if v.get("pass", True))
    det_total = len(all_checks_combined)

    root_cause = attribute_root_cause(det, stat, judge, events)

    report = {
        "call_id": call_id,
        "scored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "deterministic": det,
        "statistical": stat,
        "deterministic_pass_rate": f"{det_pass}/{det_total}",
        "llm_judge": judge,
        "root_cause_analysis": root_cause,
        "overall_pass": det_pass == det_total,
    }
    return report


def save_report(report: dict) -> Path:
    out = _evals_dir() / f"{report['call_id']}.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Eval harness — score one call transcript")
    parser.add_argument("trajectory", nargs="?", help="Path to call_id.jsonl trajectory file")
    parser.add_argument("--call-id", help="Override call_id")
    parser.add_argument("--no-judge", action="store_true", help="Skip LLM judge (deterministic + statistical only)")
    parser.add_argument("--no-fixtures", action="store_true", help="Do not save failure fixtures")
    parser.add_argument("--messages-json", help="JSON string: list of {role, content} dicts")
    args = parser.parse_args(argv)

    if args.messages_json:
        messages = json.loads(args.messages_json)
        events: list[dict] = []
        call_id = args.call_id or "stdin"
    elif args.trajectory:
        path = Path(args.trajectory)
        events = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
        messages = _transcript_from_trajectory(events)
        call_id = args.call_id or path.stem
    else:
        parser.print_help()
        sys.exit(1)

    report = score_transcript(
        call_id,
        messages,
        events,
        run_judge=not args.no_judge,
        capture_fixtures=not args.no_fixtures,
    )
    out_path = save_report(report)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nReport written: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
