"""
Local web-test report -- deterministic, instant, zero-cost observability.

WHAT THIS IS: for local and staging calls, writes a complete markdown report
immediately on call teardown summarizing everything the pipeline tracks --
disposition, captured lead slots (working memory), itemized costs, Blended CPM
($/minute), voice-to-voice latencies (median, avg, P90), component latencies
(LLM TTFT, TTS TTFA), and the full transcript.

ZERO-COST / DETERMINISTIC: Does not make slow, redundant post-call LLM calls.
Runs in <1ms and never fails.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

import lead_state

REPORTS_DIR = Path(os.getenv("LOCAL_REPORTS_DIR", "outputs/call_reports"))


def _transcript_text(messages: list[dict]) -> str:
    """Format context messages into a clean, human-readable transcript."""
    lines = []
    for m in messages:
        role = m.get("role")
        if role not in ("user", "assistant"):
            continue
        content = m.get("content")
        if isinstance(content, list):
            content = " ".join(
                part.get("text", "") for part in content if isinstance(part, dict)
            )
        if content:
            speaker = "CALLER" if role == "user" else "AGENT"
            lines.append(f"{speaker}: {content}")
    return "\n".join(lines)


def write_report(
    call_id: str,
    messages: list[dict],
    *,
    lead_memory: dict[str, str] | None = None,
    metrics_summary: dict[str, Any] | None = None,
) -> str | None:
    """Write call report immediately at teardown. Fully synchronous, never raises, never blocks, immune to cancellation."""
    try:
        call = lead_state.get_call(call_id) or {}

        lead_fields = json.loads(call.get("lead_fields") or "{}")
        if lead_memory:
            lead_fields.update({k: v for k, v in lead_memory.items() if v and not k.startswith("_")})

        cost_breakdown = json.loads(call.get("cost_breakdown_json") or "{}")
        if not cost_breakdown and metrics_summary:
            cost_breakdown = metrics_summary.get("cost_breakdown") or {}

        def _iso(ts):
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "—"

        # Robust duration & timestamp derivation
        duration_s = None
        if metrics_summary and metrics_summary.get("duration_s") and float(metrics_summary["duration_s"]) > 0:
            duration_s = round(float(metrics_summary["duration_s"]), 1)
        elif call.get("ended_at") and call.get("started_at"):
            duration_s = round(call["ended_at"] - call["started_at"], 1)

        ended_ts = call.get("ended_at")
        if not ended_ts and call.get("started_at") and duration_s:
            ended_ts = call["started_at"] + duration_s
        elif not ended_ts:
            ended_ts = time.time()

        USD_TO_INR = 95.83

        cost_usd = (
            call.get("cost_usd")
            if call.get("cost_usd") is not None
            else (metrics_summary.get("cost_usd") if metrics_summary else None)
        )

        if cost_usd is None and messages:
            # Fallback estimation using configured rates
            try:
                import yaml
                base_cfg = {}
                cfg_p = Path(__file__).parent / "config.yaml"
                if cfg_p.exists():
                    with open(cfg_p, "r", encoding="utf-8") as fp:
                        base_cfg = yaml.safe_load(fp) or {}
                rates = base_cfg.get("cost_rates", {})
                stt_p = (call.get("stt_provider") or "sarvam").lower()
                tts_p = (call.get("tts_provider") or "sarvam").lower()
                llm_p = (call.get("llm_provider") or "groq").lower()

                calc_dur = duration_s or 60.0
                stt_rate = rates.get("stt", {}).get(stt_p, {}).get("per_second", 0.00008696)
                tts_rate = rates.get("tts", {}).get(tts_p, {}).get("per_character", 0.00003131)
                llm_rates = rates.get("llm", {}).get(llm_p, {}) or rates.get("llm", {}).get(f"{llm_p}:qwen/qwen3.8-27b", {})
                p_rate = llm_rates.get("prompt_per_mtok", 0.80)
                c_rate = llm_rates.get("completion_per_mtok", 4.00)

                asst_chars = sum(len(str(m.get("content", ""))) for m in messages if m.get("role") == "assistant")
                total_chars = sum(len(str(m.get("content", ""))) for m in messages)
                c_toks = max(1, asst_chars // 4)
                p_toks = max(1, 800 + (total_chars // 4))

                stt_c = round(calc_dur * stt_rate, 6)
                tts_c = round(asst_chars * tts_rate, 6)
                llm_c = round((p_toks / 1_000_000 * p_rate) + (c_toks / 1_000_000 * c_rate), 6)
                cost_usd = round(stt_c + tts_c + llm_c, 6)
                if not cost_breakdown:
                    cost_breakdown = {"stt": stt_c, "tts": tts_c, "llm": llm_c}
            except Exception as exc:
                logger.debug("Fallback cost calculation note: {}", exc)

        cost_inr = (cost_usd * USD_TO_INR) if cost_usd is not None else None

        cpm_str = "—"
        if cost_usd is not None and duration_s and duration_s > 0:
            duration_min = duration_s / 60.0
            cpm = cost_usd / duration_min
            cpm_inr = cpm * USD_TO_INR
            cpm_str = f"₹{cpm_inr:.2f}/min (${cpm:.4f}/min)"

        disposition_val = call.get("disposition")
        if not disposition_val or disposition_val == "not set":
            disposition_val = lead_state.infer_deterministic_disposition(lead_memory, messages)

        avg_voice_latency = (
            call.get("avg_voice_latency_ms")
            or (metrics_summary.get("avg_voice_latency_ms") if metrics_summary else None)
        )
        median_voice_latency = (
            call.get("median_voice_latency_ms")
            or (metrics_summary.get("median_voice_latency_ms") if metrics_summary else None)
        )
        p90_voice_latency = (
            call.get("p90_voice_latency_ms")
            or (metrics_summary.get("p90_voice_latency_ms") if metrics_summary else None)
        )
        avg_llm_ttft = (
            call.get("avg_llm_ttft_ms")
            or (metrics_summary.get("avg_llm_ttft_ms") if metrics_summary else None)
        )
        avg_tts_ttfa = (
            call.get("avg_tts_ttfa_ms")
            or (metrics_summary.get("avg_tts_ttfa_ms") if metrics_summary else None)
        )
        avg_tts_ttfa_effective = (
            call.get("avg_tts_ttfa_effective_ms")
            or (metrics_summary.get("avg_tts_ttfa_effective_ms") if metrics_summary else None)
            or avg_tts_ttfa
        )
        avg_tts_ttfa_raw = (
            call.get("avg_tts_ttfa_raw_ms")
            or (metrics_summary.get("avg_tts_ttfa_raw_ms") if metrics_summary else None)
        )
        ttfa_display = f"{avg_tts_ttfa_effective} ms" if avg_tts_ttfa_effective is not None else (f"{avg_tts_ttfa} ms" if avg_tts_ttfa else "—")
        if avg_tts_ttfa_raw and avg_tts_ttfa_effective and avg_tts_ttfa_raw != avg_tts_ttfa_effective:
            ttfa_display = f"{avg_tts_ttfa_effective} ms (Effective post-trim | Raw pre-trim: {avg_tts_ttfa_raw} ms)"

        turns = (
            call.get("turns")
            or (metrics_summary.get("turns") if metrics_summary else None)
        )

        avg_stt_final = (
            call.get("avg_stt_final_ms")
            or (metrics_summary.get("avg_stt_final_ms") if metrics_summary else None)
        )
        cache_hit_pct = (
            metrics_summary.get("cache_hit_pct") if metrics_summary else None
        )
        cache_hits = (
            metrics_summary.get("cache_hits") if metrics_summary else 0
        )
        tts_chars_per_min = (
            metrics_summary.get("tts_chars_per_min") if metrics_summary else None
        )

        lines = [
            f"# Call report — `{call_id}`",
            "",
            f"- **Started:** {_iso(call.get('started_at'))}",
            f"- **Ended:** {_iso(ended_ts)}",
            f"- **Duration:** {duration_s}s" if duration_s is not None else "- **Duration:** —",
            f"- **Campaign:** {call.get('campaign_id') or '—'}",
            f"- **Disposition:** `{disposition_val or 'INCOMPLETE'}`",
            f"- **Avg voice-to-voice latency:** {avg_voice_latency or '—'} ms",
            f"- **Median voice-to-voice latency:** {median_voice_latency or '—'} ms",
            f"- **P90 voice-to-voice latency:** {p90_voice_latency or '—'} ms",
            f"- **Latency split:** STT Final: {avg_stt_final or '—'} ms | LLM TTFT: {avg_llm_ttft or '—'} ms | TTS TTFA: {ttfa_display}",
            f"- **Phrase Cache Hit:** {cache_hit_pct if cache_hit_pct is not None else '—'}% ({cache_hits} hits)",
            f"- **TTS Characters / min:** {tts_chars_per_min if tts_chars_per_min is not None else '—'}",
            f"- **Turns:** {turns or '—'}",
            f"- **Estimated cost:** ₹{cost_inr:.4f} (${cost_usd:.6f})" if cost_usd is not None else "- **Estimated cost:** not available",
            f"- **Blended CPM (Cost per Minute):** {cpm_str}",
        ]

        if cost_breakdown:
            lines.append("")
            lines.append("## Cost breakdown (Entire call total)")
            llm_c = float(cost_breakdown.get("llm", 0.0))
            stt_c = float(cost_breakdown.get("stt", 0.0))
            tts_c = float(cost_breakdown.get("tts", 0.0))
            total_c = llm_c + stt_c + tts_c
            total_calc = total_c if total_c > 0 else (cost_usd or 1.0)
            duration_min = (duration_s / 60.0) if (duration_s and duration_s > 0) else 1.0

            llm_inr = llm_c * USD_TO_INR
            stt_inr = stt_c * USD_TO_INR
            tts_inr = tts_c * USD_TO_INR
            total_inr = total_c * USD_TO_INR

            stt_p = (
                (metrics_summary.get("stt_provider") if metrics_summary else None)
                or call.get("stt_provider")
                or "deepgram"
            )
            tts_p = (
                (metrics_summary.get("tts_provider") if metrics_summary else None)
                or call.get("tts_provider")
                or "elevenlabs"
            )
            llm_p = (
                (metrics_summary.get("llm_provider") if metrics_summary else None)
                or call.get("llm_provider")
                or "groq"
            )

            llm_label = {
                "groq": "Groq Qwen 3.8-27B",
                "cerebras": "Cerebras Qwen 3.8-27B",
                "openrouter": "OpenRouter Qwen 3.6-27B",
                "sarvam": "Sarvam 105B",
                "openai": "OpenAI",
                "deepseek": "DeepSeek",
            }.get(str(llm_p).lower(), str(llm_p).title())

            stt_label = {
                "deepgram": "Deepgram Nova-3",
                "sarvam": "Sarvam Saaras v4",
                "whisper": "OpenAI Whisper",
                "openai": "OpenAI Whisper",
            }.get(str(stt_p).lower(), str(stt_p).title())

            tts_label = {
                "elevenlabs": "ElevenLabs Turbo v2.5",
                "sarvam": "Sarvam Bulbul v3",
                "murf": "Murf Falcon 2",
                "deepgram": "Deepgram Aura",
                "cartesia": "Cartesia Sonic",
            }.get(str(tts_p).lower(), str(tts_p).title())

            lines.append(f"- **LLM ({llm_label}):** ₹{llm_inr:.4f} (${llm_c:.6f}) ({llm_c / total_calc * 100:.1f}% of call | ₹{(llm_inr / duration_min):.2f}/min)")
            lines.append(f"- **STT ({stt_label}):** ₹{stt_inr:.4f} (${stt_c:.6f}) ({stt_c / total_calc * 100:.1f}% of call | ₹{(stt_inr / duration_min):.2f}/min)")
            lines.append(f"- **TTS ({tts_label}):** ₹{tts_inr:.4f} (${tts_c:.6f}) ({tts_c / total_calc * 100:.1f}% of call | ₹{(tts_inr / duration_min):.2f}/min)")
            for k, v in cost_breakdown.items():
                if k not in ("llm", "stt", "tts", "stt_provider", "tts_provider", "llm_provider"):
                    v_val = float(v) if isinstance(v, (int, float, str)) and str(v).replace(".", "").isdigit() else 0.0
                    lines.append(f"- **{k}:** ₹{(v_val * USD_TO_INR):.4f} (${v_val:.6f})")
            lines.append(f"- **Total Estimated Cost (Entire call):** ₹{total_inr:.4f} (${total_c:.6f})")

        analysis = json.loads(call.get("analysis_json") or "{}")
        if analysis and any(analysis.values()):
            lines.append("")
            lines.append("## Post-call AI Analysis")
            if analysis.get("recommended_disposition"):
                lines.append(f"- **Recommended Disposition:** `{analysis.get('recommended_disposition')}`")
            if analysis.get("resolution_summary"):
                lines.append(f"- **Resolution Summary:** {analysis.get('resolution_summary')}")
            if analysis.get("main_issue_or_intent"):
                lines.append(f"- **Primary Intent / Concern:** {analysis.get('main_issue_or_intent')}")
            if analysis.get("sentiment"):
                lines.append(f"- **Sentiment:** {analysis.get('sentiment')}")
            if analysis.get("objections"):
                obs = analysis.get("objections")
                obs_str = ", ".join(obs) if isinstance(obs, list) else str(obs)
                lines.append(f"- **Objections:** {obs_str}")

        lines.append("")
        lines.append("## Lead preferences captured (Working Memory)")
        if lead_fields:
            for k, v in sorted(lead_fields.items()):
                lines.append(f"- **{k.capitalize()}:** {v}")
        else:
            lines.append("_None captured in this call._")

        lines.append("")
        lines.append("## Transcript")
        lines.append("```")
        lines.append(_transcript_text(messages) or "(empty)")
        lines.append("```")

        safe_call_id = re.sub(r"[^a-zA-Z0-9_\-]", "", str(call_id)) or "unknown"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORTS_DIR / f"{safe_call_id}.md"
        path.write_text("\n".join(lines), encoding="utf-8")

        # Convenience copy so you never have to look up the call_id at all --
        # just open the same filename after every test call.
        latest = REPORTS_DIR / "latest.md"
        latest.write_text("\n".join(lines), encoding="utf-8")

        logger.info("[{}] Local test report written to {}", call_id, path)
        return str(path)
    except Exception as e:
        logger.warning("[{}] Failed to write local test report: {}", call_id, e)
        return None


async def write_report_async(
    call_id: str,
    messages: list[dict],
    *,
    lead_memory: dict[str, str] | None = None,
    metrics_summary: dict[str, Any] | None = None,
) -> str | None:
    """Async wrapper around write_report for compatibility."""
    return write_report(
        call_id,
        messages,
        lead_memory=lead_memory,
        metrics_summary=metrics_summary,
    )

