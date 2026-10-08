"""
Live Call Evidence Extractor for Voice Bot Hardening v5.

Extracts and displays complete timestamped verification evidence for a given call_id:
- Joined Lead name & phone from DB
- SiteVisit row with ISO date, original date, slot, config, WhatsApp status & message_id
- Outbox state and payload
- Raw timestamped log lines for critical checkpoints:
  * Opening intro playback (turn 1 only)
  * Short noise / VAD rejection (no bot response)
  * Name capture & recall after pruning
  * Pruned history events and [ACTIVE LEAD STATE: ...] working memory sync
  * Flat selection ("3 BHK Large with study") & bot memory recall
  * Date normalization and DB read-back verification
  * Tool calls ok / failed counts
  * WhatsApp opt-in question and dispatch status
  * Barge-in interruption
  * Farewell completion and graceful hangup
  * Per-turn STT/LLM/TTS latencies
"""

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import select

from leads.db import get_session
from leads.models import Lead, SiteVisit, Touchpoint, OutboxItem


def find_call_log_file(call_id: str, logs_dir: Path) -> Path | None:
    """Finds the log file corresponding to a call_id."""
    if not logs_dir.exists():
        return None

    # 1. Direct file match: call_{call_label}_{stream_id}.log
    for f in logs_dir.glob(f"call_*_{call_id}.log"):
        return f

    # 2. File with call_id in name
    for f in logs_dir.glob(f"*{call_id}*.log"):
        return f

    # 3. Search inside voicebot_*.log files for call_id
    for f in sorted(logs_dir.glob("voicebot_*.log"), reverse=True):
        try:
            with open(f, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if call_id in line:
                        return f
        except Exception:
            continue

    return None


def get_latest_live_call_id(logs_dir: Path) -> str | None:
    """Finds the latest call_id from call log files."""
    if not logs_dir.exists():
        return None
    call_logs = sorted(
        [f for f in logs_dir.glob("call_web-*.log")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if call_logs:
        # Extract call_id from filename: call_web-<uuid>_<call_id>.log
        match = re.search(r"(web-[a-f0-9-]+)", call_logs[0].name)
        if match:
            return match.group(1)
    return None


async def extract_call_evidence(target_call_id: str | None = None):
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / "logs"

    call_id = target_call_id
    if not call_id:
        call_id = get_latest_live_call_id(logs_dir)

    async with get_session() as session:
        # If still no call_id, check DB
        if not call_id:
            stmt_sv_latest = select(SiteVisit).order_by(SiteVisit.created_at.desc()).limit(1)
            latest_sv = (await session.execute(stmt_sv_latest)).scalars().first()
            if latest_sv and latest_sv.call_id:
                call_id = latest_sv.call_id

        if not call_id:
            print("ERROR: No call_id specified and no call logs or SiteVisit rows found.")
            return

        print("=" * 100)
        print(f"VOICE BOT HARDENING v5 - EVIDENCE REPORT FOR CALL: {call_id}")
        print("=" * 100)

        # 1. DATABASE EVIDENCE
        stmt_sv = select(SiteVisit).where(SiteVisit.call_id == call_id)
        sv = (await session.execute(stmt_sv)).scalars().first()

        lead = None
        if sv and sv.lead_id:
            lead = await session.get(Lead, sv.lead_id)

        stmt_tp = select(Touchpoint).where(Touchpoint.call_id == call_id).order_by(Touchpoint.occurred_at.asc())
        touchpoints = (await session.execute(stmt_tp)).scalars().all()

        stmt_ob = select(OutboxItem).order_by(OutboxItem.created_at.desc()).limit(10)
        outbox_items = (await session.execute(stmt_ob)).scalars().all()
        call_outbox = [ob for ob in outbox_items if (ob.payload or {}).get("call_id") == call_id]

        print("\n--- 1. DATABASE RECORD (JOINED LEAD & SITE VISIT) ---")
        if lead:
            print(f"Lead ID:               {lead.id}")
            print(f"Lead Name:             {lead.name}")
            print(f"Lead Phone:            {lead.phone}")
            print(f"Lead Status:           {lead.status}")
            print(f"Visit Genuine:         {lead.visit_genuine}")
        else:
            print("Lead:                  No Lead record linked to this call_id.")

        if sv:
            print(f"SiteVisit ID:          {sv.id}")
            print(f"Visit Date (ISO):      {sv.visit_date_iso}")
            print(f"Visit Date (Original): {sv.visit_date_original}")
            print(f"Time Slot:             {sv.time_slot}")
            print(f"Configuration:         {sv.configuration}")
            print(f"Booking Status:        {sv.status}")
            print(f"WhatsApp Opt-In:       {sv.whatsapp_opt_in}")
            print(f"WhatsApp Status:       {sv.whatsapp_status}")
            print(f"WhatsApp Message ID:   {sv.whatsapp_message_id}")
            print(f"Created At:            {sv.created_at}")
        else:
            print("SiteVisit:             No SiteVisit record found for this call_id.")

        if touchpoints:
            print(f"Touchpoints Recorded:  {len(touchpoints)}")
            for tp in touchpoints:
                print(f"  - [{tp.occurred_at}] kind={tp.kind} summary={tp.summary}")

        if call_outbox:
            print(f"Outbox Items:          {len(call_outbox)}")
            for ob in call_outbox:
                print(f"  - ID={ob.id} target={ob.target} status={ob.status} attempts={ob.attempts} error={ob.last_error}")
                print(f"    Payload: {json.dumps(ob.payload)}")

    # 2. LOG FILE EVIDENCE
    print("\n--- 2. TIMESTAMPED RAW LOG EVIDENCE ---")
    log_file = find_call_log_file(call_id, logs_dir)
    if not log_file or not log_file.exists():
        print(f"Log file for call_id '{call_id}' not found in {logs_dir}.")
        print("=" * 100)
        return

    print(f"Log File Source: {log_file}")
    with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
        log_lines = fh.readlines()

    relevant_patterns = [
        ("OPENING INTRO", re.compile(r"AudioCache.*opening_intro", re.IGNORECASE)),
        ("VAD / USER SPEECH", re.compile(r"(Sarvam VAD|START_SPEECH|END_SPEECH|User started speaking|User stopped speaking)", re.IGNORECASE)),
        ("STT TRANSCRIPT", re.compile(r"(TranscriptionFrame|final transcript|transcript=)", re.IGNORECASE)),
        ("PRUNING & WORKING MEMORY", re.compile(r"(Pruned conversation history|ACTIVE LEAD STATE|Synced working memory)", re.IGNORECASE)),
        ("BOOK SITE VISIT TOOL", re.compile(r"(Tool book_site_visit called|book_site_visit normalized|Verified SiteVisit in DB|book_site_visit missing)", re.IGNORECASE)),
        ("WHATSAPP INTEGRATION", re.compile(r"(WhatsApp sent|WhatsApp API call|status=not_configured|queued.*whatsapp|Meta error)", re.IGNORECASE)),
        ("BARGE-IN INTERRUPTION", re.compile(r"(Barge-in interruption|purged.*stale audio)", re.IGNORECASE)),
        ("CLOSING & HANGUP", re.compile(r"(Farewell detected|Farewell finished|Hangup completed reason|hangup grace_seconds)", re.IGNORECASE)),
        ("METRICS SUMMARY & REWINDS", re.compile(r"(call_summary|DUPLICATE_ASSISTANT_RECORD|CONTEXT_REWIND|tool_calls_ok|tool_calls_failed)", re.IGNORECASE)),
    ]

    matched_by_cat = {cat: [] for cat, _ in relevant_patterns}
    turn_latencies = []

    for line in log_lines:
        # Check call_id relevance if parsing shared log
        if "voicebot_" in log_file.name and call_id not in line:
            continue

        clean_line = line.strip()
        for cat, pat in relevant_patterns:
            if pat.search(clean_line):
                matched_by_cat[cat].append(clean_line)

        # Per-turn latencies
        if "event=stt_final" in clean_line or "event=llm_time_to_first_token" in clean_line or "event=tts_time_to_first_audio" in clean_line or "event=tts_silence_trimmed" in clean_line:
            turn_latencies.append(clean_line)

    for cat, lines in matched_by_cat.items():
        print(f"\n[SECTION: {cat}] ({len(lines)} lines)")
        if not lines:
            print("  (None found)")
        else:
            for l in lines[:15]:  # print up to 15 per section
                print(f"  {l}")
            if len(lines) > 15:
                print(f"  ... (+{len(lines) - 15} more lines)")

    print(f"\n[SECTION: PER-TURN LATENCY EVENTS] ({len(turn_latencies)} events)")
    for l in turn_latencies[:20]:
        print(f"  {l}")

    print("\n" + "=" * 100)
    print("EVIDENCE EXTRACTION COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract live call evidence for hardening v5 verification")
    parser.add_argument("--call-id", "-c", type=str, default=None, help="Call ID to extract evidence for (e.g. web-06adb506...)")
    args = parser.parse_args()

    asyncio.run(extract_call_evidence(args.call_id))
