"""
evals/regression.py — Offline regression runner (items 10 + 5).

Runs the eval harness over the last N saved trajectories and produces a
pass/fail report. No live calls, no spend (deterministic-only by default;
pass --judge to enable the Groq LLM judge which costs ~$0.001 per call).

With --fixtures (item 5 / Roark closed-loop): also replays all snapshots in
evals/fixtures/ that were auto-captured when real calls failed deterministic
or statistical checks. These fixtures are permanent regression tests — any
future run that passes them means the failure was fixed.

Usage:
    # From voice-bot/ directory:
    python -m evals.regression                        # last 20, deterministic + statistical
    python -m evals.regression --n 50                 # last 50
    python -m evals.regression --judge                # include LLM judge
    python -m evals.regression --fixtures             # also replay failure fixtures
    python -m evals.regression --from-reports         # use existing call_reports/*.md

Output:
    - Console: one line per call (PASS/FAIL + summary of failed checks)
    - File:    <DATA_DIR>/evals/regression_<YYYY-MM-DD>.json

Exit code 0 if all checks pass, 1 if any fail (CI-friendly).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Source: trajectory JSONL files
# ---------------------------------------------------------------------------

def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "outputs"))


def _collect_trajectories(n: int) -> list[Path]:
    """Find the last N trajectory JSONL files, newest first."""
    base = _data_dir() / "trajectories"
    if not base.exists():
        return []
    files: list[Path] = []
    for day_dir in sorted(base.iterdir(), reverse=True):
        if not day_dir.is_dir():
            continue
        for f in sorted(day_dir.glob("*.jsonl"), reverse=True):
            files.append(f)
            if len(files) >= n:
                return files
    return files


def _load_trajectory(path: Path) -> tuple[list[dict], list[dict]]:
    """Returns (messages, raw_events) from a trajectory JSONL file."""
    events: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    messages: list[dict] = []
    for ev in events:
        if ev.get("event") == "stt":
            messages.append({"role": "user", "content": ev.get("text", "")})
        elif ev.get("event") in ("tts", "llm_turn"):
            messages.append({"role": "assistant", "content": ev.get("response_text") or ev.get("text", "")})
    return messages, events


# ---------------------------------------------------------------------------
# Source: saved failure fixtures (item 5 / Roark closed-loop)
# ---------------------------------------------------------------------------

def _collect_fixtures() -> list[tuple[str, list[dict], list[dict], dict]]:
    """
    Load all saved fixture files from evals/fixtures/<check_name>/*.json.

    Fixtures are written by harness.save_fixture() when a real call fails a
    deterministic or statistical check, or by human override in the review UI.
    Returns (call_id, messages, events, metadata).
    """
    fixtures_dir = Path(__file__).parent / "fixtures"
    if not fixtures_dir.exists():
        return []
    results: list[tuple[str, list[dict], list[dict], dict]] = []
    for check_dir in sorted(fixtures_dir.iterdir()):
        if not check_dir.is_dir():
            continue
        for fixture_file in sorted(check_dir.glob("*.json")):
            try:
                data = json.loads(fixture_file.read_text(encoding="utf-8"))
                call_id = f"fixture:{data.get('check_name', check_dir.name)}:{fixture_file.stem}"
                messages = data.get("messages", [])
                events = data.get("events", [])
                meta = {
                    "ground_truth_verdict": data.get("ground_truth_verdict"),
                    "human_override": data.get("human_override"),
                    "check_name": data.get("check_name", check_dir.name),
                }
                results.append((call_id, messages, events, meta))
            except Exception:
                pass
    return results


# ---------------------------------------------------------------------------
# Source: existing call_reports/*.md (fallback for calls before item 8)
# ---------------------------------------------------------------------------

def _collect_from_reports(n: int) -> list[tuple[str, list[dict], list[dict], dict]]:
    """Parse existing Markdown call reports into minimal message lists."""
    reports_dir = _data_dir() / "call_reports"
    if not reports_dir.exists():
        return []
    files = sorted(reports_dir.glob("*.md"), reverse=True)[:n]
    results: list[tuple[str, list[dict], list[dict], dict]] = []
    for f in files:
        call_id = f.stem
        messages: list[dict] = []
        try:
            text = f.read_text(encoding="utf-8")
            import re
            m = re.search(r"##[^#]*Transcript[^\n]*\n(.*?)(\n##|\Z)", text, re.DOTALL | re.IGNORECASE)
            if m:
                for line in m.group(1).strip().splitlines():
                    line = line.strip()
                    if line.startswith("**Caller"):
                        content = re.sub(r"^\*\*[^*]+\*\*:?\s*", "", line)
                        messages.append({"role": "user", "content": content})
                    elif line.startswith("**Pooja") or line.startswith("**Bot") or line.startswith("**Assistant"):
                        content = re.sub(r"^\*\*[^*]+\*\*:?\s*", "", line)
                        messages.append({"role": "assistant", "content": content})
        except Exception:
            pass
        results.append((call_id, messages, [], {}))
    return results


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_regression(
    n: int = 20,
    run_judge: bool = False,
    from_reports: bool = False,
    include_fixtures: bool = False,
) -> dict:
    from evals.harness import score_transcript, save_report

    sources: list[tuple[str, list[dict], list[dict], dict]] = []

    if from_reports:
        sources = _collect_from_reports(n)
        if not sources:
            print("No call reports found — run some calls first or use trajectory files.", file=sys.stderr)
    else:
        paths = _collect_trajectories(n)
        if not paths:
            print(
                f"No trajectory files found under {_data_dir() / 'trajectories'}.\n"
                "Trajectories are written by the Trajectory logger (item 8) for every call.\n"
                "If you have older calls, run with --from-reports instead.",
                file=sys.stderr,
            )
        for path in paths:
            call_id = path.stem
            messages, events = _load_trajectory(path)
            meta: dict[str, Any] = {}
            eval_file = _data_dir() / "evals" / f"{call_id}.json"
            if eval_file.exists():
                try:
                    rep_data = json.loads(eval_file.read_text(encoding="utf-8"))
                    if rep_data.get("human_override"):
                        meta["ground_truth_verdict"] = rep_data["human_override"].get("verdict")
                        meta["human_override"] = rep_data["human_override"]
                except Exception:
                    pass
            sources.append((call_id, messages, events, meta))

    if include_fixtures:
        fixture_sources = _collect_fixtures()
        if fixture_sources:
            print(f"[fixtures] Loaded {len(fixture_sources)} fixture(s) from evals/fixtures/", file=sys.stderr)
            sources.extend(fixture_sources)
        else:
            print("[fixtures] No fixtures found in evals/fixtures/", file=sys.stderr)

    if not sources:
        return {"error": "no_sources", "checked": 0, "passed": 0, "failed": 0, "rows": []}

    rows: list[dict] = []
    passed = 0
    failed = 0

    for item in sources:
        call_id = item[0]
        messages = item[1]
        events = item[2]
        meta = item[3] if len(item) > 3 else {}

        if not messages:
            rows.append({"call_id": call_id, "status": "SKIP", "reason": "empty transcript"})
            continue
        try:
            # Do not re-capture fixtures during a regression run (avoid fixture explosion)
            report = score_transcript(call_id, messages, events, run_judge=run_judge, capture_fixtures=False)
            save_report(report)
            det = {**report.get("deterministic", {}), **report.get("statistical", {})}
            failed_checks = [k for k, v in det.items() if not v.get("pass", True)]
            ok = report.get("overall_pass", True)

            # Coval pattern: respect human ground truth override if present
            ground_truth = meta.get("ground_truth_verdict")
            if ground_truth:
                if ground_truth.lower() == "pass":
                    ok = True
                elif ground_truth.lower() == "fail":
                    ok = False
                    override_reason = (meta.get("human_override") or {}).get("reason", "Manual human override")
                    failed_checks.append(f"human_ground_truth_override: {override_reason}")

            if ok:
                passed += 1
            else:
                failed += 1
            rows.append({
                "call_id": call_id,
                "status": "PASS" if ok else "FAIL",
                "det_pass_rate": report.get("deterministic_pass_rate"),
                "failed_checks": failed_checks,
                "ground_truth_override": meta.get("human_override"),
            })
        except Exception as exc:
            failed += 1
            rows.append({"call_id": call_id, "status": "ERROR", "reason": str(exc)})

    summary = {
        "run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "checked": len(rows),
        "passed": passed,
        "failed": failed,
        "pass_rate": f"{passed}/{len(rows)}",
        "llm_judge_enabled": run_judge,
        "fixtures_included": include_fixtures,
        "rows": rows,
    }
    return summary


def save_regression_report(summary: dict) -> Path:
    evals_dir = _data_dir() / "evals"
    evals_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    out = evals_dir / f"regression_{today}.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def print_regression_report(summary: dict) -> None:
    checked = summary.get("checked", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    fixtures = " (fixtures included)" if summary.get("fixtures_included") else ""
    print(f"\n{'='*60}")
    print(f"REGRESSION: {passed}/{checked} passed  ({failed} failed){fixtures}")
    print(f"{'='*60}")
    for row in summary.get("rows", []):
        status = row["status"]
        call_id = row["call_id"]
        icon = "✓" if status == "PASS" else ("~" if status == "SKIP" else "✗")
        line = f"  {icon} [{status}] {call_id}"
        if row.get("failed_checks"):
            line += f"  — failed: {', '.join(row['failed_checks'])}"
        if row.get("reason"):
            line += f"  ({row['reason']})"
        print(line)
    print()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Offline regression — run eval harness over last N saved calls"
    )
    parser.add_argument("--n", type=int, default=20, help="Number of recent calls to check (default: 20)")
    parser.add_argument("--judge", action="store_true", help="Enable LLM judge (spends Groq tokens)")
    parser.add_argument("--from-reports", action="store_true", help="Use call_reports/*.md instead of trajectories")
    parser.add_argument(
        "--fixtures",
        action="store_true",
        help="Also replay all saved failure fixtures from evals/fixtures/ (Roark closed-loop)",
    )
    args = parser.parse_args(argv)

    summary = run_regression(
        n=args.n,
        run_judge=args.judge,
        from_reports=args.from_reports,
        include_fixtures=args.fixtures,
    )
    print_regression_report(summary)
    out = save_regression_report(summary)
    print(f"Report written: {out}", file=sys.stderr)

    # Exit 1 if any failures (CI-friendly)
    sys.exit(0 if summary.get("failed", 0) == 0 else 1)


if __name__ == "__main__":
    main()
