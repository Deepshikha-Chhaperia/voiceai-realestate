"""
monitor.py — Lightweight health checker for the voicebot stack.

Checks every MONITOR_INTERVAL seconds (default: 60):
  • GET /health on the api container (checks postgres, redis, worker status)
  • Sends a Telegram alert when any component goes down or comes back up.

Uses only existing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env vars.
No new paid services. No extra Python packages beyond stdlib + urllib.

Run by the `monitor` compose service:
    entrypoint: ["python", "/scripts/monitor.py"]
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

TARGET = os.environ.get("MONITOR_TARGET", "http://api:8000/health")
INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "60"))
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
PUBLIC_HOST = os.environ.get("PUBLIC_HOST", "api:8000").strip()


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def send_telegram(text: str) -> None:
    if not TOKEN or not CHAT_ID:
        print(f"[monitor] Telegram not configured — alert (mock):\n{text}", flush=True)
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    body = json.dumps({"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                print(f"[monitor] Telegram returned {resp.status}", flush=True)
    except Exception as exc:
        print(f"[monitor] Telegram send failed: {exc}", flush=True)


def check_health() -> tuple[bool, str, dict]:
    """Returns (is_healthy, reason, payload)."""
    try:
        with urllib.request.urlopen(TARGET, timeout=8) as resp:
            payload = json.loads(resp.read())
            overall = payload.get("status") == "healthy"
            if not overall:
                unhealthy = [k for k, v in payload.items() if k != "status" and v not in ("ok", True, "healthy")]
                reason = "degraded: " + ", ".join(unhealthy) if unhealthy else "status != healthy"
            else:
                reason = "ok"
            return overall, reason, payload
    except urllib.error.URLError as exc:
        return False, f"connection failed: {exc.reason}", {}
    except Exception as exc:
        return False, f"error: {exc}", {}


def main() -> None:
    print(f"[monitor] started — checking {TARGET} every {INTERVAL}s", flush=True)
    last_healthy: bool | None = None

    while True:
        is_healthy, reason, payload = check_health()
        ts = utcnow()

        if is_healthy != last_healthy:
            if is_healthy:
                msg = (
                    f"✅ VOICEBOT RECOVERED\n"
                    f"Host: {PUBLIC_HOST}\n"
                    f"Time: {ts}\n"
                    f"Status: {json.dumps(payload, indent=2)}"
                )
                print(f"[monitor] {ts} RECOVERED", flush=True)
            else:
                msg = (
                    f"🚨 VOICEBOT DOWN\n"
                    f"Host: {PUBLIC_HOST}\n"
                    f"Time: {ts}\n"
                    f"Reason: {reason}\n"
                    f"Payload: {json.dumps(payload, indent=2) if payload else '(no response)'}"
                )
                print(f"[monitor] {ts} DOWN — {reason}", flush=True)
            send_telegram(msg)
            last_healthy = is_healthy
        else:
            # Repeat alert every 10 minutes if still down
            if not is_healthy:
                print(f"[monitor] {ts} still down — {reason}", flush=True)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
