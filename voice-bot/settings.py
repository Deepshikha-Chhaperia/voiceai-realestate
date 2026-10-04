"""Startup configuration checks. The only place that decides prod vs. local demo."""

import hmac
import os
import secrets

from loguru import logger

_MIN_SECRET_LEN = 24


def safe_eq(a: str, b: str) -> bool:
    """Constant-time string compare; unlike hmac.compare_digest on str, never raises on non-ASCII."""
    return hmac.compare_digest(a.encode(), b.encode())


def is_local_demo() -> bool:
    return os.getenv("LOCAL_DEMO", "").lower() in ("1", "true")


def validate_startup() -> None:
    """Production (default): required secrets must be set and strong, else raise.
    LOCAL_DEMO: generate missing secrets, bind to loopback only. Auth stays enforced."""
    if is_local_demo():
        if os.getenv("HOST", "127.0.0.1") != "127.0.0.1":
            raise RuntimeError("LOCAL_DEMO refuses to start unless HOST=127.0.0.1")
        for name in ("WS_TOKEN", "DASHBOARD_API_KEY"):
            if not os.getenv(name):
                os.environ[name] = secrets.token_urlsafe(32)
                if name == "DASHBOARD_API_KEY":
                    logger.warning(
                        "LOCAL DEMO ONLY - generated DASHBOARD_API_KEY={} (never use LOCAL_DEMO in production)",
                        os.environ[name],
                    )
        os.environ.setdefault("PUBLIC_HOST", "localhost:8000")
        return

    problems = []
    for name, min_len in (("WS_TOKEN", _MIN_SECRET_LEN), ("DASHBOARD_API_KEY", _MIN_SECRET_LEN), ("PUBLIC_HOST", 1)):
        value = os.getenv(name, "")
        if not value:
            problems.append(f"{name} is not set")
        elif len(value) < min_len:
            problems.append(f"{name} must be at least {min_len} characters")
    if problems:
        for p in problems:
            logger.critical("Startup check failed: {}", p)
        raise RuntimeError("Server refused to start: " + "; ".join(problems))
