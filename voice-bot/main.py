import asyncio
import hmac
import json
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from loguru import logger
import redis.asyncio as redis
import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator

from bot import run_bot, warmup_providers
import lead_state
import leads.api
import leads.dashboard
import leads.db

class CallManager:
    """
    Manages campaign state for calls. 
    Tries to use Redis for production readiness/scale.
    Falls back to in-memory dictionary if Redis is unavailable (dev mode/failure).
    """
    def __init__(self):
        self.redis: redis.Redis | None = None
        self.memory: dict[str, dict] = {}
    
    async def connect(self, url: str = "redis://localhost"):
        try:
            client = redis.from_url(url, decode_responses=True)
            await client.ping()
            self.redis = client
            logger.info(f"Connected to Redis at {url}")
        except Exception as e:
            self.redis = None
            env = os.getenv("ENV", "dev")
            if env == "prod":
                logger.error(f"Redis connection failed in prod: {e}")
                raise RuntimeError(f"Redis is required in prod: {e}")
            logger.warning(f"Redis connection failed ({e}). using IN-MEMORY storage.")

    async def get(self, call_id: str) -> dict | None:
        if self.redis:
            try:
                data = await self.redis.get(f"call:{call_id}")
                return json.loads(data) if data else None
            except Exception as e:
                logger.error(f"Redis get failed: {e}")
                return self.memory.get(call_id)
        return self.memory.get(call_id)

    async def save(self, call_id: str, data: dict, ttl: int = 300):
        if self.redis:
            try:
                await self.redis.setex(f"call:{call_id}", ttl, json.dumps(data))
                return
            except Exception as e:
                logger.error(f"Redis save failed: {e}")
        self.memory[call_id] = data

    async def delete(self, call_id: str):
        if self.redis:
            try:
                await self.redis.delete(f"call:{call_id}")
            except Exception as e:
                logger.error(f"Redis delete failed: {e}")
        self.memory.pop(call_id, None)

    async def close(self):
        if self.redis:
            await self.redis.aclose()

CONFIG = None
CALL_MANAGER = CallManager()

# Concurrency limit to bound simultaneous pipeline instances and protect resources
MAX_CONCURRENT_CALLS = int(os.getenv("MAX_CONCURRENT_CALLS", "50"))
_call_semaphore: asyncio.Semaphore | None = None


def get_call_semaphore() -> asyncio.Semaphore:
    global _call_semaphore
    if _call_semaphore is None:
        _call_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CALLS)
    return _call_semaphore


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize AI services once at startup, tear down on shutdown.
    This runs BEFORE the first request is served. If any service fails to
    initialize (bad API key, missing package), the server won't start.
    """
    global CONFIG

    is_prod = os.getenv("ENV", "prod").lower() == "prod"
    is_local_dev = os.getenv("LOCAL_DEV", "").lower() == "true"

    if is_prod:
        missing = []
        if not os.getenv("META_APP_SECRET"):
            missing.append("META_APP_SECRET (required to verify Meta webhook signatures)")
        if not os.getenv("DASHBOARD_API_KEY") and not is_local_dev:
            missing.append("DASHBOARD_API_KEY (set LOCAL_DEV=true to allow open access in dev)")
        if not os.getenv("WS_TOKEN"):
            missing.append("WS_TOKEN (required to authenticate WebSocket /ws connections)")
        if missing:
            for m in missing:
                logger.critical("Missing required env var in production: {}", m)
            raise RuntimeError(
                "Server refused to start: missing required env vars.\n"
                + "\n".join(f"  - {m}" for m in missing)
            )

    market = os.getenv("MARKET", "india").lower()
    logger.info(f"Initializing voice bot services for market: {market}...")

    # Load base config first
    base_config = {}
    base_config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    if os.path.exists(base_config_path):
        with open(base_config_path, "r", encoding="utf-8") as f:
            base_config = yaml.safe_load(f) or {}

    # Load market profile config (e.g. profiles/india.yaml)
    profile_path = os.path.join(os.path.dirname(__file__), "profiles", f"{market}.yaml")
    if os.path.exists(profile_path):
        with open(profile_path, "r", encoding="utf-8") as f:
            profile_config = yaml.safe_load(f) or {}
        CONFIG = {**base_config, **profile_config}
        if "cost_rates" in base_config and "cost_rates" not in profile_config:
            CONFIG["cost_rates"] = base_config["cost_rates"]
        logger.info(f"Loaded profile config from {profile_path}")
    else:
        CONFIG = base_config
        logger.info("Loaded default config.yaml")

    # Initialize Redis (or fallback)
    redis_url = os.getenv("REDIS_URL", "redis://localhost")
    await CALL_MANAGER.connect(redis_url)

    # Durable call-record store (SQLite fallback for legacy calls)
    lead_state.init_db()

    # Async PostgreSQL Leads DB schema init
    try:
        await leads.db.init_models()
        logger.info("PostgreSQL Leads database initialized successfully.")
    except Exception as e:
        logger.warning(f"Leads database initialization warning: {e}")

    # Pre-load all active provider classes once at boot so per-call start is instantaneous (<500ms).
    try:
        from bot import ServiceFactory
        active = CONFIG.get("active_providers", {})
        providers = CONFIG.get("providers", {})
        for slot, name in active.items():
            class_path = providers.get(slot, {}).get(name, {}).get("class_path")
            if class_path:
                ServiceFactory._import_class(class_path)
        logger.info("Active AI provider classes pre-warmed at boot.")

        # Pre-warm standby provider instances so incoming calls start in <300ms
        from bot import _replenish_standby
        asyncio.create_task(_replenish_standby(CONFIG, active, 8000, None))
        asyncio.create_task(_replenish_standby(CONFIG, active, 16000, None))
    except Exception as e:
        logger.warning(f"Provider pre-warming warning: {e}")

    logger.info("Voice bot config loaded.")
    yield
    
    await CALL_MANAGER.close()
    try:
        from bot import _vobiz_http_client
        await _vobiz_http_client.aclose()
    except Exception:
        pass
    logger.info("Shutting down...")


app = FastAPI(title="Plug-and-Play Voice Bot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Leads Webhooks and Dashboard routers
app.include_router(leads.api.router)
app.include_router(leads.dashboard.router)


# INBOUND CALL HANDLER
@app.post("/inbound")
async def handle_inbound_call(request: Request):
    """
    When someone calls our Vobiz phone number, Vobiz sends a POST request.
    We respond with VXML (XML instructions) telling Vobiz to Open a bidirectional audio stream to our WebSocket endpoint.
    
    The audio format is µ-law at 8kHz — the standard for telephone networks
    VobizFrameSerializer handles the conversion to PCM that Pipecat expects.
    """
    # Determine the WebSocket URL from the incoming request's host header.
    host = request.headers.get("host")
    scheme = "wss" if request.url.scheme == "https" else "ws"
    stream_url = f"{scheme}://{host}/ws"

    # Instruct Vobiz to open a bidirectional 8kHz µ-law audio stream
    vxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true" audioTrack="inbound" contentType="audio/x-mulaw;rate=8000" keepCallAlive="true">{stream_url}</Stream>
</Response>"""

    logger.info(f"Inbound call directed to {stream_url}")
    return Response(content=vxml, media_type="application/xml")


# OUTBOUND CALL HANDLER
E164_PHONE_PATTERN = re.compile(r"^\+[1-9]\d{6,14}$")


class OutboundCallRequest(BaseModel):
    to: str  # Phone number to call
    customer_name: str = "there"  # Name for greeting personalization
    campaign_prompt: str | None = None  # Override campaign prompt (if None, uses config.yaml)
    greeting: str | None = None  # Override greeting (if None, uses config.yaml)
    campaign_id: str | None = None  # Optional campaign identifier for metrics rollup
    lead_id: str | None = None

    @field_validator("to")
    @classmethod
    def validate_phone_number(cls, v: str) -> str:
        """Validate that 'to' is a valid E.164 phone number."""
        if not E164_PHONE_PATTERN.match(v):
            raise ValueError(
                f"Invalid phone number format: '{v}'. Must be a valid E.164 phone number (e.g. +919876543210)"
            )
        return v


async def initiate_outbound_call(
    to_phone: str,
    customer_name: str = "Alex",
    campaign_id: str | None = None,
    campaign_prompt: str | None = None,
    greeting: str | None = None,
    lead_id: str | None = None,
    host: str | None = None,
    scheme: str = "http",
) -> dict:
    """Core helper to initiate an outbound call via Vobiz REST API or mock in local testing."""
    call_id = str(uuid.uuid4())
    camp_id = campaign_id or f"uncategorized-{datetime.now().strftime('%Y-%m-%d')}"
    initial_data = {
        "customer_name": customer_name,
        "campaign_prompt": campaign_prompt,
        "greeting": greeting,
        "campaign_id": camp_id,
        "lead_id": lead_id,
    }
    await CALL_MANAGER.save(call_id, initial_data)
    logger.info(f"[{call_id}] Campaign data stored (lead_id={lead_id})")

    lead_state.upsert_call(
        call_id,
        campaign_id=camp_id,
        phone=to_phone,
        customer_name=customer_name,
        lead_id=lead_id,
    )

    host_val = host or os.getenv("PUBLIC_HOST", "localhost:8000")
    answer_url = f"{scheme}://{host_val}/outbound-answer?call_id={call_id}"

    auth_id = os.getenv("VOBIZ_AUTH_ID")
    auth_token = os.getenv("VOBIZ_AUTH_TOKEN")
    from_number = os.getenv("VOBIZ_FROM_NUMBER")

    if not all([auth_id, auth_token, from_number]):
        logger.info(f"Vobiz credentials not configured. Created call session for {to_phone} (call_id={call_id})")
        return {"status": "success", "call_id": call_id, "answer_url": answer_url, "simulated": True}

    vobiz_url = f"https://api.vobiz.ai/api/v1/Account/{auth_id}/Call/"
    payload = {
        "from": from_number,
        "to": to_phone,
        "answer_url": answer_url,
        "answer_method": "POST",
    }
    headers = {
        "X-Auth-ID": auth_id,
        "X-Auth-Token": auth_token,
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.post(vobiz_url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            provider_call_id = result.get("callId") or result.get("call_uuid") or result.get("uuid")
            if provider_call_id:
                campaign_data = await CALL_MANAGER.get(call_id)
                if campaign_data:
                    campaign_data["provider_call_id"] = provider_call_id
                    await CALL_MANAGER.save(call_id, campaign_data)
                lead_state.upsert_call(call_id, provider_call_id=provider_call_id)
            logger.info(f"Outbound call initiated to {to_phone}, answer_url={answer_url}")
            return {"status": "success", "call_id": call_id, "vobiz_response": result}
        except httpx.HTTPStatusError as e:
            error_body = e.response.text
            logger.error(f"Vobiz API error: {e} | Response: {error_body}")
            raise HTTPException(500, f"Failed to initiate outbound call. Vobiz error: {error_body}")
        except httpx.HTTPError as e:
            logger.error(f"Vobiz API error: {e}")
            raise HTTPException(500, "Failed to initiate outbound call. Check server logs.")


@app.post("/outbound")
async def start_outbound_call(
    request: Request = None,
    body: OutboundCallRequest = None,
    x_api_key: str | None = Header(None),
    # Keyword arguments for internal programmatic calls from worker:
    to_phone: str | None = None,
    customer_name: str | None = None,
    campaign_id: str | None = None,
    lead_id: str | None = None,
):
    """Initiate an outbound call via the Vobiz REST API."""
    # Programmatic invocation from background worker
    if body is None and to_phone is not None:
        return await initiate_outbound_call(
            to_phone=to_phone,
            customer_name=customer_name or "there",
            campaign_id=campaign_id,
            lead_id=lead_id,
        )

    # API KEY AUTHENTICATION - fail closed if not configured
    required_key = os.getenv("OUTBOUND_API_KEY", "")
    if not required_key:
        logger.error("OUTBOUND_API_KEY not set in environment! Cannot process outbound call.")
        raise HTTPException(status_code=500, detail="Server misconfiguration: OUTBOUND_API_KEY not set")
    elif not hmac.compare_digest(x_api_key or "", required_key):
        raise HTTPException(status_code=401, detail="Invalid API Key")

    host = request.headers.get("host") if request else None
    scheme = ("https" if request.url.scheme == "https" else "http") if request else "http"

    return await initiate_outbound_call(
        to_phone=body.to,
        customer_name=body.customer_name,
        campaign_id=body.campaign_id,
        campaign_prompt=body.campaign_prompt,
        greeting=body.greeting,
        lead_id=body.lead_id,
        host=host,
        scheme=scheme,
    )


# OUTBOUND ANSWER CALLBACK
@app.post("/outbound-answer")
async def handle_outbound_answer(request: Request):
    """
    Vobiz callback when the outbound callee picks up.
    Identical to /inbound - we return VXML that tells Vobiz to open a bidirectional audio stream to /ws. The only difference
    is the ?type=outbound query param so the bot knows to use the outbound greeting instead of the inbound one.
    """
    host = request.headers.get("host")
    # wss (websocket secure) for https, ws for http
    scheme = "wss" if request.url.scheme == "https" else "ws"
    call_id = request.query_params.get("call_id", "")
    stream_url = f"{scheme}://{host}/ws?type=outbound&call_id={call_id}"

    vxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true" audioTrack="inbound" contentType="audio/x-mulaw;rate=8000" keepCallAlive="true">{stream_url}</Stream>
</Response>"""

    logger.info(f"Outbound call answered, directed to {stream_url}")
    return Response(content=vxml, media_type="application/xml")

# WEBSOCKET (audio pipeline)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Unified WebSocket handler for both inbound and outbound calls.
    
    The full flow is:
      1. Vobiz connects and sends a "start" message with stream metadata
      2. Set up the Pipecat transport with Vobiz-specific serialization (µ-law -> PCM)
      3. Voice Activity Detection (VAD) for when the human is speaking vs silent
      4. Bot pipeline: Audio In -> ASR -> LLM -> TTS -> Audio Out
      5. When the call ends, Vobiz closes the WebSocket

    The call is wrapped in asyncio.wait_for() to enforce a maximum duration.
    This prevents stuck WebSocket connections from consuming resources.
    """
    ws_token = os.getenv("WS_TOKEN")
    if ws_token:
        provided = websocket.query_params.get("token") or (
            websocket.headers.get("authorization", "").removeprefix("Bearer ").strip()
        )
        if not hmac.compare_digest(provided, ws_token):
            await websocket.close(code=4403)
            return

    await websocket.accept()
    logger.info("WebSocket accepted")

    # Determine if this is an inbound or outbound call from the query string.
    call_type = websocket.query_params.get("type", "inbound")
    logger.info(f"Call type: {call_type}")

    # 3. stream_id 
    # Why is it None? Because the call just connected.

    # User picks up phone -> WebSocket connects (Silent).
    # Vobiz immediately sends "Start": "GO! Here is your ID (abc-123)".
    # We read that message -> set stream_id="abc-123" -> Bot starts talking.
    stream_id = None

    semaphore = get_call_semaphore()
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=1.0)
    except asyncio.TimeoutError:
        logger.warning(
            f"At capacity ({MAX_CONCURRENT_CALLS} concurrent calls) -- "
            f"rejecting new {call_type} connection"
        )
        await websocket.close(code=1013, reason="At capacity")
        return

    try:
        # Handshake 'start' message is handled downstream by bot.py
        logger.info(f"WebSocket connected ({call_type})")

        # 2. Retrieve campaign data for outbound calls (pop removes it from the store)
        call_id = websocket.query_params.get("call_id", "")
        campaign_data = None
        if call_id:
            campaign_data = await CALL_MANAGER.get(call_id)
            if campaign_data:
                await CALL_MANAGER.delete(call_id)
                logger.info(f"Campaign data loaded for call {call_id}")

        # 3. Run the bot pipeline with a maximum call duration so calls automatically stop if they run too long (default 15 minutes).
        max_duration = CONFIG.get("max_call_duration_seconds", 900)

        await asyncio.wait_for(
            run_bot(
                websocket=websocket,
                call_type=call_type,
                config=CONFIG,
                stream_id=stream_id,
                campaign_data=campaign_data,
                call_id=call_id,
            ),
            timeout=max_duration,
        )

    except asyncio.TimeoutError:
        logger.warning(f"[{stream_id}] Call exceeded max duration, terminating")
    except Exception as e:
        logger.error(f"[{stream_id}] WebSocket error ({call_type}): {e}")
    finally:
        get_call_semaphore().release()
        logger.info(f"[{stream_id}] WebSocket closed ({call_type})")

# WEB BROWSER WEBSOCKET ENDPOINT (NO TELEPHONY)
@app.websocket("/ws-web")
async def websocket_web_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint strictly for local testing from the browser using standard PCM audio.
    Bypasses Vobiz frame formatting and expects 16kHz PCM.
    """
    await websocket.accept()
    logger.info("WebSocket accepted for WEB client")

    call_type = "web"
    # Unique stream identifier for each web session
    stream_id = f"web-{uuid.uuid4()}"
    campaign_id = websocket.query_params.get("campaign_id", "web-test")
    lead_id = websocket.query_params.get("lead_id")
    phone = websocket.query_params.get("phone")
    customer_name = websocket.query_params.get("name") or "Alex"

    semaphore = get_call_semaphore()
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=1.0)
    except asyncio.TimeoutError:
        logger.warning(
            f"At capacity ({MAX_CONCURRENT_CALLS} concurrent calls) -- "
            f"rejecting new web connection"
        )
        await websocket.close(code=1013, reason="At capacity")
        return

    try:
        max_duration = CONFIG.get("max_call_duration_seconds", 900)

        # We start the bot directly with our internal stream_id
        await asyncio.wait_for(
            run_bot(
                websocket=websocket,
                call_type=call_type,
                config=CONFIG,
                stream_id=stream_id,
                campaign_data={
                    "campaign_id": campaign_id,
                    "customer_name": customer_name,
                    "lead_id": lead_id,
                    "phone": phone,
                },
                call_id=stream_id,
            ),
            timeout=max_duration,
        )

    except asyncio.TimeoutError:
        logger.warning(f"[{stream_id}] Web Call exceeded max duration, terminating")
    except Exception as e:
        logger.error(f"[{stream_id}] Web WebSocket error: {e}")
    finally:
        get_call_semaphore().release()
        logger.info(f"[{stream_id}] Web WebSocket closed")


# HEALTH CHECK
@app.get("/health")
async def health():
    # Health check endpoint reports which services are configured
    active = CONFIG.get("active_providers", {})
    semaphore = get_call_semaphore()
    active_calls = MAX_CONCURRENT_CALLS - semaphore._value
    return {
        "status": "healthy",
        "configured_services": active,
        "active_calls": active_calls,
        "max_concurrent_calls": MAX_CONCURRENT_CALLS,
    }


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

# Dashboard and call reporting endpoints
def _require_dashboard_key(x_api_key: str | None) -> None:
    required_key = os.getenv("DASHBOARD_API_KEY") or os.getenv("OUTBOUND_API_KEY")
    if not required_key:
        if os.getenv("LOCAL_DEV", "").lower() == "true":
            return  # open access explicitly allowed in local dev
        raise HTTPException(500, "Server misconfiguration: no dashboard API key set")
    if not hmac.compare_digest(x_api_key or "", required_key):
        raise HTTPException(401, "Invalid API Key")


@app.get("/calls/export.csv")
async def export_calls_csv(limit: int = 1000, x_api_key: str | None = Header(None)):
    """Export call records to CSV (placed before /calls/{call_id} to prevent path shadowing)."""
    _require_dashboard_key(x_api_key)
    import csv
    import io

    from fastapi.responses import StreamingResponse

    rows = lead_state.list_calls(limit=limit)
    buf = io.StringIO()
    fieldnames = [
        "call_id", "campaign_id", "provider_call_id", "phone", "customer_name",
        "started_at", "ended_at", "conversation_stage", "disposition",
        "lead_fields", "avg_voice_latency_ms", "median_voice_latency_ms", "turns", "cost_usd", "crm_pushed",
    ]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=calls.csv"},
    )


@app.get("/calls/{call_id}")
async def get_call_detail(call_id: str, x_api_key: str | None = Header(None)):
    _require_dashboard_key(x_api_key)
    call = lead_state.get_call(call_id)
    if not call:
        raise HTTPException(404, "Call not found")
    call["lead_fields"] = json.loads(call.get("lead_fields") or "{}")
    if call.get("analysis_json"):
        call["analysis_json"] = json.loads(call["analysis_json"])
    return call


@app.get("/calls")
async def list_calls(limit: int = 200, campaign_id: str | None = None, x_api_key: str | None = Header(None)):
    _require_dashboard_key(x_api_key)
    return lead_state.list_calls(limit=limit, campaign_id=campaign_id)


@app.get("/campaigns/{campaign_id}/summary")
async def get_campaign_summary(campaign_id: str, x_api_key: str | None = Header(None)):
    """Average voice latency, total/average cost, and disposition
    breakdown for one campaign."""
    _require_dashboard_key(x_api_key)
    return lead_state.campaign_summary(campaign_id)


# Mount frontend static files for local testing via browser (http://localhost:8000)
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_dir):
    @app.get("/", include_in_schema=False)
    async def serve_index():
        index_file = os.path.join(frontend_dir, "index.html")
        return FileResponse(
            index_file,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    app.mount("/", StaticFiles(directory=frontend_dir), name="frontend")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))