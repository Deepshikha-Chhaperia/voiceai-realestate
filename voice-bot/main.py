import asyncio
import base64
import hashlib
import hmac
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from urllib.parse import quote

import httpx
from loguru import logger
import redis.asyncio as redis
import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from bot import run_bot, warmup_providers
import lead_state
import leads.dashboard
import leads.db
from settings import is_local_demo, safe_eq, validate_startup

class CallManager:
    """
    Manages campaign state for calls. 
    Tries to use Redis for production readiness/scale.
    Falls back to in-memory dictionary if Redis is unavailable (dev mode/failure).
    """
    def __init__(self):
        self.redis: redis.Redis | None = None
        self.memory: dict[str, dict] = {}
    
    async def connect(self, url: str | None = None):
        if not url:
            logger.info("REDIS_URL not set; using IN-MEMORY call storage.")
            return
        try:
            client = redis.from_url(url, decode_responses=True)
            await client.ping()
            self.redis = client
            logger.info("Connected to Redis")
        except Exception as e:
            if not is_local_demo():
                raise RuntimeError(f"REDIS_URL is set but Redis is unreachable: {e}")
            logger.warning(f"Redis unreachable ({e}); LOCAL_DEMO falling back to IN-MEMORY storage.")

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

    validate_startup()
    os.makedirs(os.getenv("DATA_DIR", "outputs"), exist_ok=True)

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

    # Redis only when REDIS_URL is set (otherwise in-memory)
    await CALL_MANAGER.connect(os.getenv("REDIS_URL"))

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

dashboard_origin = os.getenv("DASHBOARD_ORIGIN", "").strip()
if dashboard_origin:
    allowed_origins = [o.strip() for o in dashboard_origin.split(",") if o.strip()]
elif is_local_demo():
    allowed_origins = ["http://localhost:3000", "http://localhost:8000"]
else:
    allowed_origins = []

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(leads.dashboard.router)


def _tls() -> bool:
    return os.getenv("FORCE_WSS", "").lower() == "true"


def _build_ws_url(path: str = "/ws") -> str:
    """Stream URL for Vobiz. Host is always PUBLIC_HOST (never the Host header); wss iff FORCE_WSS."""
    sep = "&" if "?" in path else "?"
    return f"{'wss' if _tls() else 'ws'}://{os.getenv('PUBLIC_HOST')}{path}{sep}token={os.getenv('WS_TOKEN')}"


def _verify_vobiz_signature(request: Request) -> None:
    """Verify Vobiz's HMAC-SHA256 webhook signature (key = VOBIZ_AUTH_TOKEN, base64 over URL + nonce).

    V2 signs url+nonce, V3 signs url+"."+nonce. UNCONFIRMED: whether the signed URL includes the
    query string and whether V3 is base64 - both are accepted; confirm with one real call, then
    narrow this. Set VERIFY_VOBIZ_SIGNATURE=false only to debug that first call.
    """
    if os.getenv("VERIFY_VOBIZ_SIGNATURE", "true").lower() != "true":
        return
    if is_local_demo():
        logger.warning("LOCAL_DEMO: skipping Vobiz signature verification")
        return
    token = os.getenv("VOBIZ_AUTH_TOKEN", "")
    if not token:
        raise HTTPException(500, "Server misconfiguration: VOBIZ_AUTH_TOKEN not set")
    base = f"{'https' if _tls() else 'http'}://{os.getenv('PUBLIC_HOST')}{request.url.path}"
    urls = (base, f"{base}?{request.url.query}") if request.url.query else (base,)
    for version, sep in (("v2", ""), ("v3", ".")):
        sig = request.headers.get(f"x-vobiz-signature-{version}", "")
        nonce = request.headers.get(f"x-vobiz-signature-{version}-nonce", "")
        for url in urls if sig and nonce else ():
            digest = hmac.new(token.encode(), f"{url}{sep}{nonce}".encode(), hashlib.sha256).digest()
            if safe_eq(sig, base64.b64encode(digest).decode()):
                return
    raise HTTPException(401, "Invalid or missing Vobiz signature")


# INBOUND CALL HANDLER
@app.post("/inbound")
async def handle_inbound_call(request: Request):
    """
    When someone calls our Vobiz phone number, Vobiz sends a POST request.
    We respond with VXML (XML instructions) telling Vobiz to Open a bidirectional audio stream to our WebSocket endpoint.

    The audio format is µ-law at 8kHz — the standard for telephone networks.
    VobizFrameSerializer handles the conversion to PCM that Pipecat expects.
    """
    _verify_vobiz_signature(request)
    stream_url = _build_ws_url("/ws")

    # Instruct Vobiz to open a bidirectional 8kHz µ-law audio stream
    vxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true" audioTrack="inbound" contentType="audio/x-mulaw;rate=8000" keepCallAlive="true">{stream_url}</Stream>
</Response>"""

    logger.info(f"Inbound call directed to stream host {os.getenv('PUBLIC_HOST')}")
    return Response(content=vxml, media_type="application/xml")



async def initiate_outbound_call(
    to_phone: str,
    customer_name: str = "Alex",
    campaign_id: str | None = None,
    campaign_prompt: str | None = None,
    greeting: str | None = None,
    lead_id: str | None = None,
) -> dict:
    """Core helper to initiate an outbound call via Vobiz REST API or mock in local testing.

    Enforces calling hours (09:00-21:00 IST) and checks Do-Not-Call registry before placing call.
    """
    # 1. Calling hours check (IST: 09:00 - 21:00)
    from leads.worker import is_within_calling_hours
    if not is_within_calling_hours("Asia/Kolkata", "09:00", "21:00"):
        logger.warning(f"Outbound call to {to_phone} blocked: outside legal calling hours (09:00-21:00 IST)")
        return {"status": "blocked", "reason": "outside_calling_hours", "phone": to_phone}

    # 2. Do-Not-Call (DNC) check - fails closed: a lookup error aborts the call
    from leads.db import get_session
    from leads.models import DoNotCall
    from sqlalchemy import select
    async with get_session() as session:
        stmt = select(DoNotCall).where(DoNotCall.phone == to_phone).limit(1)
        res = await session.execute(stmt)
        if res.scalar_one_or_none():
            logger.warning(f"Outbound call to {to_phone} blocked: number is on Do-Not-Call list")
            return {"status": "blocked", "reason": "do_not_call", "phone": to_phone}

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

    await lead_state.upsert_call_async(
        call_id,
        campaign_id=camp_id,
        phone=to_phone,
        customer_name=customer_name,
        lead_id=lead_id,
    )

    host_val = os.getenv("PUBLIC_HOST", "localhost:8000")
    scheme = "https" if os.getenv("FORCE_WSS", "").lower() == "true" else "http"
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


class OutboundCallRequest(BaseModel):
    to: str = Field(pattern=r"^\+[1-9]\d{1,14}$")  # E.164
    customer_name: str = "there"
    campaign_prompt: str | None = None
    greeting: str | None = None
    campaign_id: str | None = None
    lead_id: str | None = None


@app.post("/outbound")
async def start_outbound_call(body: OutboundCallRequest, x_api_key: str | None = Header(None)):
    """Initiate an outbound call. Fails closed: OUTBOUND_API_KEY must be set and match."""
    required_key = os.getenv("OUTBOUND_API_KEY", "")
    if not required_key:
        raise HTTPException(500, "Server misconfiguration: OUTBOUND_API_KEY not set")
    if not safe_eq(x_api_key or "", required_key):
        raise HTTPException(401, "Invalid API Key")
    return await initiate_outbound_call(
        to_phone=body.to,
        customer_name=body.customer_name,
        campaign_id=body.campaign_id,
        campaign_prompt=body.campaign_prompt,
        greeting=body.greeting,
        lead_id=body.lead_id,
    )


# OUTBOUND ANSWER CALLBACK
@app.post("/outbound-answer")
async def handle_outbound_answer(request: Request):
    """
    Vobiz callback when the outbound callee picks up.
    Returns VXML directing Vobiz to open a bidirectional audio stream to /ws.
    The ?type=outbound query param tells the bot to use the outbound greeting.
    """
    _verify_vobiz_signature(request)
    call_id = quote(request.query_params.get("call_id", ""), safe="")
    stream_url = _build_ws_url(f"/ws?type=outbound&call_id={call_id}")

    vxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true" audioTrack="inbound" contentType="audio/x-mulaw;rate=8000" keepCallAlive="true">{stream_url}</Stream>
</Response>"""

    logger.info(f"Outbound call answered (call_id={call_id}), directed to stream host {os.getenv('PUBLIC_HOST')}")
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
    ws_token = os.getenv("WS_TOKEN", "")
    provided = websocket.query_params.get("token") or (
        websocket.headers.get("authorization", "").removeprefix("Bearer ").strip()
    )
    if not ws_token or not safe_eq(provided, ws_token):
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

# WEB BROWSER WEBSOCKET ENDPOINT — local demo only
if is_local_demo():
    @app.websocket("/ws-web")
    async def websocket_web_endpoint(websocket: WebSocket):
        """
        WebSocket endpoint strictly for local testing from the browser using standard PCM audio.
        Bypasses Vobiz frame formatting and expects 16kHz PCM.
        Only available when LOCAL_DEMO=1 (loopback only).
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
    return {"status": "healthy"}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

# Dashboard and call reporting endpoints
def _require_dashboard_key(x_api_key: str | None) -> None:
    required_key = os.getenv("DASHBOARD_API_KEY", "")
    if not required_key or not safe_eq(x_api_key or "", required_key):
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
if is_local_demo():
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
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", 8000)))