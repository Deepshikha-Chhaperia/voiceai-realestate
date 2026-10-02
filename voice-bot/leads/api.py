"""
FastAPI router for lead capture webhooks (Website, Meta Lead Ads, LP).
"""

import hashlib
import hmac
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
from loguru import logger
from pydantic import BaseModel

from leads.ingest import LeadIn, ingest_lead


router = APIRouter(tags=["Leads Webhooks"])


class WebsiteLeadPayload(BaseModel):
    name: str | None = None
    phone: str
    project: str | None = None
    consent: bool = True
    utm: dict[str, Any] | None = None


@router.post("/webhooks/website")
async def website_webhook(
    body: WebsiteLeadPayload,
    x_webhook_secret: str | None = Header(None, alias="X-Webhook-Secret"),
):
    """Website form lead capture."""
    expected = os.getenv("WEBSITE_WEBHOOK_SECRET")
    if not expected:
        if os.getenv("LOCAL_DEV", "").lower() != "true":
            raise HTTPException(status_code=500, detail="Server misconfiguration: WEBSITE_WEBHOOK_SECRET not set")
    elif not hmac.compare_digest(x_webhook_secret or "", expected):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    if not body.consent:
        raise HTTPException(status_code=400, detail="Consent is mandatory")

    lead_in = LeadIn(
        name=body.name,
        phone=body.phone,
        source="website",
        project_id=body.project,
        utm=body.utm,
        consent=body.consent,
        raw=body.model_dump(),
    )
    lead = await ingest_lead(lead_in)
    return {"status": "ok", "lead_id": str(lead.id), "tier": lead.tier}


@router.get("/lp", response_class=HTMLResponse)
async def landing_page():
    """High-converting luxury demo landing page for real estate lead generation."""
    project_title = "Meridian Residences"
    location_title = "Prime Tech Corridor"
    price_tag = "Luxury 2 & 3 BHK from ₹95 Lakhs*"
    rera_tag = "RERA/P/2026/00482"
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{project_title} — {location_title}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body {{ font-family: 'Plus Jakarta Sans', -apple-system, sans-serif; }}
  </style>
</head>
<body class="bg-slate-50 text-slate-800 min-h-screen flex flex-col justify-between antialiased">
  <!-- Top Navigation Bar -->
  <header class="bg-white/95 backdrop-blur border-b border-slate-200 sticky top-0 z-50">
    <div class="max-w-6xl mx-auto px-4 sm:px-6 py-4 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <div class="w-9 h-9 rounded-lg bg-slate-900 text-white flex items-center justify-center font-bold text-sm tracking-wider">
          M
        </div>
        <div>
          <span class="text-xs font-bold tracking-widest text-slate-900 block uppercase">MERIDIAN GROUP</span>
          <span class="text-[11px] text-slate-500 font-medium">Luxury Living & Urban Spaces</span>
        </div>
      </div>
      <div class="flex items-center gap-3">
        <span class="inline-flex items-center gap-1.5 text-xs font-semibold px-3 py-1 rounded-full bg-emerald-50 text-emerald-700 border border-emerald-200">
          <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
          Pre-Launch Bookings Open
        </span>
      </div>
    </div>
  </header>

  <!-- Hero Section -->
  <main class="max-w-6xl mx-auto px-4 sm:px-6 py-10 sm:py-16 w-full flex-1">
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-10 lg:gap-14 items-center">
      
      <!-- Left Content -->
      <div class="lg:col-span-7 space-y-6">
        <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-amber-50 border border-amber-200 text-amber-800 text-xs font-semibold">
          <span>★</span> Premium Residential Enclave • {location_title}
        </div>

        <h1 class="text-3xl sm:text-5xl font-extrabold text-slate-900 tracking-tight leading-[1.15]">
          Experience Serene Living at <span class="text-emerald-700">{project_title}</span>
        </h1>

        <p class="text-base sm:text-lg text-slate-600 leading-relaxed">
          Thoughtfully crafted homes surrounded by 80% lush green landscape. Strategically positioned next to premier tech parks, top international schools, and metro corridors.
        </p>

        <!-- Spec highlights -->
        <div class="grid grid-cols-2 sm:grid-cols-3 gap-3 pt-2">
          <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-sm">
            <span class="text-xs text-slate-500 font-medium block">Starting Price</span>
            <span class="text-base font-bold text-slate-900 mt-0.5 block">{price_tag.split('from ')[-1]}</span>
          </div>
          <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-sm">
            <span class="text-xs text-slate-500 font-medium block">Configurations</span>
            <span class="text-base font-bold text-slate-900 mt-0.5 block">2, 3 & 4 BHK</span>
          </div>
          <div class="bg-white border border-slate-200 rounded-xl p-3.5 shadow-sm col-span-2 sm:col-span-1">
            <span class="text-xs text-slate-500 font-medium block">Possession</span>
            <span class="text-base font-bold text-emerald-700 mt-0.5 block">Q4 2027</span>
          </div>
        </div>

        <!-- Amenities Checklist -->
        <div class="grid grid-cols-2 gap-2 text-xs text-slate-700 pt-1 font-medium">
          <div class="flex items-center gap-2">
            <span class="text-emerald-600 font-bold">✓</span> 45,000 Sq.Ft. Luxury Clubhouse
          </div>
          <div class="flex items-center gap-2">
            <span class="text-emerald-600 font-bold">✓</span> Olympic-Size Temperature Pool
          </div>
          <div class="flex items-center gap-2">
            <span class="text-emerald-600 font-bold">✓</span> 5 Mins to Metro Station
          </div>
          <div class="flex items-center gap-2">
            <span class="text-emerald-600 font-bold">✓</span> 100% Vastu Compliant Units
          </div>
        </div>

        <div class="pt-2 text-[11px] text-slate-500">
          RERA Registration: <strong class="text-slate-700">{rera_tag}</strong>
        </div>
      </div>

      <!-- Right Form Card -->
      <div class="lg:col-span-5">
        <div class="bg-white rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-200 p-6 sm:p-8 relative">
          <div class="absolute -top-3 right-6 bg-slate-900 text-white text-[10px] font-bold uppercase tracking-wider px-3 py-1 rounded-full shadow">
            Fast Response AI
          </div>

          <div class="mb-5">
            <h2 class="text-xl font-bold text-slate-900">Request Instant Callback</h2>
            <p class="text-xs text-slate-500 mt-1">Get complete pricing, floor plans & an instant AI voice consultation.</p>
          </div>

          <form id="leadForm" class="space-y-4" onsubmit="submitForm(event)">
            <div>
              <label class="block text-xs font-semibold text-slate-700 mb-1">Full Name</label>
              <input type="text" id="name" required placeholder="Alex Sharma" class="w-full bg-slate-50 border border-slate-300 rounded-xl px-4 py-2.5 text-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900 focus:bg-white transition">
            </div>

            <div>
              <label class="block text-xs font-semibold text-slate-700 mb-1">Mobile Number (with Country Code)</label>
              <input type="tel" id="phone" required placeholder="+919876543210" class="w-full bg-slate-50 border border-slate-300 rounded-xl px-4 py-2.5 text-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900 focus:bg-white transition">
            </div>

            <div>
              <label class="block text-xs font-semibold text-slate-700 mb-1">Preferred Unit Type</label>
              <select id="unit" class="w-full bg-slate-50 border border-slate-300 rounded-xl px-3.5 py-2.5 text-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900 focus:bg-white transition">
                <option value="2 BHK">Luxury 2 BHK (~1,250 sq.ft.)</option>
                <option value="3 BHK" selected>Premium 3 BHK (~1,680 sq.ft.)</option>
              </select>
            </div>

            <div class="flex items-start gap-2 pt-1">
              <input type="checkbox" id="consent" checked required class="mt-1 rounded border-slate-300 text-slate-900 focus:ring-0">
              <label for="consent" class="text-[11px] text-slate-500 leading-tight">
                I agree to be contacted by Meridian AI Voice Concierge and receive instant brochure updates via WhatsApp.
              </label>
            </div>

            <button type="submit" id="submitBtn" class="w-full bg-slate-900 hover:bg-slate-800 text-white font-semibold py-3.5 rounded-xl text-sm transition flex items-center justify-center gap-2 shadow-md hover:shadow-lg">
              <span>Request Instant Call & Brochure</span>
            </button>
          </form>

          <div id="result" class="hidden mt-4 p-4 bg-emerald-50 border border-emerald-200 rounded-xl text-xs text-emerald-800 text-center leading-relaxed"></div>
        </div>
      </div>

    </div>
  </main>

  <!-- Footer -->
  <footer class="bg-white border-t border-slate-200 py-6">
    <div class="max-w-6xl mx-auto px-4 sm:px-6 flex flex-col sm:flex-row items-center justify-between gap-3 text-xs text-slate-500">
      <p>© 2026 Meridian Group. All rights reserved. Disclaimer: Artist impressions for representation only.</p>
      <div class="flex items-center gap-4">
        <a href="/dashboard" class="hover:text-slate-900 transition">CRM Dashboard</a>
        <a href="/dashboard/visits" class="hover:text-slate-900 transition">Site Visits</a>
      </div>
    </div>
  </footer>

  <script>
    async function submitForm(e) {{
      e.preventDefault();
      const btn = document.getElementById('submitBtn');
      const res = document.getElementById('result');
      const unit = document.getElementById('unit').value;
      btn.disabled = true;
      btn.innerHTML = '<span>Placing AI voice call in ~15s...</span>';

      try {{
        const response = await fetch('/webhooks/website', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{
            name: document.getElementById('name').value,
            phone: document.getElementById('phone').value,
            project: unit,
            consent: document.getElementById('consent').checked,
            utm: {{ source: 'lp_demo', unit_preference: unit }}
          }})
        }});
        const data = await response.json();
        if (response.ok) {{
          res.classList.remove('hidden');
          res.innerHTML = '<strong>Request Received!</strong><br>Our AI property advisor is connecting to your phone right now. Please keep your line open.';
          btn.innerHTML = '<span>Call Dispatched ✓</span>';
        }} else {{
          alert('Error: ' + (data.detail || 'Submission failed'));
          btn.disabled = false;
          btn.innerHTML = '<span>Request Instant Call & Brochure</span>';
        }}
      }} catch (err) {{
        alert('Failed to connect to server');
        btn.disabled = false;
        btn.innerHTML = '<span>Request Instant Call & Brochure</span>';
      }}
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@router.get("/webhooks/meta")
async def meta_verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    """Meta webhook verification challenge."""
    verify_token = os.getenv("META_VERIFY_TOKEN")
    if verify_token and hub_mode == "subscribe" and hub_token == verify_token:
        return Response(content=hub_challenge, media_type="text/plain")
    raise HTTPException(status_code=403, detail="Verification token mismatch")


@router.post("/webhooks/meta")
async def meta_lead_webhook(
    request: Request,
    x_hub_signature_256: str | None = Header(None, alias="X-Hub-Signature-256"),
):
    """Meta Lead Ads webhook ingestion."""
    raw_body = await request.body()
    app_secret = os.getenv("META_APP_SECRET")

    # Verify signature — mandatory in all environments unless LOCAL_DEV=true
    if not app_secret:
        if os.getenv("LOCAL_DEV", "").lower() != "true":
            raise HTTPException(
                status_code=500, detail="Server misconfiguration: META_APP_SECRET not set"
            )
        logger.debug("META_APP_SECRET not set; skipping signature verification (LOCAL_DEV mode)")
    else:
        if not x_hub_signature_256:
            raise HTTPException(status_code=401, detail="Missing X-Hub-Signature-256 header")
        expected_sig = "sha256=" + hmac.new(
            app_secret.encode("utf-8"), raw_body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(x_hub_signature_256, expected_sig):
            logger.warning("Invalid Meta webhook signature")
            raise HTTPException(status_code=401, detail="Invalid signature")

    data = await request.json()
    entries = data.get("entry", [])
    ingested_leads = []

    for entry in entries:
        changes = entry.get("changes", [])
        for change in changes:
            val = change.get("value", {})
            leadgen_id = str(val.get("leadgen_id", ""))
            ad_id = str(val.get("ad_id", ""))
            adset_id = str(val.get("adset_id", ""))
            campaign_id = str(val.get("campaign_id", "meta-ad-campaign"))

            # Extract fields if simulator payload sent field_data
            field_data = val.get("field_data", [])
            phone = None
            name = None
            for f in field_data:
                fname = f.get("name", "").lower()
                fvals = f.get("values", [])
                val_str = fvals[0] if fvals else ""
                if any(p in fname for p in ("phone", "contact", "mobile")):
                    phone = val_str
                elif any(p in fname for p in ("name", "full_name", "first_name")):
                    name = val_str

            if not phone:
                phone = val.get("phone", "+919876543210")
            if not name:
                name = val.get("name", "Meta Lead")

            lead_in = LeadIn(
                name=name,
                phone=phone,
                source="meta",
                external_id=leadgen_id or f"meta-{int(datetime.now(timezone.utc).timestamp())}",
                campaign_id=campaign_id,
                adset_id=adset_id,
                ad_id=ad_id,
                raw=val,
            )
            lead = await ingest_lead(lead_in)
            ingested_leads.append(str(lead.id))

    return {"status": "ok", "ingested": ingested_leads}


