"""Small, testable call-boundary fixes. No network during import."""
import asyncio
import re
from urllib.parse import urlparse

SHORT_GOODBYE = "Thank you. Goodbye!"
FAQ_TEXTS = {
    "faq_vastu": "It is one hundred percent Vastu compliant with East or North entry.",
    "faq_possession": "Possession is expected in late twenty twenty seven.",
    "faq_amenities": "Amenities include a pool, gym, clubhouse and round the clock security.",
}


def valid_transcript(text):
    clean = text.strip()
    if not clean:
        return False
    if re.fullmatch(r"[\W_]+", clean):
        return False
    if re.fullmatch(r"\d+(?::\d{2})?(?:\s*(?:am|pm))?[.!?]?", clean, re.I):
        return True
    return any(c.isalpha() for c in clean)


def terminal_answer(text, filler=False, connector=False):
    return bool(text and text.rstrip().endswith((".", "?", "!", "।")) and not filler and not connector)


def faq_key(text):
    """Only narrow, single-intent English FAQ questions, never corrections or mixed requests."""
    t = text.lower().strip().rstrip(".?!")
    for key, pattern in {
        "faq_vastu": r"(?:can you tell me (?:about )?|is (?:it|the project) |what about )?vastu(?: compliant)?",
        "faq_possession": r"(?:when is (?:the )?possession|what is (?:the )?possession date|tell me (?:the )?possession date)",
        "faq_amenities": r"(?:can you tell me about the amenities|what (?:are (?:the )?|)amenities(?: are there)?|tell me (?:about )?(?:the )?amenities)",
    }.items():
        if re.fullmatch(pattern, t):
            return key
    return None


def brochure_payload(call_id, phone, name, date, time, config, consent):
    """Fail closed. No placeholder phone or inferred consent."""
    cfg = config.get("brochure_delivery") or {}
    urls = [cfg.get("brochure_url", ""), cfg.get("floorplan_url", "")]
    if not consent:
        return None, "needs_consent"
    if not re.fullmatch(r"\+?[1-9]\d{9,14}", phone or ""):
        return None, "needs_phone"
    if not cfg.get("template_name"):
        return None, "not_configured"
    if not all(urlparse(u).scheme == "https" and urlparse(u).netloc for u in urls):
        return None, "not_configured"
    return {"action": "send_brochure", "call_id": call_id, "phone": phone,
            "name": name or "Valued Customer", "visit_date_iso": date or "Not booked",
            "time_slot": time or "Not booked", "brochure_url": urls[0],
            "floorplan_url": urls[1], "template_name": cfg.get("template_name", ""),
            "template_language": cfg.get("template_language", "en_US"),
            "consent": True}, None


def brochure_reply(result):
    if result.get("status") == "sent" and result.get("message_id"):
        return "The brochure is on its way."
    if result.get("status") == "queued":
        return "I'll WhatsApp you the brochure and floor plans."
    if result.get("status") == "needs_consent":
        return "May I send you the brochure and floor plans on WhatsApp?"
    if result.get("error_code") == 131031:
        return "Our WhatsApp business account is unavailable. The brochure has not been sent."
    return "I can't send the brochure right now. Our advisor can help."


async def finish_brochure(params, result, task, guard):
    from pipecat.frames.frames import TTSSpeakFrame, FunctionCallResultProperties
    guard._whatsapp_succeeded_this_turn = bool(result.get("status") == "sent" and result.get("message_id"))
    frame = TTSSpeakFrame(text=brochure_reply(result), append_to_context=True)
    frame.is_deterministic_confirmation = True
    await params.result_callback(result, properties=FunctionCallResultProperties(run_llm=False))
    await task.queue_frames([frame])


async def queue_goodbye(coordinator, task):
    from pipecat.frames.frames import TTSSpeakFrame
    # If the model has already emitted its farewell, use that line rather than queue another.
    if getattr(coordinator, "_current_audio_active", False):
        coordinator._dedicated_goodbye_queued = True
        coordinator.request_ending()
        return
    if getattr(coordinator, "_dedicated_goodbye_queued", False):
        return
    coordinator._dedicated_goodbye_queued = True
    coordinator.request_ending()
    await task.queue_frames([TTSSpeakFrame(text=SHORT_GOODBYE, append_to_context=True)])


class TTSStallState:
    """One cached hedge per turn; cancel on audio/interruption/end. Never retries a send or LLM."""
    def __init__(self, hedge, delay=2.0):
        self.hedge = hedge
        self.delay = delay
        self.timer = None
        self.used = False

    def arm(self):
        if not self.used and self.timer is None:
            self.timer = asyncio.create_task(self._wait())

    async def _wait(self):
        try:
            await asyncio.sleep(self.delay)
            self.used = True
            self.timer = None
            await self.hedge()
        except asyncio.CancelledError:
            pass

    def cancel(self, new_turn=False):
        if self.timer:
            self.timer.cancel()
            self.timer = None
        if new_turn:
            self.used = False

async def dispatch_brochure(payload, client_factory=None):
    """One approved template containing both URLs. No free-form send outside a verified 24h window."""
    import os
    import httpx
    from services.whatsapp_sender import normalize_phone_e164
    token = os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
    number = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
    template = payload.get("template_name")
    if not token or not number or not template:
        return {"status": "not_configured", "ok": False, "error": "Brochure template or credentials not configured"}
    if not payload.get("consent"):
        return {"status": "needs_consent", "ok": False, "error": "WhatsApp consent missing"}
    fields = [payload.get(k, "") for k in ("name", "visit_date_iso", "time_slot", "brochure_url", "floorplan_url")]
    if not payload.get("phone") or not all(fields):
        return {"status": "failed", "ok": False, "error": "Brochure payload missing phone or content"}
    version = os.getenv("WHATSAPP_API_VERSION", "v21.0").strip()
    request = {"messaging_product": "whatsapp", "to": normalize_phone_e164(payload["phone"]).lstrip("+"),
        "type": "template", "template": {"name": template,
            "language": {"code": payload.get("template_language", "en_US")},
            "components": [{"type": "body", "parameters": [{"type": "text", "text": str(v)} for v in fields]}]}}
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=8.0) as client:
            response = await client.post(f"https://graph.facebook.com/{version}/{number}/messages",
                headers={"Authorization": f"Bearer {token}"}, json=request)
            data = response.json()
            message_id = ((data.get("messages") or [{}])[0]).get("id")
            if response.is_success and message_id:
                return {"status": "sent", "ok": True, "message_id": message_id}
            error = data.get("error") or {}
            code = error.get("code")
            return {"status": "failed", "ok": False, "error_code": code,
                    "error": f"Meta error {code}: {error.get('message', 'send not accepted')}",
                    "http_status": response.status_code}
    except Exception as exc:
        # A timeout can be accepted by Meta; do not auto-retry an uncertain send.
        return {"status": "uncertain", "ok": False, "error": f"Send outcome unknown ({type(exc).__name__})"}


def explicit_caller_name(text):
    """Only unmistakable self-identification, never an agent line or pronoun 'its'."""
    patterns = (
        r"^(?:hi[, ]+|hello[, ]+)?(?:my name is|call me|mera naam)\s+([A-Za-z]+(?:[ -][A-Za-z]+){0,2})(?:\s+hai)?[.!]?$",
        r"^([A-Za-z]+(?:[ -][A-Za-z]+){0,2})\s+(?:here|speaking|this side)[.!]?$",
    )
    forbidden = {'amenities','vastu','looking','interested','fine','ready','not','yes','no','brochure','property','ananya','meridian'}
    for pattern in patterns:
        match = re.fullmatch(pattern, text.strip(), re.I)
        if match:
            value = re.sub(r"\s+hai$", "", match.group(1).strip(), flags=re.I)
            if not any(w.lower() in forbidden for w in value.split()):
                return value.title()
    return None


def clarification_for(text):
    hinglish = bool(re.search(r"\b(?:meko|mereko|bata|batao|aap|chuda|achani|na|haan|nahi)\b|[\u0900-\u097f]", text, re.I))
    return "Sorry, repeat kar sakte hain?" if hinglish else "Sorry, could you repeat that?"


def unambiguous_visit_time(text):
    """No business-hours guess: explicit meridiem/daypart or 24-hour 00/13..23 only."""
    t = text.lower().strip()
    if re.search(r"\b(?:am|pm|a\.m\.|p\.m\.|morning|afternoon|evening|night|subah|dopahar|shaam|raat)\b|सुबह|शाम|दोपहर|रात", t):
        return True
    m = re.fullmatch(r"(\d{1,2}):([0-5]\d)[.!]?", t)
    return bool(m and (int(m.group(1)) == 0 or 13 <= int(m.group(1)) <= 23))


def pure_farewell(text):
    """Only a complete signoff. 'Before I say bye, tell me the price' is not one."""
    raw = text.lower().strip()
    raw = re.sub(r"^(?:are|arre|अरे)\s+", "", raw)
    raw = raw.replace("ठीक है", "okay").replace("धन्यवाद", "thank you").replace("बाय", "bye")
    if any(c.isalpha() and not ('a' <= c <= 'z') for c in raw):
        return False
    t = re.sub(r"[^a-z\s]", " ", raw)
    words = t.split()
    return bool(words and (any(w in {'bye', 'goodbye'} for w in words) or words == ['okay','thank','you'])
                and all(w in {'bye', 'goodbye', 'no', 'nahi', 'nahin', 'okay', 'ok', 'yeah', 'sure', 'thank', 'you', 'thanks', 'yes', 'alright', 'now', 'then', 'for', 'your', 'time', 'take', 'care'} for w in words))

# Versioned keys: never reuse old WAVs for changed words. Missing WAVs use live Sarvam.
CALL5_TEXTS = {
    'faq_amenities_v2': 'There is a pool, gym, clubhouse, jogging track and round the clock security.',
    'faq_vastu_hi': 'Haan, project hundred percent Vastu compliant hai, East ya North entry ke saath.',
    'faq_3bhk_options': 'We have fifteen eighty square feet from one point four five crore, or twenty one hundred from one point seven crore. Which size would you prefer?',
    'faq_3bhk_large': 'The larger three BHK has a study and two balconies, with nineteen twenty to twenty one hundred square feet, from one point seven crore.',
}


def campaign_faq(text, memory, config):
    """Only bundled campaign facts and complete single-intent requests, no embedding guesses."""
    script = str(config.get('real_estate_sales_script', '')).lower()
    required = ('meridian residences', 'prime tech corridor', '100% vastu', 'east/north', 'pool', 'gym', 'clubhouse', 'security', 'jogging track', '1580', '2100', 'study', 'balconies', 'one point seven')
    if not all(v in script for v in required):
        return None
    t = text.strip().lower().rstrip('.?!')
    key = faq_key(text)
    if key:
        return ('faq_amenities_v2', CALL5_TEXTS['faq_amenities_v2']) if key == 'faq_amenities' else (key, FAQ_TEXTS[key])
    if re.fullmatch(r'(?:haan[, ]+|okay[, ]+)?(?:vastu (?:ke baare mein )?(?:batao|bata do|bata sakte ho)|(?:aap )?vastu ka bol sakte ho kya hai)', t):
        return 'faq_vastu_hi', CALL5_TEXTS['faq_vastu_hi']
    if re.fullmatch(r'(?:yeah |yes |haan )?(?:i(?: am|\'m) looking for (?:a )?)?(?:3|three)\s*bhk', t):
        return 'faq_3bhk_options', CALL5_TEXTS['faq_3bhk_options']
    configuration = str(memory.get('configuration') or memory.get('bhk') or '').lower()
    if '3' in configuration and re.fullmatch(r'(?:the )?(?:larger|large|bigger) (?:one|flat|unit)', t):
        return 'faq_3bhk_large', CALL5_TEXTS['faq_3bhk_large']
    return None

def genuine_late_question(text):
    t = text.strip().lower()
    return bool(re.search(r"\b(?:wait|actually|question|tell|what|why|how|when|where|which|price|cost|visit|amenities|parking|ruko|suno|kya|kitna|batao)\b", t))


def brochure_requested(text):
    t = text.lower().strip()
    return bool(not re.search(r"\b(?:no|not|dont|don't|mat|nahi|stop)\b", t)
                and re.search(r"\b(?:send|share|bhejo|bhej|chahiye)\b.*\b(?:brochure|floor ?plans?|details)\b|\b(?:brochure|floor ?plans?)\b.*\b(?:send|share|bhejo|please)\b", t))


def brochure_decision(messages, pending=False):
    """Only caller words plus the immediately preceding specific channel question."""
    indices = [i for i,m in enumerate(messages) if m.get('role') == 'user' and isinstance(m.get('content'),str)]
    if not indices:
        return 'not_requested'
    i = indices[-1]
    t = messages[i]['content'].lower().strip().rstrip('.!?')
    previous = next((str(m.get('content','')) for m in reversed(messages[:i]) if m.get('role') == 'assistant'), '').lower()
    specific = 'whatsapp' in previous and 'brochure' in previous and '?' in previous
    if specific and re.fullmatch(r"(?:no|no thanks|nahi|mat|don't send|dont send)", t):
        return 'declined'
    if brochure_requested(t):
        return 'consent' if 'whatsapp' in t else 'needs_consent'
    normalized_words = re.sub(r'[^a-z ]',' ',t).split()
    if specific and normalized_words and all(w in {'yeah','yes','please','sure','okay','ok','haan','ji','theek','hai','share','send','that','it','works','fine'} for w in normalized_words):
        return 'consent'
    return 'not_requested'


def safe_tts_text(text):
    clean = text.strip().strip('"“”').strip()
    return clean if any(c.isalpha() or c.isdigit() for c in clean) else ''


def fragment_transcript(text):
    t = re.sub(r"[^a-z' ]", '', text.lower()).strip()
    return t in {"c", "i", "i'm", "im", "i said i"}


def whatsapp_answer(messages, pending):
    indices = [i for i,m in enumerate(messages) if m.get('role') == 'user']
    if not indices or not pending:
        return None
    i = indices[-1]
    previous = next((str(m.get('content','')).lower() for m in reversed(messages[:i]) if m.get('role') == 'assistant'), '')
    target_words = ('location', 'details') if pending == 'location' else ('brochure', 'floor plans')
    if 'whatsapp' not in previous or '?' not in previous or not any(w in previous for w in target_words):
        return None
    answer_text = str(messages[i].get('content','')).lower()
    for word, replacement in {'हाँ':'yes','हां':'yes','जी':'ji','ठीक है':'okay','नहीं':'no','नही':'no','मत':'no'}.items():
        answer_text = answer_text.replace(word, replacement)
    t = re.sub(r'[^a-z ]', ' ', answer_text)
    words = t.split()
    if pending == 'location' and re.match(r'^(?:ah\s+)?(?:yes|yeah|sure|okay|ok)\b', t.strip()) and re.search(r'\b(?:send|share)\b.*\blocation\b', t) and not re.search(r'\b(?:no|not|dont|stop)\b', t):
        return 'consent'
    if words and all(w in {'yes','yeah','sure','please','okay','ok','haan','ji','send','share','it','that','works','fine'} for w in words):
        return 'consent'
    if words and all(w in {'no','thanks','thank','you','nahi','mat'} for w in words):
        return 'declined'
    return None


CALL7_TEXTS = {
    'consent_brochure_v1': 'May I send you the brochure and floor plans on WhatsApp?',
    'clarify_visit_time_v2': 'What time would you like to visit?',
    'location_queued_v2': "I'll WhatsApp you the location.",
    'recovery_connection_v1': "I'm having connection trouble. Please give me a moment.",
}


def visit_time(text):
    """Concrete caller time. Bare hours use the owner's daytime convention, not availability."""
    t = text.lower().strip().rstrip('.?!।')
    for word, number in {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,'eleven':11,'twelve':12,'एक':1,'दो':2,'तीन':3,'चार':4,'पांच':5,'पाँच':5,'छह':6,'सात':7,'आठ':8,'नौ':9,'दस':10,'ग्यारह':11,'बारह':12}.items():
        t = re.sub(r'(?<!\w)' + word + r'(?!\w)', str(number), t)
    if re.search(r'\d+\s*(?:bhk|balcon|bed|crore|lakh|sq)', t):
        return None
    numeric = re.search(r'(?<!\d)(\d{1,2})(?::(\d{2}))?', t)
    if numeric and (int(numeric.group(1)) > 23 or int(numeric.group(2) or 0) > 59):
        return None
    if numeric and int(numeric.group(1)) == 12 and re.search(r'\bam\b|morning|सुबह',t):
        return '12:' + (numeric.group(2) or '00') + ' AM'
    if numeric and int(numeric.group(1)) == 0:
        return '12:' + (numeric.group(2) or '00') + ' AM'
    from leads.worker import normalize_visit_time
    return normalize_visit_time(t)


def visit_intent(text, memory):
    t=text.lower()
    if re.search(r"don't|dont|not interested|cancel|नहीं.*(?:आना|विजिट)|नही.*आना", t):
        return False
    direct = bool(re.search(r'\b(?:(?:can i|i will|i want to|let me|may i)\s+(?:come|visit)|(?:book|schedule).{0,20}visit|come visit)\b|आ जाऊ|आऊ|आना',t))
    slot_followup = bool(memory.get('site_visit') and memory.get('disposition')!='SITE_VISIT_BOOKED'
                         and (visit_time(text) or re.fullmatch(r'(?:(?:kal|tomorrow|today|कल|आज)[ .।]*)+',t.strip())))
    dated_visit = bool(re.search(r'\b(?:visit|come)\b',t) and re.search(r'\b(?:tomorrow|today|kal|at)\b',t) and not re.search(r'\b(?:where|why|how|what)\b',t))
    return direct or slot_followup or dated_visit


def whatsapp_ready():
    """Only check presence of existing config, never expose values or send a probe."""
    import os
    return bool(os.getenv('WHATSAPP_ACCESS_TOKEN', '').strip() and os.getenv('WHATSAPP_PHONE_NUMBER_ID', '').strip())


def postcall_whatsapp_plan(memory, config, action):
    """Private manual preparation, independent of Meta credentials/templates."""
    if action not in {'brochure', 'location'}:
        return None, "I can't prepare those details right now."
    actions = set(memory.get('_postcall_whatsapp_actions') or []) | {action}
    plan = {'actions': sorted(actions), 'delivery': 'manual', 'status': 'prepared_not_sent'}
    check = manual_whatsapp_message({**memory, '_postcall_whatsapp_actions': sorted(actions)}, config)
    error = 'Our team needs to confirm the links first.' if check['blockers'] else None
    return plan, error


def manual_whatsapp_message(memory, config):
    """One copyable draft after teardown; never invent links or booking facts."""
    from urllib.parse import urlparse
    actions = set(memory.get('_postcall_whatsapp_actions') or [])
    if not actions:
        return {'status': 'not_requested', 'text': '', 'blockers': []}
    cfg = config.get('brochure_delivery', {}) or {}
    lines = ['Thank you for your interest.']
    blockers = []
    requested = []
    if 'brochure' in actions:
        requested += [('Brochure', 'brochure_url'), ('Floor plans', 'floorplan_url')]
    if 'location' in actions:
        requested += [('Location', 'postcall_location_url')]
    for label, key in requested:
        value = str(cfg.get(key) or '').strip()
        parsed = urlparse(value)
        if parsed.scheme == 'https' and parsed.netloc:
            lines.append(f'{label}: {value}')
        else:
            blockers.append(f'{label}: configure brochure_delivery.{key} with a verified HTTPS URL')
    # Existing verified booking state only; never a proposed/pending slot.
    if str(memory.get('site_visit', '')).startswith(('Confirmed (', 'Booked (')):
        day, slot = memory.get('visit_date_iso'), memory.get('time_slot')
        if day and slot:
            lines.append(f'Your site visit is booked for {day} at {slot}.')
        else:
            blockers.append('Verified booking date/time unavailable; check the booking record')
    return {'status': 'needs_configuration' if blockers else 'prepared_not_sent',
            'text': '\n'.join(lines), 'blockers': blockers}


def record_manual_whatsapp(memory, config, action):
    """Remember scoped consent once, even when delivery links are missing."""
    already = action in (memory.get('_postcall_whatsapp_actions') or [])
    plan, error = postcall_whatsapp_plan(memory, config, action)
    if plan:
        memory['_postcall_whatsapp_plan'] = plan
        memory['_postcall_whatsapp_actions'] = plan['actions']
        memory.pop('_whatsapp_consent_action', None)
        if action == 'brochure':
            memory.pop('_brochure_consent_pending', None)
    return None if already else error or 'Our team will WhatsApp you the details after this call.'


def closing_after_work(text, memory):
    """Thank-you closes completed work, not an unrelated in-progress question."""
    raw = text.lower().replace('ठीक है', 'okay').replace('ਧੰਨਵਾਦ', 'thank you')
    if any(c.isalpha() and not ('a' <= c <= 'z') for c in raw):
        return False  # don't erase a non-Latin follow-up question into a signoff
    t = re.sub(r'[^a-z ]', ' ', raw)
    words = t.split()
    done = bool(memory.get('_postcall_whatsapp_actions')) or memory.get('disposition') == 'SITE_VISIT_BOOKED'
    allowed = {'okay','ok','yeah','yes','sure','thank','thanks','you','bye','goodbye','no','nothing','all','good','that','is','it','alright'}
    return bool(done and words and all(w in allowed for w in words) and
                (('thank' in words or 'thanks' in words) or 'nothing' in words))
