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
    # Filler/refusal refers to the previous visit offer, not this explicit FAQ.
    vastu = re.fullmatch(r"(?:(?:uh|um)[, ]+)?(?:no[, ]+)*can you tell me (?:about )?(?:the )?vastu(?: (?:first|once))*", t)
    if vastu:
        return 'faq_vastu'
    for key, pattern in {
        "faq_vastu": r"(?:can you tell me (?:about )?|is (?:it|the project) |what about )?vastu(?: compliant)?",
        "faq_possession": r"(?:when is (?:the )?possession|what is (?:the )?possession date|tell me (?:the )?possession date)",
        "faq_amenities": r"(?:can you tell me about the amenities|what (?:are (?:the )?|)amenities(?: are there)?|tell me (?:about )?(?:the )?amenities)",
    }.items():
        if re.fullmatch(pattern, t):
            return key
    return None


_TOPIC = {
    "faq_vastu": r"(?:vastu|vaastu|वास्तु)(?: compliant| compliance)?",
    "faq_possession": r"(?:(?:possession|पजेशन|पज़ेशन|handover|hand over)(?: date| time| timeline)?|(?:कब तक|kab tak)(?: का| ka)?(?: है| hai)?|kab milega|when (?:will it be|is it) ready)",
    "faq_amenities": r"(?:amenities|amenity|सुविधाएं|सुविधाएँ)",
}
FAQ_COMBO_CLIPS = {"faq_vastu": "faq_vastu", "faq_possession": "faq_possession", "faq_amenities": "faq_amenities_v2"}
_LEAD = (r"(?:(?:uh|um|okay|ok|so|and|also|please|actually|haan|acha|accha)[, ]+)*"
         r"(?:(?:can|could) you (?:please )?(?:tell|share|give) me (?:about |the )?(?:details )?(?:about |on |of )?"
         r"|tell me (?:about )?|i (?:want|would like) to (?:know|ask) about |i want to know about |do you have |batao |bataiye )?")
_SEG = (r"(?:(?:the|about|on|what about|what are|what is|what's|when is|when's|is it|is the project|is there|are there|any|tell me about)\s+)*"
        r"({topics})(?:\s+(?:details|information|info|status|are there|there|hai|hain|kya hai|kab hai|kya hain))*")
_SPLIT = r"\s*(?:,|&|\band\b|\baur\b|\bplus\b|\bas well as\b|\balso\b|और)\s*"


_FILLER = r"(?:(?:uh|um|umm|hmm|actually|okay|ok|so|well|then|and|also|please|haan|acha|accha|hello|sir|ma'am)\b[ ,]*)+"


def faq_keys(text):
    """Two or more cached FAQ topics in one question, in asked order. None unless EVERY clause is a cached topic.

    Sentence breaks, fillers (uh, actually, okay) and STT fragment joins are ignored; any other clause means None."""
    t = re.sub(r"\s+", " ", text.lower().strip())
    t = re.sub(r"[.?!।;]+", ",", t)
    parts = []
    for raw in re.split(_SPLIT, t):
        part = re.sub(r"^" + _FILLER, "", raw.strip(" ,"))
        part = re.sub(r"^" + _LEAD, "", part)
        part = re.sub(r"(?:[, ]+(?:please|batao|bataiye|bata do|details))+$", "", part).strip(" ,")
        if part:
            parts.append(part)
    keys = []
    for part in parts:
        hit = None
        for key, topic in _TOPIC.items():
            if re.fullmatch(_SEG.replace("{topics}", topic), part):
                hit = key
                break
        if not hit:
            return None
        if hit not in keys:
            keys.append(hit)
    return keys if len(keys) >= 2 else None


def combo_key(keys):
    return "faq_combo_" + "_".join(k[4:] for k in keys)


def combo_parts(key):
    return ["faq_" + p for p in key[len("faq_combo_"):].split("_")]


def combo_text(keys):
    texts = {**FAQ_TEXTS, "faq_amenities": CALL5_TEXTS["faq_amenities_v2"]}
    return " ".join(texts[k] for k in keys)


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
    from enterprise.meta import send
    return await send({**payload,"action":"brochure","consent_scope":"brochure"},client_factory)


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
    comparison = re.fullmatch(r"(?:what(?:'s| is) (?:the )?difference(?: between (?:the )?(?:standard and large|two|both)(?: (?:ones|units|options))?)?|(?:can you )?compare (?:the )?(?:standard and large|two|both)(?: (?:ones|units|options))?|(?:dono|standard aur large) (?:mein |me )?(?:kya )?difference(?: hai)?)", t)
    comparison = comparison or re.fullmatch(r"(?:actually )?(?:वो )?(?:छोटा वाला और ब[ड़ड़]े वाले में|standard और large में) difference क्या होगा", t)
    comparison_facts = ('1750', '1920', 'east facing', 'east/north facing', '1 balcony', '2 balconies', 'one point four five to one point six five', 'one point seven to one point eight')
    if comparison and '3' in str(memory.get('configuration') or memory.get('bhk') or '') and all(v in script for v in comparison_facts) and re.search(r'one point seven to one point eight crores?[.,\n]', script):
        return 'compare_3bhk_v1', 'Standard: 1580-1750 square feet, 1.45-1.65 crore. Large: 1920-2100 sq ft, 1.7-1.8 crore, study, extra balcony.'
    keys = faq_keys(text)
    if keys:
        return combo_key(keys), combo_text(keys)
    key = faq_key(text)
    if key:
        return ('faq_amenities_v2', CALL5_TEXTS['faq_amenities_v2']) if key == 'faq_amenities' else (key, FAQ_TEXTS[key])
    if re.fullmatch(r'(?:haan[, ]+|okay[, ]+)?(?:vastu (?:ke baare mein )?(?:batao|bata do|bata sakte ho)|(?:aap )?vastu ka bol sakte ho kya hai)', t):
        return 'faq_vastu_hi', CALL5_TEXTS['faq_vastu_hi']
    if re.fullmatch(r'(?:yeah |yes |haan )?(?:i(?: am|\'m) looking for (?:a )?)?(?:3|three)\s*bhk', t):
        return 'faq_3bhk_options', CALL5_TEXTS['faq_3bhk_options']
    configuration = str(memory.get('configuration') or memory.get('bhk') or '').lower()
    price_facts = ('one point four five to one point six five crores.', 'one point seven to one point eight crores.')
    if re.fullmatch(r"(?:अच्छा नहीं मेरे को मतलब |अच्छा |मुझे |मेरे को )?(?:3|three) bhk का कितना पड़ेगा", t) and all(v in script for v in price_facts):
        return 'faq_3bhk_price_v11', 'Standard one point four five to one point six five crore, aur Large one point seven to one point eight crore hai.'
    large_choice = re.fullmatch(r"(?:हाँ ठीक है मतलब मेरेको वो |मुझे |मेरेको |मेरे को )?ब(?:ड़|ड़)ा वाला (?:चाहिए|चाहिये)(?: ऐसे तो)?[।.! ]*", t)
    if '3' in configuration and large_choice:
        return 'faq_3bhk_large', CALL5_TEXTS['faq_3bhk_large']
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
    for word, replacement in {'हाँ':'yes','हां':'yes','जी':'ji','ठीक है':'okay','नहीं':'no','नही':'no','मत':'no','आप':'you','भेज दीजिए':'send','भेज दीजिये':'send','ना':'please'}.items():
        answer_text = answer_text.replace(word, replacement)
    if any(c.isalpha() and not ('a' <= c <= 'z') for c in answer_text):
        return None
    t = re.sub(r'[^a-z ]', ' ', answer_text)
    words = t.split()
    if pending == 'location' and re.match(r'^(?:ah\s+)?(?:yes|yeah|sure|okay|ok)\b', t.strip()) and re.search(r'\b(?:send|share)\b.*\blocation\b', t) and not re.search(r'\b(?:no|not|dont|stop)\b', t):
        return 'consent'
    if words and all(w in {'yes','yeah','sure','please','okay','ok','haan','ji','send','share','it','that','works','fine','you'} for w in words):
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


_MONTHS = r"(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec)"
_DATE_NUM_RE = re.compile(r"\b\d{1,2}(?:st|nd|rd|th)?\s+(?:of\s+)?" + _MONTHS + r"\b|\b" + _MONTHS + r"\s+\d{1,2}(?:st|nd|rd|th)?\b")


def _strip_calendar_dates(text):
    return _DATE_NUM_RE.sub(' ', text)


def visit_time(text):
    """Concrete caller time. Bare hours use the owner's daytime convention, not availability."""
    t = text.lower().strip().rstrip('.?!।')
    t = _strip_day_offsets(t)  # "after two days" is a date offset, never 2 o'clock
    t = _strip_calendar_dates(t)  # "11 October" is a date, never 11 o'clock
    t = re.sub(r"(?<!\w)(?:kar|karo|kardo|de|dijiye|kijiye|कर|दे)\s+(?:do|दो)(?!\w)", ' ', t)  # "book kar do" = "please book", never 2 o'clock
    for word, number in {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,'eleven':11,'twelve':12,'एक':1,'दो':2,'तीन':3,'चार':4,'पांच':5,'पाँच':5,'छह':6,'सात':7,'आठ':8,'नौ':9,'दस':10,'ग्यारह':11,'बारह':12}.items():
        t = re.sub(r'(?<!\w)' + word + r'(?!\w)', str(number), t)
    if re.search(r'\d+\s*(?:bhk|balcon|bed|crore|lakh|sq)', t):
        return None
    marked = re.search(r"(?<!\d)\d{1,2}(?::\d{2})?\s*(?:बजे|baje|bajey|o'?clock|a\.?m\b|p\.?m\b)", t)
    if marked:  # a number that carries a time marker wins over any other number in the sentence
        t = re.sub(r"\d+(?::\d{2})?", ' ', t[:marked.start()]) + t[marked.start():marked.end()] + re.sub(r"\d+(?::\d{2})?", ' ', t[marked.end():])
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
    from enterprise.meta import state
    return state()['ready']


def postcall_whatsapp_plan(memory, config, action):
    """Private manual preparation, independent of Meta credentials/templates."""
    if action not in {'brochure', 'location'}:
        return None, "I can't prepare those details right now."
    actions = set(memory.get('_postcall_whatsapp_actions') or []) | {action}
    plan = {'actions': sorted(actions), 'delivery': 'manual', 'status': 'prepared_not_sent'}
    check = manual_whatsapp_message({**memory, '_postcall_whatsapp_actions': sorted(actions)}, config)
    error = ("I can't share the location right now." if action == 'location' else "I can't share the brochure right now.") if check['blockers'] else None
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
    from loguru import logger
    logger.info('[WA-TOOL] {} requested/consented mid-call; NOTHING is sent now, delivery is deferred until the call has ended', action)
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
    done = (bool(memory.get('_postcall_whatsapp_actions')) or memory.get('disposition') == 'SITE_VISIT_BOOKED') and not memory.get('_whatsapp_consent_action') and not memory.get('_brochure_consent_pending')
    allowed = {'okay','ok','yeah','yes','sure','thank','thanks','you','bye','goodbye','no','nothing','all','good','that','is','it','alright','cool'}
    return bool(done and words and all(w in allowed for w in words) and
                (('thank' in words or 'thanks' in words) or 'nothing' in words))


def goal_complete(memory):
    """A verified visit and a resolved channel decision, never a requested slot."""
    return bool(memory.get('disposition') == 'SITE_VISIT_BOOKED'
                and str(memory.get('site_visit', '')).startswith(('Confirmed (', 'Booked ('))
                and memory.get('visit_date_iso') and memory.get('time_slot')
                and not memory.get('_whatsapp_consent_action')
                and not memory.get('_brochure_consent_pending'))


async def queue_goal_close(memory, coordinator, emit, acknowledgment):
    from pipecat.frames.frames import TTSSpeakFrame
    close = coordinator is not None and goal_complete(memory)
    if close and getattr(coordinator, '_dedicated_goodbye_queued', False):
        return
    frame = TTSSpeakFrame(text=acknowledgment + (' ' + SHORT_GOODBYE if close else ''), append_to_context=True)
    frame.is_deterministic_confirmation = True
    if close:
        coordinator._dedicated_goodbye_queued = True
        coordinator.request_ending()
    await emit(frame)


def manual_location_offer(memory, config):
    """Never offer a link we cannot prepare; no consent or delivery inferred."""
    if memory is None:
        return ''
    url = str(((config or {}).get('brochure_delivery') or {}).get('postcall_location_url') or '').strip()
    parsed = urlparse(url)
    if 'location' in (memory.get('_postcall_whatsapp_actions') or []):
        memory['_manual_close_after_booking'] = True
        return ''
    if parsed.scheme == 'https' and parsed.netloc:
        if memory.get('_whatsapp_consent_action') not in (None, 'location'):
            return ''
        memory['_whatsapp_consent_action'] = 'location'
        return ' Want the location on WhatsApp after this call?'
    if memory.get('_whatsapp_consent_action') == 'location':
        memory.pop('_whatsapp_consent_action', None)
    memory['_manual_close_after_booking'] = True
    return ''


def caller_asked_something(messages):
    """True when the last caller turn is a real request/question (3+ words, not a bare farewell/ack)."""
    last = next((str(m.get('content','')) for m in reversed(messages) if m.get('role') == 'user' and isinstance(m.get('content'), str)), '')
    words = re.findall(r"[^\W\d_]+", last.lower())
    if len(words) < 3 and '?' not in last:
        return False
    return not re.fullmatch(r"(?:okay|ok|thanks?|thank you|bye|goodbye|theek hai|thik hai|haan|ji|accha|achha|hmm|yes|no|\s)+", ' '.join(words))


def _mask_phone(phone):
    d = re.sub(r'\D', '', str(phone or ''))
    return ('*' * max(0, len(d) - 4) + d[-4:]) if d else 'none'


def log_postcall_whatsapp(call_id, memory, config, phone, delivery=None):
    """Visible post-call WhatsApp trace. Runs only after the call ended. Never sends and never claims success.

    delivery is the enterprise.meta.enqueue_postcall() result when enterprise is on; None otherwise (dry run)."""
    from loguru import logger
    actions = list(memory.get('_postcall_whatsapp_actions') or [])
    if not actions:
        logger.info('[WA-POSTCALL] [{}] call ended; no WhatsApp action was requested', call_id)
        return {'status': 'not_requested'}
    logger.info('[WA-POSTCALL] [{}] call ended; actions={} recipient={}', call_id, actions, _mask_phone(phone))
    status = (delivery or {}).get('status')
    if status == 'queued':
        logger.info('[WA-POSTCALL] [{}] QUEUED for Meta send (work_ids={}); the sender confirms only after the provider accepts', call_id, delivery.get('work_ids'))
        return delivery
    gaps = (delivery or {}).get('gaps') or ['Meta automation not enabled/configured (enterprise off or keys missing)']
    preview = manual_whatsapp_message(memory, config)
    logger.warning('[WA-POSTCALL] [{}] DRY RUN, NOT SENT. Reason: {}', call_id, '; '.join(map(str, gaps))[:300])
    logger.info('[WA-POSTCALL] [{}] would send: {}', call_id, str(preview.get('text', ''))[:400].replace('\n', ' | '))
    return {'status': 'dry_run', 'gaps': gaps, 'sent': False}


_ASSENT = {'yes','yeah','yep','yup','sure','ok','okay','correct','right','fine','please','haan','han','haa','ji','bilkul','theek','thik','hai','ha','sahi','done','confirm','confirmed'}


def assented_slot(messages):
    """(iso_date, time) when the caller's latest turn is a plain yes to OUR immediately preceding question that named BOTH a day and a time."""
    users = [i for i, m in enumerate(messages) if m.get('role') == 'user' and isinstance(m.get('content'), str)]
    if not users:
        return None
    i = users[-1]
    raw = messages[i]['content'].lower().replace('हाँ', 'haan').replace('हां', 'haan').replace('जी', 'ji').replace('ठीक है', 'theek hai')
    words = re.sub(r"[^a-z ]", ' ', raw).split()
    if not words or len(words) > 4 or not all(w in _ASSENT for w in words):
        return None
    prev = next((str(m.get('content', '')) for m in reversed(messages[:i]) if m.get('role') == 'assistant'), '')
    if '?' not in prev:
        return None
    from bot import _extract_lead_preferences
    fixed = re.sub(r'\b([ap])\s*\.?m\b\.?', r'\1m', prev, flags=re.I)
    fields = _extract_lead_preferences(fixed)
    asked_time = fields.get('time_slot') or visit_time(fixed)
    asked_date = fields.get('visit_date_iso')
    if not (asked_date and asked_time):
        return None
    return asked_date, asked_time


def slot_assented(messages, arguments):
    """Caller said a plain yes to OUR immediately preceding question that named BOTH the exact day and time.

    The booking still has to match what we asked. A bare yes to anything else never books."""
    from leads.worker import normalize_visit_date
    from zoneinfo import ZoneInfo
    from datetime import datetime
    asked = assented_slot(messages)
    if not asked:
        return False
    now = datetime.now(ZoneInfo('Asia/Kolkata'))
    requested_date = normalize_visit_date(str(arguments.get('date', '')), now)
    requested_time = visit_time(str(arguments.get('time', '')))
    return bool(requested_date and requested_time and requested_date[0] == asked[0] and requested_time == asked[1])


_NUM_WORDS = {'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9,'ten':10,'ek':1,'do':2,'teen':3,'char':4,'paanch':5,'एक':1,'दो':2,'तीन':3,'चार':4,'पांच':5}
_NUMTOK = r"(\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten)"
_OFFSET_RE = re.compile(
    r"(?:\b(?:after|in)\s+" + _NUMTOK + r"\s+days?\b"
    r"|\b" + _NUMTOK + r"\s+days?\s+(?:later|from\s+now|ahead)\b"
    r"|(?<!\w)(\d{1,2}|ek|do|teen|char|paanch|एक|दो|तीन|चार|पांच)\s+(?:din|दिन)\s+(?:baad|bad|me|mein|बाद|में)(?!\w))"
)


def _strip_day_offsets(text):
    return _OFFSET_RE.sub(' ', text)


def relative_visit_date(text, now):
    """ISO date for 'after two days' / 'do din baad' / 'day after tomorrow' / 'parso'. None when not stated."""
    from datetime import timedelta
    t = str(text).lower()
    m = _OFFSET_RE.search(t)
    if m:
        raw = next(g for g in m.groups() if g)
        n = int(raw) if raw.isdigit() else _NUM_WORDS.get(raw)
        if n and 0 < n <= 14:
            return (now + timedelta(days=n)).date().isoformat()
    if re.search(r"day after tomorrow|\bparso\b|परसों|परसो", t):
        return (now + timedelta(days=2)).date().isoformat()
    return None


def held_slot_message(arguments, now):
    """Tool-result text for a held book_site_visit. It must never read like a refusal: a later plain yes has to be bookable."""
    from leads.worker import normalize_visit_date
    args = arguments or {}
    d = normalize_visit_date(str(args.get('date', '')), now)
    t = visit_time(str(args.get('time', '')))
    if d and t:
        from datetime import date as _date
        y, m, dd = [int(x) for x in d[0].split('-')]
        day = _date(y, m, dd)
        label = f"{day.strftime('%A')}, {day.day} {day.strftime('%B')} at {t}"
        return (f"The caller has not clearly stated this slot yet, so nothing is booked. Ask exactly one short question: "
                f"\"Shall I book {label}?\" and say nothing else about booking. When the caller answers yes, call book_site_visit "
                f"with date {d[0]} and time {t} straight away. Never tell the caller you cannot confirm or that they did not say something.")
    return ("The caller has not given both a day and a time yet, so nothing is booked. In one short sentence ask for the missing day or time. "
            "Never tell the caller you cannot confirm or that they did not say something.")
