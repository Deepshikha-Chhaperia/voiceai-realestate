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
_LEAD = (r"(?:(?:hey|hi|yeah|yes|ya|ji|listen|sorry|uh|um|okay|ok|so|and|also|please|actually|haan|acha|accha)[, ]+)*"
         r"(?:(?:can|could) you (?:please )?(?:tell|share|give) me (?:about |the )?(?:details )?(?:about |on |of )?"
         r"|tell me (?:about )?|i (?:want|would like) to (?:know|ask) about |i want to know about |do you have |batao |bataiye )?")
_SEG = (r"(?:(?:the|about|on|what about|what are|what is|what's|when is|when's|is it|is the project|is there|are there|any|tell me about)\s+)*"
        r"({topics})(?:\s+(?:details|information|info|status|are there|there|hai|hain|kya hai|kab hai|kya hain))*")
_SPLIT = r"\s*(?:,|&|\band\b|\baur\b|\bplus\b|\bas well as\b|\balso\b|और)\s*"


_FILLER = r"(?:(?:hey|hi|hii|hello|yeah|yes|yep|ya|ji|listen|sorry|one more thing|also tell me|uh|um|umm|hmm|actually|okay|ok|so|well|then|and|also|please|haan|acha|accha|sir|ma'am)\b[ ,]*)+"


# Coverage matching (fix23). A question is answered from cache only when EVERY word in it is either part of a cached topic
# or a plain function/filler word. One unknown content word (price, parking, a number...) sends the WHOLE question live.
_COVER_TOPICS = (
    ("faq_vastu", r"(?:vastu|vaastu|वास्तु)(?: compliant| compliance)?"),
    ("faq_possession", r"(?:(?:possession|पजेशन|पज़ेशन|handover|hand over)(?: date| time| timeline| timeframe)?"
                       r"|(?:कब तक|kab tak)(?: का| ka)?(?: है| hai)?|kab milega|when (?:will it be|is it) ready)"),
    ("faq_amenities", r"(?:amenities|amenity|facilities|facility|swimming pool|pool|gym|club ?house|सुविधाएं|सुविधाएँ|सुविधा)"),
)
_COVER_RE = re.compile("|".join(f"(?P<{k}>{p})" for k, p in _COVER_TOPICS))
_HINDI = frozenset("kya hai hain ho hoga hogi ka ki ke ko mein se bhi aur yaar bhai batao bataiye bataye bata dijiye mujhe mereko mere muje aap aapko toh na haan acha accha achha theek thik wala wali vale baare bare kab tak milega pehle pahle thoda thodi sab sabhi ye yeh vo woh ab bolo bol dena de dijiye mujhko humein hame humko hamein hum main mai hoon hu janna jaanna chahta chahti mereko".split())
_FILLER_WORDS = frozenset((
    "hey hi hii hello yeah yes yep ya ji listen sorry uh um umm uhh hmm hm er ah oh okay ok so well then and also too actually basically just "
    "please sir madam maam ma'am sure alright right fine now first once quickly briefly deal "
    "can could would should will may might shall do does have has had get got let lets let's want need wanna know tell give share say show explain "
    "i i'm i'd i'll i've me my mine you your we us our it its it's is are was were be been being am there there's that that's this these those "
    "what what's which the a an of on about in at for with to as regarding related more some any all both every project details detail info information status "
    "kind of sort of"
).split()) | _HINDI
_DEV_WORDS = frozenset("और है हैं क्या का की के को में भी बताओ बताइए बताइये बता यार जी हाँ हां अच्छा मुझे आप ठीक तो हो थोड़ा थोड़ी पहले सब ये वो मेरे मेरेको अब ना बोलो बारे बताएं बताएँ बतायें बताना सुनाओ सुनाइए हमें हमको हम मुझको मैं हूँ हूं जानना चाहता चाहती कब उसके इसके उसका इसका था थी थे होगा होगी रहा रही आज".split())
_PRE_NORM = ((re.compile(r"बता (?:दो|दीजिए|दीजिये|देना)(?![\u0900-\u097F])"), "बता"), (re.compile(r"\band all\b|ये सब|\bye sab\b|\bsab kuch\b|\bi mean\b"), " "), (re.compile(r"\bbata do\b"), "bata"), (re.compile(r"\bdo na\b|दो ना"), " "), (re.compile(r"\b(?:how|what) about\b"), " "), (re.compile(r"\bkind of\b|\bsort of\b|\bone more thing\b|\banother thing\b|\bby the way\b|\bwagera\b|\bvagaira\b"), " "))


_NEG = frozenset("no nope nahi nahin नहीं नही".split())
_ASK_MARKS = frozenset("tell batao bata bataiye bataye बताओ बताइए बताइये बता बताएं बताएँ बतायें बताना सुनाओ सुनाइए बारे baare bare about what what's kya क्या know जानना janna jaanna explain give share show".split())
_ANYWHEN = frozenset(("when", "kab", "कब"))


# Property topics a caller could ask about instead of (or besides) the cached ones. Fence for the preamble tolerance above:
# any of these in the preamble sends the whole question live. This is a closed domain set, not a filler list.
_OTHER_TOPICS = frozenset((
    "price prices pricing cost costs rate rates budget kitna kitni kitne emi loan loans finance bank payment payments discount offer offers "
    "parking park metro station school schools hospital hospitals mall market location locations address direction directions distance nearby "
    "floor floors tower towers size sizes area areas sqft sq feet carpet bhk flat flats unit units builder developer rera registration "
    "maintenance charges brochure floorplan plan plans whatsapp visit book booking pet pets lift elevator water power electricity backup "
    "mat dont don't not never without except only just skip stop ignore wait later baad pehle first sirf bina chhod "
    "bedroom bedrooms room rooms bed two three four five six seven eight nine ten teen char paanch panch तीन चार पांच पाँच कमरा कमरे बेडरूम "
    "east west north south facing view corner side furnished furnishing lease rent resale ready construction status progress delivery tax gst stamp duty nri security garden pool "
    "मत बिना सिर्फ छोड़ बाद पहले रुको कीमत दाम रेट बजट पैसे कितना कितनी कितने लोन बैंक पार्किंग मेट्रो स्कूल अस्पताल लोकेशन पता दूरी फ्लोर टावर साइज एरिया बिल्डर रजिस्ट्रेशन "
    "मेंटेनेंस ब्रोशर प्लान व्हाट्सएप विजिट बुकिंग लिफ्ट पानी बिजली किराया ऑफर डिस्काउंट"
).split())

# Grammar words (fix32): pronouns, copulas, politeness, particles, request verbs. They carry no property content, so any number of them
# may stand in the preamble before an explicit ask ("आप एक काम करो ना मेरे को सबसे पहले वो बता दो ना amenities और possession").
# Not here on purpose: nouns, numbers above one, property words, negations/refusals (_NEG, _OTHER_TOPICS) and anything that could name a topic.
_GRAMMAR = frozenset((
    "आप आपको आपसे आपका आपकी आपके तुम तुमको तू मैं मैंने मुझे मुझको मेरे मेरा मेरी मेरेको मेरे को हम हमें हमको हमारे हमारा हमारी वो वह ये यह इस उस इन उन "
    "एक काम करो कर करना करिए करिये कीजिए कीजिये करके कृपया प्लीज़ प्लीज ज़रा जरा ज़रूर जरूर सबसे सबसे पहले पहले सबसे पहिले फिर तब तो ही भी बस सिर्फ़ ना न नाअ यार भाई भैया दीदी सर मैडम जी "
    "है हैं हो हूँ हूं था थी थे होगा होगी होंगे रहा रही रहे सकते सकता सकती सकें लगा लगे लिए लिये से में पर का की के कुछ थोड़ा थोड़ी बहुत वाला वाली वाले "
    "सा सी सारा सारे सारी ज्यादा ज़्यादा काफी काफ़ी कोई किसी मतलब मतलब कि की तरह बारे वगैरह वैसे अच्छा अरे हाँ हां ठीक चलो चलिए देखो देखिए सुनो सुनिए सुनिये बोलो बोलिए पूछना पूछ पूछो जानना जानने चाहिए चाहता चाहती चाहते आज अभी वैसे "
    "aap aapko aapse aapka aapki tum tumko main mujhe mujhko mera meri mere mereko hum humein humko wo woh vo ye yeh is us in un ek kaam karo kar karna kariye kijiye karke "
    "zara jara zaroor jaroor sabse pehle pahle phir tab toh to hi bhi bas na nah yaar bhai bhaiya didi sir madam ji hai hain ho hoon tha thi the hoga hogi honge raha rahi rahe "
    "sakte sakta sakti lagta liye se mein par ka ki ke kuch thoda thodi bahut wala wali wale waise acha accha arre haan theek chalo chaliye dekho suno suniye bolo boliye "
    "sa si zyada jyada kaafi kaafi koi kisi matlab ki tarah puchna pucho janna jaanna chahiye chahta chahti chahte aaj abhi "
    "actually basically honestly seriously literally really kindly simply obviously anyway anyways like yaar bro dude man well okay ok so um uh umm hmm hey hi hello please "
    "just kindly could can would you your you're me my i i'm we our us if possible first of all firstly initially quickly "
    "do does did will shall let lets let's want wanna like need needed going gonna "
).split())

# Ask-phrase verbs (fix33): "बता सकते हो क्या", "bata sakti ho", "बता दोगे", "ये क्या बोलते हैं". Speaking/ability verbs and their endings carry no property
# content, so they are accepted in any position, like the other function words. Negations are still caught by _NEG.
_ASK_VERBS = frozenset((
    "सकते सकती सकता सकें सकूँ सकूं सको दोगे दोगी दोगे दीजियेगा दीजिएगा दीजिये दीजिए दीजिए बोलते बोलती बोलता बोलिए बोलिये बोलो कहते कहती कहता कहिए कहिये कहो सुनाइये सुनाइएगा सुनाना बताइयेगा बताइएगा "
    "बताया बताना बतलाइए जानकारी "
    "sakte sakti sakta sakein sako dogey doge dogi dijiyega dijiye bolte bolti bolta boliye bolo kehte kehti kehta kahiye kaho sunaiye sunana bataiyega batana jankari "
).split())

_VOCATIVE = frozenset("ji जी sir सर madam मैडम maam ma'am".split())


def faq_cover(text, lead_no=False):
    """Cached FAQ topics in a question, in asked order, or None. All-or-nothing: any uncovered word means None.

    Returns (keys, has_hindi). Never guesses: unknown words, numbers, negations and mixed requests all go live.
    lead_no: allow ONE discourse "no/nahi/नहीं" (max two) at the very start, before the first topic, only when the rest is an
    explicit ask (tell/batao/about/what/kya...). Any other negation, or no ask word, stays live."""
    t = re.sub(r"\s+", " ", str(text or "").lower().replace("\u2019", "'").strip())
    if not t or len(t) > 220:
        return None
    for rx, rep in _PRE_NORM:
        t = rx.sub(rep, t)
    keys, parts, last = [], [], 0
    for m in _COVER_RE.finditer(t):
        k = m.lastgroup
        if k not in keys:
            keys.append(k)
        parts.append(t[last:m.start()])
        last = m.end()
    if not keys:
        return None
    parts.append(t[last:])
    rest = " § ".join(parts)
    if re.search(r"(?<!can )(?<!could )(?<!may )\bi know\b", rest):
        return None
    words = [re.sub(r"'s$", "", w) if w not in _FILLER_WORDS else w for w in re.split(r"[\s,.?!।;:\-\u2013\u2014\"()]+", rest) if w]
    hindi = bool(re.search(r"[\u0900-\u097F]", t))
    first_topic = words.index("§") if "§" in words else len(words)
    negs = [n for n, w in enumerate(words) if w in _NEG]
    if negs:
        if not lead_no or len(negs) > 2 or negs[-1] >= first_topic or negs[-1] > 9 or not any(w in _ASK_MARKS for w in words):
            return None
    prev = ""
    has_ask = any(w in _ASK_MARKS for w in words)
    pre_unknown = 0
    for i, w in enumerate(words):
        if w == "§" or w in _NEG:
            prev = w
            continue
        if w == "like":
            if prev in {"i", "we", "you", "they", "really", "also"}:
                return None
        elif w in _ANYWHEN:
            if "faq_possession" not in keys:
                return None
        elif w not in _FILLER_WORDS and w not in _DEV_WORDS and w not in _ASK_VERBS:
            # Preamble tolerance (fix32): before the first cached topic, with an explicit ask and no negation, any number of grammar words
            # (_GRAMMAR) may precede the topics. Anything else, including a content word like "jacuzzi", a property topic
            # (_OTHER_TOPICS), a digit, or any word between/after topics, sends the whole question live.
            if w in _GRAMMAR and i < first_topic and has_ask and w not in _OTHER_TOPICS and not re.search(r"\d", w):
                prev = w
                continue
            # One unknown token is allowed only as a name addressing the bot ("निया जी", "Ananya ji"): the next word is a vocative marker.
            if i < first_topic and has_ask and pre_unknown < 1 and words[i + 1:i + 2] and words[i + 1] in _VOCATIVE and w not in _OTHER_TOPICS and not re.search(r"\d", w):
                pre_unknown += 1
                prev = w
                continue
            if has_ask:
                try:
                    from loguru import logger
                    logger.info("FAQ_COVER_MISS topics={} blocker={!r}", keys, w)
                except Exception:
                    pass
            return None
        if w in _HINDI or re.search(r"[\u0900-\u097F]", w):
            hindi = True
        prev = w
    return keys, hindi


def faq_keys(text, lead_no=False):
    """Two or more cached FAQ topics in one question (coverage match, see faq_cover). None otherwise."""
    r = faq_cover(text, lead_no)
    return r[0] if r and len(r[0]) >= 2 else None


def faq_single(text, lead_no=False):
    """One cached English FAQ topic asked with only filler words. English only: a Hindi/mixed single ask stays live (the clip is English)."""
    r = faq_cover(text, lead_no)
    return r[0][0] if r and len(r[0]) == 1 and not r[1] else None


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
    'faq_2bhk_options': 'We have the two B H K from eleven hundred fifty to thirteen hundred twenty square feet, with one balcony, from ninety five lakhs. Would you like to schedule a site visit?',
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
    lead_no = not (memory.get('_whatsapp_consent_action') or memory.get('_wa_pack_state') or memory.get('_brochure_consent_pending'))
    keys = faq_keys(text, lead_no)
    if keys:
        return combo_key(keys), combo_text(keys)
    key = faq_key(text)
    if key:
        return ('faq_amenities_v2', CALL5_TEXTS['faq_amenities_v2']) if key == 'faq_amenities' else (key, FAQ_TEXTS[key])
    if re.fullmatch(r'(?:haan[, ]+|okay[, ]+)?(?:vastu (?:ke baare mein )?(?:batao|bata do|bata sakte ho)|(?:aap )?vastu ka bol sakte ho kya hai)', t):
        return 'faq_vastu_hi', CALL5_TEXTS['faq_vastu_hi']
    single = faq_single(text, lead_no)
    if single:
        return ('faq_amenities_v2', CALL5_TEXTS['faq_amenities_v2']) if single == 'faq_amenities' else (single, FAQ_TEXTS[single])
    if re.fullmatch(r'(?:yeah |yes |haan )?(?:i(?: am|\'m) looking for (?:a )?)?(?:2|two)\s*bhk', t) and all(v in script for v in ('1150', '1320', 'ninety-five lakhs', '1 balcony')):
        return 'faq_2bhk_options', CALL5_TEXTS['faq_2bhk_options']
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
    # lone connectors/hesitations/curses are broken STT, never an answer: re-asking the last question over them is the bug
    return t in {"c", "i", "i'm", "im", "i said i", "and", "but", "so", "then", "or", "the", "a", "to", "uh", "um", "shit", "damn"}


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
    # WhatsApp visit pack. Standalone lines only: they follow a caller turn, never a live-voice line, so cached audio cannot overlap speech.
    'wa_pack_best_number_v1': "Sure, what is the best number for WhatsApp?",
    'wa_pack_rest_v1': "Okay, please say the rest of the number.",
    'wa_pack_same_retry_v1': "Sorry, is it the same number you're calling from, yes or no?",
    'wa_pack_number_retry_v1': "Sorry, please say the ten digit number for WhatsApp.",
    'wa_pack_invalid_v1': "Sorry, that doesn't look like a valid mobile number. Please say all ten digits again.",
    'wa_pack_again_v1': "Sorry about that. Please say the number again.",
    'wa_pack_right_retry_v1': "Sorry, is that number right, yes or no?",
    'wa_pack_decline_bye_v1': "No problem, I won't send anything. Thank you. Goodbye!",
    'wa_pack_done_ready_bye_v1': "Done, I'll send it right after this call. Thank you. Goodbye!",
    'wa_pack_done_team_bye_v1': "Our team will WhatsApp you the details after this call. Thank you. Goodbye!",
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


def booking_correction(text, memory, now):
    """After a verified booking, a caller who names a different concrete time (and/or day) is correcting it: (date_iso, 'H:MM PM').

    None for farewells, durations ("5 minutes"), cancellations, the same slot again, or nothing concrete."""
    if not memory or memory.get('disposition') != 'SITE_VISIT_BOOKED' or not memory.get('visit_date_iso') or not memory.get('time_slot'):
        return None
    t = str(text or '').lower()
    if re.search(r"don't|dont|cancel|not interested|\bmin(?:ute)?s?\b|\bhours?\b|\bseconds?\b|\bweeks?\b|\bmonths?\b|\b(?:bhk|crore|lakh|lac|sq|balcon\w*)\b", t):
        return None
    slot = visit_time(text)
    if not slot:
        # after a booking a bare number right behind a change/hedge cue is a time ("make it six", "maybe five")
        cue = re.search(r"(?:\bno[,.]?|\bnahi[,.]?|नहीं[,.]?|make it|make that|change it to|change to|shift it to|move it to|instead|actually|how about|what about|maybe|perhaps|shayad|शायद)\s+(?:to\s+|at\s+)?(\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|paanch|teen|chaar|saat|aath|nau|das|[\u0900-\u097F]+)(?!\w)(?!\s*(?:bhk|crore|lakh|people|log|persons))", t)
        if cue:
            slot = visit_time(cue.group(1))
    if not slot:
        # a date-only change ("make it tomorrow") keeps the booked time; the caller is always asked before it is written
        if re.search(r"\btomorrow\b|\bkal\b|कल|day after tomorrow|parso|परसों|परसो", t) and (relative_visit_date(text, now) or "tomorrow" in t or "kal" in t.split() or "कल" in t):
            slot = memory['time_slot']
        else:
            return None
    date = relative_visit_date(text, now)
    if not date and re.search(r"\btomorrow\b|\bkal\b|कल", t) and not re.search(r"day after|parso|परसों|परसो", t):
        from datetime import timedelta
        date = (now + timedelta(days=1)).date().isoformat()
    date = date or memory['visit_date_iso']
    if date == memory['visit_date_iso'] and slot == memory['time_slot']:
        return None
    return date, slot


_AMBIGUOUS = re.compile(r"\b(?:or|if|ya)\b|या\b")  # two candidate times, or a conditional: not one stated time
_ASIDE = re.compile(r"\b(?:free|busy|available|meeting|reach|takes?|leaving|working|office|lunch|people|persons|kids)\b")  # a time inside a sentence about something else
_NUMW = r"(?:\d{1,2}(?::\d{2})?|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|paanch|teen|chaar|saat|aath|nau|das|[\u0900-\u097F]+)"
_HEDGED_TIME = re.compile(r"\b(?:maybe|around|about|perhaps|approx\w*|shayad|lagbhag|kareeb)\s+(?:like\s+)?(?:at\s+|around\s+)?" + _NUMW + r"(?!\w)|शायद|लगभग|करीब")
_CUE = re.compile(r"\b(?:make it|change|shift|move|reschedule|instead|actually|rather|make that|set it)\b|(?:\bat|\bby)\s+(?:\d|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)|बजे|\bbaje\b|o'?clock|\b[ap]\.?m\b|\d:\d\d")


_CHANGE_CMD = re.compile(r"\b(?:change|shift|move|reschedule|make it|make that|set it|put it|badal\w*|badlo)\b|बदल|चेंज|शिफ्ट")


def explicit_change(text):
    """The caller commands a change ("change it to five", "change करके 5 बजे कर दो"). A bare time is not a command."""
    return bool(_CHANGE_CMD.search(str(text or "").lower()))


def correction_is_certain(text):
    """A time is stated: write it and say it back in one line, hedged or not ("maybe like around five").
    Ask only for two candidate times, a conditional, or a time inside a sentence about something else."""
    t = str(text or "").lower().strip()
    if _AMBIGUOUS.search(t) or _ASIDE.search(t):
        return False
    if _CUE.search(t) or _HEDGED_TIME.search(t):
        return True
    return len(re.findall(r"\w+", t)) <= 2


_NO_REPLY = {"no", "nope", "nahi", "nahin", "na", "नहीं", "नही", "dont", "don't", "wrong", "cancel"}


def correction_reply(text):
    """'yes' / 'no' / None for the caller's answer to "So, five o'clock instead?". Anything else drops the question without writing."""
    raw = str(text or "").lower().replace("हाँ", "haan").replace("हां", "haan").replace("जी", "ji").replace("ठीक है", "theek hai").replace("नहीं", "nahi").replace("नही", "nahi")
    words = re.sub(r"[^a-z' ]", " ", raw).split()
    if not words or len(words) > 4:
        return None
    if any(w in _NO_REPLY for w in words):
        return "no"
    return "yes" if all(w in _ASSENT for w in words) else None


def correction_decision(text, memory, now, pending=None, age=0.0, strict=False):
    """The whole post-booking correction policy as one pure function.
    Returns ('none',) | ('write', date, slot) | ('ask', date, slot) | ('keep',) | ('drop',).
    A write happens only for a stated, unhedged time, or after the caller said yes to our question."""
    def judge(corr):
        return ('write' if correction_is_certain(text) and corr[1] != memory.get('time_slot') else 'ask', corr[0], corr[1])  # date-only (same time) is always asked
    new = booking_correction(text, memory, now)
    if pending and age <= 45:
        if new and new == (pending[0], pending[1]):
            return ('write', pending[0], pending[1])  # caller restated the same time: that is a yes
        if new:
            return judge(new)  # a further correction ("no, six", "yes but make it six") is judged on its own words
        ans = correction_reply(text)
        if ans == 'yes':
            return ('write', pending[0], pending[1])
        if ans == 'no':
            return ('keep',)
        return ('drop',)
    if pending:
        return ('drop',)  # stale question: never write on a late "yes"
    if not new:
        return ('none',)
    j = judge(new)
    # strict (no goodbye in progress): a bare time mentioned mid-conversation is never written without asking; an explicit change command with a stated time is
    if strict and not (j[0] == 'write' and explicit_change(text)):
        return ('ask', j[1], j[2])
    return j


# ---- STT repair: fixes misheard domain words only (it is not intent matching) ----
_DOMAIN = ("amenities", "possession", "balcony", "balconies", "brochure", "clubhouse", "apartment", "apartments", "location", "parking", "security", "jogging", "square")
_TOPICS = ("amenities", "possession")  # cached FAQ topics a conjunction slot may be repaired to
_REAL_WORDS = frozenset("session sessions packing dogging vacation position positions procession scare squares locations apartments".split())
_BOT_NAME_HEARD = frozenset("ajay anaya annaya anya aniya ananiya ananya ayana aanya anaiya ananaya".split())
_SKEL = lambda w: re.sub(r"[aeiou]", "", w)


_DV_CONS = {"क":"k","ख":"kh","ग":"g","घ":"gh","ङ":"n","च":"ch","छ":"chh","ज":"j","झ":"jh","ञ":"n","ट":"t","ठ":"th","ड":"d","ढ":"dh","ण":"n",
            "त":"t","थ":"th","द":"d","ध":"dh","न":"n","प":"p","फ":"ph","ब":"b","भ":"bh","म":"m","य":"y","र":"r","ल":"l","व":"v","श":"sh","ष":"sh",
            "स":"s","ह":"h"}
_DV_NUKTA = {"क":"k","ख":"kh","ग":"g","ज":"z","ड":"r","ढ":"rh","फ":"f"}
_DV_IND = {"अ":"a","आ":"a","इ":"i","ई":"i","उ":"u","ऊ":"u","ए":"e","ऐ":"e","ओ":"o","औ":"o","ऑ":"o","ऍ":"e","ऋ":"ri"}
_DV_MATRA = {"ा":"a","ि":"i","ी":"i","ु":"u","ू":"u","े":"e","ै":"e","ो":"o","ौ":"o","ॉ":"o","ॅ":"e","ृ":"ri"}


def translit_devanagari(word):
    """Deterministic Devanagari -> rough Latin (no model, no network). Only used to compare sounds; never shown to the caller."""
    w = str(word)
    out = []
    i = 0
    while i < len(w):
        c = w[i]
        nxt = w[i + 1] if i + 1 < len(w) else ""
        if c in _DV_CONS:
            if nxt == "\u093c":
                out.append(_DV_NUKTA.get(c, _DV_CONS[c]))
                i += 1
                nxt = w[i + 1] if i + 1 < len(w) else ""
            else:
                out.append(_DV_CONS[c])
            if nxt in _DV_MATRA:
                out.append(_DV_MATRA[nxt]); i += 1
            elif nxt == "\u094d":
                i += 1
            elif i + 1 < len(w):
                out.append("a")  # inherent vowel; dropped at word end
        elif c in _DV_IND:
            out.append(_DV_IND[c])
        elif c in "\u0902\u0901":
            out.append("n")
        i += 1
    return "".join(out)


def phon_key(word):
    """Coarse consonant key shared by English spellings and transliterated Hindi: amenities and एमेनिटीज़ both give 'mnts'."""
    t = str(word).lower()
    for a in ("ssion", "tion", "sion"):
        t = t.replace(a, "shan")
    for a, b in (("chh", "s"), ("sh", "s"), ("ch", "s"), ("kh", "k"), ("gh", "k"), ("th", "t"), ("dh", "t"), ("bh", "b"), ("ph", "p"), ("jh", "s")):
        t = t.replace(a, b)
    t = re.sub(r"[aeiouhy]", "", t).translate(str.maketrans("zjcqgdfw", "sskkktpv"))
    return re.sub(r"(.)\1+", r"\1", t)


def _sim(a, b):
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio(), SequenceMatcher(None, _SKEL(a), _SKEL(b)).ratio()


def repair_stt(text, protected=()):
    """Conservative post-STT repair: (new_text, [(heard, fixed, rule)]).
    Rule vocative: "Hey Ajay," -> "Hey," (the bot's own name misheard; it carries no intent).
    Rule near: a Latin-script word of 7+ letters that is a near misspelling of a domain word (spelling AND consonant skeleton both >= 0.8).
    Rule slot: the unknown word right after "and/aur/&" that follows a topic word (possession and the manacles), when its consonant
    skeleton is >= 0.6 of a cached topic and its length is within one letter. Numbers, times, names and short words are never touched."""
    raw = str(text or "")
    snaps = []
    protected = {str(p).lower() for p in protected if p}
    m = re.match(r"^(\s*(?:hey|hi|hello))\s+([A-Za-z]+)\s*,", raw, re.I)
    if m and m.group(2).lower() in _BOT_NAME_HEARD:
        snaps.append((m.group(2), "", "vocative"))
        raw = m.group(1) + "," + raw[m.end():]
    tokens = list(re.finditer(r"[A-Za-z]+|[\u0900-\u097F]+", raw))
    out, last, prev_topic, after_and = [], 0, False, False
    for tk in tokens:
        w = tk.group(0); lw = w.lower()
        gap = raw[last:tk.start()]
        fixed = None
        dev = bool(re.match(r"[\u0900-\u097F]", w))
        if dev:
            lw = translit_devanagari(w)
        adjacent_digit = bool(re.search(r"\d\s*$", raw[:tk.start()])) or bool(re.match(r"\s*\d", raw[tk.end():]))
        if dev and w.lower() not in protected and not adjacent_digit and len(lw) >= 5:
            from difflib import SequenceMatcher
            key = phon_key(lw)
            def kr(d):
                return SequenceMatcher(None, key, phon_key(d)).ratio()
            best = max(_DOMAIN, key=kr)
            if len(key) >= 3 and kr(best) >= 0.9 and 0.7 <= len(lw) / len(best) <= 1.4:
                fixed = (best, "hindi-near")
            elif after_and and prev_topic:
                best = max(_TOPICS, key=kr)
                if kr(best) >= 0.65 and abs(len(key) - len(phon_key(best))) <= 1 and best not in raw.lower():
                    fixed = (best, "hindi-slot")
        elif lw not in protected and not adjacent_digit and lw not in _REAL_WORDS and lw not in _DOMAIN and len(lw) >= 6:
            if len(lw) >= 7:
                best = max(_DOMAIN, key=lambda d: _sim(lw, d)[0])
                r, k = _sim(lw, best)
                if r >= 0.8 and k >= 0.8 and abs(len(lw) - len(best)) <= 2 and lw[0] == best[0]:
                    fixed = (best, "near")
            if not fixed and after_and and prev_topic:
                best = max(_TOPICS, key=lambda d: _sim(lw, d)[1])
                if _sim(lw, best)[1] >= 0.6 and abs(len(lw) - len(best)) <= 1 and best not in raw.lower():
                    fixed = (best, "slot")
        if fixed:
            snaps.append((w, fixed[0], fixed[1]))
            w = fixed[0]
        out.append(gap + w)
        last = tk.end()
        conj = lw in ("and", "aur", "also") or w == "और"
        if conj:
            after_and = True
        elif lw not in ("the", "a", "an", "about", "of", "on", "for", "my", "your", "our", "its") and w not in ("का", "की", "के", "भी", "में", "को", "से", "वो", "वह"):
            after_and = False
        prev_topic = (lw in _TOPICS or (fixed and fixed[0] in _TOPICS)) if not conj and lw not in ("the", "a", "an") else prev_topic
    out.append(raw[last:])
    return "".join(out), snaps


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
    if action not in {'brochure', 'location', 'visit_pack'}:
        return None, "I can't prepare those details right now."
    actions = set(memory.get('_postcall_whatsapp_actions') or []) | {action}
    if 'visit_pack' in actions:
        actions = {'visit_pack'}  # one message already carries confirmation, location and brochure
    plan = {'actions': sorted(actions), 'delivery': 'manual', 'status': 'prepared_not_sent'}
    check = manual_whatsapp_message({**memory, '_postcall_whatsapp_actions': sorted(actions)}, config)
    error = ("I can't share the details right now." if action == 'visit_pack' else "I can't share the location right now." if action == 'location' else "I can't share the brochure right now.") if check['blockers'] else None
    return plan, error


def manual_whatsapp_message(memory, config):
    """One copyable draft after teardown; never invent links or booking facts."""
    from urllib.parse import urlparse
    actions = set(memory.get('_postcall_whatsapp_actions') or [])
    if not actions:
        return {'status': 'not_requested', 'text': '', 'blockers': []}
    cfg = config.get('brochure_delivery', {}) or {}
    lines = [str(cfg.get('visit_pack_intro') or 'Thank you for your interest.')]
    blockers = []
    requested = []
    if 'visit_pack' in actions:
        found = []
        for label, key in (('Location', 'postcall_location_url'), ('Brochure', 'brochure_url'), ('Floor plans', 'floorplan_url')):
            value = str(cfg.get(key) or '').strip()
            parsed = urlparse(value)
            if parsed.scheme == 'https' and parsed.netloc:
                found.append(f'{label}: {value}')
        if not found:
            blockers.append('visit pack: configure at least one HTTPS URL in brochure_delivery')
        lines += found
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
    if error is None and action == 'visit_pack':
        try:
            ready = whatsapp_ready()
        except Exception:
            ready = False
        default = "Done, I'll send it right after this call." if ready else 'Our team will WhatsApp you the details after this call.'
    else:
        default = 'Our team will WhatsApp you the details after this call.'
    return None if already else error or default


_DIGIT_WORDS = {
    'zero': '0', 'oh': '', 'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9',
    'shunya': '0', 'ek': '1', 'do': '2', 'teen': '3', 'char': '4', 'chaar': '4', 'paanch': '5', 'panch': '5', 'chhe': '6', 'chhah': '6', 'che': '6', 'chah': '6',
    'saat': '7', 'aath': '8', 'nau': '9',
    'शून्य': '0', 'एक': '1', 'दो': '2', 'तीन': '3', 'चार': '4', 'पांच': '5', 'पाँच': '5', 'छह': '6', 'छः': '6', 'छे': '6', 'सात': '7', 'आठ': '8', 'नौ': '9',
}
_REPEAT = {'double': 2, 'dabal': 2, 'triple': 3, 'teeple': 3, 'तीन बार': 3, 'डबल': 2, 'ट्रिपल': 3}
_PACK_YES = {'yes', 'yeah', 'yep', 'yup', 'sure', 'ok', 'okay', 'correct', 'right', 'fine', 'same', 'haan', 'han', 'ha', 'haa', 'ji', 'bilkul', 'theek', 'thik', 'hai', 'sahi', 'please', 'it', 'is', 'that', 'this', 'number', 'the'}
_PACK_YES_STRONG = {'yes', 'yeah', 'yep', 'yup', 'sure', 'ok', 'okay', 'correct', 'right', 'same', 'haan', 'han', 'ha', 'haa', 'ji', 'bilkul', 'theek', 'thik', 'sahi'}
_PACK_NO = {'no', 'nope', 'nahi', 'nahin', 'different', 'another', 'other', 'alag', 'wrong', 'galat', 'not'}
_PACK_DECLINE = re.compile(r"(?:\bno thanks?\b|\bno need\b|\bnot (?:required|needed|necessary)\b|\bdon'?t (?:send|want)\b|\bdo not send\b|\bnahi chahiye\b|\bmat bhej|\bskip\b|\bno whatsapp\b|\bnot interested\b|\bleave it\b|\bजरूरत नहीं\b|\bनहीं चाहिए\b|\bमत भेज)", re.I)


def pack_phone(raw):
    """E.164 Indian mobile from digits, or ''. Never invents or pads digits."""
    d = re.sub(r'\D', '', str(raw or ''))
    if len(d) == 12 and d.startswith('91'):
        d = d[2:]
    elif len(d) == 11 and d.startswith('0'):
        d = d[1:]
    return '+91' + d if len(d) == 10 and d[0] in '6789' else ''


def spoken_digits(text):
    """Digits the caller said, in order: numerals, English/Hindi digit words, double/triple. Other words are ignored."""
    t = str(text or '').lower()
    t = re.sub(r'[०-९]', lambda m: str(ord(m.group()) - 0x966), t)
    t = re.sub(r"(?<!\w)(?:kar|karo|kardo|de|dijiye|kijiye|कर|दे)\s+(?:do|दो)(?!\w)", ' ', t)  # "kar do" = please do, not 2
    out, repeat = [], 1
    for tok in re.findall(r"\d+|[^\s\d,.\-:;!?()]+", t):
        if tok.isdigit():
            out.append(tok * repeat)
            repeat = 1
        elif tok in _REPEAT:
            repeat = _REPEAT[tok]
        elif tok in _DIGIT_WORDS:
            out.append(_DIGIT_WORDS[tok] * repeat)
            repeat = 1
        else:
            repeat = 1
    return ''.join(out)


def _pack_has_content(config):
    from urllib.parse import urlparse
    cfg = (config or {}).get('brochure_delivery') or {}
    return any(urlparse(str(cfg.get(k) or '').strip()).scheme == 'https' and urlparse(str(cfg.get(k) or '').strip()).netloc
               for k in ('postcall_location_url', 'brochure_url', 'floorplan_url'))


def visit_pack_offer(memory, config, caller_phone, is_web, ready=False):
    """Spoken offer appended to a VERIFIED booking confirmation. '' (old behaviour) for web calls, when disabled, or with nothing to send."""
    if memory is None or is_web or not (config or {}).get('visit_pack_whatsapp_enabled', False) or not _pack_has_content(config):
        return ''
    if memory.get('_wa_pack_state') or 'visit_pack' in (memory.get('_postcall_whatsapp_actions') or []):
        return ''
    who = "I'll" if ready else "Our team will"
    caller = pack_phone(caller_phone)
    memory['_whatsapp_consent_action'] = 'pack'
    memory['_wa_pack_tries'] = 0
    if caller:
        memory['_wa_pack_caller'] = caller
        memory['_wa_pack_state'] = 'asked_same'
        return f" {who} WhatsApp you the confirmation, location and brochure. Is this the same number you're calling from?"
    memory['_wa_pack_state'] = 'ask_number'
    return f" {who} WhatsApp you the confirmation, location and brochure. What is the best number for it?"


def _read_back(phone):
    names = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']
    return ', '.join(names[int(c)] for c in phone[-10:])


def _pack_yes_no(text):
    raw = str(text or '').lower()
    for k, v in {'हाँ': 'haan', 'हां': 'haan', 'जी': 'ji', 'ठीक है': 'theek hai', 'सही': 'sahi', 'नहीं': 'nahi', 'नही': 'nahi', 'गलत': 'galat'}.items():
        raw = raw.replace(k, v)
    words = re.sub(r"[^a-z ]", ' ', raw).split()
    if not words:
        return None
    if any(w in _PACK_NO for w in words):
        return 'no'
    if len(words) <= 6 and any(w in _PACK_YES_STRONG for w in words):
        return 'yes'
    return None


def pack_step(text, memory):
    """One caller turn of the WhatsApp visit-pack flow. Returns (line_to_speak, outcome) with outcome None | 'consent' | 'declined'.
    Nothing is ever recorded for sending unless the caller said yes to a number we read back (or to their own calling number)."""
    state = memory.get('_wa_pack_state')
    if not state:
        return None, None

    def finish(outcome, phone=''):
        memory.pop('_wa_pack_state', None); memory.pop('_wa_pack_cand', None); memory.pop('_wa_pack_digits', None)
        memory.pop('_whatsapp_consent_action', None)
        if outcome == 'consent':
            memory['_wa_pack_phone'] = phone
        return None, outcome

    if _PACK_DECLINE.search(str(text or '')):
        return finish('declined')
    tries = memory.get('_wa_pack_tries', 0)

    def give_up():
        return finish('declined')

    def ask_again(line):
        memory['_wa_pack_tries'] = tries + 1
        if memory['_wa_pack_tries'] > 3:
            return give_up()
        return line, None

    def read_back(phone):
        memory['_wa_pack_cand'] = phone
        memory['_wa_pack_state'] = 'confirm_other'
        memory['_wa_pack_digits'] = ''
        return f"I have {_read_back(phone)}. Is that right?", None

    digits = spoken_digits(text)
    if state == 'asked_same':
        if len(digits) >= 10:
            phone = pack_phone(digits[-12:] if digits.startswith('91') and len(digits) >= 12 else digits[-10:])
            if phone:
                return read_back(phone)
        yn = _pack_yes_no(text)
        if yn == 'yes':
            return finish('consent', memory.get('_wa_pack_caller', ''))
        if yn == 'no':
            memory['_wa_pack_state'] = 'ask_number'
            memory['_wa_pack_digits'] = ''
            return CALL7_TEXTS['wa_pack_best_number_v1'], None
        return ask_again(CALL7_TEXTS['wa_pack_same_retry_v1'])
    if state == 'ask_number':
        acc = (memory.get('_wa_pack_digits') or '') + digits
        phone = pack_phone(acc)
        if phone:
            return read_back(phone)
        if len(acc) >= 10:
            memory['_wa_pack_digits'] = ''
            return ask_again(CALL7_TEXTS['wa_pack_invalid_v1'])
        if acc:
            memory['_wa_pack_digits'] = acc
            return CALL7_TEXTS['wa_pack_rest_v1'], None
        return ask_again(CALL7_TEXTS['wa_pack_number_retry_v1'])
    if state == 'confirm_other':
        if len(digits) >= 10:
            phone = pack_phone(digits[-12:] if digits.startswith('91') and len(digits) >= 12 else digits[-10:])
            if phone:
                return read_back(phone)
        yn = _pack_yes_no(text)
        if yn == 'yes':
            return finish('consent', memory.get('_wa_pack_cand', ''))
        if yn == 'no':
            memory['_wa_pack_state'] = 'ask_number'
            memory['_wa_pack_digits'] = ''
            return ask_again(CALL7_TEXTS['wa_pack_again_v1'])
        return ask_again(CALL7_TEXTS['wa_pack_right_retry_v1'])
    return None, None


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


async def queue_goal_close(memory, coordinator, emit, acknowledgment, cached_texts=()):
    from pipecat.frames.frames import TTSSpeakFrame
    close = coordinator is not None and goal_complete(memory)
    if close and getattr(coordinator, '_dedicated_goodbye_queued', False):
        return
    text = acknowledgment + (' ' + SHORT_GOODBYE if close else '')
    frame = TTSSpeakFrame(text=text, append_to_context=True)
    # A loaded pre-rendered clip with exactly this text plays from cache; otherwise live TTS, exempt from the claim filter.
    frame.is_deterministic_confirmation = text not in cached_texts
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
    if memory.get('_wa_pack_test'):
        logger.info('[WA-PACK-TEST] [{}] consent recorded for actions={}; web test, dry run, nothing sent', call_id, actions)
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
