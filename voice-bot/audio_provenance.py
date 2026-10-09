"""Cache identity. No network, credentials or API calls during import."""
import hashlib
import json
import os
import wave
from pathlib import Path
import yaml
from call_repairs import SHORT_GOODBYE, FAQ_TEXTS, CALL5_TEXTS, CALL7_TEXTS

PHRASES = {"short_goodbye": SHORT_GOODBYE, **FAQ_TEXTS, **CALL5_TEXTS, **CALL7_TEXTS,
    # Outbound & Inbound Greetings
    "greeting_alex": "Hi, am I speaking with Alex?",
    "greeting_generic": "Hi, this is Ananya from Meridian Group. Is this a good time to talk?",
    "inbound_greeting": "Hello, thank you for calling Meridian Group. How may I assist you today?",
    "opening_intro": "Great, this is Ananya from Meridian Group. Are you looking for a 2 or 3 BHK?",

    # Acknowledgments & Affirmations (English & Hindi)
    "ack_sure": "Sure, absolutely.",
    "ack_understood": "Understood.",
    "ack_got_it": "Okay, got it.",
    "ack_ji_bilkul": "Ji bilkul.",
    "ack_haanji": "Haanji, bilkul.",
    "ack_theek_hai": "Theek hai.",

    # Clarifications & Repeats
    "checkin_generic": "Hello? Are you still there?",
    "clarify_repeat": "Sorry, I didn't catch that. Could you say that again?",
    "clarify_repeat_hi": "Sorry, main sun nahi paayi. Kya aap repeat kar sakte hain?",
    "clarify_property": "Hello? Are you looking for a property?",

    # Common Objections & Closures
    "brochure_close": "Sure, our team will share the brochure and floor plans on WhatsApp shortly. Have a wonderful day!",
    "objection_pivot": "Totally understand, is it the location, price, or just not the right time?",
    "visit_confirm": "Wonderful, I have noted your preference for the site visit.",
    "transfer_announcement": "Please hold while I connect you to a senior property advisor.",

    # Farewells
    "final_farewell": "Understood, thanks for your time. Have a wonderful day!",
    "farewell_polite": "Thank you for your time. Have a great day!",
    "farewell_hi": "Dhanyavaad, aapka din shubh rahe!",

    # Fillers (Slow-turn fallback)
    "filler_en": "Sure, one moment, let me check that for you.",
    "filler_hi": "Haan, ek second, main check karti hoon.",
}


def effective_config(root):
    cfg = yaml.safe_load((root / 'config.yaml').read_text(encoding='utf-8')) or {}
    profile = root / 'profiles' / (os.getenv('MARKET', 'india').lower() + '.yaml')
    if profile.exists():
        cfg = {**cfg, **(yaml.safe_load(profile.read_text(encoding='utf-8')) or {})}
    return cfg


def identity(key, text, config):
    provider = config.get('active_providers', {}).get('tts', 'sarvam')
    p = config.get('providers', {}).get('tts', {}).get(provider, {}).get('params', {})
    hindi = any(w in key for w in ('_hi', 'ji_bilkul', 'haanji', 'theek_hai'))
    return dict(schema=1, provider=provider, model=p.get('model', 'bulbul:v3'),
                speaker=p.get('voice_id', 'pooja'), language='hi-IN' if hindi else p.get('language', 'en-IN'),
                sample_rate=16000, pace=float(p.get('pace', 1.02)),
                temperature=float(p.get('temperature', .6)), text=text, api='sarvam-rest')


def fresh_audio(path, key, text, config):
    try:
        entry = json.loads(path.with_suffix('.json').read_text(encoding='utf-8'))
        if entry.get('identity') != identity(key, text, config):
            return False
        if entry.get('sha256') != hashlib.sha256(path.read_bytes()).hexdigest():
            return False
        with wave.open(str(path), 'rb') as wav:
            return wav.getframerate() == 16000 and wav.getnchannels() == 1 and wav.getsampwidth() == 2 and wav.getnframes() > 0
    except (OSError, ValueError, wave.Error, EOFError):
        return False


def write_provenance(path, key, text, config):
    entry = dict(identity=identity(key, text, config), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    path.with_suffix('.json').write_text(json.dumps(entry, indent=2) + '\n', encoding='utf-8')
