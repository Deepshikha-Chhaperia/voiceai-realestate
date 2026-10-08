"""
Pre-generates static TTS audio files for high-frequency bot phrases
using configured TTS provider (Sarvam bulbul:v3, voice: pooja, 8kHz mono PCM).

Usage:
    cd voice-bot
    python generate_static_audio.py

Output: voice-bot/static_audio/india/*.wav
"""

import asyncio
import base64
import os
import sys
import wave
from pathlib import Path

import aiohttp
import yaml
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

CONFIG_PATH = Path(__file__).parent / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

ACTIVE_TTS = CONFIG.get("active_providers", {}).get("tts", "sarvam")
TTS_CONFIG = CONFIG.get("providers", {}).get("tts", {}).get(ACTIVE_TTS, {})
PARAMS = TTS_CONFIG.get("params", {})

MODEL_ID = PARAMS.get("model", "bulbul:v3")
VOICE_ID = PARAMS.get("voice_id", "pooja")
LANGUAGE_CODE = PARAMS.get("language", "en-IN")
SAMPLE_RATE = int(PARAMS.get("sample_rate", 8000))
SARVAM_API_KEY = os.getenv(TTS_CONFIG.get("api_key_env", "SARVAM_API_KEY"), "")

OUTPUT_DIR = Path(__file__).parent / "static_audio"
INDIA_DIR = OUTPUT_DIR / "india"

PHRASES: dict[str, str] = {
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
    "objection_pivot": "Totally understand, is it the location, price, or just not the right time?",
    "brochure_close": "Sure, our team will share the brochure and floor plans on WhatsApp shortly. Have a wonderful day!",
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


async def synthesize_sarvam(
    session: aiohttp.ClientSession, text: str, phrase_key: str
) -> bytes | None:
    """Call Sarvam REST TTS -> return raw 8kHz PCM bytes from WAV."""
    url = "https://api.sarvam.ai/text-to-speech"
    headers = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    # Autodetect language code for Hindi phrases
    is_hindi = any(w in phrase_key for w in ("_hi", "ji_bilkul", "haanji", "theek_hai"))
    lang = "hi-IN" if is_hindi else LANGUAGE_CODE

    payload = {
        "inputs": [text],
        "target_language_code": lang,
        "speaker": VOICE_ID,
        "pitch": 0,
        "pace": float(PARAMS.get("pace", 1.02)),
        "loudness": 1.5,
        "speech_sample_rate": SAMPLE_RATE,
        "enable_preprocessing": False,
        "model": MODEL_ID,
    }

    try:
        async with session.post(
            url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"  [ERROR] HTTP {resp.status} for '{phrase_key}': {body[:300]}")
                return None
            data = await resp.json()
            audios = data.get("audios", [])
            if not audios:
                print(f"  [ERROR] No audio returned for '{phrase_key}'")
                return None
            wav_bytes = base64.b64decode(audios[0])
            return wav_bytes
    except Exception as e:
        print(f"  [ERROR] Exception for '{phrase_key}': {e}")
        return None


async def main():
    if not SARVAM_API_KEY:
        print("ERROR: SARVAM_API_KEY not found in environment/.env. Aborting.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    INDIA_DIR.mkdir(exist_ok=True)
    print(f"Output dir:  {INDIA_DIR}")
    print(f"Provider:    {ACTIVE_TTS} | Model: {MODEL_ID} | Voice: {VOICE_ID} | Sample Rate: {SAMPLE_RATE}Hz\n")

    async with aiohttp.ClientSession() as session:
        for key, text in PHRASES.items():
            out_path = INDIA_DIR / f"{key}.wav"
            if out_path.exists() and out_path.stat().st_size > 0:
                print(f"  [EXISTS] {out_path.name}")
                continue
            print(f"Synthesizing '{key}'...")
            print(f"  Text: {text}")
            wav_bytes = await synthesize_sarvam(session, text, key)
            if wav_bytes:
                with open(out_path, "wb") as f:
                    f.write(wav_bytes)
                print(f"  [OK] Saved {out_path.name} ({len(wav_bytes) / 1024:.1f} KB)")
            else:
                print(f"  [SKIP] '{key}' -- failed")
            print()

    print("--- Verification ---")
    for key in PHRASES:
        path = INDIA_DIR / f"{key}.wav"
        status = "[OK]" if path.exists() else "[MISSING]"
        size = f"({path.stat().st_size / 1024:.1f} KB)" if path.exists() else ""
        print(f"  {status} {path.name} {size}")


if __name__ == "__main__":
    asyncio.run(main())
