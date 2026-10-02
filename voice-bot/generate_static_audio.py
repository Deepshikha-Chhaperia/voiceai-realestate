"""
One-time script: pre-generates static TTS audio files for the most-used
bot phrases using ElevenLabs TTS (Sarah voice, 16kHz mono PCM).

Usage:
    cd voice-bot
    python generate_static_audio.py

Output: voice-bot/static_audio/india/*.wav (16kHz mono 16-bit PCM WAV)
"""

import asyncio
import os
import wave
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
VOICE_ID = "EXAVITQu4vr4xnSDxMaL"  # Sarah — premade voice
MODEL_ID = "eleven_turbo_v2_5"       # Fastest model
SAMPLE_RATE = 16000                  # Native 16kHz for crystal-clear web & phone audio

OUTPUT_DIR = Path(__file__).parent / "static_audio"
INDIA_DIR = OUTPUT_DIR / "india"

PHRASES = {
    "greeting_alex": "Hi, am I speaking with Alex?",
    "greeting_generic": "Hi, this is Ananya from Meridian Group. Is this a good time to talk?",
    "opening_intro": "Great, this is Ananya from Meridian Group. Are you looking for a 2 or 3 BHK?",
    "brochure_close": "Sure, our team will share the brochure and floor plans on WhatsApp shortly. Have a wonderful day!",
    "objection_pivot": "Totally understand, is it the location, price, or just not the right time?",
    "final_farewell": "Understood, thanks for your time. Have a wonderful day!",
}


async def synthesize(session: aiohttp.ClientSession, text: str, phrase_key: str) -> bytes | None:
    """Call ElevenLabs streaming TTS -> return raw 16kHz PCM bytes."""
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}/stream?output_format=pcm_{SAMPLE_RATE}"
    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "text": text,
        "model_id": MODEL_ID,
    }
    try:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"  [ERROR] HTTP {resp.status} for '{phrase_key}': {body[:300]}")
                return None
            pcm_bytes = await resp.read()
            return pcm_bytes
    except Exception as e:
        print(f"  [ERROR] Exception for '{phrase_key}': {e}")
        return None


def save_wav(out_path: Path, pcm_data: bytes, sample_rate: int = 16000):
    with wave.open(str(out_path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_data)


async def main():
    if not ELEVENLABS_API_KEY:
        print("ERROR: ELEVENLABS_API_KEY not found in .env. Aborting.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    INDIA_DIR.mkdir(exist_ok=True)
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"India dir:  {INDIA_DIR}")
    print(f"Sample rate: {SAMPLE_RATE}Hz | Voice: {VOICE_ID} | Model: {MODEL_ID}\n")

    async with aiohttp.ClientSession() as session:
        for key, text in PHRASES.items():
            india_path = INDIA_DIR / f"{key}.wav"
            print(f"Synthesizing '{key}'...")
            print(f"  Text: {text}")
            pcm_data = await synthesize(session, text, key)
            if pcm_data:
                save_wav(india_path, pcm_data, sample_rate=SAMPLE_RATE)
                print(f"  [OK] Saved {india_path.name} ({len(pcm_data) / 1024:.1f} KB)")
            else:
                print(f"  [SKIP] '{key}' -- will fall back to live TTS")
            print()

    print("--- Verification ---")
    for key in PHRASES:
        path = INDIA_DIR / f"{key}.wav"
        status = "[OK]" if path.exists() else "[MISSING]"
        size = f"({path.stat().st_size / 1024:.1f} KB)" if path.exists() else ""
        print(f"  {status} {path.name} {size}")


if __name__ == "__main__":
    asyncio.run(main())
