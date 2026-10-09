"""
Pre-generates static TTS audio files for high-frequency bot phrases
using configured TTS provider (Sarvam bulbul:v3, voice: pooja, 16kHz mono PCM).

Usage:
    cd voice-bot
    python generate_static_audio.py

Output: voice-bot/static_audio/india/*.wav
"""

import asyncio
import argparse
import hashlib
import json
from call_repairs import SHORT_GOODBYE, FAQ_TEXTS
import base64
from call_repairs import CALL5_TEXTS, CALL7_TEXTS
import os
import sys
import wave
from pathlib import Path

import aiohttp
import yaml
from dotenv import load_dotenv

from audio_provenance import PHRASES, effective_config, fresh_audio, write_provenance

load_dotenv(Path(__file__).parent / ".env")

CONFIG = effective_config(Path(__file__).parent)

ACTIVE_TTS = CONFIG.get("active_providers", {}).get("tts", "sarvam")
TTS_CONFIG = CONFIG.get("providers", {}).get("tts", {}).get(ACTIVE_TTS, {})
PARAMS = TTS_CONFIG.get("params", {})

MODEL_ID = PARAMS.get("model", "bulbul:v3")
VOICE_ID = PARAMS.get("voice_id", "pooja")
LANGUAGE_CODE = PARAMS.get("language", "en-IN")
SAMPLE_RATE = 16000  # native browser rate; downsample only for 8k telephony
SARVAM_API_KEY = os.getenv(TTS_CONFIG.get("api_key_env", "SARVAM_API_KEY"), "")

OUTPUT_DIR = Path(__file__).parent / "static_audio"
INDIA_DIR = OUTPUT_DIR / "india"


async def synthesize_sarvam(
    session: aiohttp.ClientSession, text: str, phrase_key: str
) -> bytes | None:
    """Call Sarvam REST TTS -> return native 16kHz PCM bytes from WAV."""
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
        "pace": float(PARAMS.get("pace", 1.02)),
        "temperature": float(PARAMS.get("temperature", 0.6)),
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


def validate_wav(path):
    try:
        with wave.open(str(path), "rb") as wav:
            return wav.getnchannels() == 1 and wav.getsampwidth() == 2 and wav.getnframes() > 0 and wav.getframerate() in (8000, 16000, 22050, 24000, 44100, 48000)
    except (OSError, wave.Error, EOFError):
        return False


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-only", action="store_true", help="Validate WAVs without calling Sarvam")
    parser.add_argument("--call3-only", action="store_true", help="Render only the four new call3 phrases")
    parser.add_argument("--call5-only", action="store_true", help="Render only versioned call5 FAQ phrases")
    parser.add_argument("--call7-only", action="store_true", help="Render only four new call7 operational phrases")
    args = parser.parse_args()
    phrases = {k: PHRASES[k] for k in ("short_goodbye", *FAQ_TEXTS)} if args.call3_only else PHRASES
    if args.call5_only:
        phrases = CALL5_TEXTS
    if args.call7_only:
        phrases = CALL7_TEXTS
    if ACTIVE_TTS != "sarvam":
        raise SystemExit("STOP: this generator supports Sarvam only; provider config was not changed")
    if not args.check_only and not SARVAM_API_KEY:
        print("ERROR: SARVAM_API_KEY not found in environment/.env. Aborting.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    INDIA_DIR.mkdir(exist_ok=True)
    print(f"Output dir:  {INDIA_DIR}")
    print(f"Provider:    {ACTIVE_TTS} | Model: {MODEL_ID} | Voice: {VOICE_ID} | Sample Rate: {SAMPLE_RATE}Hz\n")

    from contextlib import AsyncExitStack
    async with AsyncExitStack() as stack:
        session = None if args.check_only else await stack.enter_async_context(aiohttp.ClientSession())
        for key, text in phrases.items():
            out_path = INDIA_DIR / f"{key}.wav"
            if fresh_audio(out_path, key, text, CONFIG):
                print(f"  [EXISTS] {out_path.name}")
                continue
            if args.check_only:
                continue
            print(f"Synthesizing '{key}'...")
            print(f"  Text: {text}")
            wav_bytes = await synthesize_sarvam(session, text, key)
            if wav_bytes:
                with open(out_path, "wb") as f:
                    f.write(wav_bytes)
                if validate_wav(out_path):
                    write_provenance(out_path, key, text, CONFIG)
                print(f"  [OK] Saved {out_path.name} ({len(wav_bytes) / 1024:.1f} KB)")
            else:
                print(f"  [SKIP] '{key}' -- failed")
            print()

    print("--- Verification ---")
    missing = []
    for key in phrases:
        path = INDIA_DIR / f"{key}.wav"
        if not fresh_audio(path, key, phrases[key], CONFIG):
            missing.append(key)
        status = "[OK]" if key not in missing else "[MISSING]"
        size = f"({path.stat().st_size / 1024:.1f} KB)" if path.exists() else ""
        print(f"  {status} {path.name} {size}")

    if missing:
        raise SystemExit("Audio preflight failed: " + ", ".join(missing))

if __name__ == "__main__":
    asyncio.run(main())
