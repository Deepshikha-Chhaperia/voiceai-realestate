# Multilingual AI Voice Agent for Enterprise Real Estate

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Pipecat AI](https://img.shields.io/badge/Pipecat-v1.8.1-orange.svg)](https://github.com/pipecat-ai/pipecat)
[![Groq LPU](https://img.shields.io/badge/Inference-Groq%20LPU-f55.svg)](https://groq.com)
[![Sarvam AI](https://img.shields.io/badge/Speech-Sarvam%20AI-purple.svg)](https://sarvam.ai)

A production-grade, ultra-low-latency (<600ms voice-to-voice) conversational AI voice agent engineered for enterprise real estate sales qualification. Built with an event-driven streaming pipeline supporting Indian code-switching (English, Hindi, and Hinglish), intelligent barge-in interruption handling, deterministic lead scoring, automated site visit booking, and instant CRM/Telegram dispatch.

---

## Architecture & Data Flow

```mermaid
flowchart TD
    subgraph LiveCall["I. Real-Time In-Call Pipeline (<600ms Voice Loop)"]
        direction TB
        subgraph AudioIO["1. The Ear (Audio Transport)"]
            User(["Buyer / Caller"]) <-->|WebSockets / 16kHz PCM| WebClient["Web Client / SIP Telephony"]
            WebClient --> RNNoise["RNNoise Filter (Noise Suppression)"]
            RNNoise --> VAD["Silero VAD (Voice Activity Detection)"]
            VAD --> STT["Sarvam Saaras v4 STT (Hinglish/Hindi/English)"]
        end

        subgraph Intelligence["2. The Brain (Cognitive Core)"]
            STT --> Aggregator["Pipecat Stream Aggregator"]
            Aggregator --> LiveContext["In-Call Context & Language State\n(Tracks BHK, Budget & Code-Switching)"]
            LiveContext --> PromptEngine["Dynamic Real Estate Prompt Builder"]
            PromptEngine --> GroqLLM["Groq LPU Inference (Qwen 3.8-27B / Llama 3.3-70B)"]
        end

        subgraph SpeechSynthesis["3. The Voice (Low-Latency TTS)"]
            GroqLLM --> ChunkStream["Streaming Token Parser & Spoken Text Guard"]
            ChunkStream --> SarvamTTS["Sarvam Bulbul v3 / Murf Streaming TTS"]
            SarvamTTS --> Serializer["Audio Serializer (Post-Interruption Guard)"]
            Serializer --> WebClient
        end
    end

    LiveCall -.->|Call Ends: Final Transcript & Disposition| PostCall["II. Post-Call Lifecycle & CRM Dispatch"]

    subgraph CRMDispatch["Post-Call Automation & Scoring"]
        PostCall --> LeadScorer["Deterministic Lead Scoring Engine\n(Evaluates Complete Transcript, Duration & Disposition)"]
        LeadScorer --> DB[("SQLite / PostgreSQL Lead Store")]
        DB --> Outbox["Async Worker Queue & Outbox Engine"]
        Outbox --> Telegram["Telegram Instant Sales Rep Alert"]
        Outbox --> GoogleSheets["Google Sheets CRM Export"]
    end
```

---

## Core Capabilities

- **Sub-600ms Latency Budget:** 
  - **STT:** ~140ms with Sarvam Saaras v4 streaming WebSocket.
  - **LLM TTFT:** ~280ms–350ms powered by Groq LPUs with pre-warmed context caches.
  - **TTS TTFA:** ~150ms–200ms via chunked Sarvam Bulbul v3 synthesis.
  - **Blended Cost:** ~₹1.80/min ($0.019/min) across STT, LLM, and TTS.
- **Natural Barge-In & Interruption Handling:** Active audio stream cancellation with a 1.0s post-interruption guard window to prevent packet clipping or echo.
- **Fail-Safe Watchdog:** Detects silence and WebSocket stalls, firing automatic soft conversational nudges.
- **Multilingual Code-Switching:** Fluent handling of Indian English, Hindi, and everyday Hinglish (*"3 BHK mein payment plan offer hai kya?"*).
- **Lead Qualification & Site Visit Booking:** Real-time extraction of budget, configuration (2/3/4 BHK), and timeline, coupled with automated calendar appointment locking.
- **Automated Lead Scoring & CRM Dispatch:** Multi-factor scoring (Hot/Warm/Cold, 0–100 scale) and instant Telegram alerts dispatched to sales reps with call audio, full transcript, and customer qualification profile.

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Pipeline Framework** | Pipecat AI (v1.8.1) | Frame-based audio pipeline orchestration |
| **Inference Engine** | Groq LPU / Cerebras | Sub-350ms TTFT running Qwen 3.8-27B and Llama 3.3-70B |
| **Speech-to-Text** | Sarvam Saaras v4 / Deepgram | Low-latency Hinglish & Indian English ASR |
| **Text-to-Speech** | Sarvam Bulbul v3 / Murf | Low-latency neural voice synthesis |
| **Backend & Transport** | FastAPI + WebSockets | High-concurrency async streaming gateway |
| **Data & CRM Outbox** | Redis + SQLite / PostgreSQL | Call state persistence & reliable webhook dispatch |
| **Audio Processing** | Web Audio API + RNNoise + Silero VAD | Real-time 16kHz PCM capture and noise suppression |

---

## Quickstart

### 1. Installation
```bash
git clone https://github.com/Deepshikha-Chhaperia/voicebot.git
cd voicebot/voice-bot

python -m venv .venv
# Windows: .venv\Scripts\activate | Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration
Create a `.env` file in `voice-bot/`:
```env
ENV=dev
SARVAM_API_KEY=your_sarvam_api_key
GROQ_API_KEY=your_groq_api_key
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
MAX_CONCURRENT_CALLS=50
```

### 3. Run Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Open `http://localhost:8000` to start testing via the web client interface.

---

## Author

**Deepshikha Chhaperia**  
GitHub: [@Deepshikha-Chhaperia](https://github.com/Deepshikha-Chhaperia)
