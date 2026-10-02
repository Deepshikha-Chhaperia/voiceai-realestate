"""ElevenLabs HTTP streaming TTS adapter for Pipecat 1.8.x."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import aiohttp
from loguru import logger

from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    StartFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.services.settings import TTSSettings
from pipecat.services.tts_service import TTSService


class HttpElevenLabsTTSService(TTSService):
    """ElevenLabs HTTP streaming TTS adapter."""

    def __init__(
        self,
        *,
        api_key: str,
        voice_id: str | None = None,
        voice: str | None = None,
        model: str = "eleven_turbo_v2_5",
        sample_rate: int | None = None,
        request_timeout_seconds: float = 30.0,
        aiohttp_session: aiohttp.ClientSession | None = None,
        settings: TTSSettings | None = None,
        **kwargs: Any,
    ) -> None:
        actual_voice = voice_id or voice or "XB0fDUnXU5powFXDhCwa"
        default_settings = TTSSettings(
            model=model,
            voice=actual_voice,
            language=None,
        )

        if settings is not None:
            default_settings.apply_update(settings)

        super().__init__(
            sample_rate=sample_rate,
            settings=default_settings,
            **kwargs,
        )

        self._api_key = api_key
        self._voice_id = actual_voice
        self._model = model
        self._request_timeout_seconds = request_timeout_seconds
        self._optimize_streaming_latency = int(kwargs.get("optimize_streaming_latency", 3))
        self._chunk_size = int(kwargs.get("chunk_size", 640))
        self._session = aiohttp_session
        self._owns_session = aiohttp_session is None

        logger.debug(
            "ElevenLabs HTTP TTS initialized model={} voice={} latency_opt={} chunk_size={} sample_rate={}",
            self._model,
            self._voice_id,
            self._optimize_streaming_latency,
            self._chunk_size,
            sample_rate,
        )

    @property
    def voice_id(self) -> str:
        return self._voice_id

    def _get_or_create_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=60.0, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(connector=connector)
            self._owns_session = True
        return self._session

    async def start(self, frame: StartFrame) -> None:
        """Initialize the service using Pipecat's runtime audio settings."""
        await super().start(frame)
        self._get_or_create_session()

    async def cleanup(self) -> None:
        """Release resources owned by this adapter."""
        if self._owns_session and self._session is not None:
            if not self._session.closed:
                await self._session.close()
        await super().cleanup()

    async def run_tts(
        self,
        text: str,
        context_id: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncGenerator[Frame | None, None]:
        """Synthesize text and stream raw PCM audio frames."""
        text = (text or "").strip()
        if not text:
            return

        session = self._get_or_create_session()

        runtime_sample_rate = self.sample_rate or 8000
        url = (
            f"https://api.elevenlabs.io/v1/text-to-speech/{self._voice_id}/stream"
            f"?output_format=pcm_{runtime_sample_rate}"
            f"&optimize_streaming_latency={self._optimize_streaming_latency}"
        )
        headers = {
            "xi-api-key": self._api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "text": text,
            "model_id": self._model,
        }

        timeout = aiohttp.ClientTimeout(
            total=self._request_timeout_seconds,
            connect=3.0,
            sock_connect=3.0,
            sock_read=self._request_timeout_seconds,
        )

        await self.start_ttfb_metrics()
        await self.start_tts_usage_metrics(text)

        first_audio = True
        response_started = False

        try:
            async with session.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    body = (await response.text())[:500]
                    logger.error(
                        "ElevenLabs TTS API Error [{}] for voice {}: {}",
                        response.status,
                        self._voice_id,
                        body,
                    )
                    yield ErrorFrame(error=f"ElevenLabs TTS HTTP {response.status}: {body}")
                    return

                response_started = True
                yield TTSStartedFrame(context_id=context_id)

                # ElevenLabs with output_format=pcm_{sample_rate} returns raw PCM bytes (no RIFF header).
                # Buffer to guarantee strict 16-bit PCM sample alignment (multiples of 2 bytes) across TCP chunks.
                chunk_size = self._chunk_size
                pending = bytearray()

                async for network_chunk in response.content.iter_chunked(chunk_size):
                    if not network_chunk:
                        continue

                    pending.extend(network_chunk)
                    even_len = len(pending) - (len(pending) % 2)
                    if even_len < 2:
                        continue

                    chunk_to_send = bytes(pending[:even_len])
                    del pending[:even_len]

                    if first_audio:
                        await self.stop_ttfb_metrics()
                        first_audio = False

                    yield TTSAudioRawFrame(
                        audio=chunk_to_send,
                        sample_rate=runtime_sample_rate,
                        num_channels=1,
                        context_id=context_id,
                    )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.exception("ElevenLabs TTS streaming error: {}", exc)
            yield ErrorFrame(error=f"ElevenLabs TTS Exception: {exc}")

        finally:
            if first_audio:
                await self.stop_ttfb_metrics()
            if response_started:
                yield TTSStoppedFrame(context_id=context_id)
