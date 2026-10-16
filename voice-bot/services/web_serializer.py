import time
import base64
import json
import array
from loguru import logger
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.frames.frames import (
    AudioRawFrame,
    BotStoppedSpeakingFrame,
    Frame,
    InputAudioRawFrame,
    StartFrame,
    InterruptionFrame,
)

# How long after sending clearAudio to keep dropping outgoing audio frames
# instead of forwarding them as playAudio.
# Lowered to 0.25s to prevent dropping fresh bot speech on fast turns.
_POST_CLEAR_DROP_WINDOW_S = 0.25


class WebPCMFrameSerializer(FrameSerializer):
    def __init__(self, stream_id: str):
        self._stream_id = stream_id
        self._active_generation_id = 0
        self._interrupted_generation_id = -1
        self._booking_conf_playing = False
        self._booking_conf_deadline = 0.0
        self._drop_audio_until = 0.0
        self._is_first_audio_frame = True

    async def setup(self, frame: StartFrame):
        pass

    def on_interruption(self) -> None:
        """Call when barge-in detected - activates drop window and marks active generation interrupted."""
        self._drop_audio_until = time.monotonic() + _POST_CLEAR_DROP_WINDOW_S
        self._interrupted_generation_id = self._active_generation_id
        self._booking_conf_playing = False
        self._booking_conf_deadline = 0.0
        self._is_first_audio_frame = True
        logger.debug(
            "[WebPCM] TX -> clearAudio stream_id={} interrupted_gen={} guard_window={}s",
            self._stream_id,
            self._interrupted_generation_id,
            _POST_CLEAR_DROP_WINDOW_S,
        )

    def next_generation(self, generation_id: int | None = None) -> int:
        """Call when a new user turn starts - clears any pending drop window and sets active generation."""
        if generation_id is not None:
            self._active_generation_id = generation_id
        else:
            self._active_generation_id += 1
        self._drop_audio_until = 0.0
        self._booking_conf_playing = False
        self._booking_conf_deadline = 0.0
        self._is_first_audio_frame = True
        logger.debug(
            "[WebPCMSerializer] New generation started (gen={}), drop window cleared stream_id={}",
            self._active_generation_id,
            self._stream_id,
        )
        return self._active_generation_id

    async def serialize(self, frame: Frame) -> str | bytes | None:
        logger.info(f"[WebPCM] serialize frame={type(frame).__name__} stream_id={self._stream_id}")
        if isinstance(frame, BotStoppedSpeakingFrame):
            self._booking_conf_playing = False
            self._booking_conf_deadline = 0.0
            return None

        if isinstance(frame, InterruptionFrame):
            self.on_interruption()
            return json.dumps({"event": "clearAudio", "streamId": self._stream_id})

        if isinstance(frame, AudioRawFrame):
            frame_gen = getattr(frame, "generation_id", None)
            is_booking_conf = getattr(frame, "is_booking_confirmation", False)

            now = time.monotonic()
            if is_booking_conf:
                self._booking_conf_playing = True
                self._booking_conf_deadline = now + 10.0
            elif self._booking_conf_playing and now < self._booking_conf_deadline:
                # Booking confirmation has priority; drop colliding other replies
                return None
            else:
                self._booking_conf_playing = False
                # 1. Stale older generation frame
                if frame_gen is not None and frame_gen < self._active_generation_id:
                    return None

                # 2. Frame arriving during active interruption drop window
                if now < self._drop_audio_until:
                    # Drop untagged frames or frames belonging to the interrupted generation
                    if frame_gen is None or frame_gen <= self._interrupted_generation_id:
                        logger.warning(
                            "[WebPCM] Dropped {} bytes of audio inside the "
                            "post-interruption guard window (gen={}) "
                            "stream_id={}",
                            len(frame.audio),
                            frame_gen,
                            self._stream_id,
                        )
                        return None

            audio_bytes = frame.audio
            if self._is_first_audio_frame and len(audio_bytes) >= 2:
                trimmed_bytes, has_speech = self._strip_leading_silence(
                    audio_bytes, frame.sample_rate
                )
                if not has_speech:
                    # Drop pure digital silence chunk while waiting for speech onset
                    return None
                self._is_first_audio_frame = False
                audio_bytes = trimmed_bytes

            payload = base64.b64encode(audio_bytes).decode("utf-8")
            return json.dumps(
                {
                    "event": "playAudio",
                    "streamId": self._stream_id,
                    "media": {
                        "contentType": "audio/x-l16",
                        "sampleRate": frame.sample_rate,
                        "payload": payload,
                    },
                }
            )

        return None

    @staticmethod
    def _strip_leading_silence(
        pcm_bytes: bytes, sample_rate: int = 16000, threshold: int = 300
    ) -> tuple[bytes, bool]:
        """Strip digital silence zero-padding before speech starts (saves 150-250ms latency)."""
        valid_len = len(pcm_bytes) - (len(pcm_bytes) % 2)
        if valid_len < 2:
            return pcm_bytes, True
        samples = array.array("h")
        samples.frombytes(pcm_bytes[:valid_len])
        first_idx = None
        for i in range(len(samples)):
            if abs(samples[i]) > threshold:
                first_idx = i
                break
        if first_idx is None:
            return b"", False
        pre_roll = int(sample_rate * 0.01)  # 10ms pre-roll ramp for smooth acoustic onset
        keep_idx = max(0, first_idx - pre_roll)
        return samples[keep_idx:].tobytes(), True

    async def deserialize(self, data: str | bytes) -> Frame | None:
        try:
            message = json.loads(data)
        except json.JSONDecodeError:
            return None
        if message.get("event") == "media":
            media = message.get("media", {})
            payload_base64 = media.get("payload")
            if not payload_base64:
                return None

            payload = base64.b64decode(payload_base64)
            input_rate = int(media.get("sampleRate", 16000))
            return InputAudioRawFrame(audio=payload, num_channels=1, sample_rate=input_rate)
        return None