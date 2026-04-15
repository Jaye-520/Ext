"""Faster-Whisper ASR 引擎"""
from pathlib import Path
from typing import List, Dict, Any
from uuid import uuid4

from faster_whisper import WhisperModel

from .logger import get_logger

logger = get_logger(__name__)


class ASREngine:
    def __init__(self, model_size: str = "small"):
        self.model_size = model_size
        self.model: WhisperModel = None

    def load_model(self):
        self.model = WhisperModel(
            self.model_size,
            device="auto",
            compute_type="float16" if self.model_size != "base" else "int8",
        )
        logger.info("asr_model_loaded", model_size=self.model_size)

    async def recognize(self, audio_data: bytes) -> List[Dict[str, Any]]:
        if self.model is None:
            self.load_model()

        tmp_wav = Path(f"/tmp/{uuid4()}.wav")
        tmp_wav.write_bytes(audio_data)

        try:
            segments, _ = self.model.transcribe(
                str(tmp_wav),
                language=None,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )

            result = []
            for s in segments:
                result.append({
                    "start_time": s.start,
                    "end_time": s.end,
                    "text": s.text,
                    "confidence": getattr(s, "probability", 1.0),
                })

            logger.info("asr_completed", segments=len(result))
            return result

        finally:
            tmp_wav.unlink(missing_ok=True)
