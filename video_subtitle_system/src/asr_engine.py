"""Faster-Whisper ASR 引擎"""
import os
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional
from uuid import uuid4

from faster_whisper import WhisperModel

from .logger import get_logger

logger = get_logger(__name__)


class ASREngine:
    def __init__(self, model_size: str = "small", hf_token: Optional[str] = None):
        self.model_size = model_size
        self.hf_token = hf_token
        self.model: WhisperModel = None

    def load_model(self):
        if self.hf_token:
            os.environ["HF_TOKEN"] = self.hf_token

        compute_type = "int8" if self.model_size == "base" else "float32"
        self.model = WhisperModel(
            self.model_size,
            device="auto",
            compute_type=compute_type,
        )
        logger.info("asr_model_loaded", model_size=self.model_size)

    async def load_model_async(self):
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.load_model)

    async def recognize(self, audio_data: bytes) -> List[Dict[str, Any]]:
        if self.model is None:
            await self.load_model_async()

        tmp_wav = Path(tempfile.gettempdir()) / f"{uuid4()}.wav"
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
