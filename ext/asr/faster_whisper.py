"""Faster-Whisper离线ASR方案"""

import asyncio
import os
from pathlib import Path
from typing import Optional
from loguru import logger

try:
    from faster_whisper import WhisperModel

    FASTER_WHISPER_AVAILABLE = True
except ImportError:
    FASTER_WHISPER_AVAILABLE = False
    logger.warning("faster-whisper not installed, run: uv pip install faster-whisper")

from ext.asr.base import ASRBase, ASRResult, ASRSegment
from ext.asr.audio_extractor import AudioExtractor
from ext.config import get_settings


class FasterWhisperASR(ASRBase):
    """
    Faster-Whisper离线ASR
    不依赖云API，适合测试
    """

    def __init__(
        self,
        model_size: str = None,
        device: str = None,
    ):
        if not FASTER_WHISPER_AVAILABLE:
            raise ImportError("faster-whisper not installed")

        settings = get_settings()
        self.model_size = (
            model_size
            if model_size
            else settings.faster_whisper.get("model_size", "base")
        )
        self.device = device if device else settings.faster_whisper.get("device", "cpu")
        self.audio_extractor = AudioExtractor()

        logger.info(f"FasterWhisperASR: model={self.model_size}, device={self.device}")
        self.model = WhisperModel(self.model_size, device=self.device)

    async def transcribe(
        self,
        audio_path: str,
        language: Optional[str] = "zh",
    ) -> ASRResult:
        """转录音频文件"""
        if not os.path.exists(audio_path):
            raise FileNotFoundError(f"Audio file not found: {audio_path}")

        # 获取音频时长
        duration = await self._get_audio_duration(audio_path)

        # 在线程池中运行Whisper（CPU密集型）
        loop = asyncio.get_event_loop()
        transcribe_result = await loop.run_in_executor(
            None, lambda: self.model.transcribe(audio_path, language=language or "zh")
        )

        # transcribe返回的是(segments, info)元组
        segments_raw, info = transcribe_result

        segments = []
        for seg in segments_raw:
            segments.append(
                ASRSegment(
                    start=seg.start,
                    end=seg.end,
                    text=seg.text.strip(),
                    confidence=seg.avg_logprob if seg.avg_logprob else 0.0,
                )
            )

        avg_confidence = (
            sum(s.confidence for s in segments) / len(segments) if segments else 0.0
        )

        logger.info(
            f"ASR completed: {len(segments)} segments, duration={duration:.1f}s"
        )

        return ASRResult(
            language=language or "zh",
            duration=duration,
            segments=segments,
            confidence=avg_confidence,
        )

    async def extract_audio(
        self,
        video_path: str,
        output_path: str = None,
    ) -> str:
        """从视频提取音频"""
        return await self.audio_extractor.extract(video_path, output_path)

    async def _get_audio_duration(self, audio_path: str) -> float:
        """获取音频时长"""
        return await self.audio_extractor._get_duration(audio_path)
