"""FFmpeg 音频提取器"""
import asyncio
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

from .logger import get_logger

logger = get_logger(__name__)


class AudioExtractor:
    async def extract(self, video_path: Path) -> bytes:
        """Async version — runs in a thread to avoid blocking the event loop."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._extract_impl, video_path)

    def extract_sync(self, video_path: Path) -> bytes:
        """Synchronous version — uses subprocess.run, suitable for ThreadPoolExecutor."""
        return self._extract_impl(video_path)

    def _extract_impl(self, video_path: Path) -> bytes:
        wav_path = Path(tempfile.gettempdir()) / f"{uuid4()}.wav"
        video_path = Path(video_path)

        try:
            proc = subprocess.run(
                [
                    "ffmpeg", "-i", str(video_path),
                    "-ar", "16000",
                    "-ac", "1",
                    "-f", "wav",
                    "-y",
                    str(wav_path),
                ],
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                timeout=120,
            )

            if proc.returncode != 0:
                error_msg = proc.stderr.decode(errors="ignore")[-200:]
                raise RuntimeError(f"FFmpeg failed: {error_msg}")

            audio_data = wav_path.read_bytes()
            logger.info("audio_extracted", video_path=str(video_path))
            return audio_data

        finally:
            video_path.unlink(missing_ok=True)
            wav_path.unlink(missing_ok=True)
