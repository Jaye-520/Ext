"""FFmpeg 音频提取器"""
import asyncio
from pathlib import Path
from uuid import uuid4

from .logger import get_logger

logger = get_logger(__name__)


class AudioExtractor:
    async def extract(self, video_path: Path) -> bytes:
        wav_path = Path(f"/tmp/{uuid4()}.wav")
        video_path = Path(video_path)

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", str(video_path),
                "-ar", "16000",
                "-ac", "1",
                "-f", "wav",
                "-y",
                str(wav_path),
                stderr=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
            )
            _, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode(errors="ignore")[-200:]
                raise RuntimeError(f"FFmpeg failed: {error_msg}")

            audio_data = wav_path.read_bytes()
            logger.info("audio_extracted", video_path=str(video_path))
            return audio_data

        finally:
            video_path.unlink(missing_ok=True)
            wav_path.unlink(missing_ok=True)