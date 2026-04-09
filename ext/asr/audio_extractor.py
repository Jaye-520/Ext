"""FFmpeg音频提取器"""

import asyncio
import os
from pathlib import Path
from loguru import logger


class AudioExtractor:
    """
    FFmpeg音频提取器
    从视频/音频中提取音频
    """

    def __init__(
        self,
        sample_rate: int = 16000,
        channels: int = 1,
        audio_format: str = "wav",
    ):
        self.sample_rate = sample_rate
        self.channels = channels
        self.audio_format = audio_format
        self.temp_dir = Path("temp/audio")
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"AudioExtractor: sample_rate={sample_rate}, channels={channels}, format={audio_format}")

    async def extract(
        self,
        video_path: str,
        output_path: str = None,
        start_time: float = None,
        duration: float = None,
    ) -> str:
        """从视频提取音频"""
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video file not found: {video_path}")

        if not output_path:
            video_name = Path(video_path).stem
            segment_suffix = f"_{int(start_time)}s" if start_time else ""
            output_path = str(self.temp_dir / f"{video_name}{segment_suffix}.{self.audio_format}")

        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            video_path,
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            str(self.sample_rate),
            "-ac",
            str(self.channels),
        ]

        if start_time is not None:
            cmd.extend(["-ss", str(start_time)])
        if duration is not None:
            cmd.extend(["-t", str(duration)])

        cmd.append(output_path)

        logger.info(f"Extracting audio: {video_path} -> {output_path}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            logger.error(f"FFmpeg error: {error_msg}")
            raise RuntimeError(f"FFmpeg error: {error_msg}")

        logger.info(f"Audio extracted: {output_path}")
        return output_path

    async def _get_duration(self, video_path: str) -> float:
        """获取视频时长"""
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"FFprobe error: {stderr.decode()}")

        return float(stdout.decode().strip())

    def cleanup(self, audio_path: str) -> None:
        """清理临时音频文件"""
        if audio_path and os.path.exists(audio_path):
            os.remove(audio_path)
            logger.debug(f"Cleaned up: {audio_path}")
