"""pHash指纹计算模块"""

import asyncio
import os
from pathlib import Path
from typing import Optional
import httpx
import imagehash
from PIL import Image
from loguru import logger

try:
    import cv2

    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logger.warning("opencv-python not installed, installing...")
    # 稍后会安装


class VideoFingerprint:
    """
    视频pHash指纹计算
    用于抖音视频去重
    """

    def __init__(self, hash_size: int = 8):
        self.hash_size = hash_size
        logger.info(f"VideoFingerprint: hash_size={hash_size}")

    async def compute_phash(self, video_url: str) -> str:
        """
        从视频URL计算pHash

        Args:
            video_url: 视频下载URL

        Returns:
            str: pHash指纹(64位十六进制字符串)
        """
        temp_video = None
        temp_dir = Path("temp/video")
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 下载视频(前几帧即可)
            video_path = await self._download_video(video_url, temp_dir)

            # 提取关键帧
            frame = await self._extract_key_frame(video_path)

            if frame is None:
                raise ValueError("Failed to extract frame from video")

            # 计算pHash
            phash = imagehash.phash(Image.fromarray(frame))

            logger.info(f"Computed pHash: {phash}")
            return str(phash)

        finally:
            # 清理临时文件
            if temp_video and os.path.exists(temp_video):
                os.remove(temp_video)

    async def _download_video(self, url: str, temp_dir: Path) -> str:
        """下载视频(只下载前面一点用于提取帧)"""
        video_path = temp_dir / f"temp_{os.urandom(8).hex()}.mp4"

        async with httpx.AsyncClient(timeout=60.0) as client:
            # 使用Stream方式下载，只取前1MB(够提取帧了)
            async with client.stream("GET", url, follow_redirects=True) as response:
                response.raise_for_status()

                with open(video_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        f.write(chunk)
                        # 限制下载大小，够提取帧即可
                        if f.tell() > 1024 * 1024:  # 1MB
                            break

        return str(video_path)

    async def _extract_key_frame(self, video_path: str) -> Optional[bytes]:
        """提取关键帧"""
        if not CV2_AVAILABLE:
            # 尝试安装cv2
            logger.warning("opencv-python not available, trying alternative...")
            return await self._extract_frame_ffmpeg(video_path)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._extract_frame_cv2, video_path)

    def _extract_frame_cv2(self, video_path: str) -> Optional[bytes]:
        """用OpenCV提取关键帧"""
        import cv2

        cap = cv2.VideoCapture(video_path)

        # 读取第一帧
        ret, frame = cap.read()
        cap.release()

        if not ret:
            return None

        # 转为RGB
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return frame_rgb

    async def _extract_frame_ffmpeg(self, video_path: str) -> Optional[bytes]:
        """用FFmpeg提取关键帧(备用方案)"""
        import subprocess

        # 提取第1秒的帧
        cmd = [
            "ffmpeg",
            "-y",
            "-ss",
            "00:00:01",
            "-i",
            video_path,
            "-vframes",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "-",
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, _ = await proc.communicate()

        if proc.returncode != 0 or not stdout:
            return None

        return stdout

    def compute_similarity(self, phash1: str, phash2: str) -> float:
        """
        计算两个pHash的相似度

        Args:
            phash1: pHash1
            phash2: pHash2

        Returns:
            float: 相似度(0-1), >0.9视为相同
        """
        h1 = imagehash.hex_to_hash(phash1)
        h2 = imagehash.hex_to_hash(phash2)

        # 汉明距离 / 64
        distance = h1 - h2
        similarity = 1.0 - (distance / 64.0)

        return similarity
