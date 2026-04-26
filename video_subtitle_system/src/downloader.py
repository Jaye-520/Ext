"""视频下载器（Douyin直链 / B站DASH）"""
import asyncio
from pathlib import Path
from typing import Optional
import tempfile
from uuid import uuid4

import httpx

from .logger import get_logger

logger = get_logger(__name__)

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


class VideoDownloader:
    def __init__(self):
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    async def close(self):
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def download(self, url: str, platform: str) -> Path:
        tmp_path = Path(tempfile.gettempdir()) / f"{uuid4()}.mp4"
        try:
            if platform == "douyin":
                await self._stream_to_file(url, str(tmp_path))
            else:
                await self._dash_download(url, str(tmp_path))
            logger.info("video_downloaded", platform=platform, path=str(tmp_path))
            return tmp_path
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def _stream_to_file(self, url: str, tmp_path: str):
        client = await self._get_client()
        async with client.stream("GET", url, follow_redirects=True) as resp:
            resp.raise_for_status()
            downloaded = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    downloaded += len(chunk)
                    if downloaded > MAX_FILE_SIZE:
                        raise RuntimeError(f"File too large: {downloaded} bytes exceeds limit of {MAX_FILE_SIZE}")
                    f.write(chunk)

    async def _dash_download(self, url: str, tmp_path: str):
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "-o", tmp_path,
            "--merge-output-format", "mp4",
            "--max-filesize", f"{MAX_FILE_SIZE // (1024*1024)}M",
            "--socket-timeout", "60",
            url,
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            error_msg = stderr.decode(errors="ignore")[-200:]
            raise RuntimeError(f"yt-dlp failed: {error_msg}")
