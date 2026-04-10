"""
pHash视频指纹模块

用于计算视频的感知哈希指纹，实现视频去重
- 使用OpenCV提取视频关键帧
- 使用imagehash计算pHash
- 支持汉明距离相似度计算
- 自动检测并跳过非视频文件
"""

import imagehash
import cv2
import httpx
from PIL import Image
from pathlib import Path
from typing import Optional, Tuple

# 下载视频时使用的HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.douyin.com/",
}

# 非视频的Content-Type列表
AUDIO_CONTENT_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/mp4",
    "audio/m4a",
    "audio/x-m4a",
    "audio/wav",
    "audio/ogg",
    "application/octet-stream",
}


class VideoFingerprint:
    """
    视频指纹计算器

    通过提取视频第一帧计算pHash指纹
    自动检测并跳过非视频文件
    """

    def __init__(self):
        self.temp_dir = Path(__file__).parent.parent.parent / "temp"
        self.temp_dir.mkdir(exist_ok=True)

    def compute_phash_sync(self, video_url: str) -> Optional[str]:
        """
        计算视频的pHash指纹

        Args:
            video_url: 视频下载URL

        Returns:
            64位pHash十六进制字符串，失败返回None
        """
        # 先检查Content-Type，确保是视频
        if not self._is_video_url(video_url):
            return None

        video_path = self.temp_dir / "video" / f"{hash(video_url)}.mp4"
        video_path.parent.mkdir(exist_ok=True)

        try:
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                response = client.get(video_url, headers=HEADERS)
                response.raise_for_status()
                with open(video_path, "wb") as f:
                    f.write(response.content)

            frame = self._extract_frame(video_path)
            if frame is None:
                return None

            pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            return str(imagehash.phash(pil_image))
        except Exception:
            return None
        finally:
            if video_path.exists():
                video_path.unlink()

    def _is_video_url(self, url: str) -> bool:
        """
        检查URL是否可能返回视频内容

        使用HEAD请求检查Content-Type
        """
        try:
            with httpx.Client(timeout=10, follow_redirects=True) as client:
                response = client.head(url, headers=HEADERS)
                content_type = response.headers.get("content-type", "").lower()
                # 如果是音频类型，直接拒绝
                if content_type in AUDIO_CONTENT_TYPES:
                    return False
                # 如果包含video关键字，接受
                if "video" in content_type:
                    return True
                # 未知类型仍然尝试下载（可能是重定向后的URL）
                return True
        except Exception:
            # 如果检查失败，仍然尝试下载
            return True

    def _extract_frame(self, video_path: str) -> Optional[Image.Image]:
        """
        提取视频第一帧

        Args:
            video_path: 视频文件路径

        Returns:
            帧图像，失败返回None
        """
        cap = cv2.VideoCapture(video_path)
        ret, frame = cap.read()
        cap.release()
        return frame if ret else None


def compute_similarity(phash1: str, phash2: str) -> float:
    """
    计算两个pHash的相似度

    Args:
        phash1: 第一个指纹
        phash2: 第二个指纹

    Returns:
        相似度 (0.0-1.0)，1.0表示完全相同
    """
    h1 = imagehash.hex_to_hash(phash1)
    h2 = imagehash.hex_to_hash(phash2)
    return 1.0 - ((h1 - h2) / 64.0)


def find_similar(phash: str, candidates: list) -> Tuple[Optional[str], float]:
    """
    在候选列表中查找相似视频

    Args:
        phash: 待查询的指纹
        candidates: 候选指纹列表，每项包含 aweme_id 和 phash

    Returns:
        (匹配的aweme_id, 相似度)，未找到返回(None, 0.0)
    """
    target = imagehash.hex_to_hash(phash)
    best_match = None
    best_similarity = 0.0

    for row in candidates:
        stored = imagehash.hex_to_hash(row["phash"])
        similarity = 1.0 - ((target - stored) / 64.0)
        if similarity >= 0.90 and similarity > best_similarity:
            best_match = row["aweme_id"]
            best_similarity = similarity

    if best_match:
        return best_match, best_similarity
    return None, 0.0
