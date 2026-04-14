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
import hashlib
import httpx
from PIL import Image
from pathlib import Path
from typing import Optional, Tuple

# 下载视频时使用的HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.douyin.com/",
}

# 纯音频Content-Type列表（确定没有视频的）
# 注意: audio/mp4 不在此列表，因为抖音很多视频返回此类型，实际包含视频流
AUDIO_CONTENT_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/m4a",
    "audio/x-m4a",
    "audio/wav",
    "audio/ogg",
    # audio/mp4 可能是视频，需要下载后检测
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
        计算视频的pHash指纹（从URL下载）

        Args:
            video_url: 视频下载URL

        Returns:
            64位pHash十六进制字符串，失败返回None
        """
        # 先检查Content-Type，确保是视频
        if not self._is_video_url(video_url):
            return None

        video_path = self.temp_dir / "video" / f"{hashlib.md5(video_url.encode()).hexdigest()[:12]}.mp4"
        video_path.parent.mkdir(exist_ok=True)

        try:
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                response = client.get(video_url, headers=HEADERS)
                response.raise_for_status()
                
                # 检查内容大小
                if len(response.content) < 1024:
                    return None
                
                with open(video_path, "wb") as f:
                    f.write(response.content)

            # 通过文件头检测是否为纯音频
            if not self._is_video_file(str(video_path)):
                return None

            return self.compute_phash_from_file(str(video_path))
        except Exception:
            return None
        finally:
            if video_path.exists():
                video_path.unlink()

    def compute_phash_from_file(self, video_path: str) -> Optional[str]:
        """
        计算视频的pHash指纹（从本地文件）

        Args:
            video_path: 视频文件路径

        Returns:
            64位pHash十六进制字符串，失败返回None
        """
        try:
            frame = self._extract_frame(video_path)
            if frame is None:
                return None

            pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            return str(imagehash.phash(pil_image))
        except Exception:
            return None

    def _is_video_url(self, url: str) -> bool:
        """
        检查URL是否可能返回视频内容

        使用HEAD请求检查Content-Type和状态码
        返回True表示应该尝试下载，False表示确定不是视频
        """
        try:
            with httpx.Client(timeout=10, follow_redirects=True) as client:
                response = client.head(url, headers=HEADERS)
                
                # URL过期或无效
                if response.status_code == 404:
                    return False
                if response.status_code >= 400:
                    return False
                    
                content_type = response.headers.get("content-type", "").lower()
                # 如果是纯音频类型，直接拒绝
                if content_type in AUDIO_CONTENT_TYPES:
                    return False
                # 其他类型（包括audio/mp4）都尝试下载，由后续步骤检测
                return True
        except Exception:
            # 如果检查失败，仍然尝试下载
            return True

    @staticmethod
    def _is_video_file(file_path: str) -> bool:
        """
        通过文件头检测是否为视频文件（而非纯音频）

        Returns:
            True 是视频文件，False 是纯音频或其他
        """
        try:
            with open(file_path, 'rb') as f:
                header = f.read(32)
            
            # 检查MP4/M4A文件类型
            if header[4:8] == b'ftyp':
                # ftyp后面是品牌标识
                brand = header[8:12]
                # M4A 是纯音频
                if brand in {b'M4A ', b'M4B ', b'M4P '}:
                    return False
                # M4V, mp42, isom, avc1 等通常是视频
                return True
            
            # 其他视频格式签名
            video_signatures = {
                b'\x00\x00\x00\x14ftyp': 'mp4',
                b'\x00\x00\x00\x1cftyp': 'mp4',
                b'\x00\x00\x00 ftyp': 'mp4',
            }
            
            # 默认假设是视频（让后续处理决定）
            return True
        except Exception:
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
