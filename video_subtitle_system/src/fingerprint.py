"""视频指纹（多帧 pHash 合并）"""
from pathlib import Path

import cv2
import imagehash
from PIL import Image

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


class VideoFingerprint:
    def __init__(self, db: Database):
        self.db = db

    async def compute(self, video_path: Path) -> str:
        video_path = Path(video_path)
        cap = cv2.VideoCapture(str(video_path))

        try:
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if fps <= 0 or total_frames <= 0:
                logger.warning("phash_skipped", reason="invalid_video_metadata", fps=fps, total_frames=total_frames, video_path=str(video_path))
                return ""

            timestamps = [1, 3, 5]
            hashes = []

            for t in timestamps:
                frame_pos = int(t * fps)
                if frame_pos < total_frames:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                    ret, frame = cap.read()
                    if ret:
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        img = Image.fromarray(frame_rgb)
                        hashes.append(imagehash.phash(img))

            if not hashes:
                logger.warning("phash_skipped", reason="no_frames_extracted", video_path=str(video_path))
                return ""

            merged = hashes[0].hash
            for h in hashes[1:]:
                merged = merged & h.hash
            result = str(imagehash.ImageHash(merged))
            logger.info("phash_computed", video_path=str(video_path), phash=result)
            return result

        finally:
            cap.release()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        if not hash1 or not hash2:
            return float('inf')
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    async def is_duplicate(self, p_hash: str) -> bool:
        if not p_hash:
            return False
        fingerprints = await self.db.fetch_all(
            "SELECT p_hash FROM fingerprint WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY) ORDER BY id DESC LIMIT 1000"
        )
        for fp in fingerprints:
            if self.hamming_distance(p_hash, fp["p_hash"]) <= 5:
                logger.info("duplicate_detected", p_hash=p_hash, match=fp["p_hash"])
                return True
        return False
