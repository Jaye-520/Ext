"""存储层：幂等写入 + PROCESSING 抢占锁"""
from typing import List, Dict, Any

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


class SubtitleStorage:
    def __init__(self, db: Database):
        self.db = db

    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """原子抢占 PROCESSING 锁。返回 True 表示成功，False 表示已是 SUCCESS。"""
        # 先查是否已经是 SUCCESS，避免无谓的 UPDATE
        existing = await self.db.fetch_one(
            "SELECT status FROM task_status WHERE video_id = %s AND platform = %s",
            (video_id, platform)
        )
        if existing and existing["status"] == "SUCCESS":
            return False

        await self.db.execute(
            """INSERT INTO task_status (video_id, platform, status, retry_count)
               VALUES (%s, %s, 'PROCESSING', 0)
               ON DUPLICATE KEY UPDATE
                   status = IF(status = 'SUCCESS', 'SUCCESS', 'PROCESSING')""",
            (video_id, platform)
        )
        # 插入成功后重新确认状态（防止并发更新导致误判）
        row = await self.db.fetch_one(
            "SELECT status FROM task_status WHERE video_id = %s AND platform = %s",
            (video_id, platform)
        )
        return row is not None and row["status"] == "PROCESSING"

    async def save(self, video_id: str, platform: str, segments: List[Dict[str, Any]], p_hash: str):
        async with self.db.transaction() as (conn, cur):
            if p_hash:
                await cur.execute(
                    "INSERT IGNORE INTO fingerprint (video_id, platform, p_hash) VALUES (%s, %s, %s)",
                    (video_id, platform, p_hash)
                )
            if segments:
                subtitle_records = [
                    (video_id, platform, seg["start_time"], seg["end_time"], seg["text"], seg.get("confidence"))
                    for seg in segments
                ]
                await cur.executemany(
                    """INSERT IGNORE INTO subtitle_segment
                       (video_id, platform, start_time, end_time, text, confidence)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    subtitle_records
                )
                full_text = " ".join(seg["text"].strip() for seg in segments)
                confidences = [seg.get("confidence", 1.0) for seg in segments if seg.get("confidence") is not None]
                confidence_avg = sum(confidences) / len(confidences) if confidences else None
                await cur.execute(
                    """INSERT INTO subtitle (video_id, platform, full_text, confidence_avg)
                       VALUES (%s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE full_text=VALUES(full_text), confidence_avg=VALUES(confidence_avg)""",
                    (video_id, platform, full_text, confidence_avg)
                )
            await cur.execute(
                """INSERT INTO task_status (video_id, platform, status, error_msg)
                   VALUES (%s, %s, 'SUCCESS', NULL)
                   ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
                (video_id, platform)
            )
        logger.info("subtitle_saved", video_id=video_id, platform=platform, segments=len(segments))

    async def mark_failed(self, video_id: str, platform: str, error_msg: str):
        await self.db.execute(
            "UPDATE task_status SET status = 'FAILED', error_msg = %s WHERE video_id = %s AND platform = %s",
            (error_msg, video_id, platform)
        )
        logger.error("task_marked_failed", video_id=video_id, platform=platform, error=error_msg)

    async def reset_processing_to_pending(self):
        await self.db.execute(
            "UPDATE task_status SET status = 'PENDING' WHERE status = 'PROCESSING'"
        )
        logger.info("recovered_processing_tasks")
