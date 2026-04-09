"""存储模块"""

import json
import aiomysql
import imagehash
from datetime import datetime
from typing import Optional, Dict
from loguru import logger
from ext.config import get_settings


class Storage:
    """存储模块 - 存入爬虫数据库"""

    def __init__(self):
        self.settings = get_settings()
        self.pool = None

    async def connect(self):
        """连接数据库"""
        db_config = self.settings.crawler_db
        self.pool = await aiomysql.create_pool(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user", "root"),
            password=db_config.get("password", "123456"),
            db=db_config.get("database", "media_crawler_pro"),
            autocommit=True,
            minsize=1,
            maxsize=10,
        )
        logger.info("Storage connected")

    async def close(self):
        """关闭连接"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def save_fingerprint(
        self,
        aweme_id: str,
        video_url: str,
        phash: str,
    ) -> None:
        """保存视频指纹"""
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = """
                    INSERT INTO dy_fingerprint (aweme_id, video_url, phash, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE phash = %s, created_at = NOW()
                """
                await cur.execute(sql, (aweme_id, video_url, phash, phash))
                logger.info(f"Saved fingerprint: aweme_id={aweme_id}")

    async def save_subtitle(
        self,
        aweme_id: str,
        video_url: str,
        fingerprint: Optional[str],
        language: str,
        duration: float,
        subtitle_text: str,
        segments: list,
        confidence: float,
        status: int = 1,
        error_msg: str = None,
    ) -> None:
        """保存字幕"""
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = """
                    INSERT INTO dy_subtitle 
                    (aweme_id, video_url, fingerprint, language, duration,
                     subtitle_text, segments, confidence, status, error_msg, 
                     created_at, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE 
                        fingerprint = %s,
                        language = %s,
                        duration = %s,
                        subtitle_text = %s,
                        segments = %s,
                        confidence = %s,
                        status = %s,
                        error_msg = %s,
                        processed_at = NOW()
                """
                segments_json = json.dumps(segments, ensure_ascii=False)
                await cur.execute(
                    sql,
                    (
                        aweme_id,
                        video_url,
                        fingerprint,
                        language,
                        duration,
                        subtitle_text,
                        segments_json,
                        confidence,
                        status,
                        error_msg,
                        # ON DUPLICATE KEY UPDATE
                        fingerprint,
                        language,
                        duration,
                        subtitle_text,
                        segments_json,
                        confidence,
                        status,
                        error_msg,
                    ),
                )
                logger.info(f"Saved subtitle: aweme_id={aweme_id}")

    async def update_status(
        self,
        aweme_id: str,
        status: int,
        error_msg: str = None,
    ) -> None:
        """更新处理状态"""
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = """
                    UPDATE dy_subtitle 
                    SET status = %s, error_msg = %s, processed_at = NOW()
                    WHERE aweme_id = %s
                """
                await cur.execute(sql, (status, error_msg, aweme_id))
                logger.info(f"Updated status: aweme_id={aweme_id}, status={status}")

    async def find_similar_fingerprint(
        self, phash: str, threshold: float = 0.9, exclude_aweme_id: str = None
    ) -> Optional[Dict]:
        """
        查找相似的视频指纹

        Args:
            phash: 当前视频的pHash
            threshold: 相似度阈值，默认0.9
            exclude_aweme_id: 排除的视频ID(当前视频)
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # 获取所有已存储的指纹
                sql = "SELECT aweme_id, phash FROM dy_fingerprint"
                await cur.execute(sql)
                rows = await cur.fetchall()

                current_hash = imagehash.hex_to_hash(phash)

                for row in rows:
                    # 排除自己
                    if exclude_aweme_id and row["aweme_id"] == exclude_aweme_id:
                        continue

                    stored_hash = imagehash.hex_to_hash(row["phash"])
                    # 计算汉明距离
                    distance = current_hash - stored_hash
                    similarity = 1.0 - (distance / 64.0)

                    if similarity >= threshold:
                        logger.info(
                            f"Found similar: {row['aweme_id']}, similarity={similarity:.2f}"
                        )
                        return {
                            "aweme_id": row["aweme_id"],
                            "phash": row["phash"],
                            "similarity": float(round(similarity, 4)),
                        }

                return None

    async def get_subtitle_by_aweme_id(self, aweme_id: str) -> Optional[Dict]:
        """
        根据aweme_id获取字幕

        Args:
            aweme_id: 抖音视频ID

        Returns:
            Optional[Dict]: 字幕信息或None
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = "SELECT * FROM dy_subtitle WHERE aweme_id = %s"
                await cur.execute(sql, (aweme_id,))
                row = await cur.fetchone()

                if row:
                    # 解析segments JSON
                    segments = json.loads(row["segments"]) if row["segments"] else []
                    return {
                        "aweme_id": row["aweme_id"],
                        "language": row["language"],
                        "duration": row["duration"],
                        "subtitle_text": row["subtitle_text"],
                        "segments": segments,
                        "confidence": row["confidence"],
                    }

                return None
