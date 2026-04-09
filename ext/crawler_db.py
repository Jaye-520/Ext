"""爬虫数据库连接模块"""

import aiomysql
from loguru import logger
from ext.config import get_settings


class CrawlerDB:
    """爬虫数据库连接"""

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
        logger.info("CrawlerDB connected")

    async def close(self):
        """关闭连接"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("CrawlerDB closed")

    async def fetch_pending_videos(self, limit: int = 100) -> list:
        """
        获取待处理的视频

        只获取未处理或处理失败(可重试)的视频，排除已成功的

        Args:
            limit: 每次拉取数量

        Returns:
            list: 视频列表 [{aweme_id, video_download_url, title, ...}, ...]
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # 只查询：1.有视频URL 2.不在dy_subtitle中 3.或status=2(失败可重试)
                sql = """
                    SELECT 
                        a.id, a.aweme_id, a.video_download_url, a.title, 
                        a.user_id, a.nickname, a.create_time
                    FROM douyin_aweme a
                    WHERE a.video_download_url != '' 
                    AND a.video_download_url IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM dy_subtitle s 
                        WHERE s.aweme_id = a.aweme_id 
                        AND s.status IN (1, 3)  -- 排除已成功的和重复的
                    )
                    LIMIT %s
                """
                await cur.execute(sql, (limit,))
                rows = await cur.fetchall()
                logger.info(
                    f"Fetched {len(rows)} pending videos (excluding already successful)"
                )
                return rows

    async def check_video_exists(self, aweme_id: str) -> bool:
        """
        检查视频是否已处理(通过指纹表去重)

        Args:
            aweme_id: 抖音视频ID

        Returns:
            bool: 是否已存在
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = "SELECT COUNT(*) FROM dy_fingerprint WHERE aweme_id = %s"
                await cur.execute(sql, (aweme_id,))
                result = await cur.fetchone()
                return result[0] > 0
