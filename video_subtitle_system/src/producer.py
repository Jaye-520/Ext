"""Producer：轮询数据库，拉取任务，推入Redis队列"""
import json

from .db import Database
from .redis_client import RedisClient
from .cursor import CursorManager
from .config import PollConfig
from .logger import get_logger

logger = get_logger(__name__)


class Producer:
    def __init__(
        self,
        db: Database,
        redis: RedisClient,
        cursor_mgr: CursorManager,
        config: PollConfig,
        backpressure_threshold: int = 1000,
    ):
        self.db = db
        self.redis = redis
        self.cursor_mgr = cursor_mgr
        self.config = config
        self.backpressure_threshold = backpressure_threshold
        self._running = False

    async def start(self):
        import asyncio
        self._running = True
        logger.info("producer_started", interval=self.config.interval_seconds)
        while self._running:
            try:
                await self.poll()
            except Exception as e:
                logger.error("producer_poll_error", error=str(e))
            await asyncio.sleep(self.config.interval_seconds)

    async def stop(self):
        self._running = False
        logger.info("producer_stopped")

    async def poll(self):
        cursor = await self.cursor_mgr.get_cursor()

        queue_len = await self.redis.llen("task_queue")
        if queue_len > self.backpressure_threshold:
            logger.warning("backpressure_active", queue_len=queue_len)
            return

        bilibili_tasks = await self.db.fetch_all(
            """SELECT id as video_id, 'bilibili' as platform, video_url as url
               FROM bilibili_video
               WHERE id > %s
               ORDER BY id
               LIMIT %s""",
            (cursor.bilibili_last_id, self.config.batch_size // 2)
        )

        douyin_tasks = await self.db.fetch_all(
            """SELECT id as video_id, 'douyin' as platform, video_download_url as url
               FROM douyin_aweme
               WHERE id > %s
               ORDER BY id
               LIMIT %s""",
            (cursor.douyin_last_id, self.config.batch_size // 2)
        )

        total = 0

        for task in bilibili_tasks:
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.cursor_mgr.update_bilibili_cursor(task["video_id"])
            total += 1

        for task in douyin_tasks:
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.cursor_mgr.update_douyin_cursor(task["video_id"])
            total += 1

        if total > 0:
            logger.info("tasks_pushed", count=total, bilibili=len(bilibili_tasks), douyin=len(douyin_tasks))