"""双游标管理（bilibili / douyin 分离）"""
from dataclasses import dataclass

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class Cursor:
    bilibili_last_id: int
    douyin_last_id: int


class CursorManager:
    def __init__(self, db: Database):
        self.db = db

    async def get_cursor(self) -> Cursor:
        row = await self.db.fetch_one("SELECT bilibili_last_id, douyin_last_id FROM sync_cursor WHERE id = 1")
        if row is None:
            await self.db.execute(
                "INSERT INTO sync_cursor (id, bilibili_last_id, douyin_last_id) VALUES (1, 0, 0)"
            )
            return Cursor(bilibili_last_id=0, douyin_last_id=0)
        return Cursor(
            bilibili_last_id=row["bilibili_last_id"],
            douyin_last_id=row["douyin_last_id"],
        )

    async def update_bilibili_cursor(self, last_id: int):
        await self.db.execute(
            "UPDATE sync_cursor SET bilibili_last_id = %s WHERE id = 1",
            (last_id,)
        )
        logger.debug("cursor_updated", platform="bilibili", last_id=last_id)

    async def update_douyin_cursor(self, last_id: int):
        await self.db.execute(
            "UPDATE sync_cursor SET douyin_last_id = %s WHERE id = 1",
            (last_id,)
        )
        logger.debug("cursor_updated", platform="douyin", last_id=last_id)
