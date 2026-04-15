"""MySQL 数据库连接池"""
import aiomysql
from typing import Any, Optional, List, Dict
from contextlib import asynccontextmanager

from .config import DatabaseConfig
from .logger import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            db=self.config.database,
            autocommit=False,
            minsize=2,
            maxsize=10,
            charset="utf8mb4",
        )
        logger.info("db_connected", host=self.config.host, db=self.config.database)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("db_closed")

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    yield conn, cur
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

    async def execute(self, sql: str, args: tuple = ()) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                await conn.commit()
                return cur.rowcount

    async def fetch_one(self, sql: str, args: tuple = ()) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchone()

    async def fetch_all(self, sql: str, args: tuple = ()) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchall()

    async def executemany(self, sql: str, args: List[tuple]):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, args)
                await conn.commit()
                return cur.rowcount
