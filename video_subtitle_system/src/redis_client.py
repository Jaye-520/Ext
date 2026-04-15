"""Redis 客户端封装"""
import json
from typing import Optional, Any
import redis.asyncio as redis

from .config import RedisConfig
from .logger import get_logger

logger = get_logger(__name__)


class RedisClient:
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client: Optional[redis.Redis] = None

    async def connect(self):
        self.client = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            decode_responses=True,
        )
        await self.client.ping()
        logger.info("redis_connected", host=self.config.host, port=self.config.port)

    async def close(self):
        if self.client:
            await self.client.aclose()
            logger.info("redis_closed")

    def _dumps(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    def _loads(self, value: str) -> Any:
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    async def lpush(self, key: str, value: Any) -> int:
        return await self.client.lpush(key, self._dumps(value))

    async def rpush(self, key: str, value: Any) -> int:
        return await self.client.rpush(key, self._dumps(value))

    async def lrem(self, key: str, count: int, value: Any) -> int:
        return await self.client.lrem(key, count, self._dumps(value))

    async def brpoplpush(self, source: str, destination: str, timeout: int = 5) -> Optional[Any]:
        result = await self.client.brpoplpush(source, destination, timeout=timeout)
        return self._loads(result)

    async def rpoplpush(self, source: str, destination: str) -> Optional[Any]:
        result = await self.client.rpoplpush(source, destination)
        return self._loads(result)

    async def llen(self, key: str) -> int:
        return await self.client.llen(key)
