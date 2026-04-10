"""数据库连接池管理模块"""

import aiomysql
from typing import Optional
from ..config import config

_pool: Optional[aiomysql.Pool] = None


async def get_pool(db_key: str = "result_db") -> aiomysql.Pool:
    """
    获取数据库连接池

    Args:
        db_key: 配置中的数据库键名 (result_db 或 crawler_db)

    Returns:
        aiomysql连接池实例
    """
    global _pool
    if _pool is None:
        db_config = getattr(config, db_key)
        _pool = await aiomysql.create_pool(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            db=db_config.database,
            autocommit=True,
            minsize=5,
            maxsize=20,
            cursorclass=aiomysql.DictCursor,
        )
    return _pool


async def close_pool():
    """关闭连接池"""
    global _pool
    if _pool:
        _pool.close()
        await _pool.wait_closed()
        _pool = None


def get_pool_sync(db_key: str = "result_db"):
    """
    获取同步数据库连接 (用于Celery worker)

    Args:
        db_key: 配置中的数据库键名

    Returns:
        pymysql连接对象
    """
    import pymysql

    db_config = getattr(config, db_key)
    return pymysql.connect(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        db=db_config.database,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
