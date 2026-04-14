"""数据库连接池管理模块"""

import aiomysql
from typing import Optional, Dict
from ..config import config

_pools: Dict[str, aiomysql.Pool] = {}


async def get_pool(db_key: str = "result_db") -> aiomysql.Pool:
    """
    获取数据库连接池

    Args:
        db_key: 配置中的数据库键名 (result_db 或 crawler_db)

    Returns:
        aiomysql连接池实例
    """
    if db_key not in _pools:
        db_config = getattr(config, db_key)
        _pools[db_key] = await aiomysql.create_pool(
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
    return _pools[db_key]


async def close_pool():
    """关闭所有连接池"""
    global _pools
    for key, pool in _pools.items():
        pool.close()
        await pool.wait_closed()
    _pools = {}


def get_pool_sync(db_key: str = "result_db"):
    """
    获取同步数据库连接 (用于Celery worker)

    注意: Celery worker 是多进程的，每个进程需要独立的连接。
    此函数每次调用都创建新连接，由调用方负责关闭。

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
