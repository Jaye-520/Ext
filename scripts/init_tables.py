"""初始化数据库表"""

import asyncio
import aiomysql
from pathlib import Path
from loguru import logger
from ext.config import get_settings


async def init_tables():
    """初始化数据库表"""
    settings = get_settings()
    db_config = settings.crawler_db

    logger.info("Connecting to database...")
    pool = await aiomysql.create_pool(
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 3306),
        user=db_config.get("user", "root"),
        password=db_config.get("password", "123456"),
        db=db_config.get("database", "media_crawler_pro"),
        autocommit=True,
    )

    # 读取SQL文件
    sql_file = Path(__file__).parent.parent / "migrations" / "001_init_tables.sql"
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # 执行SQL - 简单处理，分号分割
    statements = []
    current_stmt = []
    for line in sql_content.split("\n"):
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        current_stmt.append(line)
        if line.endswith(";"):
            statements.append(" ".join(current_stmt))
            current_stmt = []

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for stmt in statements:
                if stmt and stmt != ";":
                    try:
                        await cur.execute(stmt)
                        logger.info(f"Executed: {stmt[:50]}...")
                    except Exception as e:
                        logger.warning(f"SQL执行警告: {e}")

    pool.close()
    await pool.wait_closed()
    logger.info("Tables initialized")


if __name__ == "__main__":
    asyncio.run(init_tables())
