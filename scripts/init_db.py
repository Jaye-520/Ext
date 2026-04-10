#!/usr/bin/env python3
import aiomysql
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import config


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS dy_subtitle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    aweme_id VARCHAR(64) NOT NULL UNIQUE,
    video_url VARCHAR(1024),
    fingerprint VARCHAR(64),
    language VARCHAR(8) DEFAULT 'zh',
    duration FLOAT,
    subtitle_text LONGTEXT,
    segments JSON,
    confidence FLOAT,
    status TINYINT DEFAULT 0 COMMENT '0:pending, 1:success, 2:failed, 3:duplicate',
    error_msg TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dy_fingerprint (
    id INT AUTO_INCREMENT PRIMARY KEY,
    aweme_id VARCHAR(64) NOT NULL UNIQUE,
    video_url VARCHAR(1024),
    phash VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_phash (phash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


async def init_tables():
    pool = await aiomysql.create_pool(
        host=config.result_db.host,
        port=config.result_db.port,
        user=config.result_db.user,
        password=config.result_db.password,
        db=config.result_db.database,
        autocommit=True,
    )

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for statement in CREATE_TABLES_SQL.split(";"):
                statement = statement.strip()
                if statement:
                    await cur.execute(statement)
            print("tables created successfully")

    pool.close()
    await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(init_tables())
