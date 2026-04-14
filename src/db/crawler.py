"""爬虫数据库读取模块 - 从爬虫的douyin_aweme表读取待处理视频

注意: SQL中子查询引用了dy_subtitle表，要求crawler_db和result_db必须是同一个数据库，
      否则子查询会找不到表。如果将来需要分开部署，需改用两次查询的方式。
"""

from typing import List
from .pool import get_pool, get_pool_sync


async def fetch_pending_videos(limit: int = 10) -> List[dict]:
    """
    获取待处理的视频列表

    从爬虫数据库的douyin_aweme表读取尚未处理的视频
    自动过滤纯音频文件（mp3, m4a, music等）

    Args:
        limit: 最大返回数量

    Returns:
        视频列表，每项包含 aweme_id, video_download_url, title
    """
    pool = await get_pool("crawler_db")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
                SELECT aweme_id, video_download_url, title
                FROM douyin_aweme
                WHERE video_download_url IS NOT NULL
                  AND video_download_url != ''
                  AND video_download_url NOT LIKE '%%mp3'
                  AND video_download_url NOT LIKE '%%m4a'
                  AND video_download_url NOT LIKE '%%music%%'
                  AND video_download_url NOT LIKE '%%ies-music%%'
                  AND aweme_id NOT IN (
                      SELECT aweme_id FROM dy_subtitle WHERE status IN (0, 1, 2, 3)
                  )
                LIMIT %s
            """
            await cur.execute(sql, (limit,))
            return await cur.fetchall()


def fetch_pending_videos_sync(limit: int = 10) -> List[dict]:
    """
    同步版本 - 用于Scheduler分发任务

    自动过滤纯音频文件（mp3, m4a, music等）
    过滤掉已处理(status=1)、处理中(status=0)、重复(status=3)、失败(status=2)的视频
    """
    conn = get_pool_sync("crawler_db")
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT aweme_id, video_download_url, title
                FROM douyin_aweme
                WHERE video_download_url IS NOT NULL
                  AND video_download_url != ''
                  AND video_download_url NOT LIKE '%%mp3'
                  AND video_download_url NOT LIKE '%%m4a'
                  AND video_download_url NOT LIKE '%%music%%'
                  AND video_download_url NOT LIKE '%%ies-music%%'
                  AND aweme_id NOT IN (
                      SELECT aweme_id FROM dy_subtitle WHERE status IN (0, 1, 2, 3)
                  )
                LIMIT %s
            """
            cur.execute(sql, (limit,))
            return cur.fetchall()
    finally:
        conn.close()
