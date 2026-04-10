"""存储模块 - 字幕和指纹的持久化操作"""

import json
from typing import Optional, List
from .pool import get_pool, get_pool_sync


async def save_subtitle(
    aweme_id: str,
    video_url: str,
    fingerprint: str,
    subtitle_text: str,
    segments: List[dict],
    duration: float,
    confidence: float,
    status: int = 1,
    error_msg: Optional[str] = None,
) -> None:
    """
    保存字幕结果到数据库

    Args:
        aweme_id: 视频ID
        video_url: 视频URL
        fingerprint: pHash指纹
        subtitle_text: 完整字幕文本
        segments: 字幕分段列表
        duration: 视频时长(秒)
        confidence: 置信度
        status: 状态 (1=成功, 2=失败, 3=重复)
        error_msg: 错误信息
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
                INSERT INTO dy_subtitle
                (aweme_id, video_url, fingerprint, subtitle_text, segments,
                 duration, confidence, status, error_msg, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    subtitle_text = VALUES(subtitle_text),
                    segments = VALUES(segments),
                    duration = VALUES(duration),
                    confidence = VALUES(confidence),
                    status = VALUES(status),
                    error_msg = VALUES(error_msg),
                    processed_at = NOW()
            """
            await cur.execute(
                sql,
                (
                    aweme_id,
                    video_url,
                    fingerprint,
                    subtitle_text,
                    json.dumps(segments, ensure_ascii=False),
                    duration,
                    confidence,
                    status,
                    error_msg,
                ),
            )


def save_subtitle_sync(
    aweme_id: str,
    video_url: str,
    fingerprint: str,
    subtitle_text: str,
    segments: List[dict],
    duration: float,
    confidence: float,
    status: int = 1,
    error_msg: Optional[str] = None,
) -> None:
    """同步版本 - 用于Celery worker"""
    conn = get_pool_sync()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dy_subtitle
                (aweme_id, video_url, fingerprint, subtitle_text, segments,
                 duration, confidence, status, error_msg, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    subtitle_text = VALUES(subtitle_text),
                    segments = VALUES(segments),
                    duration = VALUES(duration),
                    confidence = VALUES(confidence),
                    status = VALUES(status),
                    error_msg = VALUES(error_msg),
                    processed_at = NOW()
            """
            cur.execute(
                sql,
                (
                    aweme_id,
                    video_url,
                    fingerprint,
                    subtitle_text,
                    json.dumps(segments, ensure_ascii=False),
                    duration,
                    confidence,
                    status,
                    error_msg,
                ),
            )
    finally:
        conn.close()


async def save_fingerprint(aweme_id: str, video_url: str, phash: str) -> None:
    """
    保存视频指纹

    Args:
        aweme_id: 视频ID
        video_url: 视频URL
        phash: pHash指纹
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
                INSERT INTO dy_fingerprint (aweme_id, video_url, phash)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE phash = VALUES(phash)
            """
            await cur.execute(sql, (aweme_id, video_url, phash))


def save_fingerprint_sync(aweme_id: str, video_url: str, phash: str) -> None:
    """同步版本 - 用于Celery worker"""
    conn = get_pool_sync()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dy_fingerprint (aweme_id, video_url, phash)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE phash = VALUES(phash)
            """
            cur.execute(sql, (aweme_id, video_url, phash))
    finally:
        conn.close()


async def get_fingerprint_prefix(
    prefix: str, exclude_aweme_id: str = None
) -> List[dict]:
    """
    根据指纹前缀查询相似指纹

    Args:
        prefix: pHash前缀
        exclude_aweme_id: 排除的aweme_id（通常是当前视频自己）

    Returns:
        匹配的指纹记录列表
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            if exclude_aweme_id:
                sql = "SELECT aweme_id, phash FROM dy_fingerprint WHERE phash LIKE %s AND aweme_id != %s"
                await cur.execute(sql, (prefix + "%", exclude_aweme_id))
            else:
                sql = "SELECT aweme_id, phash FROM dy_fingerprint WHERE phash LIKE %s"
                await cur.execute(sql, (prefix + "%",))
            return await cur.fetchall()


def get_fingerprint_prefix_sync(
    prefix: str, exclude_aweme_id: str = None
) -> List[dict]:
    """同步版本 - 用于Celery worker"""
    conn = get_pool_sync()
    try:
        with conn.cursor() as cur:
            if exclude_aweme_id:
                sql = "SELECT aweme_id, phash FROM dy_fingerprint WHERE phash LIKE %s AND aweme_id != %s"
                cur.execute(sql, (prefix + "%", exclude_aweme_id))
            else:
                sql = "SELECT aweme_id, phash FROM dy_fingerprint WHERE phash LIKE %s"
                cur.execute(sql, (prefix + "%",))
            return cur.fetchall()
    finally:
        conn.close()


async def get_subtitle_by_aweme_id(aweme_id: str) -> Optional[dict]:
    """
    根据aweme_id查询字幕

    Args:
        aweme_id: 视频ID

    Returns:
        字幕记录或None
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = "SELECT * FROM dy_subtitle WHERE aweme_id = %s"
            await cur.execute(sql, (aweme_id,))
            return await cur.fetchone()


def get_subtitle_by_aweme_id_sync(aweme_id: str) -> Optional[dict]:
    """同步版本 - 用于Celery worker"""
    conn = get_pool_sync()
    try:
        with conn.cursor() as cur:
            sql = "SELECT * FROM dy_subtitle WHERE aweme_id = %s"
            cur.execute(sql, (aweme_id,))
            return cur.fetchone()
    finally:
        conn.close()


async def copy_subtitle(
    from_aweme_id: str,
    to_aweme_id: str,
    video_url: str,
    fingerprint: str,
) -> bool:
    """
    复制字幕(用于重复视频)

    Args:
        from_aweme_id: 源视频ID
        to_aweme_id: 目标视频ID
        video_url: 目标视频URL
        fingerprint: 目标视频指纹

    Returns:
        是否复制成功
    """
    src = await get_subtitle_by_aweme_id(from_aweme_id)
    if not src:
        return False

    await save_subtitle(
        aweme_id=to_aweme_id,
        video_url=video_url,
        fingerprint=fingerprint,
        subtitle_text=src["subtitle_text"],
        segments=json.loads(src["segments"]) if src["segments"] else [],
        duration=src["duration"],
        confidence=src["confidence"],
        status=3,
    )
    return True


def copy_subtitle_sync(
    from_aweme_id: str,
    to_aweme_id: str,
    video_url: str,
    fingerprint: str,
) -> bool:
    """同步版本 - 用于Celery worker"""
    src = get_subtitle_by_aweme_id_sync(from_aweme_id)
    if not src:
        return False

    save_subtitle_sync(
        aweme_id=to_aweme_id,
        video_url=video_url,
        fingerprint=fingerprint,
        subtitle_text=src["subtitle_text"],
        segments=json.loads(src["segments"]) if src["segments"] else [],
        duration=src["duration"],
        confidence=src["confidence"],
        status=3,
    )
    return True


def mark_as_processing(aweme_id: str, video_url: str) -> bool:
    """
    标记视频为处理中状态 (status=0)

    使用 INSERT IGNORE，只有不存在时才插入。
    用于 Scheduler 分发任务前，防止同一视频被重复分发。

    Args:
        aweme_id: 视频ID
        video_url: 视频URL

    Returns:
        True 表示成功标记(之前未处理)
        False 表示已被标记(正在处理中或已处理)
    """
    conn = get_pool_sync()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT IGNORE INTO dy_subtitle (aweme_id, video_url, status)
                VALUES (%s, %s, 0)
            """
            cur.execute(sql, (aweme_id, video_url))
            return cur.rowcount > 0
    finally:
        conn.close()
