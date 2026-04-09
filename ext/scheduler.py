"""定时任务模块"""

import asyncio
import os
import shutil
import signal
import sys
import time
import schedule
from pathlib import Path
from loguru import logger
from ext.config import get_settings
from ext.crawler_db import CrawlerDB
from ext.worker import process_video

PID_FILE = Path("ext/scheduler.pid")
TEMP_DIR = Path("temp")


def _cleanup_old_scheduler():
    """杀掉之前运行的scheduler实例"""
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            try:
                os.kill(old_pid, 0)
                logger.info(f"Killing old scheduler (PID: {old_pid})")
                os.kill(old_pid, signal.SIGTERM)
                time.sleep(1)
            except OSError:
                pass
        except (ValueError, FileNotFoundError):
            pass
        finally:
            PID_FILE.unlink(missing_ok=True)

    PID_FILE.write_text(str(os.getpid()))


def cleanup_temp_files():
    """清理临时文件"""
    if TEMP_DIR.exists():
        try:
            for item in TEMP_DIR.iterdir():
                try:
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                except Exception as e:
                    logger.warning(f"Failed to clean {item}: {e}")
            logger.info("Temp files cleaned")
        except Exception as e:
            logger.warning(f"Failed to clean temp dir: {e}")


def _signal_handler(signum, frame):
    """处理退出信号"""
    logger.info("Scheduler received shutdown signal, cleaning up...")
    cleanup_temp_files()
    if PID_FILE.exists():
        PID_FILE.unlink(missing_ok=True)
    sys.exit(0)


async def fetch_and_dispatch():
    """拉取视频并分发任务"""
    logger.info("=== Starting scheduled task ===")

    crawler_db = CrawlerDB()
    await crawler_db.connect()

    try:
        settings = get_settings()
        batch_size = settings.scheduler_batch_size

        videos = await crawler_db.fetch_pending_videos(limit=batch_size)
        logger.info(f"Fetched {len(videos)} videos")

        if not videos:
            logger.info("No pending videos, skipping")
            return

        dispatched = 0
        for video in videos:
            aweme_id = video.get("aweme_id")
            video_url = video.get("video_download_url")
            title = video.get("title")

            if not aweme_id or not video_url:
                logger.warning(f"Missing aweme_id or video_url: {video}")
                continue

            # 分发任务 (数据库已过滤已成功的视频)
            process_video.delay(aweme_id, video_url, title)
            logger.info(f"Dispatched task: aweme_id={aweme_id}")
            dispatched += 1

        logger.info(f"Dispatched {dispatched} tasks")

    finally:
        await crawler_db.close()

    cleanup_temp_files()

    logger.info("=== Scheduled task completed ===")


def run_scheduler():
    """运行定时任务"""
    # 清理旧实例
    _cleanup_old_scheduler()

    # 注册信号处理
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    settings = get_settings()
    interval = settings.scheduler_interval_minutes

    logger.info(f"Starting scheduler: interval={interval} minutes, PID={os.getpid()}")

    schedule.every(interval).minutes.do(lambda: asyncio.run(fetch_and_dispatch()))

    asyncio.run(fetch_and_dispatch())

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    run_scheduler()
