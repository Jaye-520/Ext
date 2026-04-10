"""
Scheduler主入口模块

定时任务调度器:
- 每N分钟从爬虫数据库读取待处理视频
- 分发Celery任务到Worker队列
- 定时清理临时目录
"""

import os
import signal
import sys
import time
import schedule
from pathlib import Path
from loguru import logger

from .config import config
from .db import crawler, storage
from .db.pool import close_pool
from .worker import celery_app, process_video


class Scheduler:
    """调度器类 - 支持优雅关闭"""

    def __init__(self):
        self.running = True
        self.setup_logging()
        self.setup_signal_handlers()

    def setup_logging(self):
        """配置日志"""
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)

        logger.add(
            log_dir / "scheduler_{time}.log",
            rotation="00:00",
            retention="7 days",
            compression="zip",
            level="INFO",
        )

    def setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum, frame):
        """处理终止信号"""
        self.running = False
        signame = signal.Signals(signum).name
        logger.info(f"Received {signame}, shutting down gracefully...")

    def dispatch_tasks(self):
        """分发任务到Celery队列"""
        try:
            videos = crawler.fetch_pending_videos_sync(config.scheduler.batch_size)
            dispatched = 0
            for v in videos:
                # 先标记为处理中，防止重复分发
                if storage.mark_as_processing(v["aweme_id"], v["video_download_url"]):
                    process_video.delay(
                        v["aweme_id"], v["video_download_url"], v.get("title")
                    )
                    dispatched += 1
                else:
                    logger.debug(
                        f"aweme_id={v['aweme_id']} already being processed, skip"
                    )
            logger.info(
                f"dispatched {dispatched} tasks, {len(videos) - dispatched} skipped"
            )
        except Exception as e:
            logger.error(f"dispatch error: {e}")

    def cleanup_temp(self):
        """清理临时目录"""
        import shutil

        temp_dir = Path(__file__).parent.parent / "temp"
        for d in [temp_dir / "video", temp_dir / "audio"]:
            if d.exists():
                shutil.rmtree(d)
                logger.debug(f"cleaned temp dir: {d}")

    def run(self):
        """运行调度器"""
        # 设置定时任务
        schedule.every(config.scheduler.interval_minutes).minutes.do(
            self.dispatch_tasks
        )
        schedule.every(config.scheduler.interval_minutes).minutes.do(self.cleanup_temp)

        logger.info(
            f"scheduler started, interval={config.scheduler.interval_minutes}min, "
            f"batch_size={config.scheduler.batch_size}"
        )

        # 主循环
        while self.running:
            schedule.run_pending()
            time.sleep(1)

        # 清理资源
        self.shutdown()

    def shutdown(self):
        """关闭调度器"""
        logger.info("scheduler stopped")
        sys.exit(0)


def run_scheduler():
    """启动调度器的便捷函数"""
    scheduler = Scheduler()
    scheduler.run()


if __name__ == "__main__":
    run_scheduler()
