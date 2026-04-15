"""守护进程入口"""
import asyncio
import signal

from src.config import load_config
from src.db import Database
from src.redis_client import RedisClient
from src.cursor import CursorManager
from src.storage import SubtitleStorage
from src.downloader import VideoDownloader
from src.audio_extractor import AudioExtractor
from src.fingerprint import VideoFingerprint
from src.asr_engine import ASREngine
from src.producer import Producer
from src.worker import Worker
from src.logger import configure_logging, get_logger

logger = get_logger(__name__)


async def recover_crashed_tasks(db: Database, redis: RedisClient):
    await db.execute(
        "UPDATE task_status SET status = 'PENDING' WHERE status = 'PROCESSING'"
    )
    logger.info("recovered_processing_tasks")

    while True:
        task = await redis.rpoplpush("processing_queue", "task_queue")
        if task is None:
            break
        logger.info("recovered_task", task=task)


async def main():
    configure_logging()
    config = load_config()

    db = Database(config.database)
    redis = RedisClient(config.redis)
    cursor_mgr = CursorManager(db)
    storage = SubtitleStorage(db)
    downloader = VideoDownloader()
    audio_extractor = AudioExtractor()
    fingerprint = VideoFingerprint(db)
    asr_engine = ASREngine(model_size=config.asr.model_size)

    asr_engine.load_model()

    await db.connect()
    await redis.connect()

    await recover_crashed_tasks(db, redis)

    producer = Producer(
        db=db,
        redis=redis,
        cursor_mgr=cursor_mgr,
        config=config.poll,
        backpressure_threshold=config.app.backpressure_threshold,
    )

    worker = Worker(
        db=db,
        redis=redis,
        storage=storage,
        downloader=downloader,
        audio_extractor=audio_extractor,
        fingerprint=fingerprint,
        asr_engine=asr_engine,
        concurrency=config.concurrency,
        max_retries=config.app.max_retries,
    )

    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info("shutdown_signal", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig, lambda s=sig: handle_signal(s))

    producer_task = asyncio.create_task(producer.start())
    worker_task = asyncio.create_task(worker.run())

    logger.info("daemon_started")

    await stop_event.wait()

    await producer.stop()
    producer_task.cancel()
    worker_task.cancel()

    await downloader.close()
    await redis.close()
    await db.close()

    logger.info("daemon_stopped")


if __name__ == "__main__":
    asyncio.run(main())
