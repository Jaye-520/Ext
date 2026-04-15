"""Worker：消费队列，执行处理流程，失败重试"""
import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor

from .db import Database
from .redis_client import RedisClient
from .storage import SubtitleStorage
from .downloader import VideoDownloader
from .audio_extractor import AudioExtractor
from .fingerprint import VideoFingerprint
from .asr_engine import ASREngine
from .config import ConcurrencyConfig
from .logger import get_logger, set_trace_id

logger = get_logger(__name__)


class Worker:
    def __init__(
        self,
        db: Database,
        redis: RedisClient,
        storage: SubtitleStorage,
        downloader: VideoDownloader,
        audio_extractor: AudioExtractor,
        fingerprint: VideoFingerprint,
        asr_engine: ASREngine,
        concurrency: ConcurrencyConfig,
        max_retries: int = 3,
    ):
        self.db = db
        self.redis = redis
        self.storage = storage
        self.downloader = downloader
        self.audio_extractor = audio_extractor
        self.fingerprint = fingerprint
        self.asr_engine = asr_engine
        self.concurrency = concurrency
        self.max_retries = max_retries

        self.download_sem = asyncio.Semaphore(concurrency.download)
        self.transcode_pool = ThreadPoolExecutor(max_workers=concurrency.transcode)
        self.asr_sem = asyncio.Semaphore(concurrency.asr)

    async def run(self):
        logger.info("worker_started")
        backoff = 1

        while True:
            try:
                task = await self.redis.brpoplpush("task_queue", "processing_queue", timeout=5)

                if task is None:
                    await asyncio.sleep(min(30, backoff))
                    backoff = min(backoff * 1.5, 30)
                    continue

                backoff = 1
                await self._process_task(task)

            except Exception as e:
                logger.error("worker_loop_error", error=str(e))

    async def _process_task(self, task: dict):
        video_id = task["video_id"]
        platform = task["platform"]
        url = task.get("url", "")
        retry_count = task.get("retry_count", 0)

        set_trace_id(f"{platform}-{video_id[:8]}")

        acquired = await self.storage.try_acquire(video_id, platform)
        if not acquired:
            logger.info("task_already_success", video_id=video_id)
            await self._ack_task(task)
            return

        try:
            t0 = time.monotonic()

            with self.download_sem:
                t_download = time.monotonic()
                video_path = await self.downloader.download(url, platform)
                download_time_ms = int((time.monotonic() - t_download) * 1000)

            p_hash = await self.fingerprint.compute(video_path)
            is_dup = await self.fingerprint.is_duplicate(p_hash)
            if is_dup:
                logger.info("duplicate_video_skipped", video_id=video_id)
                await self._ack_task(task)
                await self.storage.save(video_id, platform, [], p_hash)
                return

            t_transcode = time.monotonic()
            loop = asyncio.get_event_loop()
            audio_data = await loop.run_in_executor(
                self.transcode_pool,
                lambda: asyncio.run(self.audio_extractor.extract(video_path))
            )
            transcode_time_ms = int((time.monotonic() - t_transcode) * 1000)

            t_asr = time.monotonic()
            async with self.asr_sem:
                segments = await self.asr_engine.recognize(audio_data)
            asr_time_ms = int((time.monotonic() - t_asr) * 1000)

            await self.storage.save(video_id, platform, segments, p_hash)

            total_time_ms = int((time.monotonic() - t0) * 1000)
            logger.info("task_completed",
                video_id=video_id, platform=platform,
                download_time_ms=download_time_ms,
                transcode_time_ms=transcode_time_ms,
                asr_time_ms=asr_time_ms,
                total_time_ms=total_time_ms,
                is_duplicate=False, segments_count=len(segments))

            await self._ack_task(task)

        except Exception as e:
            logger.error("task_failed", video_id=video_id, platform=platform, error=str(e), retry=retry_count)
            await self._handle_failure(task, e)

    async def _ack_task(self, task: dict):
        await self.redis.lrem("processing_queue", 1, json.dumps(task))

    async def _handle_failure(self, task: dict, error: Exception):
        await self.redis.lrem("processing_queue", 1, json.dumps(task))
        retry_count = task.get("retry_count", 0) + 1

        if retry_count >= self.max_retries:
            await self.storage.mark_failed(task["video_id"], task["platform"], str(error))
        else:
            task["retry_count"] = retry_count
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.db.execute(
                "UPDATE task_status SET status = 'PENDING', retry_count = %s WHERE video_id = %s AND platform = %s",
                (retry_count, task["video_id"], task["platform"])
            )