"""Celery Worker模块"""

import asyncio
import shutil
from celery import Celery
from loguru import logger
from ext.config import get_settings

settings = get_settings()

redis_config = settings.redis
broker_url = f"redis://{redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}/0"

celery_app = Celery(
    "ext.worker",
    broker=broker_url,
    backend=broker_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
)


@celery_app.task(bind=True, name="ext.worker.process_video")
def process_video(self, aweme_id: str, video_url: str, title: str = None):
    """处理单个视频"""
    logger.info(f"Processing video: aweme_id={aweme_id}")
    return asyncio.run(_process_video_async(aweme_id, video_url, title))


async def _process_video_async(
    aweme_id: str, video_url: str, title: str = None
) -> dict:
    """异步处理视频"""
    from ext.fingerprint.phash import VideoFingerprint
    from ext.asr.faster_whisper import FasterWhisperASR
    from ext.storage import Storage

    storage = None
    audio_path = None
    temp_dir = None

    try:
        logger.info(f"[{aweme_id}] 1. Computing pHash fingerprint...")
        fp = VideoFingerprint()
        phash = await fp.compute_phash(video_url)
        logger.info(f"[{aweme_id}] 1. pHash computed: {phash}")

        # 2. 存储指纹
        logger.info(f"[{aweme_id}] 2. Saving fingerprint to database...")
        storage = Storage()
        await storage.connect()
        await storage.save_fingerprint(aweme_id, video_url, phash)
        logger.info(f"[{aweme_id}] 2. Fingerprint saved")

        # 3. 检查是否重复
        logger.info(f"[{aweme_id}] 3. Checking for duplicates...")
        similar = await storage.find_similar_fingerprint(
            phash, threshold=0.9, exclude_aweme_id=aweme_id
        )
        if similar:
            logger.warning(
                f"[{aweme_id}] 3. Duplicate found, similar_to={similar['aweme_id']}, similarity={similar['similarity']}"
            )

            similar_subtitle = await storage.get_subtitle_by_aweme_id(
                similar["aweme_id"]
            )
            if similar_subtitle:
                await storage.save_subtitle(
                    aweme_id=aweme_id,
                    video_url=video_url,
                    fingerprint=phash,
                    language=similar_subtitle["language"],
                    duration=similar_subtitle["duration"],
                    subtitle_text=similar_subtitle["subtitle_text"],
                    segments=similar_subtitle["segments"],
                    confidence=similar_subtitle["confidence"],
                    status=3,
                    error_msg=f"Duplicate of {similar['aweme_id']}, similarity={similar['similarity']}",
                )
                logger.info(
                    f"[{aweme_id}] 3. Copied subtitle from {similar['aweme_id']}"
                )

            return {
                "aweme_id": aweme_id,
                "status": "duplicate",
                "copied_from": similar["aweme_id"],
                "similarity": similar["similarity"],
            }

        # 4. 下载视频
        logger.info(f"[{aweme_id}] 4. Downloading video...")
        import tempfile
        import os
        import httpx

        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, f"{aweme_id}.mp4")

        async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
            # 先获取最��URL
            response = await client.get(video_url)
            final_url = str(response.url)
            logger.info(f"[{aweme_id}] 4a. Final URL: {final_url[:80]}...")

            # 检查是否是douyin.com域名，需要处理重定向
            if "douyin.com" in final_url:
                logger.info(
                    f"[{aweme_id}] 4b. Detected douyin.com URL, following redirect..."
                )
                async with client.stream(
                    "GET", final_url, follow_redirects=True
                ) as resp:
                    resp.raise_for_status()
                    with open(video_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            f.write(chunk)
            else:
                async with client.stream("GET", final_url) as resp:
                    resp.raise_for_status()
                    with open(video_path, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            f.write(chunk)

        file_size = os.path.getsize(video_path)
        logger.info(f"[{aweme_id}] 4c. Video downloaded: {file_size} bytes")

        # 5. 提取音频
        logger.info(f"[{aweme_id}] 5. Extracting audio...")
        asr = FasterWhisperASR()
        audio_path = await asr.extract_audio(video_path)
        logger.info(f"[{aweme_id}] 5a. Audio extracted: {audio_path}")

        # 6. ASR识别
        logger.info(f"[{aweme_id}] 6. Running ASR识别...")
        result = await asr.transcribe(audio_path)
        logger.info(
            f"[{aweme_id}] 6a. ASR completed: {len(result.segments)} segments, duration={result.duration:.1f}s"
        )

        # 7. 存储字幕
        logger.info(f"[{aweme_id}] 7. Saving subtitle...")
        subtitle_text = "\n".join(seg.text for seg in result.segments)
        segments_data = [
            {"start": s.start, "end": s.end, "text": s.text, "confidence": s.confidence}
            for s in result.segments
        ]

        await storage.save_subtitle(
            aweme_id=aweme_id,
            video_url=video_url,
            fingerprint=phash,
            language=result.language,
            duration=result.duration,
            subtitle_text=subtitle_text,
            segments=segments_data,
            confidence=result.confidence,
            status=1,
        )
        logger.info(f"[{aweme_id}] 7a. Subtitle saved successfully!")

        logger.info(
            f"[{aweme_id}] SUCCESS: Completed with {len(result.segments)} segments"
        )

        return {
            "aweme_id": aweme_id,
            "status": "completed",
            "segments": len(result.segments),
            "duration": result.duration,
        }

    except Exception as e:
        logger.error(f"[{aweme_id}] ERROR: {type(e).__name__}: {e}")

        if storage:
            try:
                await storage.update_status(
                    aweme_id, status=2, error_msg=f"{type(e).__name__}: {str(e)[:200]}"
                )
                logger.info(f"[{aweme_id}] ERROR status saved to database")
            except Exception as update_err:
                logger.error(
                    f"[{aweme_id}] Failed to update error status: {update_err}"
                )

    finally:
        # 清理资源
        if storage:
            try:
                await storage.close()
            except:
                pass

        if audio_path and os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except:
                pass

        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except:
                pass

        logger.info(f"[{aweme_id}] Cleanup completed")
