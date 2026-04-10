"""
Celery Worker模块

处理视频的完整流程:
1. 幂等检查 - 跳过已处理的视频
2. 计算pHash - 视频指纹
3. 保存指纹 - 先保存，确保不丢失
4. 查重 - 检查相似视频
5. 下载视频 - 获取原始文件
6. 提取音频 - 转为16kHz单声道WAV
7. ASR转写 - Faster-Whisper
8. 存储结果 - 保存到数据库
"""

import asyncio
from celery import Celery
from loguru import logger
import ffmpeg
import httpx
from pathlib import Path

from .config import config
from .db import storage
from .fingerprint.phash import VideoFingerprint, find_similar
from .asr.whisper import WhisperASR

celery_app = Celery(
    "ext",
    broker=f"redis://{config.redis.host}:{config.redis.port}/0",
    backend=f"redis://{config.redis.host}:{config.redis.port}/0",
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=True,
)


@celery_app.task(name="ext.worker.process_video", bind=True)
def process_video(self, aweme_id: str, video_url: str, title: str = None):
    """
    处理单个视频的Celery任务

    Args:
        aweme_id: 抖音视频ID
        video_url: 视频下载URL
        title: 视频标题

    Returns:
        处理结果字典
    """
    return _process_video(aweme_id, video_url, title)


def _process_video(aweme_id: str, video_url: str, title: str = None):
    """
    同步处理视频

    处理流程:
    1. 幂等检查 - 已处理则跳过
    2. 计算pHash指纹
    3. 保存指纹到数据库
    4. 查询相似指纹进行去重
    5. 下载并提取音频
    6. ASR转写
    7. 存储结果
    """
    temp_dir = Path(__file__).parent.parent / "temp"
    temp_dir.mkdir(exist_ok=True)

    try:
        existing = storage.get_subtitle_by_aweme_id_sync(aweme_id)
        if existing and existing["status"] in (1, 3):
            logger.info(f"aweme_id={aweme_id} already processed or duplicate, skip")
            return {"aweme_id": aweme_id, "status": "skipped"}

        fp = VideoFingerprint()
        phash = fp.compute_phash_sync(video_url)
        if not phash:
            raise Exception("Failed to compute video phash - not a valid video")

        # 先保存指纹，确保即使后续崩溃也能记录
        storage.save_fingerprint_sync(aweme_id, video_url, phash)

        # 查询相似指纹（排除自己）
        candidates = storage.get_fingerprint_prefix_sync(
            phash[:8], exclude_aweme_id=aweme_id
        )
        similar_id, similarity = find_similar(phash, candidates)

        if similar_id:
            logger.info(f"aweme_id={aweme_id} duplicate with {similar_id}")
            storage.copy_subtitle_sync(similar_id, aweme_id, video_url, phash)
            return {
                "aweme_id": aweme_id,
                "status": "duplicate",
                "copied_from": similar_id,
            }

        # 下载并提取音频
        file_path = _download_video(video_url, temp_dir)
        audio_path = _extract_audio(file_path, temp_dir)

        # ASR转写
        result = WhisperASR.transcribe(
            audio_path,
            model_size=config.faster_whisper.model_size,
            device=config.faster_whisper.device,
        )

        # 保存字幕结果
        storage.save_subtitle_sync(
            aweme_id=aweme_id,
            video_url=video_url,
            fingerprint=phash,
            subtitle_text=result["text"],
            segments=result["segments"],
            duration=result["duration"],
            confidence=result["confidence"],
            status=1,
        )

        logger.info(
            f"aweme_id={aweme_id} completed, duration={result['duration']:.1f}s"
        )
        return {
            "aweme_id": aweme_id,
            "status": "completed",
            "duration": result["duration"],
        }

    except Exception as e:
        logger.error(f"aweme_id={aweme_id} failed: {e}")
        storage.save_subtitle_sync(
            aweme_id=aweme_id,
            video_url=video_url,
            fingerprint="",
            subtitle_text="",
            segments=[],
            duration=0,
            confidence=0,
            status=2,
            error_msg=str(e),
        )
        return {"aweme_id": aweme_id, "status": "failed", "error": str(e)}

    finally:
        _cleanup_temp(temp_dir)


def _download_video(url: str, temp_dir: Path) -> str:
    """
    下载视频到临时目录

    Args:
        url: 视频URL
        temp_dir: 临时目录

    Returns:
        视频文件路径
    """
    file_path = temp_dir / "video" / f"{hash(url)}.mp4"
    file_path.parent.mkdir(exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.douyin.com/",
    }

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)

    return str(file_path)


def _extract_audio(file_path: str, temp_dir: Path) -> str:
    """
    从视频/音频文件提取音频

    Args:
        file_path: 源文件路径
        temp_dir: 临时目录

    Returns:
        WAV音频文件路径
    """
    audio_path = temp_dir / "audio" / f"{hash(file_path)}.wav"
    audio_path.parent.mkdir(exist_ok=True)

    ffmpeg.input(file_path).output(
        str(audio_path),
        format="wav",
        acodec="pcm_s16le",
        ar=16000,
        ac=1,
    ).run(quiet=True, overwrite_output=True)

    return str(audio_path)


def _cleanup_temp(temp_dir: Path):
    """清理临时目录"""
    import shutil

    for d in [temp_dir / "video", temp_dir / "audio"]:
        if d.exists():
            shutil.rmtree(d)
    temp_dir.mkdir(exist_ok=True)
