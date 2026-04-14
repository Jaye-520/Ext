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
import hashlib
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
    2. 下载视频
    3. 计算pHash指纹
    4. 保存指纹到数据库
    5. 查询相似指纹进行去重
    6. 提取音频 + ASR转写
    7. 存储结果
    """
    temp_dir = Path(__file__).parent.parent / "temp"
    temp_dir.mkdir(exist_ok=True)

    file_path = None
    audio_path = None

    try:
        existing = storage.get_subtitle_by_aweme_id_sync(aweme_id)
        if existing and existing["status"] in (1, 3):
            logger.info(f"aweme_id={aweme_id} already processed or duplicate, skip")
            return {"aweme_id": aweme_id, "status": "skipped"}

        # 下载视频（只下载一次，指纹和ASR共用）
        file_path = _download_video(video_url, temp_dir)

        fp = VideoFingerprint()
        phash = fp.compute_phash_from_file(str(file_path))
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

        # 提取音频
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
        _cleanup_temp_files(file_path, audio_path)


def _download_video(url: str, temp_dir: Path) -> str:
    """
    下载视频到临时目录

    Args:
        url: 视频URL
        temp_dir: 临时目录

    Returns:
        视频文件路径

    Raises:
        Exception: 下载失败或HTTP错误
    """
    import os
    
    file_path = temp_dir / "video" / f"{hashlib.md5(url.encode()).hexdigest()[:12]}.mp4"
    temp_file = file_path.with_suffix('.tmp')
    
    # 确保目录存在且有写权限
    video_dir = temp_dir / "video"
    video_dir.mkdir(parents=True, exist_ok=True)
    
    if not video_dir.exists():
        raise Exception(f"Failed to create video directory: {video_dir}")
    
    if not os.access(video_dir, os.W_OK):
        raise Exception(f"Video directory is not writable: {video_dir}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.douyin.com/",
    }

    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            
            # 检查Content-Type，拒绝纯音频
            content_type = response.headers.get("content-type", "").lower()
            if content_type in {"audio/mpeg", "audio/mp3", "audio/m4a", "audio/wav", "audio/ogg"}:
                raise Exception(f"URL returns audio content ({content_type}), not video")
            
            # 检查内容大小
            if len(response.content) < 1024:
                raise Exception(f"Downloaded content too small ({len(response.content)} bytes), possibly an error page")
            
            # 原子写入：先写入临时文件，再重命名
            with open(temp_file, "wb") as f:
                f.write(response.content)
            
            # 重命名为最终文件名
            temp_file.rename(file_path)
    except Exception as e:
        # 清理临时文件（如果存在）
        try:
            if temp_file.exists():
                temp_file.unlink()
        except Exception:
            pass
        # 重新抛出异常，保留原始错误信息
        raise

    if not file_path.exists():
        raise Exception(f"File was not written to {file_path}")
    
    if file_path.stat().st_size < 1024:
        raise Exception(f"File size too small: {file_path.stat().st_size} bytes")

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
    audio_path = temp_dir / "audio" / f"{hashlib.md5(file_path.encode()).hexdigest()[:12]}.wav"
    audio_path.parent.mkdir(exist_ok=True)

    ffmpeg.input(file_path).output(
        str(audio_path),
        format="wav",
        acodec="pcm_s16le",
        ar=16000,
        ac=1,
    ).run(quiet=True, overwrite_output=True)

    return str(audio_path)


def _cleanup_temp_files(video_path: str = None, audio_path: str = None):
    """清理临时文件（只删除指定文件，不删除整个目录）"""
    import os

    if video_path and os.path.exists(video_path):
        try:
            os.unlink(video_path)
        except Exception:
            pass

    if audio_path and os.path.exists(audio_path):
        try:
            os.unlink(audio_path)
        except Exception:
            pass
