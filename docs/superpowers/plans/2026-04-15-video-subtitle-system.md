# 视频字幕自动化提取系统实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个离线优先的视频字幕自动化提取系统，支持抖音与B站视频源，以守护进程方式持续运行。

**Architecture:** 采用 Producer（轮询DB）+ Queue（Redis可靠队列）+ Worker（asyncio协程池）的分层架构。下载/转码/推理三阶段分层并发控制，pHash去重前置到ASR之前节省计算成本。

**Tech Stack:** Python 3.11+, asyncio, aiomysql, redis-py, faster-whisper, yt-dlp, imagehash, structlog

---

## 任务总览

| # | 任务 | 核心产出 |
|---|------|----------|
| 1 | 项目脚手架 | 目录结构、config.yaml、requirements.txt |
| 2 | 建表SQL | sql/init.sql |
| 3 | 配置加载器 | src/config.py |
| 4 | 结构化日志 | src/logger.py |
| 5 | 数据库连接池 | src/db.py |
| 6 | Redis客户端 | src/redis_client.py |
| 7 | 游标管理 | src/cursor.py |
| 8 | 存储层 | src/storage.py |
| 9 | 视频下载器 | src/downloader.py |
| 10 | 音频提取器 | src/audio_extractor.py |
| 11 | pHash指纹 | src/fingerprint.py |
| 12 | ASR引擎 | src/asr_engine.py |
| 13 | Producer | src/producer.py |
| 14 | Worker | src/worker.py |
| 15 | 守护进程入口 | main.py |
| 16 | 冒烟测试 | tests/test_integration.py |

---

## Task 1: 项目脚手架

**Files:**
- Create: `video_subtitle_system/config.yaml`
- Create: `video_subtitle_system/requirements.txt`
- Create: `video_subtitle_system/sql/.gitkeep`
- Create: `video_subtitle_system/logs/.gitkeep`
- Create: `video_subtitle_system/src/__init__.py`
- Create: `video_subtitle_system/tests/__init__.py`

- [ ] **Step 1: 创建目录结构**

```bash
mkdir -p video_subtitle_system/{src,sql,logs,tests}
touch video_subtitle_system/src/__init__.py
touch video_subtitle_system/tests/__init__.py
touch video_subtitle_system/sql/.gitkeep
touch video_subtitle_system/logs/.gitkeep
```

- [ ] **Step 2: 创建 config.yaml**

```yaml
database:
  host: "10.17.4.106"
  port: 3306
  user: "root"
  password: "123456"
  database: "ruoyi-vue-pro"

redis:
  host: "127.0.0.1"
  port: 6379

asr:
  model_size: "small"

concurrency:
  download: 6
  transcode: 4
  asr: 2

poll:
  interval_seconds: 5
  batch_size: 100

app:
  backpressure_threshold: 1000
  max_retries: 3
```

- [ ] **Step 3: 创建 requirements.txt**

```
faster-whisper>=1.0.0
yt-dlp>=2024.0.0
imagehash>=4.3.0
Pillow>=10.0.0
opencv-python>=4.8.0
httpx>=0.27.0
aiomysql>=0.2.0
redis>=5.0.0
structlog>=24.0.0
PyYAML>=6.0.0
pytest>=7.0.0
pytest-asyncio>=0.21.0
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: scaffold project structure"
```

---

## Task 2: 建表SQL

**Files:**
- Create: `video_subtitle_system/sql/init.sql`

- [ ] **Step 1: 编写建表SQL**

```sql
-- 视频指纹表（pHash 去重）
CREATE TABLE IF NOT EXISTS dy_fingerprint (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    p_hash VARCHAR(64) NOT NULL COMMENT '感知哈希值',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_p_hash (p_hash),
    INDEX idx_video_platform (video_id, platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 字幕分段表
CREATE TABLE IF NOT EXISTS dy_subtitle_segment (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    start_time FLOAT NOT NULL COMMENT '开始时间（秒）',
    end_time FLOAT NOT NULL COMMENT '结束时间（秒）',
    text TEXT NOT NULL COMMENT '字幕文本',
    confidence FLOAT DEFAULT NULL COMMENT '置信度(0-1)',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_video_start (video_id, platform, start_time),
    INDEX idx_video_id (video_id),
    INDEX idx_video_start (video_id, start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 任务状态表（方案B：不触碰原表）
CREATE TABLE IF NOT EXISTS task_status (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL,
    platform ENUM('bilibili', 'douyin') NOT NULL,
    status ENUM('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED') NOT NULL DEFAULT 'PENDING',
    retry_count TINYINT UNSIGNED DEFAULT 0,
    error_msg TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_video_platform (video_id, platform),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 游标维护表
CREATE TABLE IF NOT EXISTS sync_cursor (
    id INT PRIMARY KEY DEFAULT 1,
    bilibili_last_id BIGINT UNSIGNED DEFAULT 0,
    douyin_last_id BIGINT UNSIGNED DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 2: Commit**

```bash
git add sql/init.sql && git commit -m "feat: add init.sql with table definitions"
```

---

## Task 3: 配置加载器

**Files:**
- Create: `video_subtitle_system/src/config.py`
- Create: `video_subtitle_system/tests/test_config.py`

- [ ] **Step 1: 编写配置加载器**

```python
"""配置加载模块"""
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import yaml


@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class RedisConfig:
    host: str
    port: int


@dataclass
class ASRConfig:
    model_size: str = "small"


@dataclass
class ConcurrencyConfig:
    download: int = 6
    transcode: int = 4
    asr: int = 2


@dataclass
class PollConfig:
    interval_seconds: int = 5
    batch_size: int = 100


@dataclass
class AppConfig:
    backpressure_threshold: int = 1000
    max_retries: int = 3


@dataclass
class Config:
    database: DatabaseConfig
    redis: RedisConfig
    asr: ASRConfig
    concurrency: ConcurrencyConfig
    poll: PollConfig
    app: AppConfig


def load_config(config_path: Optional[str] = None) -> Config:
    """加载配置文件"""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config.yaml"

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return Config(
        database=DatabaseConfig(**raw["database"]),
        redis=RedisConfig(**raw["redis"]),
        asr=ASRConfig(**raw.get("asr", {})),
        concurrency=ConcurrencyConfig(**raw.get("concurrency", {})),
        poll=PollConfig(**raw.get("poll", {})),
        app=AppConfig(**raw.get("app", {})),
    )
```

- [ ] **Step 2: 编写测试**

```python
import pytest
from video_subtitle_system.src.config import load_config, Config

def test_load_config():
    config = load_config()
    assert isinstance(config, Config)
    assert config.database.host == "10.17.4.106"
    assert config.redis.port == 6379
```

- [ ] **Step 3: Commit**

```bash
git add src/config.py tests/test_config.py && git commit -m "feat: add config loader"
```

---

## Task 4: 结构化日志

**Files:**
- Create: `video_subtitle_system/src/logger.py`
- Create: `video_subtitle_system/tests/test_logger.py`

- [ ] **Step 1: 编写日志模块**

```python
"""结构化日志模块"""
import uuid
import structlog
from contextvars import ContextVar
from typing import Optional

trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)


def get_trace_id() -> str:
    tid = trace_id_var.get()
    if tid is None:
        tid = str(uuid.uuid4())[:12]
        trace_id_var.set(tid)
    return tid


def set_trace_id(tid: str):
    trace_id_var.set(tid)


def configure_logging(log_level: str = "INFO"):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog.stdlib, log_level.upper(), structlog.stdlib.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str = __name__):
    return structlog.get_logger(name).bind(trace_id=get_trace_id())
```

- [ ] **Step 2: Commit**

```bash
git add src/logger.py && git commit -m "feat: add structured logger with trace_id"
```

---

## Task 5: 数据库连接池

**Files:**
- Create: `video_subtitle_system/src/db.py`
- Create: `video_subtitle_system/tests/test_db.py`

- [ ] **Step 1: 编写数据库模块**

```python
"""MySQL 数据库连接池"""
import aiomysql
from typing import Any, Optional, List, Dict
from contextlib import asynccontextmanager

from .config import DatabaseConfig
from .logger import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            db=self.config.database,
            autocommit=False,
            minsize=2,
            maxsize=10,
            charset="utf8mb4",
        )
        logger.info("db_connected", host=self.config.host, db=self.config.database)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("db_closed")

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    yield conn, cur
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

    async def execute(self, sql: str, args: tuple = ()) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                await conn.commit()
                return cur.rowcount

    async def fetch_one(self, sql: str, args: tuple = ()) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchone()

    async def fetch_all(self, sql: str, args: tuple = ()) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchall()

    async def executemany(self, sql: str, args: List[tuple]):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, args)
                await conn.commit()
                return cur.rowcount
```

- [ ] **Step 2: Commit**

```bash
git add src/db.py && git commit -m "feat: add MySQL connection pool"
```

---

## Task 6: Redis客户端

**Files:**
- Create: `video_subtitle_system/src/redis_client.py`

- [ ] **Step 1: 编写Redis模块**

```python
"""Redis 客户端封装"""
import json
from typing import Optional, Any
import redis.asyncio as redis

from .config import RedisConfig
from .logger import get_logger

logger = get_logger(__name__)


class RedisClient:
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client: Optional[redis.Redis] = None

    async def connect(self):
        self.client = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            decode_responses=True,
        )
        await self.client.ping()
        logger.info("redis_connected", host=self.config.host, port=self.config.port)

    async def close(self):
        if self.client:
            await self.client.aclose()
            logger.info("redis_closed")

    def _dumps(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    def _loads(self, value: str) -> Any:
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    async def lpush(self, key: str, value: Any) -> int:
        return await self.client.lpush(key, self._dumps(value))

    async def rpush(self, key: str, value: Any) -> int:
        return await self.client.rpush(key, self._dumps(value))

    async def lrem(self, key: str, count: int, value: Any) -> int:
        return await self.client.lrem(key, count, self._dumps(value))

    async def brpoplpush(self, source: str, destination: str, timeout: int = 5) -> Optional[Any]:
        result = await self.client.brpoplpush(source, destination, timeout=timeout)
        return self._loads(result)

    async def rpoplpush(self, source: str, destination: str) -> Optional[Any]:
        result = await self.client.rpoplpush(source, destination)
        return self._loads(result)

    async def llen(self, key: str) -> int:
        return await self.client.llen(key)
```

- [ ] **Step 2: Commit**

```bash
git add src/redis_client.py && git commit -m "feat: add Redis client with reliable queue support"
```

---

## Task 7: 游标管理

**Files:**
- Create: `video_subtitle_system/src/cursor.py`

- [ ] **Step 1: 编写游标模块**

```python
"""双游标管理（bilibili / douyin 分离）"""
from dataclasses import dataclass

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class Cursor:
    bilibili_last_id: int
    douyin_last_id: int


class CursorManager:
    def __init__(self, db: Database):
        self.db = db

    async def get_cursor(self) -> Cursor:
        row = await self.db.fetch_one("SELECT bilibili_last_id, douyin_last_id FROM sync_cursor WHERE id = 1")
        if row is None:
            await self.db.execute(
                "INSERT INTO sync_cursor (id, bilibili_last_id, douyin_last_id) VALUES (1, 0, 0)"
            )
            return Cursor(bilibili_last_id=0, douyin_last_id=0)
        return Cursor(
            bilibili_last_id=row["bilibili_last_id"],
            douyin_last_id=row["douyin_last_id"],
        )

    async def update_bilibili_cursor(self, last_id: int):
        await self.db.execute(
            "UPDATE sync_cursor SET bilibili_last_id = %s WHERE id = 1",
            (last_id,)
        )
        logger.debug("cursor_updated", platform="bilibili", last_id=last_id)

    async def update_douyin_cursor(self, last_id: int):
        await self.db.execute(
            "UPDATE sync_cursor SET douyin_last_id = %s WHERE id = 1",
            (last_id,)
        )
        logger.debug("cursor_updated", platform="douyin", last_id=last_id)
```

- [ ] **Step 2: Commit**

```bash
git add src/cursor.py && git commit -m "feat: add dual cursor manager"
```

---

## Task 8: 存储层

**Files:**
- Create: `video_subtitle_system/src/storage.py`

- [ ] **Step 1: 编写存储模块**

```python
"""存储层：幂等写入 + PROCESSING 抢占锁"""
from typing import List, Dict, Any

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


class SubtitleStorage:
    def __init__(self, db: Database):
        self.db = db

    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """原子抢占 PROCESSING 锁。返回 True 表示成功，False 表示已是 SUCCESS。"""
        result = await self.db.execute(
            """INSERT INTO task_status (video_id, platform, status, retry_count)
               VALUES (%s, %s, 'PROCESSING', 0)
               ON DUPLICATE KEY UPDATE
                   status = IF(status = 'SUCCESS', 'SUCCESS', 'PROCESSING'),
                   retry_count = IF(status = 'SUCCESS', retry_count, 0)""",
            (video_id, platform)
        )
        return result > 0

    async def save(self, video_id: str, platform: str, segments: List[Dict[str, Any]], p_hash: str):
        async with self.db.transaction() as (conn, cur):
            await cur.execute(
                "INSERT IGNORE INTO dy_fingerprint (video_id, platform, p_hash) VALUES (%s, %s, %s)",
                (video_id, platform, p_hash)
            )
            if segments:
                subtitle_records = [
                    (video_id, platform, seg["start_time"], seg["end_time"], seg["text"], seg.get("confidence"))
                    for seg in segments
                ]
                await cur.executemany(
                    """INSERT IGNORE INTO dy_subtitle_segment
                       (video_id, platform, start_time, end_time, text, confidence)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    subtitle_records
                )
            await cur.execute(
                """INSERT INTO task_status (video_id, platform, status, error_msg)
                   VALUES (%s, %s, 'SUCCESS', NULL)
                   ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
                (video_id, platform)
            )
        logger.info("subtitle_saved", video_id=video_id, platform=platform, segments=len(segments))

    async def mark_failed(self, video_id: str, platform: str, error_msg: str):
        await self.db.execute(
            "UPDATE task_status SET status = 'FAILED', error_msg = %s WHERE video_id = %s AND platform = %s",
            (error_msg, video_id, platform)
        )
        logger.error("task_marked_failed", video_id=video_id, platform=platform, error=error_msg)

    async def reset_processing_to_pending(self):
        await self.db.execute(
            "UPDATE task_status SET status = 'PENDING' WHERE status = 'PROCESSING'"
        )
        logger.info("recovered_processing_tasks")
```

- [ ] **Step 2: Commit**

```bash
git add src/storage.py && git commit -m "feat: add storage with idempotent write and acquire lock"
```

---

## Task 9: 视频下载器

**Files:**
- Create: `video_subtitle_system/src/downloader.py`

- [ ] **Step 1: 编写下载器**

```python
"""视频下载器（Douyin直链 / B站DASH）"""
import asyncio
from pathlib import Path
from typing import Optional
from uuid import uuid4

import httpx

from .logger import get_logger

logger = get_logger(__name__)


class VideoDownloader:
    def __init__(self):
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    async def close(self):
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def download(self, url: str, platform: str) -> Path:
        tmp_path = Path(f"/tmp/{uuid4()}.mp4")
        try:
            if platform == "douyin":
                await self._stream_to_file(url, str(tmp_path))
            else:
                await self._dash_download(url, str(tmp_path))
            logger.info("video_downloaded", platform=platform, path=str(tmp_path))
            return tmp_path
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def _stream_to_file(self, url: str, tmp_path: str):
        client = await self._get_client()
        async with client.stream("GET", url, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    f.write(chunk)

    async def _dash_download(self, url: str, tmp_path: str):
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "-o", tmp_path,
            "--merge-output-format", "mp4",
            "--socket-timeout", "60",
            url,
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            error_msg = stderr.decode(errors="ignore")[-200:]
            raise RuntimeError(f"yt-dlp failed: {error_msg}")
```

- [ ] **Step 2: Commit**

```bash
git add src/downloader.py && git commit -m "feat: add video downloader with streaming support"
```

---

## Task 10: 音频提取器

**Files:**
- Create: `video_subtitle_system/src/audio_extractor.py`

- [ ] **Step 1: 编写音频提取器**

```python
"""FFmpeg 音频提取器"""
import asyncio
from pathlib import Path
from uuid import uuid4

from .logger import get_logger

logger = get_logger(__name__)


class AudioExtractor:
    async def extract(self, video_path: Path) -> bytes:
        wav_path = Path(f"/tmp/{uuid4()}.wav")
        video_path = Path(video_path)

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", str(video_path),
                "-ar", "16000",
                "-ac", "1",
                "-f", "wav",
                "-y",
                str(wav_path),
                stderr=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
            )
            _, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode(errors="ignore")[-200:]
                raise RuntimeError(f"FFmpeg failed: {error_msg}")

            audio_data = wav_path.read_bytes()
            logger.info("audio_extracted", video_path=str(video_path))
            return audio_data

        finally:
            video_path.unlink(missing_ok=True)
            wav_path.unlink(missing_ok=True)
```

- [ ] **Step 2: Commit**

```bash
git add src/audio_extractor.py && git commit -m "feat: add audio extractor with FFmpeg"
```

---

## Task 11: pHash指纹

**Files:**
- Create: `video_subtitle_system/src/fingerprint.py`

- [ ] **Step 1: 编写指纹模块**

```python
"""视频指纹（多帧 pHash 合并）"""
from pathlib import Path

import cv2
import imagehash
from PIL import Image

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


class VideoFingerprint:
    def __init__(self, db: Database):
        self.db = db

    async def compute(self, video_path: Path) -> str:
        video_path = Path(video_path)
        cap = cv2.VideoCapture(str(video_path))

        try:
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            timestamps = [1, 3, 5]
            hashes = []

            for t in timestamps:
                frame_pos = int(t * fps)
                if frame_pos < total_frames:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                    ret, frame = cap.read()
                    if ret:
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        img = Image.fromarray(frame_rgb)
                        hashes.append(imagehash.phash(img))

            if not hashes:
                raise ValueError(f"No frames extracted from {video_path}")

            merged = hashes[0]
            for h in hashes[1:]:
                merged = merged & h
            result = str(merged)
            logger.info("phash_computed", video_path=str(video_path), phash=result)
            return result

        finally:
            cap.release()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    async def is_duplicate(self, p_hash: str) -> bool:
        fingerprints = await self.db.fetch_all(
            "SELECT p_hash FROM dy_fingerprint ORDER BY id DESC LIMIT 10000"
        )
        for fp in fingerprints:
            if self.hamming_distance(p_hash, fp["p_hash"]) <= 5:
                logger.info("duplicate_detected", p_hash=p_hash, match=fp["p_hash"])
                return True
        return False
```

- [ ] **Step 2: Commit**

```bash
git add src/fingerprint.py && git commit -m "feat: add multi-frame pHash fingerprint"
```

---

## Task 12: ASR引擎

**Files:**
- Create: `video_subtitle_system/src/asr_engine.py`

- [ ] **Step 1: 编写ASR模块**

```python
"""Faster-Whisper ASR 引擎"""
from pathlib import Path
from typing import List, Dict, Any
from uuid import uuid4

from faster_whisper import WhisperModel

from .logger import get_logger

logger = get_logger(__name__)


class ASREngine:
    def __init__(self, model_size: str = "small"):
        self.model_size = model_size
        self.model: WhisperModel = None

    def load_model(self):
        self.model = WhisperModel(
            self.model_size,
            device="auto",
            compute_type="float16" if self.model_size != "base" else "int8",
        )
        logger.info("asr_model_loaded", model_size=self.model_size)

    async def recognize(self, audio_data: bytes) -> List[Dict[str, Any]]:
        if self.model is None:
            self.load_model()

        tmp_wav = Path(f"/tmp/{uuid4()}.wav")
        tmp_wav.write_bytes(audio_data)

        try:
            segments, _ = self.model.transcribe(
                str(tmp_wav),
                language=None,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )

            result = []
            for s in segments:
                result.append({
                    "start_time": s.start,
                    "end_time": s.end,
                    "text": s.text,
                    "confidence": getattr(s, "probability", 1.0),
                })

            logger.info("asr_completed", segments=len(result))
            return result

        finally:
            tmp_wav.unlink(missing_ok=True)
```

- [ ] **Step 2: Commit**

```bash
git add src/asr_engine.py && git commit -m "feat: add Faster-Whisper ASR engine"
```

---

## Task 13: Producer

**Files:**
- Create: `video_subtitle_system/src/producer.py`

- [ ] **Step 1: 编写Producer**

```python
"""Producer：轮询数据库，拉取任务，推入Redis队列"""
import json

from .db import Database
from .redis_client import RedisClient
from .cursor import CursorManager
from .config import PollConfig
from .logger import get_logger

logger = get_logger(__name__)


class Producer:
    def __init__(
        self,
        db: Database,
        redis: RedisClient,
        cursor_mgr: CursorManager,
        config: PollConfig,
        backpressure_threshold: int = 1000,
    ):
        self.db = db
        self.redis = redis
        self.cursor_mgr = cursor_mgr
        self.config = config
        self.backpressure_threshold = backpressure_threshold
        self._running = False

    async def start(self):
        import asyncio
        self._running = True
        logger.info("producer_started", interval=self.config.interval_seconds)
        while self._running:
            try:
                await self.poll()
            except Exception as e:
                logger.error("producer_poll_error", error=str(e))
            await asyncio.sleep(self.config.interval_seconds)

    async def stop(self):
        self._running = False
        logger.info("producer_stopped")

    async def poll(self):
        cursor = await self.cursor_mgr.get_cursor()

        queue_len = await self.redis.llen("task_queue")
        if queue_len > self.backpressure_threshold:
            logger.warning("backpressure_active", queue_len=queue_len)
            return

        bilibili_tasks = await self.db.fetch_all(
            """SELECT id as video_id, 'bilibili' as platform, video_url as url
               FROM bilibili_video
               WHERE id > %s
               ORDER BY id
               LIMIT %s""",
            (cursor.bilibili_last_id, self.config.batch_size // 2)
        )

        douyin_tasks = await self.db.fetch_all(
            """SELECT id as video_id, 'douyin' as platform, video_download_url as url
               FROM douyin_aweme
               WHERE id > %s
               ORDER BY id
               LIMIT %s""",
            (cursor.douyin_last_id, self.config.batch_size // 2)
        )

        total = 0

        for task in bilibili_tasks:
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.cursor_mgr.update_bilibili_cursor(task["video_id"])
            total += 1

        for task in douyin_tasks:
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.cursor_mgr.update_douyin_cursor(task["video_id"])
            total += 1

        if total > 0:
            logger.info("tasks_pushed", count=total, bilibili=len(bilibili_tasks), douyin=len(douyin_tasks))
```

- [ ] **Step 2: Commit**

```bash
git add src/producer.py && git commit -m "feat: add producer with dual cursor and backpressure"
```

---

## Task 14: Worker

**Files:**
- Create: `video_subtitle_system/src/worker.py`

- [ ] **Step 1: 编写Worker**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add src/worker.py && git commit -m "feat: add worker with reliable queue and retry"
```

---

## Task 15: 守护进程入口

**Files:**
- Create: `video_subtitle_system/main.py`

- [ ] **Step 1: 编写主程序**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add main.py && git commit -m "feat: add daemon main entry with graceful shutdown"
```

---

## Task 16: 冒烟测试

**Files:**
- Create: `video_subtitle_system/tests/test_integration.py`

- [ ] **Step 1: 编写冒烟测试**

```python
"""端到端冒烟测试"""
import pytest
import asyncio
from pathlib import Path
from unittest.mock import MagicMock

from src.fingerprint import VideoFingerprint


def test_fingerprint_hamming_distance():
    hash1 = "0123456789abcdef"
    hash2 = "0123456789abcdee"

    distance = VideoFingerprint.hamming_distance(hash1, hash2)
    assert distance == 1

    distance2 = VideoFingerprint.hamming_distance(hash1, hash1)
    assert distance2 == 0


def test_storage_try_acquire():
    from src.storage import SubtitleStorage
    db = MagicMock()
    db.execute = asyncio.coroutine(lambda *a, **kw: 1)

    storage = SubtitleStorage(db)
    result = asyncio.get_event_loop().run_until_complete(
        storage.try_acquire("BV123", "bilibili")
    )
    assert result is True
```

- [ ] **Step 2: Commit**

```bash
git add tests/ && git commit -m "test: add smoke tests"
```

---

## 计划自检

1. **Spec 覆盖检查**：
   - ✅ 目录结构、配置、SQL：Task 1-2
   - ✅ 日志/DB/Redis/游标：Task 3-7
   - ✅ 存储层（幂等+抢占锁）：Task 8
   - ✅ 下载器（流式+yt-dlp）：Task 9
   - ✅ 音频提取（FFmpeg+returncode）：Task 10
   - ✅ pHash（多帧合并）：Task 11
   - ✅ ASR（置信度）：Task 12
   - ✅ Producer（双游标+背压）：Task 13
   - ✅ Worker（可靠队列+重试）：Task 14
   - ✅ 主程序（崩溃恢复+优雅退出）：Task 15

2. **占位符扫描**：无 TBD/TODO

3. **类型一致性**：
   - `cursor_mgr.update_bilibili_cursor(task["video_id"])` — video_id 类型一致
   - `storage.try_acquire(video_id, platform)` — 签名一致
   - `asr_engine.recognize(audio_data)` — bytes 输入一致
