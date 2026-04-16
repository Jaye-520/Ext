# Project Standardization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make video_subtitle_system OSS-contribution friendly: ruff linting, type hints, storage.py refactor, pytest fixtures, GitHub Actions CI, and CONTRIBUTING.md.

**Architecture:** Phase 1 adds tooling (ruff, CI, CONTRIBUTING). Phase 2 adds type hints P0→P1→P2. Phase 3 refactors storage.py into repository interfaces. Phase 4 updates docs and verifies CI.

**Tech Stack:** ruff, pyright, pytest, pytest-asyncio, GitHub Actions, uv

---

## File Map

| File | Action |
|------|--------|
| `video_subtitle_system/ruff.toml` | Create |
| `video_subtitle_system/.github/workflows/ci.yml` | Create |
| `video_subtitle_system/.github/workflows/typecheck.yml` | Create |
| `video_subtitle_system/CONTRIBUTING.md` | Create |
| `video_subtitle_system/tests/conftest.py` | Create |
| `video_subtitle_system/tests/test_storage.py` | Create |
| `video_subtitle_system/tests/test_fingerprint.py` | Create |
| `video_subtitle_system/src/repos.py` | Create |
| `video_subtitle_system/src/storage.py` | Modify |
| `video_subtitle_system/pyproject.toml` | Modify (add ruff, pyright, pytest-cov) |
| `video_subtitle_system/CLAUDE.md` | Modify |

---

## Phase 1: Tooling + Docs

### Task 1: Add ruff.toml and install tooling

**Files:**
- Create: `video_subtitle_system/ruff.toml`
- Modify: `video_subtitle_system/pyproject.toml`

- [ ] **Step 1: Create ruff.toml**

```toml
# video_subtitle_system/ruff.toml
line-length = 79
target-version = "py311"

[lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4"]
ignore = ["E501"]

[lint.isort]
known-first-party = ["src"]
```

- [ ] **Step 2: Update pyproject.toml — add ruff, pyright, pytest-cov**

Modify the `[project]` `dependencies` list and add tool sections:

```toml
[project]
dependencies = [
    ...
    "ruff>=0.9.0",
    "pyright>=1.1.0",
    "pytest-cov>=6.0.0",
]

[tool.ruff]
config-path = "ruff.toml"

[tool.pyright]
include = ["src"]
pythonVersion = "3.11"
typeCheckingMode = "basic"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 3: Sync dependencies**

Run: `cd video_subtitle_system && uv sync`
Expected: New packages installed without error

- [ ] **Step 4: Run ruff check to see current issues**

Run: `cd video_subtitle_system && uv run ruff check src/`
Expected: List of lint errors (to be fixed in later tasks)

- [ ] **Step 5: Commit**

```bash
git add video_subtitle_system/ruff.toml video_subtitle_system/pyproject.toml
git commit -m "chore: add ruff, pyright, pytest-cov dependencies and config"
```

---

### Task 2: Add GitHub Actions CI workflow

**Files:**
- Create: `video_subtitle_system/.github/workflows/ci.yml`
- Create: `video_subtitle_system/.github/workflows/typecheck.yml`

- [ ] **Step 1: Create .github/workflows directory and ci.yml**

```yaml
# video_subtitle_system/.github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: |
          cd video_subtitle_system
          uv sync --dev

      - name: Lint with ruff
        run: |
          cd video_subtitle_system
          uv run ruff check src/

      - name: Check format with ruff
        run: |
          cd video_subtitle_system
          uv run ruff format --check src/

      - name: Run tests
        run: |
          cd video_subtitle_system
          uv run pytest -v --tb=short
```

- [ ] **Step 2: Create typecheck.yml**

```yaml
# video_subtitle_system/.github/workflows/typecheck.yml
name: Type Check

on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

jobs:
  typecheck:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: |
          cd video_subtitle_system
          uv sync

      - name: Type check with pyright
        run: |
          cd video_subtitle_system
          uv run pyright src/
```

- [ ] **Step 3: Commit**

```bash
git add video_subtitle_system/.github/
git commit -m "ci: add GitHub Actions CI and typecheck workflows"
```

---

### Task 3: Write CONTRIBUTING.md

**Files:**
- Create: `video_subtitle_system/CONTRIBUTING.md`

- [ ] **Step 1: Create CONTRIBUTING.md**

```markdown
# Contributing to video-subtitle-system

## 开发环境

```bash
cd video_subtitle_system
uv sync --dev
```

## 代码风格

- Python >= 3.11
- **PEP 8**，行长度 <= 79
- 使用 `ruff check src/` 检查风格
- 使用 `ruff format src/` 自动格式化

## 类型提示

所有新代码必须包含类型提示。使用 `pyright` 验证：

```bash
uv run pyright src/
```

## 测试

```bash
# 运行所有测试
uv run pytest

# 运行单个测试文件
uv run pytest tests/test_storage.py

# 运行单个测试
uv run pytest tests/test_storage.py::test_try_acquire_returns_true_when_not_success

# 带覆盖率
uv run pytest --cov=src --cov-report=term-missing
```

## 提交规范

- 使用 clear 的 commit message
- PR 标题格式：`type: short description`
- type：`feat`, `fix`, `refactor`, `test`, `docs`, `chore`

## PR 流程

1. Fork 仓库
2. 创建特性分支 `git checkout -b feat/your-feature`
3. 编写代码，添加测试
4. 确保所有测试通过：`uv run pytest`
5. 确保 lint 通过：`uv run ruff check src/`
6. 提交并 Push
7. 打开 Pull Request

## 评审标准

- 类型提示完整
- 有测试覆盖
- 遵守 PEP 8
- 日志清晰，无硬编码敏感信息
```

- [ ] **Step 2: Commit**

```bash
git add video_subtitle_system/CONTRIBUTING.md
git commit -m "docs: add CONTRIBUTING.md"
```

---

## Phase 2: Type Hints

### Task 4: Add type hints to P0 modules (config, db, redis_client, cursor)

**Files:**
- Modify: `video_subtitle_system/src/config.py`
- Modify: `video_subtitle_system/src/db.py`
- Modify: `video_subtitle_system/src/redis_client.py`
- Modify: `video_subtitle_system/src/cursor.py`

- [ ] **Step 1: Verify config.py already has type hints (P0 complete)**

Run: `cd video_subtitle_system && uv run pyright src/config.py`
Expected: No errors

- [ ] **Step 2: Add type hints to db.py**

```python
"""MySQL 数据库连接池"""
import aiomysql
from typing import Any, Optional, List, Dict, Tuple
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from .config import DatabaseConfig
from .logger import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self, config: DatabaseConfig) -> None:
        self.config: DatabaseConfig = config
        self.pool: Optional[aiomysql.Pool] = None

    async def connect(self) -> None:
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

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("db_closed")

    @asynccontextmanager
    async def transaction(
        self,
    ) -> AsyncIterator[Tuple[aiomysql.Connection, aiomysql.DictCursor]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    yield conn, cur
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

    async def execute(self, sql: str, args: Tuple[Any, ...] = ()) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                await conn.commit()
                return cur.rowcount

    async def fetch_one(
        self, sql: str, args: Tuple[Any, ...] = ()
    ) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchone()

    async def fetch_all(
        self, sql: str, args: Tuple[Any, ...] = ()
    ) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql, args)
                return await cur.fetchall()

    async def executemany(self, sql: str, args: List[Tuple[Any, ...]]) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.executemany(sql, args)
                await conn.commit()
                return cur.rowcount
```

- [ ] **Step 3: Verify db.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/db.py`
Expected: No errors

- [ ] **Step 4: Add type hints to redis_client.py**

```python
"""Redis 客户端封装"""
import json
from typing import Optional, Any
import redis.asyncio as redis

from .config import RedisConfig
from .logger import get_logger

logger = get_logger(__name__)


class RedisClient:
    def __init__(self, config: RedisConfig) -> None:
        self.config: RedisConfig = config
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.client = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            decode_responses=True,
        )
        await self.client.ping()
        logger.info(
            "redis_connected", host=self.config.host, port=self.config.port
        )

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()
            logger.info("redis_closed")

    def _dumps(self, value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)

    def _loads(self, value: Optional[str]) -> Any:
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

    async def brpoplpush(
        self, source: str, destination: str, timeout: int = 5
    ) -> Optional[Any]:
        result = await self.client.brpoplpush(source, destination, timeout=timeout)
        return self._loads(result)

    async def rpoplpush(
        self, source: str, destination: str
    ) -> Optional[Any]:
        result = await self.client.rpoplpush(source, destination)
        return self._loads(result)

    async def llen(self, key: str) -> int:
        return await self.client.llen(key)
```

- [ ] **Step 5: Verify redis_client.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/redis_client.py`
Expected: No errors

- [ ] **Step 6: Add type hints to cursor.py**

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
    def __init__(self, db: Database) -> None:
        self.db: Database = db

    async def get_cursor(self) -> Cursor:
        row = await self.db.fetch_one(
            "SELECT bilibili_last_id, douyin_last_id FROM sync_cursor WHERE id = 1"
        )
        if row is not None:
            return Cursor(
                bilibili_last_id=row["bilibili_last_id"],
                douyin_last_id=row["douyin_last_id"],
            )
        await self.db.execute(
            "INSERT IGNORE INTO sync_cursor (id, bilibili_last_id, douyin_last_id) VALUES (1, 0, 0)"
        )
        row = await self.db.fetch_one(
            "SELECT bilibili_last_id, douyin_last_id FROM sync_cursor WHERE id = 1"
        )
        return Cursor(
            bilibili_last_id=row["bilibili_last_id"] if row else 0,
            douyin_last_id=row["douyin_last_id"] if row else 0,
        )

    async def update_bilibili_cursor(self, last_id: int) -> None:
        await self.db.execute(
            "UPDATE sync_cursor SET bilibili_last_id = %s WHERE id = 1",
            (last_id,),
        )
        logger.debug("cursor_updated", platform="bilibili", last_id=last_id)

    async def update_douyin_cursor(self, last_id: int) -> None:
        await self.db.execute(
            "UPDATE sync_cursor SET douyin_last_id = %s WHERE id = 1",
            (last_id,),
        )
        logger.debug("cursor_updated", platform="douyin", last_id=last_id)
```

- [ ] **Step 7: Verify cursor.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/cursor.py`
Expected: No errors

- [ ] **Step 8: Run full P0 check**

Run: `cd video_subtitle_system && uv run pyright src/config.py src/db.py src/redis_client.py src/cursor.py`
Expected: No errors

- [ ] **Step 9: Commit**

```bash
git add video_subtitle_system/src/config.py video_subtitle_system/src/db.py video_subtitle_system/src/redis_client.py video_subtitle_system/src/cursor.py
git commit -m "type: add type hints to P0 modules (config, db, redis_client, cursor)"
```

---

### Task 5: Add type hints to P1 modules (storage, fingerprint, downloader)

**Files:**
- Modify: `video_subtitle_system/src/storage.py`
- Modify: `video_subtitle_system/src/fingerprint.py`
- Modify: `video_subtitle_system/src/downloader.py`

- [ ] **Step 1: Add type hints to storage.py**

```python
"""存储层：幂等写入 + PROCESSING 抢占锁"""
from typing import List, Dict, Any

from .db import Database
from .logger import get_logger

logger = get_logger(__name__)


class SubtitleStorage:
    def __init__(self, db: Database) -> None:
        self.db: Database = db

    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """原子抢占 PROCESSING 锁。返回 True 表示成功，False 表示已是 SUCCESS。"""
        result = await self.db.execute(
            """INSERT INTO task_status (video_id, platform, status, retry_count)
               VALUES (%s, %s, 'PROCESSING', 0)
               ON DUPLICATE KEY UPDATE
                   status = IF(status = 'SUCCESS', 'SUCCESS', 'PROCESSING')""",
            (video_id, platform),
        )
        return result > 0

    async def save(
        self,
        video_id: str,
        platform: str,
        segments: List[Dict[str, Any]],
        p_hash: str,
    ) -> None:
        async with self.db.transaction() as (conn, cur):
            if p_hash:
                await cur.execute(
                    "INSERT IGNORE INTO fingerprint (video_id, platform, p_hash) VALUES (%s, %s, %s)",
                    (video_id, platform, p_hash),
                )
            if segments:
                subtitle_records: List[tuple[Any, ...]] = [
                    (
                        video_id,
                        platform,
                        seg["start_time"],
                        seg["end_time"],
                        seg["text"],
                        seg.get("confidence"),
                    )
                    for seg in segments
                ]
                await cur.executemany(
                    """INSERT IGNORE INTO subtitle_segment
                       (video_id, platform, start_time, end_time, text, confidence)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    subtitle_records,
                )
                full_text = " ".join(seg["text"].strip() for seg in segments)
                confidences = [
                    seg.get("confidence", 1.0)
                    for seg in segments
                    if seg.get("confidence") is not None
                ]
                confidence_avg = sum(confidences) / len(confidences) if confidences else None
                await cur.execute(
                    """INSERT INTO subtitle (video_id, platform, full_text, confidence_avg)
                       VALUES (%s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE full_text=VALUES(full_text), confidence_avg=VALUES(confidence_avg)""",
                    (video_id, platform, full_text, confidence_avg),
                )
            await cur.execute(
                """INSERT INTO task_status (video_id, platform, status, error_msg)
                   VALUES (%s, %s, 'SUCCESS', NULL)
                   ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
                (video_id, platform),
            )
        logger.info(
            "subtitle_saved",
            video_id=video_id,
            platform=platform,
            segments=len(segments),
        )

    async def mark_failed(
        self, video_id: str, platform: str, error_msg: str
    ) -> None:
        await self.db.execute(
            "UPDATE task_status SET status = 'FAILED', error_msg = %s WHERE video_id = %s AND platform = %s",
            (error_msg, video_id, platform),
        )
        logger.error(
            "task_marked_failed",
            video_id=video_id,
            platform=platform,
            error=error_msg,
        )

    async def reset_processing_to_pending(self) -> None:
        await self.db.execute(
            "UPDATE task_status SET status = 'PENDING' WHERE status = 'PROCESSING'"
        )
        logger.info("recovered_processing_tasks")
```

- [ ] **Step 2: Verify storage.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/storage.py`
Expected: No errors

- [ ] **Step 3: Add type hints to fingerprint.py**

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
    def __init__(self, db: Database) -> None:
        self.db: Database = db

    async def compute(self, video_path: Path) -> str:
        video_path = Path(video_path)
        cap = cv2.VideoCapture(str(video_path))

        try:
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            timestamps = [1, 3, 5]
            hashes: list[imagehash.ImageHash] = []

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
                logger.warning(
                    "phash_skipped",
                    reason="no_frames_extracted",
                    video_path=str(video_path),
                )
                return ""

            merged = hashes[0].hash
            for h in hashes[1:]:
                merged = merged & h.hash
            result = str(imagehash.ImageHash(merged))
            logger.info("phash_computed", video_path=str(video_path), phash=result)
            return result

        finally:
            cap.release()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        if not hash1 or not hash2:
            return float("inf")
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    async def is_duplicate(self, p_hash: str) -> bool:
        if not p_hash:
            return False
        fingerprints = await self.db.fetch_all(
            "SELECT p_hash FROM fingerprint WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY) ORDER BY id DESC LIMIT 1000"
        )
        for fp in fingerprints:
            if self.hamming_distance(p_hash, fp["p_hash"]) <= 5:
                logger.info("duplicate_detected", p_hash=p_hash, match=fp["p_hash"])
                return True
        return False
```

- [ ] **Step 4: Verify fingerprint.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/fingerprint.py`
Expected: No errors

- [ ] **Step 5: Add type hints to downloader.py**

```python
"""视频下载器（Douyin直链 / B站DASH）"""
import asyncio
from pathlib import Path
from typing import Optional

import httpx

from .logger import get_logger

logger = get_logger(__name__)

MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


class VideoDownloader:
    def __init__(self) -> None:
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    async def close(self) -> None:
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def download(self, url: str, platform: str) -> Path:
        tmp_path = Path(f"/tmp/{__import__('uuid').uuid4()}.mp4")
        try:
            if platform == "douyin":
                await self._stream_to_file(url, tmp_path)
            else:
                await self._dash_download(url, tmp_path)
            logger.info("video_downloaded", platform=platform, path=str(tmp_path))
            return tmp_path
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def _stream_to_file(self, url: str, tmp_path: Path) -> None:
        client = await self._get_client()
        async with client.stream("GET", url, follow_redirects=True) as resp:
            resp.raise_for_status()
            downloaded = 0
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    downloaded += len(chunk)
                    if downloaded > MAX_FILE_SIZE:
                        msg = f"File too large: {downloaded} bytes exceeds limit of {MAX_FILE_SIZE}"
                        raise RuntimeError(msg)
                    f.write(chunk)

    async def _dash_download(self, url: str, tmp_path: Path) -> None:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "-o", str(tmp_path),
            "--merge-output-format", "mp4",
            "--max-filesize", f"{MAX_FILE_SIZE // (1024*1024)}M",
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

- [ ] **Step 6: Verify downloader.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/downloader.py`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add video_subtitle_system/src/storage.py video_subtitle_system/src/fingerprint.py video_subtitle_system/src/downloader.py
git commit -m "type: add type hints to P1 modules (storage, fingerprint, downloader)"
```

---

### Task 6: Add type hints to P2 modules (asr_engine, audio_extractor, producer, worker)

**Files:**
- Modify: `video_subtitle_system/src/asr_engine.py`
- Modify: `video_subtitle_system/src/audio_extractor.py`
- Modify: `video_subtitle_system/src/producer.py`
- Modify: `video_subtitle_system/src/worker.py`

- [ ] **Step 1: Add type hints to asr_engine.py**

```python
"""Faster-Whisper ASR 引擎"""
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

from faster_whisper import WhisperModel

from .logger import get_logger

logger = get_logger(__name__)


class ASREngine:
    def __init__(
        self, model_size: str = "small", hf_token: Optional[str] = None
    ) -> None:
        self.model_size: str = model_size
        self.hf_token: Optional[str] = hf_token
        self.model: Optional[WhisperModel] = None

    def load_model(self) -> None:
        if self.hf_token:
            os.environ["HF_TOKEN"] = self.hf_token

        compute_type = "int8" if self.model_size == "base" else "float32"
        self.model = WhisperModel(
            self.model_size,
            device="auto",
            compute_type=compute_type,
        )
        logger.info("asr_model_loaded", model_size=self.model_size)

    async def load_model_async(self) -> None:
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.load_model)

    async def recognize(self, audio_data: bytes) -> List[Dict[str, Any]]:
        if self.model is None:
            await self.load_model_async()

        tmp_wav = Path(f"/tmp/{__import__('uuid').uuid4()}.wav")
        tmp_wav.write_bytes(audio_data)

        try:
            segments, _ = self.model.transcribe(
                str(tmp_wav),
                language=None,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )

            result: List[Dict[str, Any]] = []
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

- [ ] **Step 2: Add type hints to audio_extractor.py**

```python
"""FFmpeg 音频提取器"""
import subprocess
from pathlib import Path

from .logger import get_logger

logger = get_logger(__name__)


class AudioExtractor:
    def extract(self, video_path: Path) -> bytes:
        """Async version — runs in async context via asyncio.create_subprocess_exec."""
        return self._extract_impl(video_path)

    def extract_sync(self, video_path: Path) -> bytes:
        """Synchronous version — uses subprocess.run, suitable for ThreadPoolExecutor."""
        return self._extract_impl(video_path)

    def _extract_impl(self, video_path: Path) -> bytes:
        wav_path = Path(f"/tmp/{__import__('uuid').uuid4()}.wav")
        video_path = Path(video_path)

        try:
            proc = subprocess.run(
                [
                    "ffmpeg", "-i", str(video_path),
                    "-ar", "16000",
                    "-ac", "1",
                    "-f", "wav",
                    "-y",
                    str(wav_path),
                ],
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                timeout=120,
            )

            if proc.returncode != 0:
                error_msg = proc.stderr.decode(errors="ignore")[-200:]
                raise RuntimeError(f"FFmpeg failed: {error_msg}")

            audio_data = wav_path.read_bytes()
            logger.info("audio_extracted", video_path=str(video_path))
            return audio_data

        finally:
            video_path.unlink(missing_ok=True)
            wav_path.unlink(missing_ok=True)
```

- [ ] **Step 3: Add type hints to producer.py**

```python
"""Producer：轮询数据库，拉取任务，推入Redis队列"""
import json
from typing import List, Dict, Any

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
    ) -> None:
        self.db: Database = db
        self.redis: RedisClient = redis
        self.cursor_mgr: CursorManager = cursor_mgr
        self.config: PollConfig = config
        self.backpressure_threshold: int = backpressure_threshold
        self._running: bool = False

    async def start(self) -> None:
        import asyncio
        self._running = True
        logger.info("producer_started", interval=self.config.interval_seconds)
        while self._running:
            try:
                await self.poll()
            except Exception as e:
                logger.error("producer_poll_error", error=str(e))
            await asyncio.sleep(self.config.interval_seconds)

    async def stop(self) -> None:
        self._running = False
        logger.info("producer_stopped")

    async def poll(self) -> int:
        cursor = await self.cursor_mgr.get_cursor()
        logger.debug(
            "producer_polling",
            bilibili_cursor=cursor.bilibili_last_id,
            douyin_cursor=cursor.douyin_last_id,
        )

        queue_len = await self.redis.llen("task_queue")
        if queue_len > self.backpressure_threshold:
            logger.warning("backpressure_active", queue_len=queue_len)
            return 0

        bilibili_tasks = await self.db.fetch_all(
            """SELECT b.id as video_id, 'bilibili' as platform, b.video_url as url
               FROM bilibili_video b
               WHERE b.id > %s
                 AND NOT EXISTS (
                     SELECT 1 FROM task_status ts
                     WHERE ts.video_id = b.id AND ts.platform = 'bilibili' AND ts.status = 'SUCCESS'
                 )
               ORDER BY b.id
               LIMIT %s""",
            (cursor.bilibili_last_id, self.config.batch_size // 2),
        )

        douyin_tasks = await self.db.fetch_all(
            """SELECT d.id as video_id, 'douyin' as platform, d.video_download_url as url
               FROM douyin_aweme d
               WHERE d.id > %s
                 AND NOT EXISTS (
                     SELECT 1 FROM task_status ts
                     WHERE ts.video_id = d.id AND ts.platform = 'douyin' AND ts.status = 'SUCCESS'
                 )
               ORDER BY d.id
               LIMIT %s""",
            (cursor.douyin_last_id, self.config.batch_size // 2),
        )

        total = 0

        for task in bilibili_tasks:
            await self.cursor_mgr.update_bilibili_cursor(task["video_id"])
            await self.redis.lpush("task_queue", json.dumps(task))
            total += 1

        for task in douyin_tasks:
            await self.cursor_mgr.update_douyin_cursor(task["video_id"])
            await self.redis.lpush("task_queue", json.dumps(task))
            total += 1

        if total > 0:
            logger.info(
                "tasks_pushed",
                count=total,
                bilibili=len(bilibili_tasks),
                douyin=len(douyin_tasks),
            )

        return total
```

- [ ] **Step 4: Add type hints to worker.py**

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
    ) -> None:
        self.db: Database = db
        self.redis: RedisClient = redis
        self.storage: SubtitleStorage = storage
        self.downloader: VideoDownloader = downloader
        self.audio_extractor: AudioExtractor = audio_extractor
        self.fingerprint: VideoFingerprint = fingerprint
        self.asr_engine: ASREngine = asr_engine
        self.concurrency: ConcurrencyConfig = concurrency
        self.max_retries: int = max_retries

        self.download_sem: asyncio.Semaphore = asyncio.Semaphore(concurrency.download)
        self.transcode_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=concurrency.transcode
        )
        self.asr_sem: asyncio.Semaphore = asyncio.Semaphore(concurrency.asr)

    async def run(self) -> None:
        logger.info("worker_started")
        backoff = 1.0

        while True:
            try:
                task = await self.redis.brpoplpush(
                    "task_queue", "processing_queue", timeout=5
                )
                logger.debug(
                    "brpoplpush_result",
                    task_type=type(task).__name__,
                    task_repr=repr(task)[:200],
                )

                if task is None:
                    await asyncio.sleep(min(30, backoff))
                    backoff = min(backoff * 1.5, 30)
                    continue

                backoff = 1.0
                await self._process_task(task)

            except Exception as e:
                logger.error("worker_loop_error", error=str(e), exc_info=True)

    async def _process_task(self, task: dict) -> None:
        if not isinstance(task, dict):
            logger.warning(
                "task_type_mismatch",
                task_type=type(task).__name__,
                task_repr=repr(task)[:200],
            )
            await self._ack_task(task)
            return

        video_id: str = task["video_id"]
        platform: str = task["platform"]
        url: str = task.get("url", "")
        retry_count: int = task.get("retry_count", 0)

        set_trace_id(f"{platform}-{str(video_id)[:8]}")

        acquired = await self.storage.try_acquire(video_id, platform)
        if not acquired:
            logger.info("task_already_success", video_id=video_id)
            await self._ack_task(task)
            return

        try:
            t0 = time.monotonic()

            async with self.download_sem:
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
                lambda: self.audio_extractor.extract_sync(video_path),
            )
            transcode_time_ms = int((time.monotonic() - t_transcode) * 1000)

            t_asr = time.monotonic()
            async with self.asr_sem:
                segments = await self.asr_engine.recognize(audio_data)
            asr_time_ms = int((time.monotonic() - t_asr) * 1000)

            await self.storage.save(video_id, platform, segments, p_hash)

            total_time_ms = int((time.monotonic() - t0) * 1000)
            logger.info(
                "task_completed",
                video_id=video_id,
                platform=platform,
                download_time_ms=download_time_ms,
                transcode_time_ms=transcode_time_ms,
                asr_time_ms=asr_time_ms,
                total_time_ms=total_time_ms,
                is_duplicate=False,
                segments_count=len(segments),
            )

            await self._ack_task(task)

        except Exception as e:
            logger.error(
                "task_failed",
                video_id=video_id,
                platform=platform,
                error=str(e),
                retry=retry_count,
            )
            await self._handle_failure(task, e)

    async def _ack_task(self, task: dict) -> None:
        await self.redis.lrem("processing_queue", 1, json.dumps(task))

    async def _handle_failure(self, task: dict, error: Exception) -> None:
        await self.redis.lrem("processing_queue", 1, json.dumps(task))
        retry_count = task.get("retry_count", 0) + 1

        if retry_count >= self.max_retries:
            await self.storage.mark_failed(task["video_id"], task["platform"], str(error))
        else:
            task["retry_count"] = retry_count
            await self.redis.lpush("task_queue", json.dumps(task))
            await self.db.execute(
                "UPDATE task_status SET status = 'PENDING', retry_count = %s WHERE video_id = %s AND platform = %s",
                (retry_count, task["video_id"], task["platform"]),
            )
```

- [ ] **Step 5: Verify all P2 modules**

Run: `cd video_subtitle_system && uv run pyright src/asr_engine.py src/audio_extractor.py src/producer.py src/worker.py`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add video_subtitle_system/src/asr_engine.py video_subtitle_system/src/audio_extractor.py video_subtitle_system/src/producer.py video_subtitle_system/src/worker.py
git commit -m "type: add type hints to P2 modules (asr_engine, audio_extractor, producer, worker)"
```

---

## Phase 3: storage.py Refactor + Tests

### Task 7: Create repositories.py interfaces and refactor storage.py

**Files:**
- Create: `video_subtitle_system/src/repos.py`
- Modify: `video_subtitle_system/src/storage.py`

- [ ] **Step 1: Create repos.py with three ABC interfaces**

```python
"""Repository interfaces for storage layer.

Three focused interfaces replacing the monolithic SubtitleStorage:
- TaskStatusRepository: task lock acquisition and status transitions
- FingerprintRepository: pHash storage and deduplication
- SubtitleRepository: subtitle segment and full-text writes
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class TaskStatusRepository(ABC):
    """Task status 抢占与状态流转."""

    @abstractmethod
    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """原子抢占 PROCESSING 锁。返回 True 表示成功，False 表示已是 SUCCESS。"""
        ...

    @abstractmethod
    async def mark_success(self, video_id: str, platform: str) -> None:
        """标记任务成功。"""
        ...

    @abstractmethod
    async def mark_failed(
        self, video_id: str, platform: str, error_msg: str
    ) -> None:
        """标记任务失败。"""
        ...

    @abstractmethod
    async def reset_processing_to_pending(self) -> None:
        """崩溃恢复：将所有 PROCESSING 状态重置为 PENDING。"""
        ...


class FingerprintRepository(ABC):
    """pHash 指纹存储与查重."""

    @abstractmethod
    async def save(
        self, video_id: str, platform: str, p_hash: str
    ) -> None:
        """存储指纹（p_hash 为空时跳过）。"""
        ...

    @abstractmethod
    async def find_similar(
        self, p_hash: str, threshold: int = 5
    ) -> bool:
        """查询相似指纹，返回是否存在重复。"""
        ...


class SubtitleRepository(ABC):
    """字幕分段与全文写入."""

    @abstractmethod
    async def save_segments(
        self,
        video_id: str,
        platform: str,
        segments: List[Dict[str, Any]],
    ) -> None:
        """批量写入字幕分段，并更新 subtitle 表的 full_text。"""
        ...
```

- [ ] **Step 2: Refactor storage.py to implement the three interfaces**

```python
"""存储层：SubtitleStorage 继承三个 Repository 接口"""
from typing import List, Dict, Any

from .db import Database
from .repos import TaskStatusRepository, FingerprintRepository, SubtitleRepository
from .logger import get_logger

logger = get_logger(__name__)


class SubtitleStorage(TaskStatusRepository, FingerprintRepository, SubtitleRepository):
    """完整实现，供主程序使用。"""

    def __init__(self, db: Database) -> None:
        self.db = db

    # -- TaskStatusRepository --

    async def try_acquire(self, video_id: str, platform: str) -> bool:
        result = await self.db.execute(
            """INSERT INTO task_status (video_id, platform, status, retry_count)
               VALUES (%s, %s, 'PROCESSING', 0)
               ON DUPLICATE KEY UPDATE
                   status = IF(status = 'SUCCESS', 'SUCCESS', 'PROCESSING')""",
            (video_id, platform),
        )
        return result > 0

    async def mark_success(self, video_id: str, platform: str) -> None:
        await self.db.execute(
            """INSERT INTO task_status (video_id, platform, status, error_msg)
               VALUES (%s, %s, 'SUCCESS', NULL)
               ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
            (video_id, platform),
        )

    async def mark_failed(
        self, video_id: str, platform: str, error_msg: str
    ) -> None:
        await self.db.execute(
            "UPDATE task_status SET status = 'FAILED', error_msg = %s WHERE video_id = %s AND platform = %s",
            (error_msg, video_id, platform),
        )
        logger.error(
            "task_marked_failed",
            video_id=video_id,
            platform=platform,
            error=error_msg,
        )

    async def reset_processing_to_pending(self) -> None:
        await self.db.execute(
            "UPDATE task_status SET status = 'PENDING' WHERE status = 'PROCESSING'"
        )
        logger.info("recovered_processing_tasks")

    # -- FingerprintRepository --

    async def save(
        self, video_id: str, platform: str, p_hash: str
    ) -> None:
        if not p_hash:
            return
        async with self.db.transaction() as (conn, cur):
            await cur.execute(
                "INSERT IGNORE INTO fingerprint (video_id, platform, p_hash) VALUES (%s, %s, %s)",
                (video_id, platform, p_hash),
            )

    # -- SubtitleRepository --

    async def save_segments(
        self,
        video_id: str,
        platform: str,
        segments: List[Dict[str, Any]],
    ) -> None:
        if not segments:
            return
        async with self.db.transaction() as (conn, cur):
            subtitle_records: List[tuple[Any, ...]] = [
                (
                    video_id,
                    platform,
                    seg["start_time"],
                    seg["end_time"],
                    seg["text"],
                    seg.get("confidence"),
                )
                for seg in segments
            ]
            await cur.executemany(
                """INSERT IGNORE INTO subtitle_segment
                   (video_id, platform, start_time, end_time, text, confidence)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                subtitle_records,
            )
            full_text = " ".join(seg["text"].strip() for seg in segments)
            confidences = [
                seg.get("confidence", 1.0)
                for seg in segments
                if seg.get("confidence") is not None
            ]
            confidence_avg = sum(confidences) / len(confidences) if confidences else None
            await cur.execute(
                """INSERT INTO subtitle (video_id, platform, full_text, confidence_avg)
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE full_text=VALUES(full_text), confidence_avg=VALUES(confidence_avg)""",
                (video_id, platform, full_text, confidence_avg),
            )
        logger.info(
            "subtitle_saved",
            video_id=video_id,
            platform=platform,
            segments=len(segments),
        )

    # -- Convenience method used by Worker --

    async def save(
        self,
        video_id: str,
        platform: str,
        segments: List[Dict[str, Any]],
        p_hash: str,
    ) -> None:
        async with self.db.transaction() as (conn, cur):
            if p_hash:
                await cur.execute(
                    "INSERT IGNORE INTO fingerprint (video_id, platform, p_hash) VALUES (%s, %s, %s)",
                    (video_id, platform, p_hash),
                )
            if segments:
                subtitle_records: List[tuple[Any, ...]] = [
                    (
                        video_id,
                        platform,
                        seg["start_time"],
                        seg["end_time"],
                        seg["text"],
                        seg.get("confidence"),
                    )
                    for seg in segments
                ]
                await cur.executemany(
                    """INSERT IGNORE INTO subtitle_segment
                       (video_id, platform, start_time, end_time, text, confidence)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    subtitle_records,
                )
                full_text = " ".join(seg["text"].strip() for seg in segments)
                confidences = [
                    seg.get("confidence", 1.0)
                    for seg in segments
                    if seg.get("confidence") is not None
                ]
                confidence_avg = sum(confidences) / len(confidences) if confidences else None
                await cur.execute(
                    """INSERT INTO subtitle (video_id, platform, full_text, confidence_avg)
                       VALUES (%s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE full_text=VALUES(full_text), confidence_avg=VALUES(confidence_avg)""",
                    (video_id, platform, full_text, confidence_avg),
                )
            await cur.execute(
                """INSERT INTO task_status (video_id, platform, status, error_msg)
                   VALUES (%s, %s, 'SUCCESS', NULL)
                   ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
                (video_id, platform),
            )
        logger.info(
            "subtitle_saved",
            video_id=video_id,
            platform=platform,
            segments=len(segments),
        )
```

- [ ] **Step 3: Verify repos.py and storage.py with pyright**

Run: `cd video_subtitle_system && uv run pyright src/repos.py src/storage.py`
Expected: No errors

- [ ] **Step 4: Verify existing tests still pass**

Run: `cd video_subtitle_system && uv run pytest tests/test_integration.py -v`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add video_subtitle_system/src/repos.py video_subtitle_system/src/storage.py
git commit -m "refactor: split storage into repos.py interfaces (TaskStatusRepository, FingerprintRepository, SubtitleRepository)"
```

---

### Task 8: Add pytest fixtures and unit tests

**Files:**
- Create: `video_subtitle_system/tests/conftest.py`
- Create: `video_subtitle_system/tests/test_storage.py`
- Create: `video_subtitle_system/tests/test_fingerprint.py`

- [ ] **Step 1: Create tests/conftest.py**

```python
"""Shared pytest fixtures for unit testing without real DB/Redis."""
import pytest
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager
from pathlib import Path


@pytest.fixture
def mock_db():
    """模拟 Database，无真实 MySQL 连接。"""
    db = MagicMock()
    db.execute = AsyncMock(return_value=1)
    db.fetch_one = AsyncMock(return_value=None)
    db.fetch_all = AsyncMock(return_value=[])
    db.executemany = AsyncMock(return_value=0)

    @asynccontextmanager
    async def mock_transaction():
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock(return_value=1)
        mock_cursor.executemany = AsyncMock(return_value=0)
        yield mock_conn, mock_cursor

    db.transaction = mock_transaction
    return db


@pytest.fixture
def mock_redis():
    """模拟 RedisClient，无真实 Redis 连接。"""
    redis = MagicMock()
    redis.lpush = AsyncMock(return_value=1)
    redis.lrem = AsyncMock(return_value=1)
    redis.brpoplpush = AsyncMock(return_value=None)
    redis.rpoplpush = AsyncMock(return_value=None)
    redis.llen = AsyncMock(return_value=0)
    return redis


@pytest.fixture
def mock_storage():
    """模拟 SubtitleStorage。"""
    storage = MagicMock()
    storage.try_acquire = AsyncMock(return_value=True)
    storage.save = AsyncMock()
    storage.mark_failed = AsyncMock()
    return storage


@pytest.fixture
def mock_downloader():
    """模拟 VideoDownloader。"""
    downloader = MagicMock()
    downloader.download = AsyncMock(return_value=Path("/tmp/test.mp4"))
    return downloader


@pytest.fixture
def mock_fingerprint():
    """模拟 VideoFingerprint。"""
    fingerprint = MagicMock()
    fingerprint.compute = AsyncMock(return_value="abc123")
    fingerprint.is_duplicate = AsyncMock(return_value=False)
    return fingerprint


@pytest.fixture
def mock_asr_engine():
    """模拟 ASREngine。"""
    asr = MagicMock()
    asr.recognize = AsyncMock(return_value=[
        {"start_time": 0.0, "end_time": 2.0, "text": "测试", "confidence": 0.95}
    ])
    return asr
```

- [ ] **Step 2: Create tests/test_storage.py**

```python
"""Unit tests for SubtitleStorage."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from contextlib import asynccontextmanager

from src.storage import SubtitleStorage


class TestTryAcquire:
    @pytest.mark.asyncio
    async def test_returns_true_when_not_success(self, mock_db):
        mock_db.execute = AsyncMock(return_value=1)
        storage = SubtitleStorage(mock_db)
        result = await storage.try_acquire("BV123", "bilibili")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_already_success(self, mock_db):
        mock_db.execute = AsyncMock(return_value=0)
        storage = SubtitleStorage(mock_db)
        result = await storage.try_acquire("BV123", "bilibili")
        assert result is False


class TestSave:
    @pytest.mark.asyncio
    async def test_save_full_text_concatenation(self, mock_db):
        segments = [
            {"start_time": 0.0, "end_time": 2.5, "text": "你好世界", "confidence": 0.95},
            {"start_time": 2.5, "end_time": 5.0, "text": "这是测试", "confidence": 0.88},
            {"start_time": 5.0, "end_time": 7.0, "text": "再见", "confidence": 1.0},
        ]

        executed_sql = []

        class MockCursor:
            async def execute(self, sql, args):
                executed_sql.append((sql, args))
                return 1

            async def executemany(self, sql, args):
                executed_sql.append((sql, args))
                return len(args)

        mock_cursor = MockCursor()

        @asynccontextmanager
        async def mock_transaction():
            yield None, mock_cursor

        mock_db.transaction = mock_transaction

        storage = SubtitleStorage(mock_db)
        await storage.save("BV123", "bilibili", segments, "abc123")

        subtitle_calls = [
            (sql, args)
            for sql, args in executed_sql
            if "subtitle" in sql and "full_text" in sql
        ]
        assert len(subtitle_calls) == 1
        sql, args = subtitle_calls[0]
        assert args[0] == "BV123"
        assert args[1] == "bilibili"
        assert args[2] == "你好世界 这是测试 再见"
        expected_avg = (0.95 + 0.88 + 1.0) / 3
        assert args[3] == pytest.approx(expected_avg)

    @pytest.mark.asyncio
    async def test_save_empty_segments_skips_subtitle_table(self, mock_db):
        executed_sql = []

        class MockCursor:
            async def execute(self, sql, args):
                executed_sql.append((sql, args))
                return 1

            async def executemany(self, sql, args):
                executed_sql.append((sql, args))
                return len(args)

        mock_cursor = MockCursor()

        @asynccontextmanager
        async def mock_transaction():
            yield None, mock_cursor

        mock_db.transaction = mock_transaction

        storage = SubtitleStorage(mock_db)
        await storage.save("BV123", "bilibili", [], "abc123")

        subtitle_calls = [
            args
            for sql, args in executed_sql
            if "subtitle" in sql and "full_text" in sql
        ]
        assert len(subtitle_calls) == 0

    @pytest.mark.asyncio
    async def test_save_empty_phash_skips_fingerprint(self, mock_db):
        segments = [
            {"start_time": 0.0, "end_time": 2.5, "text": "有字幕", "confidence": 0.9},
        ]

        executed_sql = []

        class MockCursor:
            async def execute(self, sql, args):
                executed_sql.append((sql, args))
                return 1

            async def executemany(self, sql, args):
                executed_sql.append((sql, args))
                return len(args)

        mock_cursor = MockCursor()

        @asynccontextmanager
        async def mock_transaction():
            yield None, mock_cursor

        mock_db.transaction = mock_transaction

        storage = SubtitleStorage(mock_db)
        await storage.save("BV123", "bilibili", segments, "")

        fingerprint_calls = [
            args for sql, args in executed_sql if "fingerprint" in sql
        ]
        assert len(fingerprint_calls) == 0
        subtitle_calls = [
            args
            for sql, args in executed_sql
            if "subtitle" in sql and "full_text" in sql
        ]
        assert len(subtitle_calls) == 1


class TestMarkFailed:
    @pytest.mark.asyncio
    async def test_mark_failed_updates_status(self, mock_db):
        storage = SubtitleStorage(mock_db)
        await storage.mark_failed("BV123", "bilibili", "download failed")
        mock_db.execute.assert_called_once()
        call_args = mock_db.execute.call_args
        assert "FAILED" in call_args[0][0]
        assert "download failed" in call_args[0][1]
```

- [ ] **Step 3: Create tests/test_fingerprint.py**

```python
"""Unit tests for VideoFingerprint."""
import pytest
from src.fingerprint import VideoFingerprint


class TestHammingDistance:
    def test_same_hash_returns_zero(self):
        hash1 = "0123456789abcdef"
        distance = VideoFingerprint.hamming_distance(hash1, hash1)
        assert distance == 0

    def test_one_bit_diff_returns_one(self):
        hash1 = "0123456789abcdef"
        hash2 = "0123456789abcdee"
        distance = VideoFingerprint.hamming_distance(hash1, hash2)
        assert distance == 1

    def test_empty_hash_returns_infinity(self):
        distance = VideoFingerprint.hamming_distance("", "abc123")
        assert distance == float("inf")

    def test_none_hash_returns_infinity(self):
        distance = VideoFingerprint.hamming_distance("abc123", "")
        assert distance == float("inf")
```

- [ ] **Step 4: Run all tests**

Run: `cd video_subtitle_system && uv run pytest -v --tb=short`
Expected: All tests pass including new ones

- [ ] **Step 5: Commit**

```bash
git add video_subtitle_system/tests/conftest.py video_subtitle_system/tests/test_storage.py video_subtitle_system/tests/test_fingerprint.py
git commit -m "test: add pytest fixtures and unit tests for storage and fingerprint modules"
```

---

## Phase 4: Cleanup

### Task 9: Update CLAUDE.md and run full verification

**Files:**
- Modify: `video_subtitle_system/CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md module map**

In the Module Map table, add the new `repos.py` entry. The current table ends with `main.py`. Add a new row:

```
| `src/repos.py` | Abstract interfaces (TaskStatusRepository, FingerprintRepository, SubtitleRepository) |
```

And update `src/storage.py` description to:
```
| `src/storage.py` | SubtitleStorage implementing three Repository interfaces |
```

- [ ] **Step 2: Run full ruff check on entire src/**

Run: `cd video_subtitle_system && uv run ruff check src/`
Expected: Clean (no errors). If there are errors, fix them inline.

- [ ] **Step 3: Run full pyright type check**

Run: `cd video_subtitle_system && uv run pyright src/`
Expected: No errors

- [ ] **Step 4: Run full test suite**

Run: `cd video_subtitle_system && uv run pytest -v --tb=short`
Expected: All tests pass

- [ ] **Step 5: Commit CLAUDE.md update**

```bash
git add video_subtitle_system/CLAUDE.md
git commit -m "docs: update CLAUDE.md with repos.py module and storage.py description"
```

---

## Spec Coverage Check

| Spec Requirement | Task |
|-----------------|------|
| ruff.toml + ruff toolchain | Task 1 |
| GitHub Actions CI | Task 2 |
| CONTRIBUTING.md | Task 3 |
| Type hints P0 (config, db, redis, cursor) | Task 4 |
| Type hints P1 (storage, fingerprint, downloader) | Task 5 |
| Type hints P2 (asr_engine, audio_extractor, producer, worker) | Task 6 |
| repos.py interfaces | Task 7 |
| storage.py refactor | Task 7 |
| conftest.py fixtures | Task 8 |
| test_storage.py | Task 8 |
| test_fingerprint.py | Task 8 |
| CLAUDE.md update | Task 9 |
| Full CI verification | Task 9 |

All spec requirements covered. No placeholders found. Type signatures consistent across tasks.
