# 视频字幕自动化提取系统设计

**日期**：2026-04-15
**状态**：草稿

---

## 一、系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         守护进程 (Daemon)                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │   Producer   │→ │    Queue     │→ │    Worker    │                  │
│  │  (轮询DB)    │  │   (Redis)    │  │  (协程池)    │                  │
│  └──────────────┘  └──────────────┘  └──────────────┘                  │
│         ↑                 ↑                 ↑                          │
│         │                 │                 │                          │
│  ┌──────┴─────────────────┴─────────────────┴──────┐                   │
│  │              共享状态 (内存 + Redis)            │                   │
│  └────────────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
         │                                           │
         ↓                                           ↓
┌─────────────────┐                         ┌─────────────────┐
│   MySQL         │                         │   Redis         │
│  ruoyi-vue-pro  │                         │   127.0.0.1     │
│                 │                         │                 │
│ bilibili_video  │←只读                    │ task_queue      │
│ douyin_aweme    │←只读                    │ processed_cache │
│ task_status (新)│                         │                 │
│ dy_fingerprint  │                         │                 │
│ dy_subtitle_seg │                         │                 │
└─────────────────┘                         └─────────────────┘
```

---

## 二、模块划分

| 模块 | 职责 | 技术 |
|------|------|------|
| `producer` | 轮询数据库，增量拉取任务，推入Redis队列 | asyncio + aiomysql |
| `worker` | 消费队列，协调各处理阶段 | asyncio + threadpool |
| `downloader` | 视频下载（Douyin直链/B站DASH） | httpx + yt-dlp |
| `fingerprint` | pHash计算 + 相似度判断（ASR 前置） | imagehash + opencv |
| `audio_extractor` | FFmpeg音频提取（统一临时文件） | asyncio.subprocess |
| `asr_engine` | Faster-Whisper语音识别 | faster-whisper |
| `storage` | 幂等写入MySQL | aiomysql + 事务 |
| `logger` | 结构化日志 + trace_id + 性能指标 | structlog |

---

## 三、数据流

```
1. Producer 每5秒轮询（双游标分离）:
   bilibili_last_id → 拉取 bilibili_video 中 id > bilibili_last_id 的新任务
   douyin_last_id → 拉取 douyin_aweme 中 id > douyin_last_id 的新任务
各50条，合计100条

2. 幂等抢占：INSERT task_status = 'PROCESSING'（仅当非 SUCCESS 状态）
   - INSERT ... ON DUPLICATE KEY UPDATE status = IF(status='SUCCESS', 'SUCCESS', 'PROCESSING')
   - 若已为 SUCCESS → 直接跳过（不重复处理）

3. 背压机制：队列长度 > 1000 时暂停拉取

4. 推入 Redis 队列（可靠队列模式）:
   BRPOPLPUSH task_queue processing_queue（移动而非复制）

5. Worker 消费任务:
   a) 原子抢占：INSERT task_status = 'PROCESSING'（仅当非 SUCCESS 状态）
      - 若已为 SUCCESS → 直接跳过（幂等保证）
   b) 下载视频 → 临时文件（禁止全量读内存）
   c) 计算 pHash（多帧合并）→ 查重（节省后续ASR成本）
      - 取第1/3/5秒三帧，合并为最终 hash
      - 查询最近 10000 条指纹做汉明距离匹配（禁止全表扫描）
   d) FFmpeg 提取音频 → 16kHz WAV 临时文件（检查 returncode）
   e) Faster-Whisper ASR → 字幕分段（含 confidence 置信度）
   f) 批量写入 dy_subtitle_segment（executemany）
   g) 更新 task_status → SUCCESS
   h) 失败时：retry_count++ 后重新入队 task_queue（可回收，不卡死）

6. 崩溃恢复:
   - 启动时扫描 processing_queue → 重新入队 task_queue
   - 扫描 PROCESSING 状态 → 重置为 PENDING

7. cursor 更新时机：成功入队后才更新游标（避免数据丢失）

8. 失败处理:
   - 更新 task_status → FAILED + error_msg
   - 指数退避重试（最大3次）
```

---

## 四、数据库设计

### 4.1 新建表（系统自建）

```sql
-- 视频指纹表（pHash 去重）
CREATE TABLE dy_fingerprint (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    p_hash VARCHAR(64) NOT NULL COMMENT '感知哈希值',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_p_hash (p_hash),
    INDEX idx_video_platform (video_id, platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 字幕分段表
CREATE TABLE dy_subtitle_segment (
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
CREATE TABLE task_status (
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
```

### 4.2 游标维护

```sql
-- 记录最后处理的ID（持久化，避免重启丢失）
CREATE TABLE IF NOT EXISTS sync_cursor (
    id INT PRIMARY KEY DEFAULT 1,
    bilibili_last_id BIGINT UNSIGNED DEFAULT 0,
    douyin_last_id BIGINT UNSIGNED DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 五、并发模型

### 5.1 分层并发策略

| 阶段 | 资源类型 | 并发数 | 实现 |
|------|----------|--------|------|
| 下载 | IO密集 | 6 | asyncio.Semaphore(6) |
| 转码 | 中负载 | 4 | ThreadPoolExecutor(max_workers=4) |
| 推理 | CPU/GPU密集 | 2 | asyncio.Semaphore(2) |

### 5.2 Worker 架构

```python
async def process_task(task: dict, download_sem, transcode_pool, asr_sem):
    video_id = task['video_id']
    platform = task['platform']

    # a) 原子抢占 PROCESSING 锁
    acquired = await storage.try_acquire(video_id, platform)
    if not acquired:
        logger.info("task_already_success", video_id=video_id)
        return  # 已是 SUCCESS，直接跳过

    try:
        # b) 下载视频 → 临时文件
        with download_sem:
            video_path = await downloader.download(task['url'], platform)

        # c) pHash 计算 + 查重
        p_hash = await fingerprint.compute(video_path)
        is_dup = await fingerprint.is_duplicate(p_hash)

        # d) FFmpeg 音频提取
        with transcode_pool:
            audio_data = await audio_extractor.extract(video_path)

        # e) ASR 推理
        with asr_sem:
            segments = await asr_engine.recognize(audio_data)

        # f) 写入数据库
        await storage.save(video_id, platform, segments, p_hash)

        logger.info("task_completed",
            video_id=video_id, platform=platform,
            download_time_ms=..., transcode_time_ms=...,
            asr_time_ms=..., total_time_ms=...,
            is_duplicate=is_dup, segments_count=len(segments))

    except Exception as e:
        logger.error("task_failed", video_id=video_id, platform=platform, error=str(e))
        raise
```

---

## 六、核心组件设计

### 6.1 Producer（任务拉取）

```python
# 增量拉取策略：基于 sync_cursor 表（双游标分离）
async def poll_tasks():
    cursor = await get_cursor()

    # 拉取 B站 新任务（独立游标）
    bilibili_tasks = await db.fetch("""
        SELECT id as video_id, 'bilibili' as platform, video_url as url
        FROM bilibili_video
        WHERE id > :last_id
        ORDER BY id
        LIMIT 50
    """, last_id=cursor.bilibili_last_id)

    # 拉取 抖音 新任务（独立游标）
    douyin_tasks = await db.fetch("""
        SELECT id as video_id, 'douyin' as platform, video_download_url as url
        FROM douyin_aweme
        WHERE id > :last_id
        ORDER BY id
        LIMIT 50
    """, last_id=cursor.douyin_last_id)

    # 背压：队列长度 > 1000 时暂停拉取
    queue_len = await redis.llen("task_queue")
    if queue_len > 1000:
        logger.warning("backpressure_active", queue_len=queue_len)
        return []

    # 推入 Redis 队列成功后，才更新游标（避免数据丢失）
    for task in bilibili_tasks:
        await redis.lpush("task_queue", json.dumps(task))
        await update_cursor('bilibili', task['video_id'])

    for task in douyin_tasks:
        await redis.lpush("task_queue", json.dumps(task))
        await update_cursor('douyin', task['video_id'])
```

### 6.2 Downloader（视频下载）

```python
class VideoDownloader:
    async def download(self, url: str, platform: str) -> Path:
        tmp_path = Path(f"/tmp/{uuid4()}.mp4")
        try:
            if platform == 'douyin':
                await self._stream_to_file(url, str(tmp_path))
            else:
                await self._dash_download(url, str(tmp_path))
            return tmp_path
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def _stream_to_file(self, url: str, tmp_path: str):
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", url, follow_redirects=True) as resp:
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=8192):
                        f.write(chunk)

    async def _dash_download(self, url: str, tmp_path: str):
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "-o", tmp_path,
            "--merge-output-format", "mp4",
            "--socket-timeout", "60",
            url,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"yt-dlp failed: {stderr.decode(errors='ignore')[-200:]}")
```

### 6.3 AudioExtractor（音频提取）

```python
class AudioExtractor:
    async def extract(self, video_path: Path) -> bytes:
        """统一使用临时文件方式处理 FFmpeg"""
        wav_path = Path(f"/tmp/{uuid4()}.wav")
        video_path = Path(video_path)

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", str(video_path),
                "-ar", "16000",       # 16kHz
                "-ac", "1",           # 单声道
                "-f", "wav",          # WAV格式
                "-y",                  # 覆盖输出
                str(wav_path),
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode(errors='ignore')[-200:]
                raise RuntimeError(f"FFmpeg failed: {error_msg}")

            audio_data = wav_path.read_bytes()
            return audio_data

        finally:
            # 强制清理临时文件
            video_path.unlink(missing_ok=True)
            wav_path.unlink(missing_ok=True)
```

### 6.4 Fingerprint（pHash 计算）— 多帧合并，ASR 前置

```python
import imagehash
from PIL import Image
import cv2
import numpy as np

class VideoFingerprint:
    async def compute(self, video_path: Path) -> str:
        """从视频文件提取多帧合并计算 pHash（避免单帧误判）"""
        cap = cv2.VideoCapture(str(video_path))
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # 取第1秒、第3秒、第5秒三帧（合并 hash）
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

        cap.release()

        # 多帧 hash 合并：取各位交集
        if len(hashes) == 0:
            return ""
        merged = hashes[0]
        for h in hashes[1:]:
            merged = merged & h  # 按位与
        return str(merged)

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    async def is_duplicate(self, p_hash: str) -> bool:
        """仅查询最近 10000 条指纹，避免全表扫描"""
        fingerprints = await db.fetch(
            "SELECT p_hash FROM dy_fingerprint ORDER BY id DESC LIMIT 10000"
        )
        for fp in fingerprints:
            if self.hamming_distance(p_hash, fp['p_hash']) <= 5:
                return True
        return False
```

### 6.5 ASR Engine（Faster-Whisper）

```python
class ASREngine:
    def __init__(self, model_size: str = "small"):
        self.model = load_model(model_size)

    async def recognize(self, audio_data: bytes) -> list[dict]:
        # 写入临时文件（Whisper C API 需要文件路径）
        tmp_wav = f"/tmp/{uuid4()}.wav"
        Path(tmp_wav).write_bytes(audio_data)

        # ASR 推理
        segments, info = self.model.transcribe(
            tmp_wav,
            language=None,       # 自动检测
            vad_filter=True,     # VAD 过滤
            vad_parameters=dict(min_silence_duration_ms=500)
        )

        # 转结构化格式（含置信度）
        result = [
            dict(
                start_time=s.start,
                end_time=s.end,
                text=s.text,
                confidence=getattr(s, 'probability', 1.0)
            )
            for s in segments
        ]

        Path(tmp_wav).unlink(missing_ok=True)
        return result
```

### 6.6 Storage（幂等写入 + 抢占锁）

```python
class SubtitleStorage:
    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """
        原子抢占 PROCESSING 锁。
        返回 True 表示获取成功，False 表示已有 SUCCESS（不重复处理）。
        """
        result = await db.execute(
            """INSERT INTO task_status (video_id, platform, status, retry_count)
               VALUES (?, ?, 'PROCESSING', 0)
               ON DUPLICATE KEY UPDATE
                   status = IF(status = 'SUCCESS', 'SUCCESS', 'PROCESSING'),
                   retry_count = IF(status = 'SUCCESS', retry_count, 0)""",
            (video_id, platform)
        )
        # affected_rows == 1 表示成功抢占，== 0 表示已是 SUCCESS
        return result > 0

    async def save(self, video_id: str, platform: str,
                   segments: list[dict], p_hash: str):
        async with db.transaction():
            await db.execute(
                "INSERT IGNORE INTO dy_fingerprint (video_id, platform, p_hash) VALUES (?, ?, ?)",
                (video_id, platform, p_hash)
            )
            subtitle_records = [
                (video_id, platform, seg['start_time'], seg['end_time'], seg['text'], seg.get('confidence'))
                for seg in segments
            ]
            await db.executemany(
                """INSERT IGNORE INTO dy_subtitle_segment
                   (video_id, platform, start_time, end_time, text, confidence)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                subtitle_records
            )
            await db.execute(
                """INSERT INTO task_status (video_id, platform, status, error_msg)
                   VALUES (?, ?, 'SUCCESS', NULL)
                   ON DUPLICATE KEY UPDATE status='SUCCESS', error_msg=NULL, updated_at=NOW()""",
                (video_id, platform)
            )
```

---

## 七、常驻运行与容错

### 7.1 主程序（守护进程）

```python
async def main():
    # 崩溃恢复
    await recover_crashed_tasks()           # PROCESSING → PENDING
    await recover_processing_queue()        # processing_queue → task_queue

    producer_task = asyncio.create_task(producer.run())
    worker_tasks = [asyncio.create_task(worker.run()) for _ in range(WORKER_COUNT)]

    # 优雅退出
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.create_task(handle_signal(sig, stop_event))

    await stop_event.wait()
    producer_task.cancel()
    for t in worker_tasks:
        t.cancel()

async def recover_processing_queue():
    """启动时将 processing_queue 中的任务重新入队"""
    while True:
        task = await redis.rpoplpush("processing_queue", "task_queue")
        if task is None:
            break
        logger.info("recovered_task", task=task)
```

### 7.2 重试机制

| 失败类型 | 策略 |
|----------|------|
| 下载失败 | 指数退避（2s → 4s → 8s），最大3次 |
| FFmpeg失败 | 重试2次，失败后标记 FAILED |
| Whisper失败 | 直接标记 FAILED，不重试 |
| 数据库失败 | 事务回滚，队列重新入队 |

### 7.3 可靠队列与空队列处理

```python
async def worker_loop():
    backoff = 1

    while True:
        task = await redis.brpoplpush("task_queue", "processing_queue", timeout=5)

        if task is None:
            await asyncio.sleep(min(30, backoff))
            backoff = min(backoff * 1.5, 30)
        else:
            backoff = 1
            try:
                await process_task(task)
                await redis.lrem("processing_queue", 1, json.dumps(task))
            except Exception as e:
                # 失败时：移除 processing_queue，任务重新入队
                await redis.lrem("processing_queue", 1, json.dumps(task))
                retry_count = task.get('retry_count', 0) + 1
                if retry_count >= 3:
                    # 超过最大重试次数，标记失败
                    await db.execute(
                        """UPDATE task_status SET status='FAILED', error_msg=?
                           WHERE video_id=? AND platform=?""",
                        (str(e), task['video_id'], task['platform'])
                    )
                else:
                    # 重新入队 + 更新 retry_count
                    task['retry_count'] = retry_count
                    await redis.lpush("task_queue", json.dumps(task))
                    await db.execute(
                        """UPDATE task_status SET status='PENDING', retry_count=?
                           WHERE video_id=? AND platform=?""",
                        (retry_count, task['video_id'], task['platform'])
                    )
                logger.error("task_failed_and_requeued", task=task, error=str(e), retry=retry_count)
```

---

## 八、日志规范

```python
import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)

# 日志格式示例（含性能指标）
logger.info("task_completed",
    trace_id="abc123",
    video_id="BVxxxx",
    platform="bilibili",
    stage="asr",
    download_time_ms=1200,
    transcode_time_ms=450,
    asr_time_ms=3200,
    total_time_ms=4850,
    is_duplicate=False,
    segments_count=45
)

# 失败日志
logger.error("task_failed",
    trace_id="abc123",
    video_id="BVxxxx",
    platform="bilibili",
    stage="download",
    error="connection_timeout",
    retry_count=2
)
```

---

## 九、部署与性能建议

### 9.1 依赖清单

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
```

### 9.2 性能优化

1. **模型预加载**：启动时加载 Whisper 模型，避免首次推理延迟
2. **FFmpeg 复用**：对于临时文件方式，可考虑进程池复用 FFmpeg 进程
3. **批量写入**：积累多条字幕后再事务提交（需权衡实时性）
4. **Redis 连接池**：复用连接，减少建立连接开销

### 9.3 配置建议

```yaml
# config.yaml
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
```

---

## 十、目录结构

```
video_subtitle_system/
├── config.yaml
├── main.py                    # 入口，守护进程
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── config.py              # 配置加载
│   ├── db.py                  # 数据库连接
│   ├── redis_client.py       # Redis 连接
│   ├── producer.py           # 任务拉取
│   ├── worker.py             # Worker 协程
│   ├── downloader.py        # 视频下载
│   ├── audio_extractor.py   # FFmpeg 音频提取
│   ├── asr_engine.py        # Whisper ASR
│   ├── fingerprint.py        # pHash 计算
│   ├── storage.py           # 数据库写入
│   ├── cursor.py             # 游标管理
│   └── logger.py             # 结构化日志
├── sql/
│   └── init.sql              # 建表语句
└── logs/
```
