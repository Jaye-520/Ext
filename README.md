# Video Subtitle System

离线优先的视频字幕自动化提取系统。支持从 **Bilibili** 和 **Douyin** 平台批量下载视频，自动提取音频并通过 ASR（自动语音识别）生成结构化字幕数据，存入 MySQL 供下游分析使用。

---

## 特性

- **双平台支持**：同时处理 Bilibili 与抖音视频源
- **视频去重**：基于感知哈希（pHash）的指纹系统，避免重复处理相同视频
- **离线优先**：核心 ASR 引擎基于 [Faster-Whisper](https://github.com/SYSTRAN/faster-whisper)，本地运行，无需调用第三方 API
- **高并发与流控**：下载、转码、ASR 三级独立并发控制，配合 Redis 反压机制保护下游
- **容错与自愈**：任务失败自动重试（最多 3 次），守护进程崩溃恢复未完成任务
- **零侵入设计**：不修改源数据表结构，独立 `task_status` 表追踪任务生命周期
- **容器化部署**：Docker Compose 一键启动 MySQL + Redis + 应用本体

---

## 架构概览

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────────────────┐
│   MySQL     │────▶│  Producer   │────▶│  Redis Task Queue                       │
│ (source DB) │     │  (轮询拉取)  │     │  (backpressure + crash recovery)        │
└─────────────┘     └─────────────┘     └─────────────────────────────────────────┘
                                                  │
                                                  ▼
                                        ┌─────────────────────────────────────────┐
                                        │  Worker (异步消费)                       │
                                        │  1. Download (yt-dlp)                   │
                                        │  2. Fingerprint (pHash dedup)           │
                                        │  3. Audio Extract (FFmpeg)              │
                                        │  4. ASR (Faster-Whisper)                │
                                        │  5. Save to MySQL                       │
                                        └─────────────────────────────────────────┘
```

---

## 快速开始

### 环境要求

- Python >= 3.11
- MySQL 8.0+
- Redis 7.0+
- FFmpeg
- (可选) NVIDIA GPU + CUDA，用于加速 Whisper 推理

### 1. 克隆项目

```bash
git clone git@github.com:Jaye-520/Ext.git
cd Ext/video_subtitle_system
```

### 2. 配置

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`：

```yaml
database:
  host: "127.0.0.1"
  port: 3306
  user: "your_username"
  password: "your_password"
  database: "video_subtitle"

redis:
  host: "127.0.0.1"
  port: 6379
  password: null

asr:
  model_size: "small"          # tiny/base/small/medium/large-v1/large-v2/large-v3
  hf_token: "your_hf_token"    # HuggingFace 令牌（用于下载模型）

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

### 3. 初始化数据库

```bash
mysql -u your_username -p video_subtitle < sql/init.sql
mysql -u your_username -p video_subtitle < sql/source_tables.sql
```

> `source_tables.sql` 为示例源表结构，生产环境请按实际 schema 调整 Producer 中的查询语句。

### 4. 安装依赖并运行

使用 [uv](https://github.com/astral-sh/uv)（推荐）：

```bash
uv sync
uv run python main.py
```

或使用 pip：

```bash
pip install -e .
python main.py
```

---

## Docker 部署

一键启动完整环境（MySQL + Redis + App）：

```bash
cd video_subtitle_system

# 设置 HuggingFace 令牌（用于首次下载 Whisper 模型）
export HF_TOKEN=your_hf_token

# 启动
docker-compose up -d
```

服务说明：

| 服务 | 容器名 | 端口 | 说明 |
|------|--------|------|------|
| MySQL | `vs_mysql` | `3306` | 数据持久化到 `mysql_data` |
| Redis | `vs_redis` | `6379` | 任务队列与游标缓存 |
| App | `vs_app` | — | 守护进程本体 |

> 如果没有 GPU，请注释掉 `docker-compose.yml` 中 `app` 服务的 `deploy` 段落。

---

## 项目结构

```
video_subtitle_system/
├── main.py                 # 守护进程入口（Producer + Worker + 信号处理）
├── pyproject.toml          # 项目配置与依赖
├── docker-compose.yml      # 容器编排
├── Dockerfile              # 应用镜像
├── config.example.yaml     # 配置模板
├── config.yaml             # 实际配置（gitignored）
├── sql/
│   ├── init.sql            # 核心表结构（指纹、字幕、任务状态、游标）
│   └── source_tables.sql   # 示例源表结构
├── src/
│   ├── producer.py         # 轮询数据库，生产任务到 Redis
│   ├── worker.py           # 消费队列，执行下载→转码→ASR→存储
│   ├── asr_engine.py       # Faster-Whisper 封装（异步加载/识别）
│   ├── audio_extractor.py  # FFmpeg 音频提取
│   ├── downloader.py       # yt-dlp 视频下载
│   ├── fingerprint.py      # 感知哈希计算与查重
│   ├── storage.py          # 字幕与指纹落库
│   ├── db.py               # MySQL 异步连接池
│   ├── redis_client.py     # Redis 异步客户端
│   ├── cursor.py           # 游标管理（增量拉取）
│   ├── config.py           # 配置加载与校验
│   └── logger.py           # Structlog 结构化日志
└── tests/
    └── test_integration.py # 集成测试
```

---

## 核心流程

1. **Producer** 每 5 秒轮询 MySQL 源表（`bilibili_video`、`douyin_aweme`），通过游标实现增量拉取。
2. 新视频被封装为任务，推入 Redis 列表 `task_queue`；同时维护 `processing_queue` 用于崩溃恢复。
3. **Worker** 通过 `BRPOPLPUSH` 阻塞消费任务，依次执行：
   - **下载**：`yt-dlp` 拉取视频（并发数可控）
   - **指纹**：计算 pHash，查询是否已处理过相同视频
   - **转码**：`FFmpeg` 提取音频（线程池，避免阻塞事件循环）
   - **ASR**：`Faster-Whisper` 识别语音为分段字幕（Semaphore 限流）
   - **存储**：字幕、指纹、任务状态写入 MySQL
4. 任一步骤失败触发重试，达到最大重试次数后标记为 `FAILED`。
5. 进程收到 `SIGTERM`/`SIGINT` 后优雅退出，并自动恢复 `PROCESSING` 状态的任务。

---

## 数据表说明

| 表名 | 用途 |
|------|------|
| `fingerprint` | 存储视频 pHash，用于去重 |
| `subtitle_segment` | 按时间轴分段的字幕（含开始/结束时间、文本、置信度） |
| `subtitle` | 视频完整字幕文本（聚合视图） |
| `task_status` | 任务状态追踪（`PENDING/PROCESSING/SUCCESS/FAILED`） |
| `sync_cursor` | 增量同步游标（Bilibili / Douyin 各自维护） |

---

## 开发

### 运行测试

```bash
uv run pytest
```

### 日志格式

系统使用 [structlog](https://www.structlog.org/) 输出结构化 JSON 日志，便于对接 ELK / Loki：

```json
{
  "event": "task_completed",
  "video_id": "123456",
  "platform": "bilibili",
  "download_time_ms": 2300,
  "transcode_time_ms": 450,
  "asr_time_ms": 12000,
  "total_time_ms": 15000,
  "is_duplicate": false,
  "segments_count": 42
}
```

---

## 许可证

[MIT](LICENSE)
