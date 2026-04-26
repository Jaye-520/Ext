# 视频字幕自动化提取系统

离线优先的视频字幕自动化提取系统。支持从 Bilibili 和 Douyin 平台拉取视频任务，通过 ASR（自动语音识别）提取字幕，并支持视频指纹去重。

## 功能特性

- **Producer-Worker 架构**：Producer 轮询 MySQL 拉取任务，Worker 异步消费处理
- **可靠队列**：基于 Redis 的 BRPOPLPUSH 模式，支持崩溃恢复
- **分层并发控制**：下载（Semaphore）、转码（ThreadPool）、ASR（Semaphore）独立限流
- **视频指纹去重**：多帧 pHash（1s/3s/5s）合并，7 天内相似视频自动跳过
- **幂等写入**：任务状态表保证同一视频不重复处理
- **失败重试**：最多 3 次重试，超限后标记为 FAILED
- **背压控制**：队列长度超过阈值时 Producer 自动暂停拉取
- **结构化日志**：JSON 格式日志，支持 trace_id 全链路追踪

## 架构

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  MySQL      │────▶│  Producer   │────▶│ Redis       │
│ bilibili_   │     │  轮询拉取   │     │ task_queue  │
│ video /     │     │             │     │ processing_ │
│ douyin_     │     │             │     │ queue       │
│ aweme       │     │             │     │             │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │   Worker    │
                                        │             │
                                        │ download ──▶├─▶ fingerprint ──▶┐
                                        │             │                    │
                                        │ audio_extract▶┐                 │
                                        │             │   │               │
                                        │ ASR ────────▶│   │               │
                                        │             │   │               │
                                        │ storage ◀───┘◀──┘◀──────────────┘
                                        └─────────────┘
```

## 技术栈

- Python 3.11+
- [Faster-Whisper](https://github.com/SYSTRAN/faster-whisper) — ASR 引擎
- [aiomysql](https://github.com/aio-libs/aiomysql) — 异步 MySQL
- [redis-py](https://github.com/redis/redis-py) — 异步 Redis
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) — Bilibili DASH 下载
- [OpenCV](https://opencv.org/) + [imagehash](https://github.com/JohannesBuchner/imagehash) — 视频指纹
- FFmpeg — 音频提取
- [structlog](https://www.structlog.org/) — 结构化日志

## 系统依赖

除 Python 依赖外，还需要安装以下系统工具：

```bash
# Ubuntu / Debian
sudo apt-get update
sudo apt-get install -y ffmpeg

# macOS
brew install ffmpeg

# yt-dlp（用于 Bilibili 下载）
pip install yt-dlp
# 或
pipx install yt-dlp
```

另外需要运行中的 MySQL 和 Redis 服务。

## 安装

本项目使用 [uv](https://github.com/astral-sh/uv) 管理依赖：

```bash
cd video_subtitle_system
uv sync
```

或使用 pip：

```bash
cd video_subtitle_system
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## 配置

复制示例配置文件并修改：

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`：

```yaml
database:
  host: "127.0.0.1"
  port: 3306
  user: "root"
  password: "your_password"
  database: "video_subtitle"

redis:
  host: "127.0.0.1"
  port: 6379
  password: null        # 无密码可留空

asr:
  model_size: "small"   # tiny/base/small/medium/large
  hf_token: null        # HuggingFace token（可选，用于下载 gated 模型）

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

## 数据库初始化

1. 创建数据库：

```sql
CREATE DATABASE video_subtitle DEFAULT CHARSET=utf8mb4;
```

2. 导入项目表结构：

```bash
mysql -u root -p video_subtitle < sql/init.sql
```

3. **（重要）** 创建源数据表。Producer 会读取 `bilibili_video` 和 `douyin_aweme` 表，你需要确保这些表存在。参考 schema 见 `sql/source_tables.sql`（根据你的实际数据源调整）。

   如果暂时没有源数据，可以手动插入测试数据：

```sql
-- 示例：bilibili_video
CREATE TABLE IF NOT EXISTS bilibili_video (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    bvid VARCHAR(32) NOT NULL COMMENT 'BV号',
    video_url VARCHAR(512) NOT NULL COMMENT '视频链接',
    title VARCHAR(255) DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例：douyin_aweme
CREATE TABLE IF NOT EXISTS douyin_aweme (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aweme_id VARCHAR(64) NOT NULL COMMENT '抖音视频ID',
    video_download_url VARCHAR(1024) NOT NULL COMMENT '直链下载地址',
    desc_text VARCHAR(512) DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 启动

```bash
uv run python main.py
```

或使用 Python 直接运行：

```bash
.venv/bin/python main.py
```

启动后会自动：
1. 连接 MySQL 和 Redis
2. 加载 ASR 模型（首次会从 HuggingFace 下载）
3. 恢复崩溃前未完成的任务（`PROCESSING` → `PENDING`，`processing_queue` → `task_queue`）
4. 启动 Producer 和 Worker

按 `Ctrl+C` 可优雅关闭。

## 测试

```bash
uv run pytest
```

或：

```bash
.venv/bin/python -m pytest tests/ -v
```

## 项目结构

```
video_subtitle_system/
├── main.py                 # 守护进程入口
├── config.yaml             # 本地配置文件（gitignored）
├── config.example.yaml     # 配置示例
├── pyproject.toml          # Python 依赖声明
├── uv.lock                 # 依赖锁定
├── sql/
│   └── init.sql            # 项目表结构（fingerprint/subtitle/task_status 等）
├── src/
│   ├── config.py           # 配置加载
│   ├── db.py               # MySQL 连接池
│   ├── redis_client.py     # Redis 客户端
│   ├── cursor.py           # 双游标管理
│   ├── producer.py         # 任务生产者
│   ├── worker.py           # 任务消费者
│   ├── downloader.py       # 视频下载
│   ├── audio_extractor.py  # FFmpeg 音频提取
│   ├── fingerprint.py      # pHash 视频指纹
│   ├── asr_engine.py       # Faster-Whisper ASR
│   ├── storage.py          # 字幕存储 + 任务状态
│   └── logger.py           # 结构化日志
└── tests/
    └── test_integration.py # 集成测试
```

## 环境变量

| 变量 | 说明 | 示例 |
|------|------|------|
| `HF_TOKEN` | HuggingFace 访问令牌（下载 gated 模型时需要） | `hf_xxx` |
| `LOG_LEVEL` | 日志级别（代码中可扩展支持） | `INFO` / `DEBUG` |

## License

[MIT](../LICENSE)
