# Ext - 抖音字幕提取工具

自动从爬虫数据库读取抖音视频，提取字幕并存储，支持视频指纹去重。

## 特性

- **自动监控**: 持续监控爬虫数据库，新视频自动处理
- **字幕提取**: Faster-Whisper 离线 ASR，无需云 API
- **视频去重**: pHash 指纹相似度检测 (>90% 判定重复)
- **防重复分发**: Scheduler 分发前先标记处理中状态，避免重复处理
- **一键部署**: `./install.sh` 自动安装所有依赖

## 快速开始

### 方式一：一键安装 (推荐)

```bash
cd ~/progect/Ext
./install.sh
```

脚本会自动检测并安装：
- Python >= 3.10
- uv 包管理器
- Redis
- FFmpeg

### 方式二：手动安装

```bash
# 1. 安装依赖
sudo apt install redis-server ffmpeg

# 2. 安装 Python 依赖
uv sync

# 3. 配置 (编辑 config.yaml)

# 4. 初始化数据库
uv run python scripts/init_db.py

# 5. 启动
./start.sh
```

## 项目结构

```
Ext/
├── src/
│   ├── config.py           # 配置加载 (支持环境变量)
│   ├── main.py             # Scheduler 调度器
│   ├── worker.py           # Celery Worker 任务
│   ├── asr/
│   │   └── whisper.py      # Faster-Whisper ASR
│   ├── fingerprint/
│   │   └── phash.py       # pHash 指纹计算
│   └── db/
│       ├── pool.py         # 数据库连接池
│       ├── crawler.py      # 读取爬虫数据
│       └── storage.py      # 结果存储
├── scripts/
│   └── init_db.py          # 数据库初始化
├── tests/                   # 单元测试
├── install.sh              # 一键安装脚本
├── start.sh                # 启动脚本
└── config.yaml             # 配置文件
```

## 配置

编辑 `config.yaml`:

```yaml
scheduler:
  interval_minutes: 1      # 调度间隔 (分钟)
  batch_size: 10           # 每批处理数量

worker:
  concurrency: 2           # Worker 并发数

crawler_db:                 # 爬虫数据库 (只读)
  host: localhost
  port: 3306
  user: root
  password: '123456'
  database: media_crawler_pro

result_db:                 # 结果数据库
  host: localhost
  port: 3306
  user: root
  password: '123456'
  database: media_crawler_pro

redis:
  host: localhost
  port: 6379

faster_whisper:
  model_size: base         # tiny/base/small/medium/large
  device: cpu
```

### 环境变量覆盖

配置项可通过环境变量覆盖:

| 环境变量 | 说明 |
|----------|------|
| `DB_HOST` | 数据库主机 |
| `DB_PORT` | 数据库端口 |
| `DB_USER` | 数据库用户 |
| `DB_PASSWORD` | 数据库密码 |
| `DB_DATABASE` | 数据库名 |
| `REDIS_HOST` | Redis 主机 |
| `REDIS_PORT` | Redis 端口 |
| `WHISPER_MODEL` | Whisper 模型大小 |
| `SCHEDULER_INTERVAL` | 调度间隔 |

## 数据表

### dy_subtitle (字幕结果)

| 字段 | 类型 | 说明 |
|------|------|------|
| aweme_id | VARCHAR(64) | 视频ID (主键) |
| video_url | VARCHAR(1024) | 视频URL |
| fingerprint | VARCHAR(64) | pHash 指纹 |
| subtitle_text | LONGTEXT | 完整字幕 |
| segments | JSON | 字幕分段 |
| duration | FLOAT | 视频时长 |
| confidence | FLOAT | 置信度 |
| status | TINYINT | 状态码 |
| error_msg | TEXT | 错误信息 |

### dy_fingerprint (视频指纹)

| 字段 | 类型 | 说明 |
|------|------|------|
| aweme_id | VARCHAR(64) | 视频ID (主键) |
| video_url | VARCHAR(1024) | 视频URL |
| phash | VARCHAR(64) | pHash 指纹 |

### 状态码 (status)

| 值 | 含义 | 说明 |
|----|------|------|
| 0 | 处理中 | Scheduler 已分发，Worker 正在处理 |
| 1 | 成功 | 字幕提取成功 |
| 2 | 失败 | 处理失败 |
| 3 | 重复 | 相似视频，复制字幕 |

## 处理流程

```
┌─────────────────────────────────────────────────────────────┐
│                     Scheduler (每 N 分钟)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
          ┌─────────────────────────────┐
          │ fetch_pending_videos()     │
          │ status NOT IN (0,1,3)      │
          └─────────────────────────────┘
                       │
                       ▼
          ┌─────────────────────────────┐
          │ mark_as_processing()        │  ◄── 标记 status=0
          │ INSERT IGNORE              │      防止重复分发
          └─────────────────────────────┘
                       │
              ┌────────┴────────┐
              │                 │
           成功              失败(已存在)
              │                 │
              ▼                 ▼
       ┌───────────┐     ┌───────────┐
       │Celery任务 │     │   skip   │
       └─────┬─────┘     └───────────┘
             │
             ▼
       ┌─────────────────────────────┐
       │      Worker 处理中          │
       │ status=0 (处理中)          │
       └───────────────────────────┘
             │
             ▼
       ┌─────────────────────────────┐
       │  1. 计算 pHash 指纹        │
       │  2. 去重检测               │
       │  3. 下载视频               │
       │  4. 提取音频               │
       │  5. ASR 转写               │
       │  6. 保存结果               │
       └─────────────────────────────┘
                       │
                       ▼
       ┌─────────────────────────────┐
       │  完成: status=1            │
       │  失败: status=2            │
       │  重复: status=3            │
       └─────────────────────────────┘
```

## 开发

```bash
# 安装依赖
uv sync

# 运行测试
uv run pytest tests/

# 运行特定测试
uv run pytest tests/test_phash.py -v
```

## 启动服务

```bash
# 一键启动 (Redis + Worker + Scheduler)
./start.sh

# 或者手动启动
redis-server --daemonize yes
uv run celery -A src.main worker --loglevel=info --concurrency=2 &
uv run python -m src.main
```

## 查看日志

```bash
tail -f logs/scheduler.log
tail -f logs/worker.log
```
