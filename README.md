# Ext - 抖音字幕提取 + 视频指纹去重

从抖音视频中提取字幕并计算视频指纹，支持自动去重(相似视频>90%)。

## 特性

- **离线ASR** - 使用Faster-Whisper，无需APIKey
- **pHash指纹** - 视频指纹去重，识别转发/搬运视频
- **自动拉取** - 从爬虫数据库自动获取视频URL
- **Celery并发** - 支持多Worker并行处理
- **自动清理** - 每次任务后自动清理临时文件
- **进程管理** - 自动杀掉旧的定时任务实例

## 环境要求

- Python 3.10+
- Redis
- MySQL
- FFmpeg

## 快速开始

### 1. 安装依赖

```bash
# 安装uv(如果没有)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 进入项目目录
cd ~/progect/Ext

# 创建虚拟环境并安装
uv venv
uv sync
```

### 2. 配置

```bash
# 编辑config.yaml，修改数据库密码等配置
vim config.yaml
```

config.yaml 配置说明：
```yaml
scheduler:
  interval_minutes: 5    # 轮询间隔(分钟)
  batch_size: 100         # 每次处理数量

worker:
  concurrency: 2        # Worker并发数
  asr_provider: "faster-whisper"

crawler_db:             # 爬虫数据库
  host: "localhost"
  port: 3306
  user: "root"
  password: "123456"
  database: "media_crawler_pro"

redis:
  host: "localhost"
  port: 6379

faster_whisper:
  model_size: "tiny"    # tiny/base/small/medium
  device: "cpu"
```

### 3. 初始化数据库

```bash
uv run python scripts/init_tables.py
```

### 4. 启动Redis(如果未运行)

```bash
redis-server --daemonize yes
```

### 5. 启动服务

```bash
# 终端1: 启动Worker
uv run celery -A ext.worker worker --loglevel=info --concurrency=2

# 终端2: 启动定时任务 (会自动杀掉之前的实例，每次执行后清理临时文件)
uv run python -m ext.scheduler
```

### 停止服务

```bash
# 停止Worker
pkill -f "celery.*ext.worker"

# 定时任务会在接收到信号后自动退出
# 或手动杀掉: pkill -f "ext.scheduler"
```

## 手动测试

```bash
# 发送单个任务
uv run python -c "
from ext.worker import process_video
process_video.delay('视频ID', '视频URL', '标题')
"
```

## 处理流程

### 整体流程

```
定时任务 (scheduler)
│ 每5分钟执行一次
│ ├─ 读取爬虫数据库 douyin_aweme
│ │   条件: video_download_url != ''
│ │ ├─ 自动杀掉之前的scheduler实例
│ │ └─ 清理temp/目录
│ └─ Celery队列 (Redis)
│     分发给Worker处理
└─ Worker处理
    ├─ 计算pHash指纹 (视频关键帧)
    ├─ 存储指纹 + 检查相似度 (>90%为重复)
    ├─ 下载视频 + 提取音频 (FFmpeg)
    ├─ ASR识别 (Faster-Whisper)
    └─ 存储字幕 + 清理临时文件
```

### Worker处理步骤

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | pHash指纹 | 从视频关键帧计算64位哈希 |
| 2 | 去重检查 | 相似度>90%则复制原字幕，跳过ASR |
| 3 | 提取音频 | FFmpeg从视频提取wav |
| 4 | ASR识别 | Faster-Whisper离线识别 |
| 5 | 存储+清理 | 存入数据库，清理临时文件 |

### 数据表关系

```
douyin_aweme (爬虫表)
│ 读取: video_download_url != ''
├─ dy_fingerprint (指纹表)
│   用途: 存储pHash，检查重复
└─ dy_subtitle (字幕表)
    状态: 0=待处理,1=成功,2=失败,3=重复
```

## 功能说明

### 1. 定时任务 (scheduler.py)

- 每隔指定分钟数执行一次(默认5分钟)
- 每次从爬虫数据库获取指定数量的视频(默认100条)
- 自动跳过已处理的视频
- 每次执行后清理临时文件目录(temp/)
- 再次启动时会自动杀掉之前的实例
- 记录PID到文件，方便管理

### 2. Worker处理 (worker.py)

- 从Celery队列接收任务
- 计算视频的pHash指纹
- 存储指纹到数据库
- 检查相似视频(>90%判定为重复)
- 下载视频并提取音频
- 使用Faster-Whisper进行ASR识别
- 存储字幕到数据库
- 清理临时文件

### 3. 去重机制

- 使用pHash感知哈希计算视频指纹
- 相似度计算公式: `similarity = 1 - (hamming_distance / 64)`
- 相似度>90%视为相同视频(转发/搬运)
- 检测到重复后自动复制原视频字幕，不重复识别
- 重复视频状态标记为3

### 4. 临时文件管理

- 视频下载到临时目录处理
- 音频提取到temp/audio/目录
- 处理完成后自动清理
- scheduler每次执行后也清理temp/目录

### 5. 进程管理

- PID保存到ext/scheduler.pid
- 启动时检查并杀掉旧实例
- 支持SIGINT/SIGTERM信号退出

## 数据库表说明

### dy_subtitle (字幕表)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 自增ID |
| aweme_id | VARCHAR(64) | 抖音视频ID |
| video_url | VARCHAR(1024) | 视频URL |
| fingerprint | VARCHAR(64) | pHash指纹 |
| language | VARCHAR(8) | 语言 |
| duration | FLOAT | 视频时长(秒) |
| subtitle_text | LONGTEXT | 完整字幕 |
| segments | JSON | 字幕分段(时间戳+文本) |
| confidence | FLOAT | 置信度 |
| status | TINYINT | 状态:0待处理,1成功,2失败,3重复 |
| error_msg | TEXT | 错误信息 |

### dy_fingerprint (指纹表)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 自增ID |
| aweme_id | VARCHAR(64) | 抖音视频ID |
| video_url | VARCHAR(1024) | 视频URL |
| phash | VARCHAR(64) | pHash指纹 |
| created_at | DATETIME | 创建时间 |

## 常见问题

### 1. Redis连接失败
```bash
# 检查Redis是否运行
redis-cli ping
# 启动Redis
redis-server --daemonize yes
```

### 2. 数据库连接失败
```bash
# 检查MySQL
mysql -uroot -p123456 -e "SELECT 1"
```

### 3. FFmpeg未安装
```bash
sudo apt install ffmpeg
```

### 4. 磁盘空间不足
临时文件在temp/目录下，处理完会自动清理。

### 5. 定时任务卡住
```bash
# 杀掉旧的scheduler进程
pkill -f "ext.scheduler"
# 重新启动
uv run python -m ext.scheduler
```

### 6. Worker不处理任务
```bash
# 检查Worker状态
ps aux | grep celery
# 重启Worker
pkill -f "celery.*ext.worker"
uv run celery -A ext.worker worker --loglevel=info --concurrency=2
```

## 性能

- 单Worker: ~10条/分钟(tiny模型)
- 2Worker并发: ~20条/分钟
- 可根据配置调整concurrency

## 项目结构

```
Ext/
├── README.md            # 文档
├── AGENTS.md          # Agent指令
├── config.yaml        # 配置文件
├── .env.example      # 配置模板
├── .gitignore      # Git忽略
├── pyproject.toml    # uv项目配置
├── ext/
│   ├── __init__.py
│   ├── config.py    # 配置加载
│   ├── worker.py    # Celery Worker
│   ├── scheduler.py # 定时任务
│   ├── crawler_db.py # 爬虫数据库
│   ├── storage.py   # 存储
│   ├── asr/
│   │   ├── base.py
│   │   ├── audio_extractor.py
│   │   └── faster_whisper.py
│   └── fingerprint/
│       └── phash.py
├── migrations/      # SQL脚本
└── scripts/        # 工具脚本
    └── init_tables.py
```

## 配置详解

### config.yaml 完整配置

```yaml
# 定时任务配置
scheduler:
  interval_minutes: 5    # 轮询间隔(分钟)，可修改
  batch_size: 100        # 每次处理数量

# Worker配置
worker:
  concurrency: 2         # Worker并发数
  asr_provider: "faster-whisper"  # ASR方案

# 爬虫数据库配置
crawler_db:
  host: "localhost"      # 数据库地址
  port: 3306            # 端口
  user: "root"          # 用户名
  password: "123456"     # 密码
  database: "media_crawler_pro"  # 数据库名

# Redis配置
redis:
  host: "localhost"
  port: 6379

# Faster-Whisper配置
faster_whisper:
  model_size: "tiny"     # 模型选择: tiny/base/small/medium/large
  device: "cpu"         # 设备: cpu/cuda
```

## License

MIT