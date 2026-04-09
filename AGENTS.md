# Ext Agent Instructions

## Commands

```bash
cd ~/progect/Ext

# 安装
uv venv
uv sync

# 启动Redis
redis-server --daemonize yes

# 初始化数据库表
uv run python scripts/init_tables.py

# 启动Worker
uv run celery -A ext.worker worker --loglevel=info --concurrency=2

# 启动定时任务 (自动杀旧实例 + 清理临时文件)
uv run python -m ext.scheduler
```

## Config

- `config.yaml` - 所有配置

## Architecture

- **Pipeline**: 定时拉取 → pHash指纹 → 去重检查 → ASR识别 → 存储
- **Queues**: Redis
- **定时**: schedule (每5分钟)
- **去重**: pHash相似度>90%
- **清理**: 自动清理temp/

## Project Layout

```
~/progect/Ext/
├── config.yaml
├── README.md
├── AGENTS.md
├── ext/
│   ├── asr/              # ASR模块
│   ├── fingerprint/       # 指纹模块
│   ├── worker.py         # Celery Worker
│   ├── scheduler.py     # 定时任务
│   ├── crawler_db.py   # 爬虫数据库
│   ├── storage.py     # 存储
│   └── config.py    # 配置
├── migrations/        # SQL
└── scripts/          # 工具脚本
```

## Features

- 离线ASR (Faster-Whisper)
- pHash视频指纹
- 自动去重 (相似度>90%)
- 自动清理临时文件
- 自动杀旧scheduler进程
- 多Worker并发