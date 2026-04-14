# Ext Agent Instructions

## Commands

```bash
cd ~/project/Ext

# 安装依赖
uv venv
uv sync

# 启动Redis
redis-server --daemonize yes

# 初始化数据库表
uv run python scripts/init_db.py

# 启动Worker (后台)
uv run python -m celery -A src.worker worker --loglevel=info --concurrency=2 &

# 启动Scheduler (前台)
uv run python -m src.main
```

## Config

- `config.yaml` - 所有配置 (crawler_db / result_db / redis / whisper)
- 注意: crawler_db 和 result_db 必须是同一个数据库实例（SQL中有跨表查询）

## Architecture

- **Pipeline**: 定时拉取 → 下载视频 → pHash指纹 → 去重检查 → 提取音频 → ASR识别 → 存储
- **Queues**: Redis (Celery broker/backend)
- **定时**: schedule (每1分钟, 可配置)
- **去重**: pHash相似度>90%
- **清理**: 自动清理temp/

## Project Layout

```
~/project/Ext/
├── config.yaml           # 配置文件
├── start.sh              # 一键启动脚本
├── install.sh            # 一键安装脚本
├── pyproject.toml        # Python依赖
├── src/
│   ├── __init__.py
│   ├── config.py         # 配置加载 (支持环境变量覆盖)
│   ├── main.py           # Scheduler 调度器
│   ├── worker.py         # Celery Worker 任务
│   ├── asr/
│   │   ├── __init__.py
│   │   └── whisper.py    # Faster-Whisper ASR
│   ├── fingerprint/
│   │   ├── __init__.py
│   │   └── phash.py      # pHash 指纹计算
│   └── db/
│       ├── __init__.py
│       ├── pool.py        # 数据库连接池 (支持多db_key)
│       ├── crawler.py     # 读取爬虫数据
│       └── storage.py     # 结果存储
├── scripts/
│   └── init_db.py         # 数据库初始化
├── migrations/
│   └── 001_init_tables.sql
└── tests/
```

## Features

- 离线ASR (Faster-Whisper)
- pHash视频指纹
- 自动去重 (相似度>90%)
- 防重复分发 (INSERT IGNORE status=0)
- 自动清理临时文件
- 多Worker并发

## Status Codes

| 值 | 含义 | 说明 |
|----|------|------|
| 0 | 处理中 | Scheduler已分发，Worker正在处理 |
| 1 | 成功 | 字幕提取成功 |
| 2 | 失败 | 处理失败(见error_msg) |
| 3 | 重复 | 相似视频，复制字幕 |
