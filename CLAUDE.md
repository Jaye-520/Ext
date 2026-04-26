# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is an offline-first video subtitle extraction system (视频字幕自动化提取系统) that runs as a daemon process. It polls new video tasks from MySQL, processes them through a pipeline (download → audio extract → ASR), and stores subtitle segments.

Supports Douyin and Bilibili video sources. Uses pHash-based deduplication before ASR to save compute.

## Running

```bash
# Install dependencies (uses uv)
cd video_subtitle_system
uv sync

# Run the daemon
uv run python main.py

# Run tests
uv run pytest
uv run pytest tests/test_integration.py::test_fingerprint_hamming_distance  # single test
```

## Architecture

```
Producer (polls DB) → Redis Queue (task_queue) → Worker (asyncio pool)
                                                          ↓
                                              download → fingerprint →
                                              audio_extract → ASR → storage
```

- **Producer** (`src/producer.py`): Dual-cursor polling from `bilibili_video` and `douyin_aweme` tables, pushes to `task_queue`. Backpressure when queue length > threshold.
- **Worker** (`src/worker.py`): BRPOPLPUSH reliable queue pattern. Three-stage pipeline with layered concurrency (download_sem, transcode_pool, asr_sem). Exponential backoff on idle.
- **Reliable queue**: `task_queue` → `processing_queue` (BRPOPLPUSH). Tasks moved atomically; on failure re-enqueued from `processing_queue`.
- **Crash recovery** on startup: `processing_queue` drained back to `task_queue`; `PROCESSING` status reset to `PENDING`.

## Module Map

| File | Responsibility |
|------|----------------|
| `src/config.py` | YAML config → dataclasses |
| `src/logger.py` | structlog + trace_id via contextvars |
| `src/db.py` | aiomysql pool with transaction context manager |
| `src/redis_client.py` | redis.asyncio with JSON serialization |
| `src/cursor.py` | Dual cursor (`bilibili_last_id`, `douyin_last_id`) persisted in `sync_cursor` table |
| `src/storage.py` | Idempotent acquire (ON DUPLICATE KEY), batch subtitle insert |
| `src/downloader.py` | Douyin: streaming HTTP; Bilibili: yt-dlp DASH |
| `src/audio_extractor.py` | FFmpeg → 16kHz mono WAV (temp files, cleaned up in finally) |
| `src/fingerprint.py` | Multi-frame pHash (frames at 1s, 3s, 5s merged via bitwise AND) |
| `src/asr_engine.py` | Faster-Whisper with VAD filter, confidence per segment |
| `main.py` | Daemon entry: startup recovery, graceful shutdown (SIGTERM/SIGINT) |

## Database Tables (MySQL `video_subtitle`)

- `bilibili_video` / `douyin_aweme` — source tables (read-only, polled by Producer)
- `fingerprint` — pHash for deduplication (last 1000 queried for similarity)
- `subtitle_segment` — ASR output with start/end time and confidence
- `task_status` — PENDING/PROCESSING/SUCCESS/FAILED with retry count
- `sync_cursor` — bilibili/douyin polling position (id=1 row)

## Concurrency Model

| Stage | Concurrency | Mechanism |
|-------|-------------|-----------|
| Download | 6 | `asyncio.Semaphore` |
| Transcode | 4 | `ThreadPoolExecutor` (FFmpeg subprocess) |
| ASR | 2 | `asyncio.Semaphore` |
