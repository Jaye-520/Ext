#!/bin/bash
set -e

cd "$(dirname "$0")"

pkill -f "celery.*worker" 2>/dev/null || true
pkill -f "src.main" 2>/dev/null || true
sleep 1

redis-server --daemonize yes 2>/dev/null || true

uv run python scripts/init_db.py

uv run python -m celery -A src.worker worker --loglevel=info --concurrency=2 &
uv run python -m src.main
