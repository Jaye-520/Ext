#!/bin/bash
set -e

cd ~/progect/Ext

redis-server --daemonize yes 2>/dev/null || true

uv run python scripts/init_db.py

uv run celery -A src.main worker --loglevel=info --concurrency=2 &
uv run python -m src.main
