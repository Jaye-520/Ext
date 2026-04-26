FROM python:3.11-slim

# 安装系统依赖：FFmpeg、git（faster-whisper 可能需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# 安装 yt-dlp
RUN pip install --no-cache-dir yt-dlp

WORKDIR /app

# 安装 Python 依赖
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev

# 复制代码
COPY src/ ./src/
COPY sql/ ./sql/
COPY main.py ./
COPY config.example.yaml ./

# 确保模型缓存目录存在
RUN mkdir -p /root/.cache/huggingface

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["uv", "run", "python", "main.py"]
