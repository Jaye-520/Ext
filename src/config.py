"""
配置加载模块

从 config.yaml 加载配置，支持默认值和环境变量覆盖
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class DBConfig:
    """数据库配置"""

    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "123456"
    database: str = "media_crawler_pro"


@dataclass
class RedisConfig:
    """Redis配置"""

    host: str = "localhost"
    port: int = 6379


@dataclass
class FasterWhisperConfig:
    """Whisper ASR配置"""

    model_size: str = "base"
    device: str = "cpu"


@dataclass
class SchedulerConfig:
    """调度器配置"""

    interval_minutes: int = 1
    batch_size: int = 10


@dataclass
class WorkerConfig:
    """Worker配置"""

    concurrency: int = 2


@dataclass
class Config:
    """全局配置"""

    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    crawler_db: DBConfig = field(default_factory=DBConfig)
    result_db: DBConfig = field(default_factory=DBConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    faster_whisper: FasterWhisperConfig = field(default_factory=FasterWhisperConfig)


def _apply_env_overrides(data: dict) -> dict:
    """
    应用环境变量覆盖配置

    环境变量格式:
    - DB_HOST -> db.host
    - DB_PORT -> db.port
    - DB_USER -> db.user
    - DB_PASSWORD -> db.password
    - DB_DATABASE -> db.database
    - REDIS_HOST -> redis.host
    - REDIS_PORT -> redis.port
    - WHISPER_MODEL -> faster_whisper.model_size
    - WHISPER_DEVICE -> faster_whisper.device
    """
    env_mapping = {
        "DB_HOST": ("crawler_db", "host"),
        "DB_PORT": ("crawler_db", "port"),
        "DB_USER": ("crawler_db", "user"),
        "DB_PASSWORD": ("crawler_db", "password"),
        "DB_DATABASE": ("crawler_db", "database"),
        "RESULT_DB_HOST": ("result_db", "host"),
        "RESULT_DB_PORT": ("result_db", "port"),
        "RESULT_DB_USER": ("result_db", "user"),
        "RESULT_DB_PASSWORD": ("result_db", "password"),
        "RESULT_DB_DATABASE": ("result_db", "database"),
        "REDIS_HOST": ("redis", "host"),
        "REDIS_PORT": ("redis", "port"),
        "WHISPER_MODEL": ("faster_whisper", "model_size"),
        "WHISPER_DEVICE": ("faster_whisper", "device"),
        "SCHEDULER_INTERVAL": ("scheduler", "interval_minutes"),
        "SCHEDULER_BATCH": ("scheduler", "batch_size"),
        "WORKER_CONCURRENCY": ("worker", "concurrency"),
    }

    for env_key, (section, field) in env_mapping.items():
        value = os.getenv(env_key)
        if value is not None:
            if section not in data:
                data[section] = {}
            # 类型转换
            if (
                field == "port"
                or field == "interval_minutes"
                or field == "batch_size"
                or field == "concurrency"
            ):
                data[section][field] = int(value)
            else:
                data[section][field] = value

    return data


def load_config(config_path: str = None) -> Config:
    """
    加载配置文件

    Args:
        config_path: 配置文件路径，默认为项目根目录的 config.yaml

    Returns:
        Config 配置对象
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config.yaml"

    if not Path(config_path).exists():
        # 如果配置文件不存在，使用默认配置
        return Config()

    with open(config_path) as f:
        data = yaml.safe_load(f)

    if data is None:
        data = {}

    # 应用环境变量覆盖
    data = _apply_env_overrides(data)

    return Config(
        scheduler=SchedulerConfig(**data.get("scheduler", {})),
        worker=WorkerConfig(**data.get("worker", {})),
        crawler_db=DBConfig(**data.get("crawler_db", {})),
        result_db=DBConfig(**data.get("result_db", {})),
        redis=RedisConfig(**data.get("redis", {})),
        faster_whisper=FasterWhisperConfig(**data.get("faster_whisper", {})),
    )


# 全局配置实例
config = load_config()
