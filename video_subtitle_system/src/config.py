"""配置加载模块"""
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import yaml


@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class RedisConfig:
    host: str
    port: int
    password: Optional[str] = None


@dataclass
class ASRConfig:
    model_size: str = "small"
    hf_token: Optional[str] = None


@dataclass
class ConcurrencyConfig:
    download: int = 6
    transcode: int = 4
    asr: int = 2


@dataclass
class PollConfig:
    interval_seconds: int = 5
    batch_size: int = 100


@dataclass
class AppConfig:
    backpressure_threshold: int = 1000
    max_retries: int = 3


@dataclass
class Config:
    database: DatabaseConfig
    redis: RedisConfig
    asr: ASRConfig
    concurrency: ConcurrencyConfig
    poll: PollConfig
    app: AppConfig


def _validate_database(cfg: dict) -> dict:
    cfg = dict(cfg)
    cfg["port"] = int(cfg.get("port", 3306))
    return cfg


def load_config(config_path: Optional[str] = None) -> Config:
    """加载配置文件"""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config.yaml"

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    if not raw or "database" not in raw or "redis" not in raw:
        raise ValueError("config file must contain 'database' and 'redis' sections")

    return Config(
        database=DatabaseConfig(**_validate_database(raw["database"])),
        redis=RedisConfig(**raw["redis"]),
        asr=ASRConfig(**raw.get("asr", {})),
        concurrency=ConcurrencyConfig(**raw.get("concurrency", {})),
        poll=PollConfig(**raw.get("poll", {})),
        app=AppConfig(**raw.get("app", {})),
    )
