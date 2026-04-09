"""配置加载模块"""

import os
import yaml
from functools import lru_cache
from pathlib import Path


class Settings:
    """应用配置"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.environ.get("CONFIG_PATH", "config.yaml")

        config_file = Path(__file__).parent.parent / config_path
        if not config_file.exists():
            config_file = Path.cwd() / config_path

        with open(config_file, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f)

    @property
    def scheduler_interval_minutes(self) -> int:
        return self._config.get("scheduler", {}).get("interval_minutes", 5)

    @property
    def scheduler_batch_size(self) -> int:
        return self._config.get("scheduler", {}).get("batch_size", 100)

    @property
    def worker_concurrency(self) -> int:
        return self._config.get("worker", {}).get("concurrency", 4)

    @property
    def asr_provider(self) -> str:
        return self._config.get("worker", {}).get("asr_provider", "faster-whisper")

    @property
    def crawler_db(self) -> dict:
        return self._config.get("crawler_db", {})

    @property
    def redis(self) -> dict:
        return self._config.get("redis", {})

    @property
    def faster_whisper(self) -> dict:
        return self._config.get("faster_whisper", {})

    @property
    def aliyun_asr(self) -> dict:
        return self._config.get("aliyun_asr", {})


@lru_cache
def get_settings() -> Settings:
    """获取配置实例（缓存）"""
    return Settings()
