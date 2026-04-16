# 项目规范化设计

**日期**：2026-04-16
**状态**：草稿

---

## 一、目标

将 `video_subtitle_system` 打造为对外部贡献者友好的开源项目，主要改进：

1. 补全类型提示，降低 IDE 使用门槛
2. 拆分 `storage.py` 职责，简化测试 mock
3. 添加 pytest fixtures 支持独立模块测试
4. 搭建 GitHub Actions CI
5. 编写 CONTRIBUTING.md 规范 PR 流程

---

## 二、代码风格

**PEP 8** 全程遵守，重点检查项：

| 规则 | 示例 |
|------|------|
| 行长度 ≤ 79 | 折行使用 hanging indent |
| import 顺序 | `stdlib → third-party → local`，每组空行分隔 |
| 命名 | `module_name`, `ClassName`, `function_name`, `CONSTANT_NAME` |
| 空行 | 顶层函数/类之间空两行，方法之间空一行 |
| 空格 | `f(x)` 而非 `f( x )`，`x = 1` 而非 `x=1` |
| 注释 | 行内注释 `x = x + 1  # increment` 至少两空格 |

工具：`ruff`（兼容 flake8 + isort），配置文件 `ruff.toml`。

---

## 三、类型提示补全

### 3.1 优先级

**P0（必须）**：基础设施层，贡献者最常用
- `src/config.py` — 所有 dataclass 及 `load_config`
- `src/db.py` — `Database` 的所有 public 方法
- `src/redis_client.py` — `RedisClient` 的所有 public 方法
- `src/cursor.py` — `CursorManager` 及 `Cursor`

**P1（重要）**：业务层，直接被 Worker/Producer 调用
- `src/storage.py` — `SubtitleStorage` 的所有方法
- `src/fingerprint.py` — `VideoFingerprint` 的所有方法
- `src/downloader.py` — `VideoDownloader.download` 返回值

**P2（建议）**：可延后，逻辑简单
- `src/asr_engine.py` — `ASREngine.recognize` 返回值
- `src/audio_extractor.py` — `AudioExtractor` 的两个 extract 方法
- `src/producer.py` — `Producer.poll` 返回值
- `src/worker.py` — `Worker._process_task`

### 3.2 类型提示示例

```python
# src/config.py 示例
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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

def load_config(config_path: Optional[str] = None) -> Config:
    ...
```

---

## 四、storage.py 拆分方案

### 4.1 当前问题

`SubtitleStorage` 承担了三种不同职责：
1. `try_acquire` — 原子抢占锁（PROCESSING）
2. `save` — 事务写入（fingerprint + subtitle_segment + subtitle + task_status）
3. `mark_failed` / `reset_processing_to_pending` — 状态管理

这导致难以独立 mock：测试 `save` 必须完整模拟整个事务。

### 4.2 拆分方案

拆分为三个独立接口，职责单一：

```python
# src/repositories.py — 新文件

from abc import ABC, abstractmethod
from typing import List, Dict, Any

class TaskStatusRepository(ABC):
    """任务状态抢占与更新"""

    @abstractmethod
    async def try_acquire(self, video_id: str, platform: str) -> bool:
        """原子抢占 PROCESSING 锁。返回 True 表示成功，False 表示已是 SUCCESS。"""
        ...

    @abstractmethod
    async def mark_success(self, video_id: str, platform: str) -> None:
        ...

    @abstractmethod
    async def mark_failed(self, video_id: str, platform: str, error_msg: str) -> None:
        ...

    @abstractmethod
    async def reset_processing_to_pending(self) -> None:
        ...


class FingerprintRepository(ABC):
    """指纹存储与查重"""

    @abstractmethod
    async def save(self, video_id: str, platform: str, p_hash: str) -> None:
        ...

    @abstractmethod
    async def find_similar(self, p_hash: str, threshold: int = 5) -> bool:
        """查询相似指纹，返回是否存在重复"""
        ...


class SubtitleRepository(ABC):
    """字幕写入"""

    @abstractmethod
    async def save_segments(
        self,
        video_id: str,
        platform: str,
        segments: List[Dict[str, Any]],
    ) -> None:
        """批量写入字幕分段 + 更新 subtitle 表的 full_text"""
        ...
```

### 4.3 实现保留兼容

`SubtitleStorage` 继承三个接口，提供完整实现：

```python
# src/storage.py

class SubtitleStorage(TaskStatusRepository, FingerprintRepository, SubtitleRepository):
    """保留完整实现，供主程序使用"""
    ...
```

**优点：**
- 单元测试可 mock 单一接口，不需要模拟完整事务
- 贡献者清楚知道每个方法属于哪个概念领域
- 接口即文档，降低理解成本

---

## 五、pytest Fixtures 设计

### 5.1 目录结构

```
video_subtitle_system/tests/
├── __init__.py
├── conftest.py          # 共享 fixtures
├── test_storage.py      # 存储层单元测试
├── test_fingerprint.py  # 指纹层单元测试
├── test_producer.py     # Producer 单元测试
└── test_worker.py        # Worker 单元测试（可 mock 所有依赖）
```

### 5.2 conftest.py Fixtures

```python
# tests/conftest.py
import pytest
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager

@pytest.fixture
def mock_db():
    """模拟 Database，无真实 MySQL 连接"""
    db = MagicMock()
    db.execute = AsyncMock(return_value=1)
    db.fetch_one = AsyncMock(return_value=None)
    db.fetch_all = AsyncMock(return_value=[])
    db.executemany = AsyncMock(return_value=0)

    @asynccontextmanager
    async def mock_transaction():
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock(return_value=1)
        mock_cursor.executemany = AsyncMock(return_value=0)
        yield mock_conn, mock_cursor

    db.transaction = mock_transaction
    return db


@pytest.fixture
def mock_redis():
    """模拟 RedisClient，无真实 Redis 连接"""
    redis = MagicMock()
    redis.lpush = AsyncMock(return_value=1)
    redis.lrem = AsyncMock(return_value=1)
    redis.brpoplpush = AsyncMock(return_value=None)
    redis.rpoplpush = AsyncMock(return_value=None)
    redis.llen = AsyncMock(return_value=0)
    return redis


@pytest.fixture
def mock_storage():
    """模拟 SubtitleStorage"""
    storage = MagicMock()
    storage.try_acquire = AsyncMock(return_value=True)
    storage.save = AsyncMock()
    storage.mark_failed = AsyncMock()
    return storage


@pytest.fixture
def mock_downloader():
    """模拟 VideoDownloader"""
    from pathlib import Path
    downloader = MagicMock()
    downloader.download = AsyncMock(return_value=Path("/tmp/test.mp4"))
    return downloader


@pytest.fixture
def mock_fingerprint():
    """模拟 VideoFingerprint"""
    fingerprint = MagicMock()
    fingerprint.compute = AsyncMock(return_value="abc123")
    fingerprint.is_duplicate = AsyncMock(return_value=False)
    return fingerprint


@pytest.fixture
def mock_asr_engine():
    """模拟 ASREngine"""
    asr = MagicMock()
    asr.recognize = AsyncMock(return_value=[
        {"start_time": 0.0, "end_time": 2.0, "text": "测试", "confidence": 0.95}
    ])
    return asr
```

### 5.3 测试示例

```python
# tests/test_storage.py
import pytest
from src.storage import SubtitleStorage

@pytest.mark.asyncio
async def test_try_acquire_returns_true_when_not_success(mock_db):
    mock_db.execute = AsyncMock(return_value=1)
    storage = SubtitleStorage(mock_db)
    result = await storage.try_acquire("BV123", "bilibili")
    assert result is True


@pytest.mark.asyncio
async def test_try_acquire_returns_false_when_already_success(mock_db):
    mock_db.execute = AsyncMock(return_value=0)  # ON DUPLICATE KEY 未插入
    storage = SubtitleStorage(mock_db)
    result = await storage.try_acquire("BV123", "bilibili")
    assert result is False
```

---

## 六、CI 工作流

### 6.1 GitHub Actions 配置

```yaml
# .github/workflows/ci.yml

name: CI

on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: |
          cd video_subtitle_system
          uv sync --dev

      - name: Lint with ruff
        run: |
          cd video_subtitle_system
          uv run ruff check src/

      - name: Check format with ruff
        run: |
          cd video_subtitle_system
          uv run ruff format --check src/

      - name: Run tests
        run: |
          cd video_subtitle_system
          uv run pytest -v --tb=short

  typecheck:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: |
          cd video_subtitle_system
          uv sync

      - name: Type check with pyright
        run: |
          cd video_subtitle_system
          uv run pyright src/
```

### 6.2 ruff.toml 配置

```toml
# video_subtitle_system/ruff.toml
line-length = 79
target-version = "py311"

[lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4"]
ignore = ["E501"]  # line-length 由 formatter 处理

[lint.isort]
known-first-party = ["src"]
```

---

## 七、CONTRIBUTING.md 框架

```markdown
# Contributing to video-subtitle-system

## 开发环境

```bash
cd video_subtitle_system
uv sync --dev
```

## 代码风格

- Python ≥ 3.11
- **PEP 8**，行长度 ≤ 79
- 使用 `ruff check src/` 检查风格
- 使用 `ruff format src/` 自动格式化

## 类型提示

所有新代码必须包含类型提示。使用 `pyright` 验证：

```bash
uv run pyright src/
```

## 测试

```bash
# 运行所有测试
uv run pytest

# 运行单个测试文件
uv run pytest tests/test_storage.py

# 运行单个测试
uv run pytest tests/test_storage.py::test_try_acquire_returns_true_when_not_success

# 带覆盖率
uv run pytest --cov=src --cov-report=term-missing
```

## 提交规范

- 使用 clear 的 commit message
- PR 标题格式：`type: short description`
- type：`feat`, `fix`, `refactor`, `test`, `docs`, `chore`

## PR 流程

1. Fork 仓库
2. 创建特性分支 `git checkout -b feat/your-feature`
3. 编写代码，添加测试
4. 确保所有测试通过：`uv run pytest`
5. 确保 lint 通过：`uv run ruff check src/`
6. 提交并 Push
7. 打开 Pull Request

## 评审标准

- 类型提示完整
- 有测试覆盖
- 遵守 PEP 8
- 日志清晰，无硬编码敏感信息
```

---

## 八、实施顺序

### Phase 1：工具链 + 文档（独立于代码改动）

1. 添加 `ruff.toml`
2. 添加 `.github/workflows/ci.yml`
3. 编写 `CONTRIBUTING.md`
4. 添加 `conftest.py`

### Phase 2：类型提示补全

按 P0 → P1 → P2 顺序逐模块添加类型提示，每完成一个模块跑一次 `ruff check` + `pyright`。

### Phase 3：storage.py 重构

1. 新建 `repositories.py`，定义三个接口
2. 实现 `TaskStatusRepositoryImpl`、`FingerprintRepositoryImpl`、`SubtitleRepositoryImpl`
3. `SubtitleStorage` 继承三个接口
4. 添加对应单元测试

### Phase 4：收尾

1. 更新 `CLAUDE.md` 中的模块 Map（加入 `repositories.py`）
2. 确保 CI 全部 green
3. 本地完整跑一次 `uv run pytest`

---

## 九、风险评估

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| 类型提示补全引入 bug | 低 | 中 | CI 有 pytest 保护，每个模块单独验证 |
| storage.py 重构影响现有逻辑 | 低 | 高 | 仅拆分接口，不改实现逻辑；添加 test_storage.py 覆盖 |
| ruff format 大面积改格式 | 中 | 低 | 是一次性改动，可 review |
