"""端到端冒烟测试"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager

from src.fingerprint import VideoFingerprint
from src.storage import SubtitleStorage


def test_fingerprint_hamming_distance():
    hash1 = "0123456789abcdef"
    hash2 = "0123456789abcdee"

    distance = VideoFingerprint.hamming_distance(hash1, hash2)
    assert distance == 1

    distance2 = VideoFingerprint.hamming_distance(hash1, hash1)
    assert distance2 == 0


async def test_storage_try_acquire():
    from src.db import Database

    db = MagicMock(spec=Database)
    # 第一次 fetch_one 返回 None（记录不存在），第二次返回 PROCESSING
    db.fetch_one = AsyncMock(side_effect=[None, {"status": "PROCESSING"}])
    db.execute = AsyncMock(return_value=1)

    storage = SubtitleStorage(db)
    result = await storage.try_acquire("BV123", "bilibili")
    assert result is True

    # 测试已 SUCCESS 的情况
    db.fetch_one = AsyncMock(return_value={"status": "SUCCESS"})
    result = await storage.try_acquire("BV123", "bilibili")
    assert result is False


async def test_storage_save_full_text():
    """验证 save() 正确拼接 full_text 并写入 subtitle 表"""
    segments = [
        {"start_time": 0.0, "end_time": 2.5, "text": "你好世界", "confidence": 0.95},
        {"start_time": 2.5, "end_time": 5.0, "text": "这是测试", "confidence": 0.88},
        {"start_time": 5.0, "end_time": 7.0, "text": "再见", "confidence": 1.0},
    ]

    executed_sql = []

    class MockCursor:
        async def execute(self, sql, args):
            executed_sql.append((sql, args))
            return 1

        async def executemany(self, sql, args):
            executed_sql.append((sql, args))
            return len(args)

    mock_cursor = MockCursor()

    @asynccontextmanager
    async def mock_transaction():
        yield None, mock_cursor

    class MockDB:
        @asynccontextmanager
        async def transaction(self):
            yield None, mock_cursor

    storage = SubtitleStorage(MockDB())
    await storage.save("BV123", "bilibili", segments, "abc123")

    assert len(executed_sql) >= 3, f"Expected >= 3 SQL calls, got {len(executed_sql)}"

    # 检查 subtitle 表写入（full_text 空格拼接，confidence_avg 正确）
    subtitle_calls = [(sql, args) for sql, args in executed_sql if "subtitle" in sql and "full_text" in sql]
    assert len(subtitle_calls) == 1, f"Expected 1 subtitle insert, got {len(subtitle_calls)}: {subtitle_calls}"
    sql, args = subtitle_calls[0]
    assert args[0] == "BV123"
    assert args[1] == "bilibili"
    assert args[2] == "你好世界 这是测试 再见", f"Got: {args[2]}"
    expected_avg = (0.95 + 0.88 + 1.0) / 3
    assert args[3] == pytest.approx(expected_avg), f"Got confidence_avg: {args[3]}"

    # 检查 fingerprint 和 subtitle_segment 也被写入
    fingerprint_calls = [args for sql, args in executed_sql if "fingerprint" in sql]
    assert len(fingerprint_calls) == 1
    segment_calls = [args for sql, args in executed_sql if "subtitle_segment" in sql]
    assert len(segment_calls) == 1


async def test_storage_save_empty_segments():
    """空片段时不应写入 subtitle 表"""
    executed_sql = []

    class MockCursor:
        async def execute(self, sql, args):
            executed_sql.append((sql, args))
            return 1

        async def executemany(self, sql, args):
            executed_sql.append((sql, args))
            return len(args)

    mock_cursor = MockCursor()

    class MockDB:
        @asynccontextmanager
        async def transaction(self):
            yield None, mock_cursor

    storage = SubtitleStorage(MockDB())
    await storage.save("BV123", "bilibili", [], "abc123")

    subtitle_calls = [args for sql, args in executed_sql if "subtitle" in sql and "full_text" in sql]
    assert len(subtitle_calls) == 0, f"Empty segments should not write to subtitle table, got: {subtitle_calls}"
    # fingerprint 仍应写入（查重用）
    fingerprint_calls = [args for sql, args in executed_sql if "fingerprint" in sql]
    assert len(fingerprint_calls) == 1


async def test_storage_save_empty_phash_skips_fingerprint():
    """p_hash 为空时不应写入 fingerprint 表（AV1 视频场景）"""
    segments = [
        {"start_time": 0.0, "end_time": 2.5, "text": "有字幕的内容", "confidence": 0.9},
    ]

    executed_sql = []

    class MockCursor:
        async def execute(self, sql, args):
            executed_sql.append((sql, args))
            return 1

        async def executemany(self, sql, args):
            executed_sql.append((sql, args))
            return len(args)

    mock_cursor = MockCursor()

    class MockDB:
        @asynccontextmanager
        async def transaction(self):
            yield None, mock_cursor

    storage = SubtitleStorage(MockDB())
    await storage.save("BV123", "bilibili", segments, "")

    # subtitle_segment 和 subtitle 应写入，但 fingerprint 不应写入
    fingerprint_calls = [args for sql, args in executed_sql if "fingerprint" in sql]
    assert len(fingerprint_calls) == 0, f"Empty p_hash should not write fingerprint, got: {fingerprint_calls}"
    subtitle_calls = [args for sql, args in executed_sql if "subtitle" in sql and "full_text" in sql]
    assert len(subtitle_calls) == 1, "subtitle should still be written"
    segment_calls = [args for sql, args in executed_sql if "subtitle_segment" in sql]
    assert len(segment_calls) == 1, "subtitle_segment should still be written"
