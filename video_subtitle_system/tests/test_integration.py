"""端到端冒烟测试"""
import pytest
import asyncio
from unittest.mock import MagicMock

from src.fingerprint import VideoFingerprint


def test_fingerprint_hamming_distance():
    hash1 = "0123456789abcdef"
    hash2 = "0123456789abcdee"

    distance = VideoFingerprint.hamming_distance(hash1, hash2)
    assert distance == 1

    distance2 = VideoFingerprint.hamming_distance(hash1, hash1)
    assert distance2 == 0


def test_storage_try_acquire():
    from src.storage import SubtitleStorage
    db = MagicMock()
    db.execute = asyncio.coroutine(lambda *a, **kw: 1)

    storage = SubtitleStorage(db)
    result = asyncio.get_event_loop().run_until_complete(
        storage.try_acquire("BV123", "bilibili")
    )
    assert result is True
