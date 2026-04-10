"""存储模块测试"""

import pytest
from unittest.mock import patch, MagicMock
from src.db.storage import (
    save_subtitle_sync,
    get_subtitle_by_aweme_id_sync,
    save_fingerprint_sync,
    copy_subtitle_sync,
)


class TestSaveSubtitleSync:
    """字幕保存测试"""

    @patch("src.db.storage.get_pool_sync")
    def test_save_subtitle_calls_execute(self, mock_get_pool):
        """验证save_subtitle_sync正确执行SQL"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_pool.return_value = mock_conn

        save_subtitle_sync(
            aweme_id="test123",
            video_url="http://test.com/video.mp4",
            fingerprint="abc123",
            subtitle_text="测试字幕",
            segments=[{"start": 0, "end": 3, "text": "测试字幕"}],
            duration=3.0,
            confidence=0.9,
            status=1,
        )

        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()


class TestGetSubtitleByAwemeIdSync:
    """字幕查询测试"""

    @patch("src.db.storage.get_pool_sync")
    def test_get_existing_subtitle(self, mock_get_pool):
        """验证查询已存在的字幕"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "aweme_id": "test123",
            "subtitle_text": "测试字幕",
            "status": 1,
        }
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_pool.return_value = mock_conn

        result = get_subtitle_by_aweme_id_sync("test123")

        assert result["aweme_id"] == "test123"
        assert result["subtitle_text"] == "测试字幕"

    @patch("src.db.storage.get_pool_sync")
    def test_get_nonexistent_subtitle(self, mock_get_pool):
        """查询不存在的字幕返回None"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_pool.return_value = mock_conn

        result = get_subtitle_by_aweme_id_sync("nonexistent")

        assert result is None


class TestSaveFingerprintSync:
    """指纹保存测试"""

    @patch("src.db.storage.get_pool_sync")
    def test_save_fingerprint_calls_execute(self, mock_get_pool):
        """验证save_fingerprint_sync正确执行SQL"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_pool.return_value = mock_conn

        save_fingerprint_sync(
            aweme_id="test123",
            video_url="http://test.com/video.mp4",
            phash="abc123def456",
        )

        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()


class TestCopySubtitleSync:
    """字幕复制测试"""

    @patch("src.db.storage.save_subtitle_sync")
    @patch("src.db.storage.get_subtitle_by_aweme_id_sync")
    def test_copy_existing_subtitle(self, mock_get, mock_save):
        """验证复制已存在的字幕"""
        mock_get.return_value = {
            "aweme_id": "source123",
            "subtitle_text": "原始字幕",
            "segments": '[{"start": 0, "end": 3, "text": "原始字幕"}]',
            "duration": 3.0,
            "confidence": 0.9,
        }

        result = copy_subtitle_sync(
            from_aweme_id="source123",
            to_aweme_id="target456",
            video_url="http://test.com/target.mp4",
            fingerprint="target_phash",
        )

        assert result is True
        mock_save.assert_called_once()

    @patch("src.db.storage.get_subtitle_by_aweme_id_sync")
    def test_copy_nonexistent_returns_false(self, mock_get):
        """复制不存在的字幕返回False"""
        mock_get.return_value = None

        result = copy_subtitle_sync(
            from_aweme_id="nonexistent",
            to_aweme_id="target456",
            video_url="http://test.com/target.mp4",
            fingerprint="target_phash",
        )

        assert result is False
