"""pHash指纹模块测试"""

import pytest
from src.fingerprint.phash import (
    compute_similarity,
    find_similar,
    VideoFingerprint,
)


class TestComputeSimilarity:
    """相似度计算测试"""

    def test_identical_phash_returns_one(self):
        """相同指纹的相似度应为1.0"""
        phash = "aabbccddeeff0011"
        assert compute_similarity(phash, phash) == 1.0

    def test_all_same_bits_returns_one(self):
        """全1的指纹相似度为1.0"""
        s1 = "ffffffffffffffff"
        s2 = "ffffffffffffffff"
        assert compute_similarity(s1, s2) == 1.0

    def test_one_bit_difference(self):
        """差1位的相似度应该在0.9到1.0之间"""
        s1 = "ffffffffffffffff"
        s2 = "fffffffffffffffe"
        sim = compute_similarity(s1, s2)
        assert 0.9 < sim < 1.0

    def test_completely_different(self):
        """完全不同的指纹相似度较低"""
        s1 = "0000000000000000"
        s2 = "ffffffffffffffff"
        sim = compute_similarity(s1, s2)
        assert 0.0 <= sim < 0.5

    def test_similarity_is_symmetric(self):
        """相似度计算是对称的"""
        s1 = "aabbccddeeff0011"
        s2 = "aabbccddeeff0022"
        assert compute_similarity(s1, s2) == compute_similarity(s2, s1)


class TestFindSimilar:
    """相似视频查找测试"""

    def test_exact_match_found(self):
        """完全匹配应该被找到"""
        candidates = [
            {"aweme_id": "video1", "phash": "aabbccddeeff0011"},
            {"aweme_id": "video2", "phash": "1122334455667788"},
        ]
        result, sim = find_similar("aabbccddeeff0011", candidates)
        assert result == "video1"
        assert sim == 1.0

    def test_similar_match_found(self):
        """相似匹配应该被找到"""
        candidates = [
            {"aweme_id": "video1", "phash": "aabbccddeeff0011"},
        ]
        # 相似度 > 90% 应该匹配
        result, sim = find_similar("aabbccddeeff0012", candidates)
        assert result == "video1"
        assert sim >= 0.90

    def test_no_match_below_threshold(self):
        """低于阈值不匹配"""
        candidates = [
            {"aweme_id": "video1", "phash": "0000000000000000"},
        ]
        # 相似度 < 90% 不应该匹配
        result, sim = find_similar("ffffffffffffffff", candidates)
        assert result is None
        assert sim == 0.0

    def test_empty_candidates(self):
        """空候选列表返回None"""
        result, sim = find_similar("aabbccddeeff0011", [])
        assert result is None
        assert sim == 0.0

    def test_multiple_candidates_returns_best_match(self):
        """多个候选时返回相似度最高的"""
        candidates = [
            {"aweme_id": "video_far", "phash": "0000000000000000"},
            {"aweme_id": "video_close", "phash": "aabbccddeeff0011"},
            {"aweme_id": "video_closest", "phash": "aabbccddeeff0010"},
        ]
        result, sim = find_similar("aabbccddeeff0010", candidates)
        assert result == "video_closest"
        assert sim >= 0.90


class TestVideoFingerprint:
    """视频指纹计算测试"""

    def test_headers_defined(self):
        """HEADERS应该正确定义"""
        fp = VideoFingerprint()
        assert hasattr(fp, "temp_dir")

    def test_temp_dir_created(self, tmp_path, monkeypatch):
        """临时目录应该被创建"""
        # 临时修改temp_dir到测试目录
        monkeypatch.setattr(
            VideoFingerprint,
            "__init__",
            lambda self: setattr(self, "temp_dir", tmp_path),
        )
        fp = VideoFingerprint()
        fp.temp_dir.mkdir(exist_ok=True)
        assert fp.temp_dir.exists()
