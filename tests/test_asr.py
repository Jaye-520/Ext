"""ASR模块测试"""

import pytest
from unittest.mock import patch, MagicMock
from src.asr.whisper import WhisperASR


class TestWhisperASR:
    """Whisper ASR测试"""

    def test_model_singleton(self):
        """验证模型单例"""
        WhisperASR._model = None
        model1 = WhisperASR.get_model("tiny", "cpu")
        model2 = WhisperASR.get_model("tiny", "cpu")
        assert model1 is model2

    def test_get_model_returns_same_instance(self):
        """多次调用get_model返回同一实例"""
        WhisperASR._model = None
        model1 = WhisperASR.get_model("tiny", "cpu")
        model2 = WhisperASR.get_model("tiny", "cpu")
        assert model1 is model2


class TestTranscribe:
    """转写功能测试"""

    @patch("src.asr.whisper.WhisperASR.get_model")
    @patch("src.asr.whisper.WhisperASR._convert_to_wav")
    def test_transcribe_returns_dict(self, mock_convert, mock_get_model):
        """验证转写返回正确格式的字典"""
        mock_model = MagicMock()
        mock_segments = [
            MagicMock(start=0.0, end=3.0, text="测试"),
            MagicMock(start=3.0, end=6.0, text="字幕"),
        ]
        mock_info = MagicMock(
            language="zh",
            duration=6.0,
        )
        mock_model.transcribe.return_value = (mock_segments, mock_info)
        mock_get_model.return_value = mock_model
        mock_convert.return_value = "/tmp/test.wav"

        result = WhisperASR.transcribe("/tmp/test.wav", "tiny", "cpu")

        assert "language" in result
        assert "duration" in result
        assert "segments" in result
        assert "text" in result
        assert "confidence" in result

    @patch("src.asr.whisper.WhisperASR.get_model")
    @patch("src.asr.whisper.WhisperASR._convert_to_wav")
    def test_transcribe_segments_format(self, mock_convert, mock_get_model):
        """验证segments格式正确"""
        mock_model = MagicMock()
        mock_segments = [
            MagicMock(start=0.0, end=3.0, text="第一段"),
            MagicMock(start=3.0, end=6.0, text="第二段"),
        ]
        mock_info = MagicMock(language="zh", duration=6.0)
        mock_model.transcribe.return_value = (mock_segments, mock_info)
        mock_get_model.return_value = mock_model
        mock_convert.return_value = "/tmp/test.wav"

        result = WhisperASR.transcribe("/tmp/test.wav", "tiny", "cpu")

        assert len(result["segments"]) == 2
        assert result["segments"][0]["text"] == "第一段"
        assert result["segments"][1]["text"] == "第二段"

    @patch("src.asr.whisper.WhisperASR.get_model")
    @patch("src.asr.whisper.WhisperASR._convert_to_wav")
    def test_transcribe_text_concatenation(self, mock_convert, mock_get_model):
        """验证text字段是所有segments的拼接"""
        mock_model = MagicMock()
        mock_segments = [
            MagicMock(start=0.0, end=3.0, text="第一段"),
            MagicMock(start=3.0, end=6.0, text="第二段"),
        ]
        mock_info = MagicMock(language="zh", duration=6.0)
        mock_model.transcribe.return_value = (mock_segments, mock_info)
        mock_get_model.return_value = mock_model
        mock_convert.return_value = "/tmp/test.wav"

        result = WhisperASR.transcribe("/tmp/test.wav", "tiny", "cpu")

        assert result["text"] == "第一段第二段"
