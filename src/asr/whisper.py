import ffmpeg
from faster_whisper import WhisperModel
from typing import Dict
import tempfile
import os
import mimetypes


class WhisperASR:
    _model = None

    @classmethod
    def get_model(cls, model_size: str = "base", device: str = "cpu"):
        if cls._model is None:
            cls._model = WhisperModel(model_size, device=device)
        return cls._model

    @classmethod
    def transcribe(
        cls, audio_path: str, model_size: str = "base", device: str = "cpu"
    ) -> Dict:
        model = cls.get_model(model_size, device)
        # 如果输入已经是WAV格式(16kHz mono)，直接使用，否则先转换
        wav_path = audio_path if cls._is_target_wav(audio_path) else cls._convert_to_wav(audio_path)
        try:
            segments_generator, info = model.transcribe(wav_path, language="zh")
            # 注意: segments 是 generator，需要先转成 list
            result_segments = []
            for seg in segments_generator:
                result_segments.append(
                    {"start": seg.start, "end": seg.end, "text": seg.text, "avg_logprob": seg.avg_logprob}
                )
            text = "".join(s["text"] for s in result_segments)
            # 计算平均置信度
            avg_confidence = (
                sum(s["avg_logprob"] for s in result_segments) / len(result_segments)
                if result_segments
                else 0.0
            )
            return {
                "language": info.language,
                "duration": info.duration,
                "segments": result_segments,
                "confidence": round(max(0, avg_confidence), 4),
                "text": text,
            }
        finally:
            if wav_path != audio_path and os.path.exists(wav_path):
                os.unlink(wav_path)

    @staticmethod
    def _is_target_wav(path: str) -> bool:
        """检查文件是否已经是目标WAV格式(16kHz mono PCM)"""
        if not path.lower().endswith(".wav"):
            return False
        try:
            probe = ffmpeg.probe(path)
            stream = next(s for s in probe["streams"] if s["codec_type"] == "audio")
            return (
                stream.get("sample_rate") == "16000"
                and stream.get("channels") == 1
                and stream.get("codec_name") == "pcm_s16le"
            )
        except Exception:
            return False

    @staticmethod
    def _convert_to_wav(audio_path: str) -> str:
        wav_path = tempfile.mktemp(suffix=".wav")
        ffmpeg.input(audio_path).output(
            wav_path, format="wav", acodec="pcm_s16le", ar=16000, ac=1
        ).run(quiet=True, overwrite_output=True)
        return wav_path
