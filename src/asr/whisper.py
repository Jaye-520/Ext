import ffmpeg
from faster_whisper import WhisperModel
from typing import Dict
import tempfile
import os


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
        wav_path = cls._convert_to_wav(audio_path)
        try:
            segments, info = model.transcribe(wav_path, language="zh")
            result_segments = []
            for seg in segments:
                result_segments.append(
                    {"start": seg.start, "end": seg.end, "text": seg.text}
                )
            text = "".join(s["text"] for s in result_segments)
            return {
                "language": info.language,
                "duration": info.duration,
                "segments": result_segments,
                "confidence": 0.0,
                "text": text,
            }
        finally:
            if os.path.exists(wav_path):
                os.unlink(wav_path)

    @staticmethod
    def _convert_to_wav(audio_path: str) -> str:
        wav_path = tempfile.mktemp(suffix=".wav")
        ffmpeg.input(audio_path).output(
            wav_path, format="wav", acodec="pcm_s16le", ar=16000, ac=1
        ).run(quiet=True, overwrite_output=True)
        return wav_path
