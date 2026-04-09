"""ASR抽象基类"""

from abc import ABC, abstractmethod
from typing import List, Optional
from pydantic import BaseModel


class ASRSegment(BaseModel):
    """字幕分段"""

    start: float
    end: float
    text: str
    confidence: float = 0.0


class ASRResult(BaseModel):
    """ASR结果"""

    language: str = "zh"
    duration: float = 0.0
    segments: List[ASRSegment]
    confidence: float = 0.0


class ASRBase(ABC):
    """ASR模块抽象基类"""

    @abstractmethod
    async def transcribe(
        self,
        audio_path: str,
        language: Optional[str] = None,
    ) -> ASRResult:
        """转录音频文件"""
        pass

    @abstractmethod
    async def extract_audio(
        self,
        video_path: str,
        output_path: str,
    ) -> str:
        """从视频中提取音频"""
        pass
