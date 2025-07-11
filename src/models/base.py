from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime


@dataclass
class News:
    """뉴스 데이터 모델"""

    title: str
    content: str
    url: str
    published_date: str
    source: str
    author: Optional[str] = None
    clickbait_score: Optional[float] = None
    reasoning: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        # 제목과 본문 길이 검증
        if len(self.title) < 9:
            raise ValueError(f"제목이 너무 짧습니다: {len(self.title)}자")
        if len(self.content) < 100:
            raise ValueError(f"본문이 너무 짧습니다: {len(self.content)}자")
        if len(self.content) > 700:
            self.content = self.content[:700]


@dataclass
class AnswerFormat:
    """OpenAI 응답 형식 모델"""

    clickbait_score: float
    reasoning: str

    def __post_init__(self):
        # 점수 범위 검증
        if not (0.0 <= self.clickbait_score <= 10.0):
            raise ValueError(f"낚시성 점수는 0.0~10.0 사이여야 합니다: {self.clickbait_score}")


@dataclass
class BatchJob:
    """배치 작업 정보 모델"""

    batch_id: str
    status: str
    created_at: datetime
    input_file_id: Optional[str] = None
    output_file_id: Optional[str] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    processed_count: int = 0
    total_count: int = 0
