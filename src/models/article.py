"""
기사 데이터 모델
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Article:
    """기사 데이터 모델"""

    title: str
    content: str
    journalist_name: str
    publisher: str
    published_at: datetime
    naver_url: str
    journalist_id: Optional[str] = None
    clickbait_score: Optional[int] = None
    score_explanation: Optional[str] = None
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        """데이터 검증"""
        if len(self.title.strip()) < 9:
            raise ValueError("제목은 최소 9자 이상이어야 합니다")

        if len(self.content.strip()) < 100:
            raise ValueError("내용은 최소 100자 이상이어야 합니다")

        if not self.naver_url.startswith(("https://n.news.naver.com", "https://m.entertain.naver.com")):
            raise ValueError("유효하지 않은 네이버 뉴스 URL입니다")

        if self.clickbait_score is not None and not (0 <= self.clickbait_score <= 100):
            raise ValueError("낚시 점수는 0-100 사이의 값이어야 합니다")

    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "title": self.title,
            "content": self.content,
            "journalist_id": self.journalist_id,
            "publisher": self.publisher,
            "clickbait_score": self.clickbait_score,
            "score_explanation": self.score_explanation,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "naver_url": self.naver_url,
        }


@dataclass
class Journalist:
    """기자 데이터 모델"""

    name: str
    publisher: str
    naver_uuid: Optional[str] = None
    article_count: int = 0
    average_score: float = 0.0
    max_score: int = 0
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        """데이터 검증"""
        if len(self.name.strip()) < 2:
            raise ValueError("기자명은 최소 2자 이상이어야 합니다")

        if len(self.publisher.strip()) < 2:
            raise ValueError("언론사명은 최소 2자 이상이어야 합니다")

    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "name": self.name,
            "publisher": self.publisher,
            "naver_uuid": self.naver_uuid,
            "article_count": self.article_count,
            "average_score": self.average_score,
            "max_score": self.max_score,
        }
