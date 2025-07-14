"""
데이터 모델 테스트
"""

import pytest
from datetime import datetime
from src.models.article import Article, Journalist


class TestArticle:
    """Article 모델 테스트"""

    def test_valid_article_creation(self):
        """유효한 기사 생성 테스트"""
        article = Article(
            title="이것은 유효한 제목입니다",
            content="이것은 유효한 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 테스트용 내용입니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자가 넘었을 것입니다.",
            journalist_name="홍길동",
            publisher="조선일보",
            published_at=datetime.now(),
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        assert article.title == "이것은 유효한 제목입니다"
        assert article.journalist_name == "홍길동"
        assert article.publisher == "조선일보"

    def test_short_title_validation(self):
        """짧은 제목 검증 테스트"""
        with pytest.raises(ValueError, match="제목은 최소 9자 이상이어야 합니다"):
            Article(
                title="짧은제목",
                content="이것은 유효한 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 테스트용 내용입니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자가 넘었을 것입니다.",
                journalist_name="홍길동",
                publisher="조선일보",
                published_at=datetime.now(),
                naver_url="https://n.news.naver.com/article/023/0003123456",
            )

    def test_short_content_validation(self):
        """짧은 내용 검증 테스트"""
        with pytest.raises(ValueError, match="내용은 최소 100자 이상이어야 합니다"):
            Article(
                title="이것은 유효한 제목입니다",
                content="짧은내용",
                journalist_name="홍길동",
                publisher="조선일보",
                published_at=datetime.now(),
                naver_url="https://n.news.naver.com/article/023/0003123456",
            )

    def test_invalid_url_validation(self):
        """유효하지 않은 URL 검증 테스트"""
        with pytest.raises(ValueError, match="유효하지 않은 네이버 뉴스 URL입니다"):
            Article(
                title="이것은 유효한 제목입니다",
                content="이것은 유효한 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 테스트용 내용입니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자가 넘었을 것입니다.",
                journalist_name="홍길동",
                publisher="조선일보",
                published_at=datetime.now(),
                naver_url="https://invalid-url.com",
            )

    def test_clickbait_score_validation(self):
        """낚시 점수 검증 테스트"""
        with pytest.raises(ValueError, match="낚시 점수는 0-100 사이의 값이어야 합니다"):
            Article(
                title="이것은 유효한 제목입니다",
                content="이것은 유효한 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 테스트용 내용입니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자가 넘었을 것입니다.",
                journalist_name="홍길동",
                publisher="조선일보",
                published_at=datetime.now(),
                naver_url="https://n.news.naver.com/article/023/0003123456",
                clickbait_score=101,
            )

    def test_to_dict_conversion(self):
        """딕셔너리 변환 테스트"""
        published_at = datetime.now()
        article = Article(
            title="이것은 유효한 제목입니다",
            content="이것은 유효한 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 테스트용 내용입니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자가 넘었을 것입니다.",
            journalist_name="홍길동",
            publisher="조선일보",
            published_at=published_at,
            naver_url="https://n.news.naver.com/article/023/0003123456",
            clickbait_score=75,
        )

        result = article.to_dict()

        assert result["title"] == "이것은 유효한 제목입니다"
        assert result["publisher"] == "조선일보"
        assert result["clickbait_score"] == 75
        assert result["published_at"] == published_at.isoformat()


class TestJournalist:
    """Journalist 모델 테스트"""

    def test_valid_journalist_creation(self):
        """유효한 기자 생성 테스트"""
        journalist = Journalist(name="홍길동", publisher="조선일보", naver_uuid="uuid123")

        assert journalist.name == "홍길동"
        assert journalist.publisher == "조선일보"
        assert journalist.naver_uuid == "uuid123"
        assert journalist.article_count == 0
        assert journalist.average_score == 0.0

    def test_short_name_validation(self):
        """짧은 기자명 검증 테스트"""
        with pytest.raises(ValueError, match="기자명은 최소 2자 이상이어야 합니다"):
            Journalist(name="홍", publisher="조선일보")

    def test_short_publisher_validation(self):
        """짧은 언론사명 검증 테스트"""
        with pytest.raises(ValueError, match="언론사명은 최소 2자 이상이어야 합니다"):
            Journalist(name="홍길동", publisher="조")

    def test_to_dict_conversion(self):
        """딕셔너리 변환 테스트"""
        journalist = Journalist(
            name="홍길동", publisher="조선일보", naver_uuid="uuid123", article_count=5, average_score=75.5, max_score=90
        )

        result = journalist.to_dict()

        assert result["name"] == "홍길동"
        assert result["publisher"] == "조선일보"
        assert result["naver_uuid"] == "uuid123"
        assert result["article_count"] == 5
        assert result["average_score"] == 75.5
        assert result["max_score"] == 90
