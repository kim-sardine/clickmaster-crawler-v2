"""
네이버 크롤러 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytz

from src.crawlers.naver_crawler import NaverNewsCrawler
from src.models.article import Article


class TestNaverNewsCrawler:
    """NaverNewsCrawler 테스트"""

    @pytest.fixture
    def crawler(self):
        """크롤러 인스턴스 픽스처"""
        with patch("src.crawlers.naver_crawler.DatabaseOperations"):
            return NaverNewsCrawler(client_id="test-client-id", client_secret="test-client-secret")

    @pytest.fixture
    def mock_api_response(self):
        """Mock API 응답 데이터"""
        return {
            "items": [
                {
                    "title": "&lt;b&gt;충격&lt;/b&gt; 테스트 뉴스 제목입니다",
                    "description": "이것은 테스트 뉴스 설명입니다. 충격적인 내용이 포함되어 있습니다.",
                    "originallink": "https://example.com/news/123",
                    "link": "https://n.news.naver.com/article/023/0003123456",
                    "pubDate": "Mon, 15 Jan 2024 10:30:00 +0900",
                },
                {
                    "title": "두 번째 &lt;b&gt;충격&lt;/b&gt; 뉴스",
                    "description": "두 번째 테스트 뉴스입니다.",
                    "originallink": "https://example.com/news/456",
                    "link": "https://n.news.naver.com/article/421/0007123456",
                    "pubDate": "Mon, 15 Jan 2024 11:00:00 +0900",
                },
            ]
        }

    def test_crawler_initialization(self, crawler):
        """크롤러 초기화 테스트"""
        assert crawler.client_id == "test-client-id"
        assert crawler.client_secret == "test-client-secret"
        assert crawler.api_url == "https://openapi.naver.com/v1/search/news.json"
        assert "X-Naver-Client-Id" in crawler.api_headers
        assert "X-Naver-Client-Secret" in crawler.api_headers

    @patch("requests.Session.get")
    def test_search_news_api_success(self, mock_get, crawler, mock_api_response):
        """API 검색 성공 테스트"""
        mock_response = Mock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = crawler.search_news_api("충격", display=10, start=1)

        assert len(result) == 2
        assert "충격" in result[0]["title"]
        mock_get.assert_called_once()

    @patch("requests.Session.get")
    def test_search_news_api_failure(self, mock_get, crawler):
        """API 검색 실패 테스트"""
        mock_get.side_effect = Exception("API Error")

        result = crawler.search_news_api("충격")

        assert result == []

    @patch("requests.Session.get")
    def test_extract_article_content_success(self, mock_get, crawler):
        """기사 본문 추출 성공 테스트"""
        mock_html = """
        <html>
            <body>
                <div id="dic_area">
                    이것은 테스트 기사의 본문입니다. 충분히 긴 내용을 포함하고 있어야 합니다.
                    추가적인 내용이 더 있어서 100자 이상이 되도록 작성되었습니다.
                    더 많은 내용을 추가해서 확실히 100자를 넘기도록 하겠습니다.
                    추가 텍스트로 100자를 확실히 넘기겠습니다.
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        content = crawler.extract_article_content("https://n.news.naver.com/article/023/0003123456")

        assert content is not None
        assert len(content) >= 100
        assert len(content) <= 700
        assert "테스트 기사의 본문" in content

    @patch("requests.Session.get")
    def test_extract_article_content_failure(self, mock_get, crawler):
        """기사 본문 추출 실패 테스트"""
        mock_get.side_effect = Exception("Network Error")

        content = crawler.extract_article_content("https://n.news.naver.com/article/023/0003123456")

        assert content is None

    @patch.object(NaverNewsCrawler, "extract_article_content")
    def test_parse_api_item_success(self, mock_extract_content, crawler):
        """API 아이템 파싱 성공 테스트"""
        # 100자 이상의 긴 내용으로 수정
        long_content = "이것은 추출된 기사 본문입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."
        mock_extract_content.return_value = long_content

        api_item = {
            "title": "&lt;b&gt;충격&lt;/b&gt; 테스트 뉴스 제목입니다",
            "description": "이것은 테스트 뉴스 설명입니다.",
            "originallink": "https://example.com/news/123",
            "link": "https://n.news.naver.com/article/023/0003123456",
            "pubDate": "Mon, 15 Jan 2024 10:30:00 +0900",
        }

        article = crawler.parse_api_item(api_item)

        assert article is not None
        assert article.title == "충격 테스트 뉴스 제목입니다"
        assert article.naver_url == "https://n.news.naver.com/article/023/0003123456"
        assert article.journalist_name == "익명"
        assert article.publisher == "네이버뉴스"
        assert isinstance(article.published_at, datetime)

    def test_parse_api_item_invalid_url(self, crawler):
        """잘못된 URL 아이템 파싱 테스트"""
        api_item = {
            "title": "테스트 뉴스",
            "description": "설명",
            "originallink": "https://example.com/news/123",
            "link": "https://invalid-url.com/news/123",  # 네이버 뉴스 URL이 아님
            "pubDate": "Mon, 15 Jan 2024 10:30:00 +0900",
        }

        article = crawler.parse_api_item(api_item)

        assert article is None

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords(self, mock_parse_item, mock_search_api, crawler):
        """키워드별 크롤링 테스트"""
        # Mock API 검색 결과
        mock_search_api.return_value = [{"title": "충격 뉴스 1"}, {"title": "충격 뉴스 2"}]

        # 100자 이상의 긴 내용으로 수정
        long_content1 = "이것은 충격적인 뉴스 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."
        long_content2 = "이것은 두 번째 충격적인 뉴스 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        # Mock 파싱 결과
        mock_article1 = Article(
            title="충격 뉴스 1입니다 테스트용",
            content=long_content1,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        mock_article2 = Article(
            title="충격 뉴스 2입니다 테스트용",
            content=long_content2,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/421/0007123456",
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2]

        # Mock 중복 체크 (중복 없음)
        crawler.db_ops.check_duplicate_article.return_value = False

        result = crawler.crawl_by_keywords(["충격"], max_articles_per_keyword=10)

        assert len(result) == 2
        assert result[0].title == "충격 뉴스 1입니다 테스트용"
        assert result[1].title == "충격 뉴스 2입니다 테스트용"

    @patch.object(NaverNewsCrawler, "crawl_by_keywords")
    def test_crawl_and_save_success(self, mock_crawl, crawler):
        """크롤링 및 저장 성공 테스트"""
        # 100자 이상의 긴 내용으로 수정
        long_content = "이것은 테스트 기사 1의 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        # Mock 크롤링 결과
        mock_articles = [
            Article(
                title="테스트 기사 1입니다 충분히 긴 제목",
                content=long_content,
                journalist_name="익명",
                publisher="네이버뉴스",
                published_at=datetime.now(pytz.timezone("Asia/Seoul")),
                naver_url="https://n.news.naver.com/article/023/0003123456",
            )
        ]
        mock_crawl.return_value = mock_articles

        # Mock 저장 결과
        crawler.db_ops.bulk_insert_articles.return_value = [{"id": "article-123"}]

        result = crawler.crawl_and_save(["충격"], max_articles_per_keyword=10)

        assert result == 1
        mock_crawl.assert_called_once_with(["충격"], 10)
        crawler.db_ops.bulk_insert_articles.assert_called_once_with(mock_articles)

    @patch.object(NaverNewsCrawler, "crawl_by_keywords")
    def test_crawl_and_save_no_articles(self, mock_crawl, crawler):
        """크롤링 결과 없음 테스트"""
        mock_crawl.return_value = []

        result = crawler.crawl_and_save(["충격"])

        assert result == 0

    def test_session_cleanup(self, crawler):
        """세션 정리 테스트"""
        mock_session = Mock()
        crawler.session = mock_session

        # __del__ 메서드 직접 호출
        crawler.__del__()

        mock_session.close.assert_called_once()
