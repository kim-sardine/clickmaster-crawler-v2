"""
네이버 크롤러 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
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
                <h2 id="title_area">테스트 뉴스 제목입니다</h2>
                <span class="media_end_head_top_logo_text">연합뉴스</span>
                <span class="byline_s">김기자 reporter@example.com</span>
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

        result = crawler.extract_article_content("https://n.news.naver.com/article/023/0003123456")

        assert result is not None
        assert result.title == "테스트 뉴스 제목입니다"
        assert result.publisher == "연합뉴스"
        assert result.reporter == "김기자"
        assert len(result.content) >= 100

    @patch("requests.Session.get")
    def test_extract_article_content_failure(self, mock_get, crawler):
        """기사 본문 추출 실패 테스트"""
        mock_get.side_effect = Exception("Network Error")

        content = crawler.extract_article_content("https://n.news.naver.com/article/023/0003123456")

        assert content is None

    @patch.object(NaverNewsCrawler, "extract_article_content")
    def test_parse_api_item_success(self, mock_extract_content, crawler):
        """API 아이템 파싱 성공 테스트"""
        from src.models.article import NaverNewsCrawlerResult

        # NaverNewsCrawlerResult 객체로 mock 설정
        long_content = "이것은 추출된 기사 본문입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        mock_crawl_result = NaverNewsCrawlerResult(
            title="크롤링된 충격 테스트 뉴스 제목입니다",
            content=long_content,
            reporter="김테스트",
            publisher="테스트뉴스",
        )
        mock_extract_content.return_value = mock_crawl_result

        api_item = {
            "title": "&lt;b&gt;충격&lt;/b&gt; 테스트 뉴스 제목입니다",
            "description": "이것은 테스트 뉴스 설명입니다.",
            "originallink": "https://example.com/news/123",
            "link": "https://n.news.naver.com/article/023/0003123456",
            "pubDate": "Mon, 15 Jan 2024 10:30:00 +0900",
        }

        article = crawler.parse_api_item(api_item)

        assert article is not None
        assert article.title == "크롤링된 충격 테스트 뉴스 제목입니다"  # 크롤링된 제목 우선 사용
        assert article.naver_url == "https://n.news.naver.com/article/023/0003123456"
        assert article.journalist_name == "김테스트"  # 크롤링된 기자명
        assert article.publisher == "테스트뉴스"  # 크롤링된 출판사명
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

    def test_parse_api_item_short_title(self, crawler):
        """짧은 제목 기사 파싱 테스트 (None 반환 확인)"""
        item = {
            "title": "짧은제목",  # 4자 (9자 미만)
            "description": "충분히 긴 설명 텍스트입니다. 100자 이상이 되도록 더 많은 내용을 추가하겠습니다. 추가 내용으로 테스트를 위한 충분한 길이를 만들어보겠습니다.",
            "originallink": "https://example.com/news/123",
            "link": "https://n.news.naver.com/mnews/article/001/0014793298",
            "pubDate": "Tue, 12 Dec 2023 10:30:00 +0900",
        }

        with patch.object(crawler, "extract_article_content", return_value=None):
            result = crawler.parse_api_item(item)

        assert result is None

    def test_parse_api_item_non_naver_link(self, crawler):
        """네이버 뉴스가 아닌 링크 파싱 테스트 (None 반환 확인)"""
        item = {
            "title": "충분히 긴 제목입니다 테스트용",
            "description": "충분히 긴 설명 텍스트입니다. 100자 이상이 되도록 더 많은 내용을 추가하겠습니다. 추가 내용으로 테스트를 위한 충분한 길이를 만들어보겠습니다.",
            "originallink": "https://example.com/news/123",
            "link": "https://www.chosun.com/politics/politics_general/2023/12/12/test/",  # 네이버 뉴스가 아님
            "pubDate": "Tue, 12 Dec 2023 10:30:00 +0900",
        }

        result = crawler.parse_api_item(item)
        assert result is None

    def test_parse_api_item_title_exactly_9_chars(self, crawler):
        """정확히 9자 제목 기사 파싱 테스트 (정상 처리 확인)"""
        item = {
            "title": "정확히아홉자제목임",  # 정확히 9자
            "description": "충분히 긴 설명 텍스트입니다. 100자 이상이 되도록 더 많은 내용을 추가하겠습니다. 추가 내용으로 테스트를 위한 충분한 길이를 만들어보겠습니다.",
            "originallink": "https://example.com/news/123",
            "link": "https://n.news.naver.com/mnews/article/001/0014793298",
            "pubDate": "Tue, 12 Dec 2023 10:30:00 +0900",
        }

        with patch.object(crawler, "extract_article_content", return_value=None):
            result = crawler.parse_api_item(item)

        assert result is not None
        assert result.title == "정확히아홉자제목임"

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords(self, mock_parse_item, mock_search_api, crawler):
        """키워드별 크롤링 테스트"""
        # Mock API 검색 결과 - 첫 번째 호출에만 결과 반환, 두 번째 호출에는 빈 리스트 반환
        mock_search_api.side_effect = [
            [{"title": "충격 뉴스 1"}, {"title": "충격 뉴스 2"}],  # 첫 번째 호출
            [],  # 두 번째 호출 (더 이상 결과 없음)
        ]

        # 100자 이상의 긴 내용으로 수정
        long_content1 = "이것은 충격적인 뉴스 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."
        long_content2 = "이것은 두 번째 충격적인 뉴스 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        # Mock 파싱 결과
        mock_article1 = Article(
            title="충격적인 뉴스 1번입니다 테스트용으로 작성됨",
            content=long_content1,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        mock_article2 = Article(
            title="충격적인 뉴스 2번입니다 테스트용으로 작성됨",
            content=long_content2,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/421/0007123456",
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2]

        # Mock 배치 중복 체크 (모두 신규)
        crawler.db_ops.check_duplicate_articles_batch.return_value = {
            "https://n.news.naver.com/article/023/0003123456": False,
            "https://n.news.naver.com/article/421/0007123456": False,
        }

        result = crawler.crawl_by_keywords(["충격"])

        assert len(result) == 2
        assert result[0].title == "충격적인 뉴스 1번입니다 테스트용으로 작성됨"
        assert result[1].title == "충격적인 뉴스 2번입니다 테스트용으로 작성됨"

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords_with_date_filter(self, mock_parse_item, mock_search_api, crawler):
        """날짜 필터링을 적용한 키워드별 크롤링 테스트"""
        target_date = datetime(2024, 1, 15)

        # Mock API 검색 결과 - 첫 번째 호출에만 결과 반환
        mock_search_api.side_effect = [
            [{"title": "충격 뉴스 1"}, {"title": "충격 뉴스 2"}, {"title": "충격 뉴스 3"}],
            [],  # 두 번째 호출에는 빈 리스트
        ]

        long_content = "이것은 테스트 기사 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다."

        # Mock 파싱 결과 - 날짜가 다른 기사들
        mock_article1 = Article(
            title="충격적인 뉴스 1번 - 대상 날짜에 해당하는 기사",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime(2024, 1, 15, 10, 30, tzinfo=pytz.timezone("Asia/Seoul")),  # 대상 날짜
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        mock_article2 = Article(
            title="충격적인 뉴스 2번 - 미래 날짜에 해당하는 기사",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime(2024, 1, 16, 11, 30, tzinfo=pytz.timezone("Asia/Seoul")),  # 미래 날짜
            naver_url="https://n.news.naver.com/article/421/0007123456",
        )

        mock_article3 = Article(
            title="충격적인 뉴스 3번 - 과거 날짜에 해당하는 기사",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime(2024, 1, 14, 9, 30, tzinfo=pytz.timezone("Asia/Seoul")),  # 과거 날짜
            naver_url="https://n.news.naver.com/article/123/0008123456",
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2, mock_article3]

        # Mock 배치 중복 체크 (날짜 필터링된 기사들만)
        crawler.db_ops.check_duplicate_articles_batch.return_value = {
            "https://n.news.naver.com/article/023/0003123456": False,  # 대상 날짜 기사
        }

        result = crawler.crawl_by_keywords(["충격"], target_date=target_date)

        # 대상 날짜(2024-01-15)의 기사만 포함되어야 함
        assert len(result) == 1
        assert result[0].title == "충격적인 뉴스 1번 - 대상 날짜에 해당하는 기사"
        assert result[0].published_at.date() == target_date.date()

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords_without_duplicate_check(self, mock_parse_item, mock_search_api, crawler):
        """중복 체크 없이 크롤링 테스트 (dry-run 모드 시뮬레이션)"""
        # Mock API 검색 결과
        mock_search_api.side_effect = [
            [{"title": "테스트 뉴스 1"}, {"title": "테스트 뉴스 2"}],
            [],  # 두 번째 호출에는 빈 리스트
        ]

        long_content = "이것은 테스트 기사 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다."

        # Mock 파싱 결과
        mock_article1 = Article(
            title="테스트 뉴스 1번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        mock_article2 = Article(
            title="테스트 뉴스 2번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/421/0007123456",
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2]

        # check_duplicates=False로 호출 (dry-run 모드)
        result = crawler.crawl_by_keywords(["테스트"], check_duplicates=False)

        # 중복 체크를 하지 않았으므로 배치 중복 체크 메서드가 호출되지 않아야 함
        crawler.db_ops.check_duplicate_articles_batch.assert_not_called()

        # 모든 기사가 포함되어야 함
        assert len(result) == 2
        assert result[0].title == "테스트 뉴스 1번입니다 충분히 긴 제목으로 작성함"
        assert result[1].title == "테스트 뉴스 2번입니다 충분히 긴 제목으로 작성함"

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords_with_duplicate_check(self, mock_parse_item, mock_search_api, crawler):
        """중복 체크 포함 크롤링 테스트 (일반 모드 시뮬레이션)"""
        # Mock API 검색 결과
        mock_search_api.side_effect = [
            [{"title": "테스트 뉴스 1"}, {"title": "테스트 뉴스 2"}],
            [],
        ]

        long_content = "이것은 테스트 기사 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다."

        # Mock 파싱 결과
        mock_article1 = Article(
            title="테스트 뉴스 1번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",
        )

        mock_article2 = Article(
            title="테스트 뉴스 2번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/421/0007123456",
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2]

        # 배치 중복 체크 설정: 첫 번째는 신규, 두 번째는 중복
        crawler.db_ops.check_duplicate_articles_batch.return_value = {
            "https://n.news.naver.com/article/023/0003123456": False,  # 첫 번째 기사: 신규
            "https://n.news.naver.com/article/421/0007123456": True,  # 두 번째 기사: 중복
        }

        # check_duplicates=True로 호출 (기본값)
        result = crawler.crawl_by_keywords(["테스트"], check_duplicates=True)

        # 배치 중복 체크가 한 번 호출되어야 함
        assert crawler.db_ops.check_duplicate_articles_batch.call_count == 1

        # 중복이 아닌 첫 번째 기사만 포함되어야 함
        assert len(result) == 1
        assert result[0].title == "테스트 뉴스 1번입니다 충분히 긴 제목으로 작성함"

    @patch.object(NaverNewsCrawler, "search_news_api")
    @patch.object(NaverNewsCrawler, "parse_api_item")
    def test_crawl_by_keywords_batch_duplicate_removal(self, mock_parse_item, mock_search_api, crawler):
        """배치 내 중복 URL 제거 테스트"""
        # Mock API 검색 결과
        mock_search_api.side_effect = [
            [{"title": "테스트 뉴스 1"}, {"title": "테스트 뉴스 1 중복"}, {"title": "테스트 뉴스 2"}],
            [],
        ]

        long_content = "이것은 테스트 기사 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다."

        # Mock 파싱 결과 - 첫 번째와 두 번째가 같은 URL
        mock_article1 = Article(
            title="테스트 뉴스 1번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",  # 같은 URL
        )

        mock_article2 = Article(
            title="테스트 뉴스 1 중복입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/023/0003123456",  # 같은 URL (중복)
        )

        mock_article3 = Article(
            title="테스트 뉴스 2번입니다 충분히 긴 제목으로 작성함",
            content=long_content,
            journalist_name="익명",
            publisher="네이버뉴스",
            published_at=datetime.now(pytz.timezone("Asia/Seoul")),
            naver_url="https://n.news.naver.com/article/421/0007123456",  # 다른 URL
        )

        mock_parse_item.side_effect = [mock_article1, mock_article2, mock_article3]

        # 배치 중복 체크 설정: 모든 기사가 신규 (배치 내 중복 제거 후)
        crawler.db_ops.check_duplicate_articles_batch.return_value = {
            "https://n.news.naver.com/article/023/0003123456": False,
            "https://n.news.naver.com/article/421/0007123456": False,
        }

        result = crawler.crawl_by_keywords(["테스트"], check_duplicates=True)

        # 배치 내 중복이 제거되어 2개 기사만 반환되어야 함
        assert len(result) == 2

        # URL이 유니크한지 확인
        urls = [article.naver_url for article in result]
        assert len(set(urls)) == 2

        # 첫 번째 기사가 우선 선택되었는지 확인
        assert "https://n.news.naver.com/article/023/0003123456" in urls
        assert "https://n.news.naver.com/article/421/0007123456" in urls

        # 배치 중복 체크가 중복 제거된 URL들로만 호출되었는지 확인
        call_args = crawler.db_ops.check_duplicate_articles_batch.call_args[0][0]
        assert len(call_args) == 2
        assert set(call_args) == {
            "https://n.news.naver.com/article/023/0003123456",
            "https://n.news.naver.com/article/421/0007123456",
        }

    @patch.object(NaverNewsCrawler, "crawl_by_keywords")
    def test_crawl_and_save_success(self, mock_crawl, crawler):
        """크롤링 및 저장 성공 테스트"""
        # 100자 이상의 긴 내용으로 수정
        long_content = "이것은 테스트 기사 1의 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        # Mock 크롤링 결과
        mock_articles = [
            Article(
                title="테스트 기사 1번입니다 충분히 긴 제목으로 작성함",
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

        result = crawler.crawl_and_save(["충격"])

        assert result == 1
        mock_crawl.assert_called_once_with(keywords=["충격"], target_date=None, check_duplicates=True)

    @patch.object(NaverNewsCrawler, "crawl_by_keywords")
    def test_crawl_and_save_with_date_filter(self, mock_crawl, crawler):
        """날짜 필터링을 적용한 크롤링 및 저장 테스트"""
        target_date = datetime(2024, 1, 15)

        long_content = "이것은 테스트 기사의 내용입니다. 충분히 긴 내용이 포함되어 있습니다. 100자 이상이 되도록 더 많은 내용을 추가했습니다. 확실히 100자를 넘기기 위해 추가적인 텍스트를 더 넣어보겠습니다. 이제 확실히 100자가 넘었을 것입니다."

        mock_articles = [
            Article(
                title="테스트 기사 1번입니다 날짜 필터링 테스트용",
                content=long_content,
                journalist_name="익명",
                publisher="네이버뉴스",
                published_at=datetime(2024, 1, 15, 10, 30, tzinfo=pytz.timezone("Asia/Seoul")),
                naver_url="https://n.news.naver.com/article/023/0003123456",
            )
        ]
        mock_crawl.return_value = mock_articles
        crawler.db_ops.bulk_insert_articles.return_value = [{"id": "article-123"}]

        result = crawler.crawl_and_save(["충격"], target_date=target_date)

        assert result == 1
        mock_crawl.assert_called_once_with(keywords=["충격"], target_date=target_date, check_duplicates=True)

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


class TestDateValidation:
    """날짜 검증 테스트 클래스"""

    def test_valid_date_format(self):
        """올바른 날짜 형식 테스트"""
        from scripts.crawl_news import validate_date_format

        # 최근 날짜로 테스트 (오늘에서 1개월 전)
        recent_date = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m-%d")
        result = validate_date_format(recent_date)

        assert isinstance(result, datetime)
        assert result.strftime("%Y-%m-%d") == recent_date

    def test_invalid_date_format(self):
        """잘못된 날짜 형식 테스트"""
        from scripts.crawl_news import validate_date_format

        with pytest.raises(ValueError, match="날짜 형식이 올바르지 않습니다"):
            validate_date_format("2024-13-15")  # 잘못된 월

    def test_too_old_date(self):
        """3개월 이전 날짜 테스트"""
        from scripts.crawl_news import validate_date_format

        # 4개월 전 날짜
        old_date = (datetime.now() - relativedelta(months=4)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="너무 과거입니다"):
            validate_date_format(old_date)

    def test_future_date(self):
        """미래 날짜 테스트"""
        from scripts.crawl_news import validate_date_format

        # 내일 날짜
        future_date = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="미래 날짜입니다"):
            validate_date_format(future_date)

    def test_boundary_date_three_months_ago(self):
        """경계값 테스트: 정확히 3개월 전"""
        from scripts.crawl_news import validate_date_format

        # 정확히 3개월 전 날짜 (허용되어야 함)
        boundary_date = (datetime.now() - relativedelta(months=3)).strftime("%Y-%m-%d")
        result = validate_date_format(boundary_date)

        assert isinstance(result, datetime)
        assert result.strftime("%Y-%m-%d") == boundary_date

    def test_today_date(self):
        """오늘 날짜 테스트"""
        from scripts.crawl_news import validate_date_format

        today = datetime.now().strftime("%Y-%m-%d")
        result = validate_date_format(today)

        assert isinstance(result, datetime)
        assert result.strftime("%Y-%m-%d") == today
