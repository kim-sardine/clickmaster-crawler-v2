import requests
import time
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any
import logging

from ..config.settings import Settings
from ..models.base import News
from ..utils.logging_utils import log_api_call, log_error
from ..utils.date_utils import parse_naver_date, is_within_date_range
from ..utils.text_utils import clean_html, extract_main_content, is_valid_title, is_valid_content

logger = logging.getLogger(__name__)


class NaverNewsCrawler:
    """네이버 뉴스 크롤러"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(Settings.get_headers())
        self.collected_urls = set()

    def search_news_by_keyword(self, keyword: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """키워드로 뉴스 검색"""
        try:
            url = "https://openapi.naver.com/v1/search/news.json"
            log_api_call(logger, "Naver Search", f"/search/news.json?query={keyword}")

            all_news = []
            start = 1

            while len(all_news) < Settings.MAX_NEWS_PER_KEYWORD:
                params = {"query": keyword, "display": Settings.NAVER_DISPLAY_COUNT, "start": start, "sort": "date"}

                response = self.session.get(url, params=params)
                response.raise_for_status()

                log_api_call(
                    logger, "Naver Search", f"page {start // Settings.NAVER_DISPLAY_COUNT + 1}", response.status_code
                )

                data = response.json()
                items = data.get("items", [])

                if not items:
                    break

                # 날짜 필터링 및 중복 제거
                filtered_items = []
                for item in items:
                    pub_date = parse_naver_date(item["pubDate"])
                    if is_within_date_range(pub_date, start_date, end_date):
                        if item["link"] not in self.collected_urls:
                            filtered_items.append(item)
                            self.collected_urls.add(item["link"])

                all_news.extend(filtered_items)

                # 다음 페이지
                start += Settings.NAVER_DISPLAY_COUNT

                # API 호출 제한 고려
                time.sleep(Settings.REQUEST_DELAY)

                # 더 이상 결과가 없으면 중단
                if len(items) < Settings.NAVER_DISPLAY_COUNT:
                    break

            logger.info(f"🔍 Found {len(all_news)} news articles for keyword: {keyword}")
            return all_news[: Settings.MAX_NEWS_PER_KEYWORD]

        except Exception as e:
            log_error(logger, e, f"searching news for keyword: {keyword}")
            return []

    def extract_article_content(self, url: str) -> Optional[Dict[str, str]]:
        """개별 기사 내용 추출"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # 네이버 뉴스 구조에 따른 내용 추출
            content_selectors = [
                "#dic_area",  # 네이버 뉴스 본문
                ".news_end",  # 일부 언론사
                "#articleBodyContents",  # 일부 언론사
                ".article_body",  # 일부 언론사
                'div[itemprop="articleBody"]',  # 일반적인 구조
                ".article-body",  # 추가 패턴
                ".news-article-body",  # 추가 패턴
            ]

            content = ""
            for selector in content_selectors:
                element = soup.select_one(selector)
                if element:
                    content = element.get_text(strip=True)
                    break

            if not content:
                # 일반적인 p 태그에서 추출 시도
                paragraphs = soup.find_all("p")
                content = " ".join([p.get_text(strip=True) for p in paragraphs])

            # 기자명 추출
            author = self._extract_author(soup)

            # 언론사명 추출
            source = self._extract_source(soup)

            return {"content": content, "author": author, "source": source}

        except Exception as e:
            log_error(logger, e, f"extracting article content: {url}")
            return None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """기자명 추출"""
        author_selectors = [
            ".byline",
            ".reporter",
            ".author",
            "[data-reporter]",
            ".journalist",
            ".writer",
            ".article-reporter",
        ]

        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                author_text = element.get_text(strip=True)
                # "기자" 등의 단어 제거
                author_text = author_text.replace("기자", "").replace("특파원", "").strip()
                if author_text:
                    return author_text

        # 텍스트에서 "XXX 기자" 패턴 찾기
        import re

        text = soup.get_text()
        author_match = re.search(r"([가-힣]{2,4})\s*기자", text)
        if author_match:
            return author_match.group(1)

        return None

    def _extract_source(self, soup: BeautifulSoup) -> str:
        """언론사명 추출"""
        source_selectors = [
            ".press",
            ".source",
            ".media",
            "[data-press]",
            ".news-source",
            ".article-source",
            ".media-name",
        ]

        for selector in source_selectors:
            element = soup.select_one(selector)
            if element:
                source_text = element.get_text(strip=True)
                if source_text:
                    return source_text

        # meta 태그에서 추출 시도
        meta_selectors = ['meta[property="og:site_name"]', 'meta[name="author"]', 'meta[property="article:publisher"]']

        for selector in meta_selectors:
            element = soup.select_one(selector)
            if element and element.get("content"):
                return element.get("content")

        return "Unknown"

    def process_news_item(self, item: Dict[str, Any]) -> Optional[News]:
        """뉴스 아이템을 News 객체로 변환"""
        try:
            # 기본 정보 추출
            title = clean_html(item["title"])
            url = item["link"]
            pub_date = parse_naver_date(item["pubDate"]).strftime("%Y-%m-%d")

            # 제목 유효성 검사
            if not is_valid_title(title):
                logger.debug(f"Invalid title: {title}")
                return None

            # 상세 내용 크롤링
            article_data = self.extract_article_content(url)
            if not article_data:
                return None

            content = extract_main_content(article_data["content"])

            # 본문 유효성 검사
            if not is_valid_content(content):
                logger.debug(f"Invalid content for URL: {url}")
                return None

            return News(
                title=title,
                content=content,
                url=url,
                published_date=pub_date,
                source=article_data["source"] or "Unknown",
                author=article_data["author"],
            )

        except Exception as e:
            log_error(logger, e, f"processing news item: {item.get('title', 'Unknown')}")
            return None

    def get_crawl_stats(self) -> Dict[str, int]:
        """크롤링 통계 반환"""
        return {"collected_urls": len(self.collected_urls)}
