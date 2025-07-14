"""
네이버 뉴스 크롤러
"""

import requests
import time
import logging
import html
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
import pytz

from src.models.article import Article
from src.database.operations import DatabaseOperations

logger = logging.getLogger(__name__)


class NaverNewsCrawler:
    """네이버 뉴스 크롤러"""

    def __init__(self, client_id: str, client_secret: str):
        """
        크롤러 초기화

        Args:
            client_id: 네이버 API 클라이언트 ID
            client_secret: 네이버 API 클라이언트 시크릿
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.db_ops = DatabaseOperations()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

        # API 설정
        self.api_url = "https://openapi.naver.com/v1/search/news.json"
        self.api_headers = {"X-Naver-Client-Id": self.client_id, "X-Naver-Client-Secret": self.client_secret}

    def search_news_api(
        self, query: str, display: int = 100, start: int = 1, sort: str = "date"
    ) -> List[Dict[str, Any]]:
        """
        네이버 뉴스 검색 API 호출

        Args:
            query: 검색 키워드
            display: 검색 결과 개수 (1-100)
            start: 검색 시작 위치 (1-1000)
            sort: 정렬 기준 (sim, date)

        Returns:
            뉴스 검색 결과 리스트
        """
        params = {"query": query, "display": display, "start": start, "sort": sort}

        try:
            response = self.session.get(self.api_url, headers=self.api_headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            logger.info(f"API 검색 완료: {query}, 결과 {len(data.get('items', []))}개")
            return data.get("items", [])

        except requests.exceptions.RequestException as e:
            logger.error(f"API 요청 실패: {e}")
            return []
        except Exception as e:
            logger.error(f"API 응답 파싱 실패: {e}")
            return []

    def extract_article_content(self, naver_url: str) -> Optional[str]:
        """
        네이버 뉴스 본문 추출

        Args:
            naver_url: 네이버 뉴스 URL

        Returns:
            기사 본문 또는 None
        """
        try:
            response = self.session.get(naver_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # 기사 본문 추출 (여러 패턴 시도)
            content_selectors = [
                "#dic_area",  # 일반 뉴스
                ".se-main-container",  # 스마트에디터
                ".se-component-content",  # 스마트에디터 새 버전
                ".news_end",  # 구버전
                "#articleBodyContents",  # 구버전
            ]

            content = None
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # 불필요한 태그 제거
                    for unwanted in content_elem.find_all(["script", "style", "em", "strong"]):
                        unwanted.decompose()

                    content = content_elem.get_text(strip=True)
                    if len(content) >= 100:  # 최소 길이 체크
                        break

            if content and len(content) >= 100:
                return content[:700]  # 최대 700자로 제한

            return None

        except Exception as e:
            logger.error(f"본문 추출 실패 {naver_url}: {e}")
            return None

    def parse_api_item(self, item: Dict[str, Any]) -> Optional[Article]:
        """
        API 검색 결과 아이템을 Article 객체로 변환

        Args:
            item: API 검색 결과 아이템

        Returns:
            Article 객체 또는 None
        """
        try:
            # HTML 엔티티 디코딩 후 태그 제거
            title = BeautifulSoup(html.unescape(item["title"]), "html.parser").get_text()
            description = BeautifulSoup(html.unescape(item["description"]), "html.parser").get_text()

            # 네이버 뉴스 URL인지 확인
            original_link = item["originallink"]
            link = item["link"]

            naver_url = link if "news.naver.com" in link else None
            if not naver_url:
                return None

            # 본문 추출
            content = self.extract_article_content(naver_url)
            if not content:
                content = description  # 본문 추출 실패 시 요약 사용

            # 발행시간 파싱
            pub_date_str = item["pubDate"]
            pub_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %z")

            # 한국 시간으로 변환
            kst = pytz.timezone("Asia/Seoul")
            pub_date = pub_date.astimezone(kst)

            # 기자명과 언론사 추출 (간단한 패턴)
            journalist_name = "익명"  # API에서는 기자명을 제공하지 않음

            article = Article(
                title=title,
                content=content,
                journalist_name=journalist_name,
                publisher="네이버뉴스",  # API 결과는 일반적으로 네이버뉴스로 처리
                published_at=pub_date,
                naver_url=naver_url,
            )

            return article

        except Exception as e:
            logger.error(f"아이템 파싱 실패: {e}")
            return None

    def crawl_by_keywords(self, keywords: List[str], max_articles_per_keyword: int = 100) -> List[Article]:
        """
        키워드별 뉴스 크롤링

        Args:
            keywords: 검색 키워드 리스트
            max_articles_per_keyword: 키워드당 최대 기사 수

        Returns:
            크롤링된 기사 리스트
        """
        all_articles = []

        for keyword in keywords:
            logger.info(f"키워드 크롤링 시작: {keyword}")

            try:
                # API 검색
                items = self.search_news_api(query=keyword, display=min(max_articles_per_keyword, 100), sort="date")

                # 기사 파싱
                for item in items:
                    article = self.parse_api_item(item)
                    if article:
                        # 중복 체크
                        if not self.db_ops.check_duplicate_article(article.naver_url):
                            all_articles.append(article)
                        else:
                            logger.debug(f"중복 기사 스킵: {article.title[:50]}...")

                    # Rate limiting
                    time.sleep(0.1)

                logger.info(f"키워드 '{keyword}' 크롤링 완료: {len([a for a in all_articles if keyword in str(a)])}개")

                # 키워드 간 대기
                time.sleep(1)

            except Exception as e:
                logger.error(f"키워드 '{keyword}' 크롤링 실패: {e}")
                continue

        logger.info(f"전체 크롤링 완료: {len(all_articles)}개 기사")
        return all_articles

    def crawl_and_save(self, keywords: List[str], max_articles_per_keyword: int = 100) -> int:
        """
        뉴스 크롤링 및 데이터베이스 저장

        Args:
            keywords: 검색 키워드 리스트
            max_articles_per_keyword: 키워드당 최대 기사 수

        Returns:
            저장된 기사 수
        """
        try:
            # 크롤링
            articles = self.crawl_by_keywords(keywords, max_articles_per_keyword)

            if not articles:
                logger.warning("크롤링된 기사가 없습니다")
                return 0

            # 데이터베이스 저장
            saved_articles = self.db_ops.bulk_insert_articles(articles)

            logger.info(f"저장 완료: {len(saved_articles)}개 기사")
            return len(saved_articles)

        except Exception as e:
            logger.error(f"크롤링 및 저장 실패: {e}")
            return 0

    def __del__(self):
        """소멸자 - 세션 정리"""
        if hasattr(self, "session"):
            self.session.close()
