"""
네이버 뉴스 크롤러 모듈
"""

import re
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pytz
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from src.config.settings import settings
from src.models.article import Article, Journalist
from src.database.operations import DatabaseOperations
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


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

    def get_title(self, soup: BeautifulSoup) -> str:
        """
        BeautifulSoup 객체에서 뉴스 제목 추출

        Args:
            soup: BeautifulSoup 객체

        Returns:
            뉴스 제목
        """
        title_area = soup.find("h2", id="title_area")
        if title_area is None:
            # 다른 제목 요소 타입 시도
            title_area = soup.find("h2", class_="media_end_head_headline")
            if title_area is None:
                return ""
        return title_area.text.strip()

    def get_publisher(self, soup: BeautifulSoup) -> str:
        """
        BeautifulSoup 객체에서 언론사 이름 추출

        Args:
            soup: BeautifulSoup 객체

        Returns:
            언론사 이름
        """
        publisher_elements = soup.find_all("span", class_="media_end_head_top_logo_text")
        if publisher_elements:
            return publisher_elements[0].text.strip()

        # 다른 언론사 요소 타입 시도
        publisher_elements = soup.find_all("em", class_="media_end_linked_more_point")
        if publisher_elements:
            return publisher_elements[0].text.strip()

        return ""

    def get_reporter(self, soup: BeautifulSoup) -> str:
        """
        BeautifulSoup 객체에서 기자 이름 추출

        Args:
            soup: BeautifulSoup 객체

        Returns:
            기자 이름
        """
        # 기자 카드 스타일 추출 시도
        journalistcard_items = soup.find_all("div", class_="media_journalistcard_item_inner")
        if journalistcard_items:
            reporters = []
            for journalistcard_item in journalistcard_items:
                reporter_elements = journalistcard_item.find_all("em", class_="media_journalistcard_summary_name_text")
                if reporter_elements:
                    reporter = reporter_elements[0].text
                    reporter = reporter.replace("기자", "").strip()
                    reporters.append(reporter)
            if reporters:
                return ",".join(reporters)

        # 바이라인 스타일 추출 시도
        bylines = soup.find_all("span", class_="byline_s")
        if bylines:
            reporter = bylines[0].text

            # 패턴 1: 공백 뒤에 오는 이메일 주소 제거
            pattern1 = r"\s+\S+@\S+\.\S+$"

            # 패턴 2: 괄호 안의 이메일 주소와 괄호 제거
            pattern2 = r"\s*\(\S+@\S+\.\S+\)"

            # 패턴 3: 맨 뒤의 직함 제거
            pattern3 = r"\s+(기자|인턴기자|인턴|캐스터|기상캐스터|PD|리포터|편집장|외신캐스터)$"

            # 패턴 순차적으로 적용
            reporter = re.sub(pattern1, "", reporter)
            reporter = re.sub(pattern2, "", reporter)
            reporter = re.sub(pattern3, "", reporter)

            return reporter.strip()

        return ""

    def get_content(self, soup: BeautifulSoup) -> str:
        """
        BeautifulSoup 객체에서 뉴스 본문 추출

        Args:
            soup: BeautifulSoup 객체

        Returns:
            뉴스 본문
        """
        # 일반적인 네이버 뉴스 본문 요소 시도
        article = soup.find("article", class_="_article_content")
        if article:
            return self.clean_content(article.text)

        # 다른 본문 요소 타입 시도
        article = soup.find("div", id="newsct_article")
        if article:
            return self.clean_content(article.text)

        # 스포츠 뉴스 본문 요소 시도
        article = soup.find("div", id="newsEndContents")
        if article:
            return self.clean_content(article.text)

        # 기존 방식들도 시도
        content_selectors = [
            "#dic_area",  # 일반 뉴스
            ".se-main-container",  # 스마트에디터
            ".se-component-content",  # 스마트에디터 새 버전
            ".news_end",  # 구버전
            "#articleBodyContents",  # 구버전
        ]

        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # 불필요한 태그 제거
                for unwanted in content_elem.find_all(["script", "style"]):
                    unwanted.decompose()

                content = content_elem.get_text(strip=True)
                return self.clean_content(content)

        return ""

    def clean_content(self, text: str) -> str:
        """
        본문 텍스트 정리

        Args:
            text: 원본 본문 텍스트

        Returns:
            정리된 텍스트
        """
        # 연속된 줄바꿈을 하나로 통합
        text = re.sub(r"\n{2,}", "\n", text)
        # 앞뒤 공백 제거
        text = text.strip()

        # 최대 700자로 제한
        if len(text) > 700:
            text = text[:700]

        return text

    def extract_article_content(self, naver_url: str) -> Optional[Article]:
        """
        네이버 뉴스에서 제목, 기자명, 출판사명, 본문 추출

        Args:
            naver_url: 네이버 뉴스 URL

        Returns:
            크롤링 결과 객체 또는 None
        """
        try:
            response = self.session.get(naver_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            title = self.get_title(soup)
            content = self.get_content(soup)
            reporter = self.get_reporter(soup)
            publisher = self.get_publisher(soup)

            if not title or not content:
                logger.warning(f"Failed to extract essential content from {naver_url}")
                return None

            return Article(
                title=title,
                content=content,
                journalist_name=reporter,
                publisher=publisher,
                published_at=datetime.now(),  # Placeholder, actual date will be fetched from API
                naver_url=naver_url,
            )

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
            api_title = BeautifulSoup(re.sub(r"&[^;]+;", "", item["title"]), "html.parser").get_text()
            description = BeautifulSoup(re.sub(r"&[^;]+;", "", item["description"]), "html.parser").get_text()

            # 네이버 뉴스 URL인지 확인
            original_link = item["originallink"]
            link = item["link"]

            naver_url = link if "news.naver.com" in link else None
            if not naver_url:
                logger.debug(f"네이버 뉴스 링크가 아님: {link}")
                return None

            # 상세 기사 정보 추출 (제목, 기자명, 출판사명, 본문)
            crawl_result = self.extract_article_content(naver_url)

            # 추출 실패 시 API 데이터 사용
            if not crawl_result:
                title = api_title
                content = description
                journalist_name = "익명"
                publisher = "네이버뉴스"
            else:
                # 크롤링된 데이터 우선 사용, 빈 값이면 API 데이터로 대체
                title = crawl_result.title if crawl_result.title else api_title
                content = crawl_result.content if crawl_result.content else description
                journalist_name = crawl_result.journalist_name if crawl_result.journalist_name else "익명"
                publisher = crawl_result.publisher if crawl_result.publisher else "네이버뉴스"

            # 기자명 정규화 (빈 값이나 공백만 있는 경우 처리)
            journalist_name = journalist_name.strip()
            if not journalist_name or journalist_name in ["기자", "사용자"]:
                journalist_name = "익명"

            # 기사 제목이 9자 미만이면 None 반환
            if len(title.strip()) < 9:
                logger.debug(f"제목이 너무 짧음(9자 미만): {title}")
                return None

            # 내용 길이 체크 및 제한
            if len(content) < 100:
                # 본문이 너무 짧으면 description을 추가
                if len(content + " " + description) >= 100:
                    content = content + " " + description
                else:
                    # 그래도 짧으면 None 반환
                    logger.warning(f"내용이 너무 짧습니다: {title[:50]}...")
                    return None

            # 발행시간 파싱
            pub_date_str = item["pubDate"]
            pub_date = date_parser.parse(pub_date_str)

            # 한국 시간으로 변환
            kst = pytz.timezone("Asia/Seoul")
            pub_date = pub_date.astimezone(kst)

            article = Article(
                title=title,
                content=content,
                journalist_name=journalist_name,
                publisher=publisher,
                published_at=pub_date,
                naver_url=naver_url,
            )

            return article

        except Exception as e:
            logger.error(f"기사 파싱 중 오류 발생: {e}")
            return None

    def crawl_by_keywords(
        self,
        keywords: List[str],
        target_date: Optional[datetime] = None,
        check_duplicates: bool = True,
    ) -> List[Article]:
        """
        키워드별 뉴스 크롤링

        Args:
            keywords: 검색 키워드 리스트
            target_date: 특정 날짜 필터링 (None이면 모든 날짜)
            check_duplicates: 중복 기사 체크 여부 (기본값: True)

        Returns:
            크롤링된 기사 리스트
        """
        all_articles = []

        for keyword in keywords:
            logger.info(f"키워드 크롤링 시작: {keyword}")
            if target_date:
                logger.info(f"대상 날짜 필터링: {target_date.strftime('%Y-%m-%d')}")
            if not check_duplicates:
                logger.info("중복 체크 비활성화 모드")

            try:
                keyword_articles = []
                start = 1
                display = 100  # API 최대 100개씩 요청

                while True:
                    # API 검색 (날짜순 정렬)
                    items = self.search_news_api(query=keyword, display=display, start=start, sort="date")

                    if not items:
                        logger.info(f"더 이상 검색 결과가 없습니다: {keyword}")
                        break

                    current_batch_articles = []
                    should_stop = False

                    # 1단계: 기사 파싱 및 날짜 필터링
                    parsed_articles = []
                    for item in items:
                        article = self.parse_api_item(item)
                        if not article:
                            continue

                        # 날짜 필터링
                        if target_date:
                            article_date = article.published_at.date()
                            target_date_only = target_date.date()

                            # 대상 날짜보다 과거면 중단
                            if article_date < target_date_only:
                                logger.info(
                                    f"과거 날짜 도달 ({article_date}), 대상 날짜: {target_date_only} - 크롤링 중단"
                                )
                                should_stop = True
                                break

                            # 대상 날짜와 일치하지 않으면 스킵
                            if article_date != target_date_only:
                                continue

                        parsed_articles.append(article)
                        # Rate limiting
                        time.sleep(0.1)

                    # 2단계: 배치 중복 체크
                    if check_duplicates and parsed_articles:
                        # 2-1단계: 배치 내 중복 URL 제거 (같은 배치에서 중복된 naver_url 처리)
                        seen_urls = set()
                        deduplicated_articles = []

                        for article in parsed_articles:
                            if article.naver_url not in seen_urls:
                                deduplicated_articles.append(article)
                                seen_urls.add(article.naver_url)
                            else:
                                logger.debug(f"배치 내 중복 URL 제거: {article.title[:50]}...")

                        if len(deduplicated_articles) < len(parsed_articles):
                            logger.info(f"배치 내 중복 제거: {len(parsed_articles)}개 → {len(deduplicated_articles)}개")

                        # 2-2단계: 데이터베이스와 중복 체크
                        naver_urls = [article.naver_url for article in deduplicated_articles]
                        duplicate_status = self.db_ops.check_duplicate_articles_batch(naver_urls)

                        # 중복이 아닌 기사들만 추가
                        for article in deduplicated_articles:
                            if not duplicate_status.get(article.naver_url, False):
                                current_batch_articles.append(article)
                            else:
                                logger.debug(f"DB 중복 기사 스킵: {article.title[:50]}...")

                        logger.info(
                            f"배치 중복 체크 완료: {len(parsed_articles)}개 중 {len(current_batch_articles)}개 신규"
                        )
                    else:
                        # 중복 체크 없이 모든 기사 추가
                        current_batch_articles.extend(parsed_articles)

                    keyword_articles.extend(current_batch_articles)

                    # 중단 조건 체크
                    if should_stop:
                        break

                    # 다음 페이지로
                    start += display

                    # API 제한 (최대 1000개까지)
                    if start > 1000:
                        logger.warning(f"API 검색 제한 도달: {keyword}")
                        break

                    # 페이지 간 대기
                    time.sleep(1)

                all_articles.extend(keyword_articles)
                logger.info(f"키워드 '{keyword}' 크롤링 완료: {len(keyword_articles)}개")

                # 키워드 간 대기
                time.sleep(1)

            except Exception as e:
                logger.error(f"키워드 '{keyword}' 크롤링 실패: {e}")
                continue

        logger.info(f"전체 크롤링 완료: {len(all_articles)}개 기사")
        return all_articles

    def crawl_and_save(
        self,
        keywords: List[str],
        target_date: Optional[datetime] = None,
    ) -> int:
        """
        뉴스 크롤링 및 데이터베이스 저장

        Args:
            keywords: 검색 키워드 리스트
            target_date: 특정 날짜 필터링 (None이면 모든 날짜)

        Returns:
            저장된 기사 수
        """
        try:
            # 크롤링 (중복 체크 포함)
            articles = self.crawl_by_keywords(
                keywords=keywords,
                target_date=target_date,
                check_duplicates=True,
            )

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
