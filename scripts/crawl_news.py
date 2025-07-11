#!/usr/bin/env python3
"""
뉴스 크롤링 및 Supabase 저장 스크립트
매일 오전 6시에 실행되어 전날 뉴스를 수집하여 Supabase에 저장
"""

import sys
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
from typing import List, Optional
from tqdm import tqdm

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import Settings
from src.config.keywords import get_keywords_by_category
from src.models.base import News
from src.database.operations import NewsOperations
from src.utils.logging_utils import setup_logger, log_function_start, log_function_end, log_error, log_progress
from src.utils.date_utils import get_yesterday_date_range, parse_naver_date, is_within_date_range
from src.utils.text_utils import clean_html, extract_main_content, is_valid_title, is_valid_content

logger = setup_logger(__name__)


class NaverNewsCrawler:
    """네이버 뉴스 크롤러"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(Settings.get_headers())
        self.collected_urls = set()

    def search_news_by_keyword(self, keyword: str, start_date: str, end_date: str) -> List[dict]:
        """키워드로 뉴스 검색"""
        try:
            url = "https://openapi.naver.com/v1/search/news.json"

            all_news = []
            start = 1

            while len(all_news) < Settings.MAX_NEWS_PER_KEYWORD:
                params = {"query": keyword, "display": Settings.NAVER_DISPLAY_COUNT, "start": start, "sort": "date"}

                response = self.session.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                items = data.get("items", [])

                if not items:
                    break

                # 날짜 필터링
                filtered_items = []
                for item in items:
                    pub_date = parse_naver_date(item["pubDate"])
                    if is_within_date_range(pub_date, start_date, end_date):
                        # URL 중복 제거
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

    def crawl_article_content(self, url: str) -> Optional[dict]:
        """개별 기사 내용 크롤링"""
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
            author = None
            author_selectors = [".byline", ".reporter", ".author", "[data-reporter]"]

            for selector in author_selectors:
                element = soup.select_one(selector)
                if element:
                    author = element.get_text(strip=True)
                    break

            # 언론사명 추출
            source = ""
            source_selectors = [".press", ".source", ".media", "[data-press]"]

            for selector in source_selectors:
                element = soup.select_one(selector)
                if element:
                    source = element.get_text(strip=True)
                    break

            return {"content": content, "author": author, "source": source}

        except Exception as e:
            log_error(logger, e, f"crawling article: {url}")
            return None

    def process_news_item(self, item: dict) -> Optional[News]:
        """뉴스 아이템 처리"""
        try:
            # 기본 정보 추출
            title = clean_html(item["title"])
            url = item["link"]
            pub_date = parse_naver_date(item["pubDate"]).strftime("%Y-%m-%d")

            # 제목 유효성 검사
            if not is_valid_title(title):
                logger.debug(f"Invalid title: {title}")
                return None

            # URL 중복 확인
            if NewsOperations.check_duplicate_url(url):
                logger.debug(f"Duplicate URL: {url}")
                return None

            # 상세 내용 크롤링
            article_data = self.crawl_article_content(url)
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


def main():
    """메인 함수"""
    try:
        log_function_start(logger, "crawl_news")

        # 환경변수 검증
        Settings.validate_required_env_vars()

        # 어제 날짜 범위 계산
        start_date, end_date = get_yesterday_date_range()
        logger.info(f"📅 Crawling news for date range: {start_date} - {end_date}")

        # 크롤러 초기화
        crawler = NaverNewsCrawler()

        # 키워드 목록 가져오기
        keywords = get_keywords_by_category()
        logger.info(f"🔍 Starting crawl with {len(keywords)} keywords")

        all_news = []
        total_collected = 0

        # 키워드별 뉴스 수집
        for i, keyword in enumerate(tqdm(keywords, desc="Keywords")):
            try:
                # 네이버 API로 뉴스 검색
                search_results = crawler.search_news_by_keyword(keyword, start_date, end_date)

                if not search_results:
                    continue

                # 각 검색 결과 처리
                keyword_news = []
                for item in tqdm(search_results, desc=f"Processing {keyword}", leave=False):
                    news = crawler.process_news_item(item)
                    if news:
                        keyword_news.append(news)

                    # 요청 제한 고려
                    time.sleep(Settings.REQUEST_DELAY)

                all_news.extend(keyword_news)
                total_collected += len(keyword_news)

                log_progress(logger, i + 1, len(keywords), f"Keywords processed")
                logger.info(f"📊 Keyword '{keyword}': {len(keyword_news)} valid articles")

            except Exception as e:
                log_error(logger, e, f"processing keyword: {keyword}")
                continue

        logger.info(f"📈 Total news collected: {total_collected}")

        # Supabase에 저장
        if all_news:
            logger.info("💾 Saving news to Supabase...")
            saved_count = NewsOperations.insert_news_batch(all_news)
            logger.info(f"✅ Successfully saved {saved_count} news articles to Supabase")
        else:
            logger.warning("⚠️ No news articles to save")

        # 최종 통계
        stats = NewsOperations.get_news_stats()
        logger.info(f"📊 Final stats: {stats}")

        log_function_end(logger, "crawl_news")

    except Exception as e:
        log_error(logger, e, "main crawl_news function")
        sys.exit(1)


if __name__ == "__main__":
    main()
