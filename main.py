#!/usr/bin/env python3
"""
클릭마스터 크롤러 메인 실행 파일
"""

import sys
import logging
from datetime import datetime

from src.config.settings import settings
from src.crawlers.naver_crawler import NaverNewsCrawler

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT,
    handlers=[
        logging.FileHandler(f"logs/main_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def main():
    """메인 실행 함수"""
    try:
        logger.info("클릭마스터 크롤러 시작")

        # 설정 검증
        if not settings.validate():
            logger.error("필수 환경변수가 설정되지 않았습니다")
            logger.error("SUPABASE_URL, SUPABASE_KEY, NAVER_CLIENT_ID, NAVER_CLIENT_SECRET이 필요합니다")
            sys.exit(1)

        # 크롤러 초기화
        crawler = NaverNewsCrawler(client_id=settings.NAVER_CLIENT_ID, client_secret=settings.NAVER_CLIENT_SECRET)

        # 기본 키워드로 크롤링 실행
        saved_count = crawler.crawl_and_save(
            keywords=settings.DEFAULT_KEYWORDS, max_articles_per_keyword=settings.MAX_ARTICLES_PER_KEYWORD
        )

        logger.info(f"크롤링 완료: {saved_count}개 기사 저장")

    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
