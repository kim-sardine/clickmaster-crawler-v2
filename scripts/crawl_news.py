#!/usr/bin/env python3
"""
네이버 뉴스 크롤링 스크립트
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings
from src.crawlers.naver_crawler import NaverNewsCrawler

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT,
    handlers=[
        logging.FileHandler(f"logs/crawl_news_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="네이버 뉴스 크롤링")
    parser.add_argument("--keywords", nargs="*", default=settings.DEFAULT_KEYWORDS, help="검색 키워드 목록")
    parser.add_argument(
        "--max-per-keyword", type=int, default=settings.MAX_ARTICLES_PER_KEYWORD, help="키워드당 최대 기사 수"
    )
    parser.add_argument("--dry-run", action="store_true", help="실제 저장 없이 테스트만 수행")

    args = parser.parse_args()

    try:
        logger.info(f"크롤링 시작: {__name__}")
        logger.info(f"키워드: {args.keywords}")
        logger.info(f"키워드당 최대 기사 수: {args.max_per_keyword}")

        # 설정 검증
        if not settings.validate():
            logger.error("필수 환경변수가 설정되지 않았습니다")
            sys.exit(1)

        # 크롤러 초기화
        crawler = NaverNewsCrawler(client_id=settings.NAVER_CLIENT_ID, client_secret=settings.NAVER_CLIENT_SECRET)

        if args.dry_run:
            logger.info("DRY RUN 모드 - 실제 저장하지 않습니다")
            # 크롤링만 수행하고 저장하지 않음
            articles = crawler.crawl_by_keywords(keywords=args.keywords, max_articles_per_keyword=args.max_per_keyword)
            logger.info(f"크롤링된 기사 수: {len(articles)}")
            saved_count = len(articles)
        else:
            # 크롤링 및 저장
            saved_count = crawler.crawl_and_save(keywords=args.keywords, max_articles_per_keyword=args.max_per_keyword)

        logger.info(f"크롤링 완료: {saved_count}개 기사 처리")

    except Exception as e:
        logger.error(f"크롤링 실행 중 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
