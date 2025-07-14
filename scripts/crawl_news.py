#!/usr/bin/env python3
"""
네이버 뉴스 크롤링 스크립트
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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


def validate_date_format(date_string: str) -> datetime:
    """
    날짜 형식 검증 및 변환

    Args:
        date_string: YYYY-MM-DD 형태의 날짜 문자열

    Returns:
        datetime 객체

    Raises:
        ValueError: 잘못된 날짜 형식 또는 과거 3개월 이전 날짜
    """
    try:
        target_date = datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형태로 입력해주세요: {date_string}")

    # 현재 날짜 기준 3개월 전 날짜 계산
    today = datetime.now()
    three_months_ago = today - relativedelta(months=3)

    # 입력된 날짜가 3개월 이전인지 확인
    if target_date.date() < three_months_ago.date():
        raise ValueError(
            f"입력된 날짜({target_date.strftime('%Y-%m-%d')})가 너무 과거입니다. "
            f"최근 3개월 이내의 날짜만 입력 가능합니다. "
            f"(최소 날짜: {three_months_ago.strftime('%Y-%m-%d')})"
        )

    # 미래 날짜인지 확인
    if target_date.date() > today.date():
        raise ValueError(
            f"입력된 날짜({target_date.strftime('%Y-%m-%d')})가 미래 날짜입니다. "
            f"오늘 날짜 이전의 날짜만 입력 가능합니다."
        )

    return target_date


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="네이버 뉴스 크롤링")
    parser.add_argument("--keywords", nargs="*", default=settings.DEFAULT_KEYWORDS, help="검색 키워드 목록")
    parser.add_argument(
        "--max-per-keyword", type=int, default=settings.MAX_ARTICLES_PER_KEYWORD, help="키워드당 최대 기사 수"
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="크롤링할 기사의 작성 날짜 (YYYY-MM-DD 형식, 최근 3개월 이내, 예: 2024-01-15)",
    )
    parser.add_argument("--dry-run", action="store_true", help="실제 저장 없이 테스트만 수행")

    args = parser.parse_args()

    try:
        logger.info(f"크롤링 시작: {__name__}")
        logger.info(f"키워드: {args.keywords}")
        logger.info(f"키워드당 최대 기사 수: {args.max_per_keyword}")

        # 날짜 파라미터 검증 (이제 필수값)
        target_date = validate_date_format(args.date)
        logger.info(f"대상 날짜: {target_date.strftime('%Y-%m-%d')}")

        # 설정 검증
        if not settings.validate():
            logger.error("필수 환경변수가 설정되지 않았습니다")
            sys.exit(1)

        # 크롤러 초기화
        crawler = NaverNewsCrawler(client_id=settings.NAVER_CLIENT_ID, client_secret=settings.NAVER_CLIENT_SECRET)

        if args.dry_run:
            logger.info("DRY RUN 모드 - 실제 저장하지 않습니다")
            # 크롤링만 수행하고 저장하지 않음 (중복 체크도 하지 않음)
            articles = crawler.crawl_by_keywords(
                keywords=args.keywords,
                max_articles_per_keyword=args.max_per_keyword,
                target_date=target_date,
                check_duplicates=False,
            )
            logger.info(f"크롤링된 기사 수: {len(articles)}")
            saved_count = len(articles)
        else:
            # 크롤링 및 저장 (중복 체크 포함)
            saved_count = crawler.crawl_and_save(
                keywords=args.keywords, max_articles_per_keyword=args.max_per_keyword, target_date=target_date
            )

        logger.info(f"크롤링 완료: {saved_count}개 기사 처리")

    except ValueError as e:
        logger.error(f"입력값 오류: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"크롤링 실행 중 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
