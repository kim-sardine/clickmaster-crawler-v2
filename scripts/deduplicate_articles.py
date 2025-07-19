#!/usr/bin/env python3
"""
중복 기사 통합 스크립트

중복 기준:
- title, content, journalist_name, publisher가 모두 동일하면 중복
- clickbait_score가 가장 높은 뉴스만 남기고 나머지 삭제
- clickbait_score가 동일하면 가장 먼저 생성된 뉴스만 남기고 나머지 삭제
- clickbait_score가 null이면 0점으로 간주
"""

import argparse
import sys
from datetime import datetime
from typing import List, Dict, Any

from src.config.settings import settings
from src.database.supabase_client import get_supabase_client
from src.utils.logging_utils import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def find_duplicate_groups() -> List[Dict[str, Any]]:
    """
    중복 기사 그룹 찾기

    Returns:
        중복 그룹 리스트 (각 그룹은 중복 기사들의 정보를 포함)
    """
    client = get_supabase_client()

    # title, content, journalist_name, publisher 기준으로 그룹화하여 중복 찾기
    query = """
    SELECT 
        title,
        content,
        journalist_name,
        publisher,
        COUNT(*) as duplicate_count,
        array_agg(
            json_build_object(
                'id', id,
                'clickbait_score', COALESCE(clickbait_score, 0),
                'created_at', created_at,
                'naver_url', naver_url
            ) 
            ORDER BY COALESCE(clickbait_score, 0) DESC, created_at ASC
        ) as articles
    FROM articles
    GROUP BY title, content, journalist_name, publisher
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
    """

    result = client.client.rpc("execute_sql", {"query": query}).execute()

    if result.data and result.data[0]["result"]:
        duplicate_groups = result.data[0]["result"]
        logger.info(
            f"중복 그룹 발견: {len(duplicate_groups)}개 그룹, 총 중복 기사 수: {sum(g['duplicate_count'] for g in duplicate_groups)}"
        )
        return duplicate_groups
    else:
        logger.info("중복 기사가 발견되지 않았습니다")
        return []


def deduplicate_group(group: Dict[str, Any], dry_run: bool = False) -> int:
    """
    중복 그룹 통합 (첫 번째 기사만 남기고 나머지 삭제)

    Args:
        group: 중복 그룹 정보
        dry_run: 실제 삭제하지 않고 시뮬레이션만 수행

    Returns:
        삭제된 기사 수
    """
    client = get_supabase_client()

    articles = group["articles"]
    keep_article = articles[0]  # 가장 높은 점수/가장 먼저 생성된 기사
    delete_articles = articles[1:]  # 나머지 삭제할 기사들

    logger.info(
        f"그룹 통합: '{group['title'][:50]}...' "
        f"(유지: {keep_article['clickbait_score']}점, 삭제: {len(delete_articles)}개)"
    )

    if dry_run:
        logger.info(f"[DRY RUN] {len(delete_articles)}개 기사 삭제 예정")
        return len(delete_articles)

    deleted_count = 0

    for article in delete_articles:
        try:
            result = client.client.table("articles").delete().eq("id", article["id"]).execute()

            if result.data:
                deleted_count += 1
                logger.debug(f"기사 삭제 완료: {article['id']}")
            else:
                logger.warning(f"기사 삭제 실패: {article['id']}")

        except Exception as e:
            logger.error(f"기사 삭제 오류 [{article['id']}]: {e}")

    return deleted_count


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="중복 기사 통합")
    parser.add_argument("--dry-run", action="store_true", help="실제 삭제하지 않고 시뮬레이션만 수행")

    args = parser.parse_args()

    logger.info("🔄 중복 기사 통합 작업 시작")
    logger.info(f"모드: {'DRY RUN' if args.dry_run else 'LIVE'}")

    try:
        # 중복 그룹 찾기
        duplicate_groups = find_duplicate_groups()

        if not duplicate_groups:
            logger.info("중복 기사가 없습니다")
            return 0

        # 모든 중복 그룹 처리
        total_deleted = 0
        successful_groups = 0

        for i, group in enumerate(duplicate_groups, 1):
            try:
                deleted_count = deduplicate_group(group, dry_run=args.dry_run)
                total_deleted += deleted_count
                successful_groups += 1

                logger.info(
                    f"그룹 {i}/{len(duplicate_groups)} 완료: "
                    f"{deleted_count}개 삭제 ({'시뮬레이션' if args.dry_run else '실제'})"
                )

            except Exception as e:
                logger.error(f"그룹 {i} 처리 실패: {e}")
                continue

        # 결과 요약
        logger.info("=" * 50)
        logger.info(f"중복 기사 통합 완료")
        logger.info(f"처리된 그룹: {successful_groups}/{len(duplicate_groups)}")
        logger.info(f"{'시뮬레이션' if args.dry_run else '실제'} 삭제된 기사: {total_deleted}개")

        if args.dry_run:
            logger.info("실제 실행하려면 --dry-run 옵션을 제거하세요")

        return total_deleted

    except Exception as e:
        logger.error(f"중복 기사 통합 작업 실패: {e}")
        return -1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(0 if exit_code >= 0 else 1)
