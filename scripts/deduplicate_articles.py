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

    try:
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

    except Exception as e:
        logger.error(f"RPC 함수 사용 중 오류 발생: {e}")
        logger.info("대안 방법으로 중복 그룹을 찾습니다")
        # RPC 함수가 실패한 경우 대안 방법 사용
        return find_duplicate_groups_alternative()


def find_duplicate_groups_alternative() -> List[Dict[str, Any]]:
    """
    RPC 함수가 없는 경우 대안 방법으로 중복 그룹 찾기
    """
    client = get_supabase_client()

    try:
        # 모든 기사 조회
        result = client.client.table("articles").select("*").execute()

        if not result.data:
            logger.info("기사가 없습니다")
            return []

        # 파이썬에서 그룹화 처리
        content_groups = {}

        for article in result.data:
            content_key = (article["title"], article["content"], article["journalist_name"], article["publisher"])

            if content_key not in content_groups:
                content_groups[content_key] = []

            content_groups[content_key].append(
                {
                    "id": article["id"],
                    "clickbait_score": article["clickbait_score"] if article["clickbait_score"] is not None else 0,
                    "created_at": article["created_at"],
                    "naver_url": article["naver_url"],
                }
            )

        # 중복 그룹만 필터링
        duplicate_groups = []
        for (title, content, journalist_name, publisher), articles in content_groups.items():
            if len(articles) > 1:
                # clickbait_score 내림차순, created_at 오름차순으로 정렬
                articles.sort(key=lambda x: (-x["clickbait_score"], x["created_at"]))

                duplicate_groups.append(
                    {
                        "title": title,
                        "content": content,
                        "journalist_name": journalist_name,
                        "publisher": publisher,
                        "duplicate_count": len(articles),
                        "articles": articles,
                    }
                )

        # 중복 수 내림차순으로 정렬
        duplicate_groups.sort(key=lambda x: -x["duplicate_count"])

        if duplicate_groups:
            total_duplicates = sum(g["duplicate_count"] for g in duplicate_groups)
            logger.info(f"중복 그룹 발견: {len(duplicate_groups)}개 그룹, 총 중복 기사 수: {total_duplicates}개")
        else:
            logger.info("중복 기사가 발견되지 않았습니다")

        return duplicate_groups

    except Exception as e:
        logger.error(f"대안 방법 중복 그룹 조회 오류: {e}")
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
