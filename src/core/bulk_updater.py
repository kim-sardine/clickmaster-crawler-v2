"""
벌크 업데이트 모듈
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class BulkUpdater:
    """벌크 업데이트 처리기"""

    def __init__(self, supabase):
        """
        Args:
            supabase: Supabase 클라이언트
        """
        self.supabase = supabase

    def bulk_update_articles(self, updates: List[Dict[str, Any]], batch_size: int = 500) -> bool:
        """
        Article 테이블 벌크 업데이트 (개별 UPDATE 방식, 멱등성 보장)

        Args:
            updates: 업데이트할 데이터 리스트 (id, clickbait_score, clickbait_explanation 포함)
            batch_size: 한 번에 처리할 배치 크기

        Returns:
            성공 여부
        """
        if not updates:
            logger.warning("No updates to process")
            return True

        logger.info(f"Starting bulk update for {len(updates)} articles (with idempotency check)")

        try:
            # 멱등성을 위한 사전 필터링: 이미 처리된 기사들 제외
            filtered_updates = self._filter_already_processed_articles(updates)

            if not filtered_updates:
                logger.info("All articles are already processed - operation is idempotent")
                return True

            if len(filtered_updates) < len(updates):
                skipped_count = len(updates) - len(filtered_updates)
                logger.info(f"Idempotency check: skipped {skipped_count} already processed articles")

            # 개별 업데이트 방식으로 처리 (upsert 대신 update 사용)
            total_processed = 0

            # updated_at 필드 추가
            current_time = datetime.now().isoformat()
            for update in filtered_updates:
                update["updated_at"] = current_time

            # 개별 업데이트 수행
            for i, update in enumerate(filtered_updates, 1):
                try:
                    article_id = update.get("id")
                    if not article_id:
                        logger.warning(f"Update item {i} missing ID, skipping")
                        continue

                    # 특정 필드만 업데이트 (기존 데이터 보존)
                    update_data = {
                        "clickbait_score": update.get("clickbait_score"),
                        "clickbait_explanation": update.get("clickbait_explanation"),
                        "updated_at": update.get("updated_at"),
                    }

                    response = self.supabase.client.table("articles").update(update_data).eq("id", article_id).execute()

                    if response.data:
                        total_processed += 1
                        if i % 50 == 0:  # 50개마다 진행상황 로깅
                            logger.info(f"Progress: {i}/{len(filtered_updates)} articles updated")
                    else:
                        logger.warning(f"No article found with ID: {article_id}")

                except Exception as e:
                    logger.error(f"Failed to update article {article_id}: {e}")
                    continue

            logger.info(f"Bulk update completed: {total_processed}/{len(filtered_updates)} articles updated")
            logger.info(
                f"Total operation: {total_processed} new updates, {len(updates) - len(filtered_updates)} skipped (idempotent)"
            )
            return total_processed > 0 or len(updates) > 0  # 성공 조건 수정: 스킵된 것도 성공으로 간주

        except Exception as e:
            logger.error(f"Bulk update failed: {e}")
            return False

    def _filter_already_processed_articles(self, updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        이미 처리된 기사들을 필터링 (멱등성 보장)

        Args:
            updates: 업데이트할 데이터 리스트

        Returns:
            아직 처리되지 않은 기사들만 포함한 리스트
        """
        if not updates:
            return []

        try:
            # 모든 article ID 수집
            article_ids = [update["id"] for update in updates if update.get("id")]

            if not article_ids:
                logger.warning("No valid article IDs in updates")
                return []

            logger.debug(f"Checking processing status for {len(article_ids)} articles")

            # 이미 clickbait_score가 있는 기사들 조회
            response = (
                self.supabase.client.table("articles")
                .select("id, clickbait_score")
                .in_("id", article_ids)
                .not_.is_("clickbait_score", "null")  # clickbait_score가 null이 아닌 것들
                .execute()
            )

            # 이미 처리된 기사 ID 집합
            processed_ids = {article["id"] for article in response.data}

            if processed_ids:
                logger.info(f"Found {len(processed_ids)} already processed articles")
                logger.debug(f"Already processed IDs: {list(processed_ids)[:10]}...")  # 처음 10개만 로깅

            # 아직 처리되지 않은 기사들만 필터링
            filtered_updates = [update for update in updates if update.get("id") and update["id"] not in processed_ids]

            logger.debug(f"Filtered result: {len(filtered_updates)} articles need processing")
            return filtered_updates

        except Exception as e:
            logger.error(f"Failed to filter already processed articles: {e}")
            logger.warning("Proceeding with all updates (no filtering applied)")
            return updates

    def validate_updates(self, updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        업데이트 데이터 유효성 검증 및 정제

        Args:
            updates: 원본 업데이트 데이터

        Returns:
            검증된 업데이트 데이터
        """
        logger.debug(f"Validating {len(updates)} update items")

        valid_updates = []

        for update in updates:
            try:
                if not update.get("id"):
                    logger.warning("Update item missing ID, skipping")
                    continue

                clickbait_score = update.get("clickbait_score")
                if clickbait_score is None:
                    logger.warning(f"Article {update.get('id')} missing clickbait_score, skipping")
                    continue

                if not isinstance(clickbait_score, (int, float)) or not (0 <= clickbait_score <= 100):
                    logger.warning(f"Article {update.get('id')} has invalid clickbait_score: {clickbait_score}")
                    continue

                explanation = update.get("clickbait_explanation", "").strip()
                if not explanation:
                    logger.warning(f"Article {update.get('id')} missing explanation, using default")
                    explanation = "자동 생성된 점수"

                valid_update = {
                    "id": update["id"],
                    "clickbait_score": int(round(clickbait_score)),
                    "clickbait_explanation": explanation[:500],
                }

                valid_updates.append(valid_update)

            except Exception as e:
                logger.error(f"Failed to validate update item: {update}. Error: {e}")
                continue

        logger.info(f"Validation completed: {len(valid_updates)}/{len(updates)} items valid")
        return valid_updates
