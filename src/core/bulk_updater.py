"""
벌크 업데이트 모듈
"""

import logging
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
        Article 테이블 벌크 업데이트

        Args:
            updates: 업데이트할 데이터 리스트 (id, clickbait_score, clickbait_explanation 포함)
            batch_size: 한 번에 처리할 배치 크기

        Returns:
            성공 여부
        """
        if not updates:
            logger.warning("No updates to process")
            return True

        logger.info(f"Starting bulk update for {len(updates)} articles")

        try:
            # 큰 배치를 작은 단위로 분할
            total_processed = 0

            for i in range(0, len(updates), batch_size):
                batch = updates[i : i + batch_size]

                # updated_at 필드 추가
                for update in batch:
                    update["updated_at"] = datetime.now().isoformat()

                # Supabase upsert 수행
                try:
                    response = self.supabase.client.table("articles").upsert(batch).execute()

                    batch_processed = len(batch)
                    total_processed += batch_processed

                    logger.info(f"Batch {i // batch_size + 1}: Updated {batch_processed} articles")

                except Exception as e:
                    logger.error(f"Failed to update batch {i // batch_size + 1}: {e}")

                    # 개별 처리로 fallback
                    success_count = self._fallback_individual_updates(batch)
                    total_processed += success_count

                    logger.info(f"Fallback completed: {success_count}/{len(batch)} articles updated")

            logger.info(f"Bulk update completed: {total_processed}/{len(updates)} articles updated")
            return total_processed > 0

        except Exception as e:
            logger.error(f"Bulk update failed: {e}")
            return False

    def _fallback_individual_updates(self, batch: List[Dict[str, Any]]) -> int:
        """
        배치 업데이트 실패 시 개별 업데이트로 fallback

        Args:
            batch: 업데이트할 배치 데이터

        Returns:
            성공한 업데이트 수
        """
        logger.warning("Falling back to individual updates")

        success_count = 0

        for update in batch:
            try:
                article_id = update.get("id")
                if not article_id:
                    logger.warning("Update item without ID, skipping")
                    continue

                # 개별 업데이트
                response = (
                    self.supabase.client.table("articles")
                    .update(
                        {
                            "clickbait_score": update.get("clickbait_score"),
                            "score_explanation": update.get("score_explanation"),
                            "updated_at": update.get("updated_at"),
                        }
                    )
                    .eq("id", article_id)
                    .execute()
                )

                success_count += 1

            except Exception as e:
                logger.error(f"Failed to update article {article_id}: {e}")
                continue

        return success_count

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
                # 필수 필드 확인
                if not update.get("id"):
                    logger.warning("Update item missing ID, skipping")
                    continue

                clickbait_score = update.get("clickbait_score")
                if clickbait_score is None:
                    logger.warning(f"Article {update.get('id')} missing clickbait_score, skipping")
                    continue

                # clickbait_score 유효성 검증
                if not isinstance(clickbait_score, (int, float)) or not (0 <= clickbait_score <= 100):
                    logger.warning(f"Article {update.get('id')} has invalid clickbait_score: {clickbait_score}")
                    continue

                # clickbait_explanation 확인
                explanation = update.get("clickbait_explanation", "").strip()
                if not explanation:
                    logger.warning(f"Article {update.get('id')} missing explanation, using default")
                    explanation = "자동 생성된 점수"

                # 정제된 데이터 생성
                valid_update = {
                    "id": int(update["id"]),
                    "clickbait_score": int(round(clickbait_score)),
                    "clickbait_explanation": explanation[:500],  # 길이 제한
                }

                valid_updates.append(valid_update)

            except Exception as e:
                logger.error(f"Failed to validate update item: {update}. Error: {e}")
                continue

        logger.info(f"Validation completed: {len(valid_updates)}/{len(updates)} items valid")
        return valid_updates

    def update_batch_status(self, batch_id: str, status: str, error_message: Optional[str] = None) -> bool:
        """
        배치 상태 업데이트

        Args:
            batch_id: 배치 ID
            status: 새로운 상태 ('completed', 'failed', 'cancelled')
            error_message: 에러 메시지 (선택사항)

        Returns:
            성공 여부
        """
        logger.info(f"Updating batch {batch_id} status to {status}")

        try:
            update_data = {
                "status": status,
                "completed_at": datetime.now().isoformat() if status in ["completed", "failed", "cancelled"] else None,
            }

            if error_message:
                update_data["error_message"] = error_message

            response = self.supabase.client.table("batch").update(update_data).eq("batch_id", batch_id).execute()

            if response.data:
                logger.info(f"Batch status updated successfully")
                return True
            else:
                logger.warning(f"No batch found with ID: {batch_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to update batch status: {e}")
            return False
