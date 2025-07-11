from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from ..models.base import News, BatchJob
from ..models.batch_status import BatchStatus
from .supabase_client import supabase_client
from ..utils.date_utils import format_datetime_for_db, get_kst_now

logger = logging.getLogger(__name__)


class NewsOperations:
    """뉴스 데이터 조작"""

    @staticmethod
    def insert_news_batch(news_list: List[News]) -> int:
        """뉴스 배치 삽입"""
        try:
            if not news_list:
                return 0

            # News 객체를 딕셔너리로 변환
            news_data = []
            for news in news_list:
                data = {
                    "title": news.title,
                    "content": news.content,
                    "url": news.url,
                    "published_date": news.published_date,
                    "source": news.source,
                    "author": news.author,
                    "created_at": format_datetime_for_db(get_kst_now()),
                }
                news_data.append(data)

            result = supabase_client.client.table("articles").insert(news_data).execute()

            logger.info(f"✅ Inserted {len(result.data)} news articles")
            return len(result.data)

        except Exception as e:
            logger.error(f"❌ Failed to insert news batch: {str(e)}")
            raise

    @staticmethod
    def check_duplicate_url(url: str) -> bool:
        """URL 중복 확인"""
        try:
            result = supabase_client.client.table("articles").select("id").eq("url", url).execute()
            return len(result.data) > 0
        except Exception as e:
            logger.error(f"❌ Failed to check duplicate URL: {str(e)}")
            return False

    @staticmethod
    def get_unprocessed_news(limit: int = 100) -> List[Dict[str, Any]]:
        """미처리 뉴스 조회 (clickbait_score가 null인 기사)"""
        try:
            result = (
                supabase_client.client.table("articles")
                .select("id, title, content")
                .is_("clickbait_score", "null")
                .limit(limit)
                .execute()
            )

            logger.info(f"📋 Found {len(result.data)} unprocessed news articles")
            return result.data

        except Exception as e:
            logger.error(f"❌ Failed to get unprocessed news: {str(e)}")
            raise

    @staticmethod
    def update_clickbait_scores(score_updates: List[Dict[str, Any]]) -> int:
        """낚시성 점수 업데이트"""
        try:
            updated_count = 0

            for update in score_updates:
                result = (
                    supabase_client.client.table("articles")
                    .update(
                        {
                            "clickbait_score": update["clickbait_score"],
                            "reasoning": update["reasoning"],
                            "updated_at": format_datetime_for_db(get_kst_now()),
                        }
                    )
                    .eq("id", update["id"])
                    .execute()
                )

                if result.data:
                    updated_count += len(result.data)

            logger.info(f"✅ Updated clickbait scores for {updated_count} articles")
            return updated_count

        except Exception as e:
            logger.error(f"❌ Failed to update clickbait scores: {str(e)}")
            raise

    @staticmethod
    def get_news_by_date_range(start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """날짜 범위로 뉴스 조회"""
        try:
            result = (
                supabase_client.client.table("articles")
                .select("*")
                .gte("published_date", start_date)
                .lte("published_date", end_date)
                .execute()
            )

            logger.info(f"📋 Found {len(result.data)} news articles in date range")
            return result.data

        except Exception as e:
            logger.error(f"❌ Failed to get news by date range: {str(e)}")
            raise

    @staticmethod
    def get_news_stats() -> Dict[str, int]:
        """뉴스 통계 조회"""
        try:
            # 전체 뉴스 수
            total_result = supabase_client.client.table("articles").select("id").execute()
            total_count = len(total_result.data)

            # 처리된 뉴스 수
            processed_result = (
                supabase_client.client.table("articles").select("id").not_.is_("clickbait_score", "null").execute()
            )
            processed_count = len(processed_result.data)

            # 미처리 뉴스 수
            unprocessed_count = total_count - processed_count

            stats = {"total": total_count, "processed": processed_count, "unprocessed": unprocessed_count}

            logger.info(f"📊 News stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ Failed to get news stats: {str(e)}")
            raise


class BatchOperations:
    """배치 작업 조작"""

    @staticmethod
    def create_batch_job(batch_id: str, input_file_id: str, total_count: int) -> bool:
        """배치 작업 생성"""
        try:
            data = {
                "batch_id": batch_id,
                "status": BatchStatus.PENDING.value,
                "input_file_id": input_file_id,
                "total_count": total_count,
                "created_at": format_datetime_for_db(get_kst_now()),
            }

            result = supabase_client.client.table("batch_jobs").insert(data).execute()

            logger.info(f"✅ Created batch job: {batch_id}")
            return len(result.data) > 0

        except Exception as e:
            logger.error(f"❌ Failed to create batch job: {str(e)}")
            raise

    @staticmethod
    def get_active_batches() -> List[Dict[str, Any]]:
        """진행 중인 배치 작업 조회"""
        try:
            result = (
                supabase_client.client.table("batch_jobs")
                .select("*")
                .in_("status", [BatchStatus.PENDING.value, BatchStatus.IN_PROGRESS.value])
                .execute()
            )

            logger.info(f"📋 Found {len(result.data)} active batch jobs")
            return result.data

        except Exception as e:
            logger.error(f"❌ Failed to get active batches: {str(e)}")
            raise

    @staticmethod
    def get_completed_batches() -> List[Dict[str, Any]]:
        """완료된 배치 작업 조회 (미처리)"""
        try:
            result = (
                supabase_client.client.table("batch_jobs")
                .select("*")
                .eq("status", BatchStatus.COMPLETED.value)
                .is_("output_file_id", "null")
                .execute()
            )

            logger.info(f"📋 Found {len(result.data)} completed unprocessed batches")
            return result.data

        except Exception as e:
            logger.error(f"❌ Failed to get completed batches: {str(e)}")
            raise

    @staticmethod
    def update_batch_status(batch_id: str, status: str, **kwargs) -> bool:
        """배치 상태 업데이트"""
        try:
            update_data = {"status": status, "updated_at": format_datetime_for_db(get_kst_now())}

            # 추가 필드 업데이트
            if "output_file_id" in kwargs:
                update_data["output_file_id"] = kwargs["output_file_id"]
            if "processed_count" in kwargs:
                update_data["processed_count"] = kwargs["processed_count"]
            if "error_message" in kwargs:
                update_data["error_message"] = kwargs["error_message"]
            if status == BatchStatus.COMPLETED.value:
                update_data["completed_at"] = format_datetime_for_db(get_kst_now())

            result = supabase_client.client.table("batch_jobs").update(update_data).eq("batch_id", batch_id).execute()

            logger.info(f"✅ Updated batch {batch_id} status to {status}")
            return len(result.data) > 0

        except Exception as e:
            logger.error(f"❌ Failed to update batch status: {str(e)}")
            raise

    @staticmethod
    def get_batch_by_id(batch_id: str) -> Optional[Dict[str, Any]]:
        """배치 ID로 조회"""
        try:
            result = supabase_client.client.table("batch_jobs").select("*").eq("batch_id", batch_id).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"❌ Failed to get batch by ID: {str(e)}")
            raise

    @staticmethod
    def cleanup_old_batches(days_old: int = 7) -> int:
        """오래된 배치 작업 정리"""
        try:
            cutoff_date = get_kst_now().replace(days=-days_old)
            cutoff_str = format_datetime_for_db(cutoff_date)

            # 완료된 오래된 배치들 삭제
            result = (
                supabase_client.client.table("batch_jobs")
                .delete()
                .eq("status", BatchStatus.COMPLETED.value)
                .lt("completed_at", cutoff_str)
                .execute()
            )

            deleted_count = len(result.data) if result.data else 0
            logger.info(f"🧹 Cleaned up {deleted_count} old batch jobs")

            return deleted_count

        except Exception as e:
            logger.error(f"❌ Failed to cleanup old batches: {str(e)}")
            raise
