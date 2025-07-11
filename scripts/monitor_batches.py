#!/usr/bin/env python3
"""
배치 상태 모니터링 및 새 배치 시작 스크립트
매시간 실행되어 진행 중인 배치를 확인하고 필요시 새 배치를 시작
"""

import sys
import os
import json
import logging
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import Settings
from src.config.prompts import create_batch_request
from src.database.operations import NewsOperations, BatchOperations
from src.models.batch_status import BatchStatus
from src.utils.logging_utils import setup_logger, log_function_start, log_function_end, log_error, log_batch_status
from src.utils.file_utils import create_temp_file, write_jsonl_file, delete_file_safely
from src.utils.date_utils import get_kst_now, calculate_age_in_hours
import openai

logger = setup_logger(__name__)


class BatchMonitor:
    """배치 모니터링 및 관리"""

    def __init__(self):
        openai.api_key = Settings.OPENAI_API_KEY
        self.client = openai.OpenAI()

    def check_active_batches(self) -> List[Dict[str, Any]]:
        """진행 중인 배치 확인"""
        try:
            active_batches = BatchOperations.get_active_batches()

            for batch in active_batches:
                batch_id = batch["batch_id"]

                try:
                    # OpenAI에서 배치 상태 확인
                    batch_obj = self.client.batches.retrieve(batch_id)
                    openai_status = batch_obj.status

                    log_batch_status(logger, batch_id, openai_status)

                    # 상태 업데이트
                    if openai_status != batch["status"]:
                        update_data = {"status": openai_status}

                        if openai_status == "completed":
                            update_data["output_file_id"] = batch_obj.output_file_id
                        elif openai_status == "failed":
                            update_data["error_message"] = (
                                str(batch_obj.errors) if batch_obj.errors else "Unknown error"
                            )

                        BatchOperations.update_batch_status(batch_id, openai_status, **update_data)

                except Exception as e:
                    log_error(logger, e, f"checking batch status: {batch_id}")

                    # 24시간 이상 응답이 없는 배치는 실패로 처리
                    age_hours = calculate_age_in_hours(batch["created_at"])
                    if age_hours > 25:  # 25시간 후 타임아웃
                        BatchOperations.update_batch_status(
                            batch_id, BatchStatus.FAILED.value, error_message=f"Timeout after {age_hours:.1f} hours"
                        )
                        logger.warning(f"⏰ Batch {batch_id} timed out after {age_hours:.1f} hours")

            return active_batches

        except Exception as e:
            log_error(logger, e, "checking active batches")
            return []

    def create_new_batch(self) -> bool:
        """새 배치 생성"""
        try:
            # 미처리 뉴스 조회
            unprocessed_news = NewsOperations.get_unprocessed_news(Settings.BATCH_SIZE)

            if not unprocessed_news:
                logger.info("📋 No unprocessed news found")
                return False

            logger.info(f"📋 Found {len(unprocessed_news)} unprocessed news articles")

            # 배치 요청 데이터 생성
            batch_requests = []
            for news in unprocessed_news:
                request = create_batch_request(news_id=str(news["id"]), title=news["title"], content=news["content"])
                batch_requests.append(request)

            # 임시 JSONL 파일 생성
            temp_file = create_temp_file(suffix=".jsonl", prefix="batch_input_")

            try:
                # JSONL 파일 작성
                written_count = write_jsonl_file(batch_requests, temp_file)
                logger.info(f"📝 Created batch input file with {written_count} requests")

                # OpenAI에 파일 업로드
                with open(temp_file, "rb") as file:
                    file_obj = self.client.files.create(file=file, purpose="batch")

                logger.info(f"📤 Uploaded input file: {file_obj.id}")

                # 배치 생성
                batch_obj = self.client.batches.create(
                    input_file_id=file_obj.id, endpoint="/v1/chat/completions", completion_window="24h"
                )

                logger.info(f"🚀 Created batch: {batch_obj.id}")

                # Supabase에 배치 정보 저장
                BatchOperations.create_batch_job(
                    batch_id=batch_obj.id, input_file_id=file_obj.id, total_count=len(batch_requests)
                )

                log_batch_status(logger, batch_obj.id, "created", f"{len(batch_requests)} requests")
                return True

            finally:
                # 임시 파일 정리
                delete_file_safely(temp_file)

        except Exception as e:
            log_error(logger, e, "creating new batch")
            return False

    def should_create_new_batch(self, active_batches: List[Dict[str, Any]]) -> bool:
        """새 배치 생성 필요 여부 확인"""
        try:
            # 진행 중인 배치가 있으면 대기
            if active_batches:
                logger.info(f"⏳ {len(active_batches)} active batches found, waiting...")
                return False

            # 미처리 뉴스가 있는지 확인
            unprocessed_news = NewsOperations.get_unprocessed_news(1)  # 1개만 확인
            if not unprocessed_news:
                logger.info("✅ No unprocessed news found")
                return False

            # 미처리 뉴스 통계
            stats = NewsOperations.get_news_stats()
            unprocessed_count = stats.get("unprocessed", 0)

            if unprocessed_count >= Settings.BATCH_SIZE:
                logger.info(f"📊 {unprocessed_count} unprocessed articles found, creating batch...")
                return True
            elif unprocessed_count > 0:
                logger.info(f"📊 Only {unprocessed_count} unprocessed articles (need {Settings.BATCH_SIZE})")
                return False

            return False

        except Exception as e:
            log_error(logger, e, "checking if new batch needed")
            return False

    def cleanup_old_data(self):
        """오래된 데이터 정리"""
        try:
            # 오래된 배치 작업 정리 (7일 이상)
            cleaned_count = BatchOperations.cleanup_old_batches(days_old=7)
            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned up {cleaned_count} old batch jobs")

        except Exception as e:
            log_error(logger, e, "cleaning up old data")


def main():
    """메인 함수"""
    try:
        log_function_start(logger, "monitor_batches")

        # 환경변수 검증
        Settings.validate_required_env_vars()

        monitor = BatchMonitor()

        # 1. 진행 중인 배치 상태 확인
        logger.info("🔍 Checking active batches...")
        active_batches = monitor.check_active_batches()

        # 2. 새 배치 생성 필요 여부 확인
        if monitor.should_create_new_batch(active_batches):
            logger.info("🆕 Creating new batch...")
            success = monitor.create_new_batch()
            if success:
                logger.info("✅ New batch created successfully")
            else:
                logger.warning("⚠️ Failed to create new batch")
        else:
            logger.info("⏸️ No new batch needed at this time")

        # 3. 데이터 정리
        monitor.cleanup_old_data()

        # 4. 최종 상태 리포트
        final_stats = NewsOperations.get_news_stats()
        final_active = BatchOperations.get_active_batches()

        logger.info(f"📊 Final Report:")
        logger.info(f"   News: {final_stats}")
        logger.info(f"   Active batches: {len(final_active)}")

        log_function_end(logger, "monitor_batches")

    except Exception as e:
        log_error(logger, e, "main monitor_batches function")
        sys.exit(1)


if __name__ == "__main__":
    main()
