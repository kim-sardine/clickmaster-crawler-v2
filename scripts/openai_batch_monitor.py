#!/usr/bin/env python3
"""
OpenAI Batch API 클릭베이트 점수 측정 - 배치 생성 및 모니터링

이 모듈은 다음 작업을 수행합니다:
1. 활성 배치 확인 및 완료된 배치 후처리
2. 미처리 뉴스에 대한 신규 배치 생성
3. 클릭베이트 점수 측정 결과를 데이터베이스에 저장

실행 방법:
    python -m scripts.openai_batch_monitor
    python scripts/openai_batch_monitor.py
"""

import os
import sys
import argparse
import logging

from src.config.settings import settings
from src.database.supabase_client import get_supabase_client
from src.core.openai_client import OpenAIClient
from src.core.prompt_generator import PromptGenerator
from src.core.bulk_updater import BulkUpdater
from src.core.batch_processor import BatchProcessor
from src.utils.logging_utils import setup_logging, get_logger

logger = get_logger(__name__)


def setup_environment():
    """환경 설정 및 검증"""
    # settings.validate()를 사용하여 기본 설정 검증
    if not settings.validate():
        raise ValueError("기본 환경 변수가 설정되지 않았습니다. settings.validate() 실패")

    # OpenAI API Key 추가 검증
    if not settings.OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다")

    logger.info("Environment setup completed")


def initialize_components():
    """컴포넌트 초기화"""
    logger.info("Initializing components")

    # Supabase 클라이언트
    supabase = get_supabase_client()

    # OpenAI 클라이언트
    openai_client = OpenAIClient(api_key=settings.OPENAI_API_KEY)

    # 프롬프트 생성기
    prompt_generator = PromptGenerator()

    # 벌크 업데이터
    bulk_updater = BulkUpdater(supabase=supabase)

    # 배치 처리기
    batch_processor = BatchProcessor(
        supabase=supabase, openai_client=openai_client, prompt_generator=prompt_generator, bulk_updater=bulk_updater
    )

    logger.info("Components initialized successfully")
    return batch_processor


def process_active_batch(batch_processor: BatchProcessor, active_batch: dict) -> bool:
    """
    활성 배치 후처리

    Args:
        batch_processor: 배치 처리기
        active_batch: 활성 배치 정보

    Returns:
        처리 성공 여부
    """
    batch_id = active_batch["batch_id"]
    logger.info(f"Processing active batch: {batch_id}")

    try:
        # OpenAI 배치 상태 확인
        batch_status = batch_processor.check_batch_completion(batch_id)

        if not batch_status:
            logger.error("Failed to check batch status")
            return False

        if batch_status == "completed":
            logger.info("Batch completed, processing results")

            # 배치 결과 처리
            success = batch_processor.process_batch_results(batch_id)

            if success:
                # 배치 상태를 완료로 업데이트
                batch_processor.update_batch_status(batch_id, "completed")
                logger.info("Batch processing completed successfully")
                return True
            else:
                # 배치 상태를 실패로 업데이트
                batch_processor.update_batch_status(batch_id, "failed", "Failed to process batch results")
                logger.error("Failed to process batch results")
                return False

        elif batch_status == "failed":
            logger.warning("Batch failed on OpenAI side")
            batch_processor.update_batch_status(batch_id, "failed", "Batch failed on OpenAI platform")
            return False

        elif batch_status == "cancelled":
            logger.warning("Batch was cancelled")
            batch_processor.update_batch_status(batch_id, "cancelled")
            return False

        else:
            logger.info(f"Batch still in progress (status: {batch_status})")
            return False

    except Exception as e:
        logger.error(f"Error processing active batch: {e}")
        batch_processor.update_batch_status(batch_id, "failed", f"Processing error: {str(e)}")
        return False


def create_new_batch(batch_processor: BatchProcessor, batch_size: int = 20) -> bool:
    """
    신규 배치 생성

    Args:
        batch_processor: 배치 처리기
        batch_size: 배치 크기 (기본값: 20)

    Returns:
        생성 성공 여부
    """
    logger.info(f"Creating new batch (size: {batch_size})")

    try:
        # 미처리 Article 조회
        pending_articles = batch_processor.get_pending_articles(limit=batch_size)

        if not pending_articles:
            logger.info("No pending articles found")
            return True  # 처리할 데이터가 없는 것은 정상

        logger.info(f"Found {len(pending_articles)} pending articles")

        # 배치 요청 생성
        batch_info = batch_processor.create_batch_request(pending_articles)

        if not batch_info:
            logger.error("Failed to create batch request")
            return False

        # 배치 정보를 데이터베이스에 저장
        saved_batch = batch_processor.save_batch_info_to_database(batch_info, len(pending_articles))

        if saved_batch:
            logger.info(f"New batch created successfully: {batch_info['id']}")
            return True
        else:
            logger.error("Failed to save batch info to database")
            return False

    except Exception as e:
        logger.error(f"Error creating new batch: {e}")
        return False


def run_batch_monitor(batch_size: int = 20, log_level: str = "INFO") -> dict:
    """
    배치 모니터링 실행

    Args:
        batch_size: 배치 크기
        log_level: 로깅 레벨

    Returns:
        실행 결과 딕셔너리
    """
    result = {"success": False, "active_batch_processed": False, "new_batch_created": False, "errors": []}

    try:
        # 로깅 설정
        setup_logging(log_level)
        logger.info("Starting OpenAI Batch Monitor")

        # 환경 설정
        setup_environment()

        # 컴포넌트 초기화
        batch_processor = initialize_components()

        # 1. 활성 배치 확인
        active_batch = batch_processor.check_active_batch()

        if active_batch:
            # 2. 활성 배치 후처리
            logger.info("Found active batch, processing results")
            success = process_active_batch(batch_processor, active_batch)
            result["active_batch_processed"] = success

            if not success:
                result["errors"].append("Failed to process active batch")
                return result
        else:
            logger.info("No active batch found")

        # 3. 신규 배치 생성
        logger.info("Creating new batch")
        success = create_new_batch(batch_processor, batch_size)
        result["new_batch_created"] = success

        if success:
            logger.info("Batch monitoring completed successfully")
            result["success"] = True
        else:
            result["errors"].append("Failed to create new batch")

        return result

    except Exception as e:
        error_msg = f"Unexpected error in batch monitor: {e}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return result


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description="OpenAI Batch API 클릭베이트 점수 측정 - 배치 생성 및 모니터링",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 예시:
  %(prog)s                          # 기본 설정으로 실행
  %(prog)s --batch-size 50          # 배치 크기 50으로 실행
  %(prog)s --log-level DEBUG        # 디버그 로그 활성화
        """,
    )

    parser.add_argument("--batch-size", type=int, default=100, help="배치 크기 (기본값: 20, 최대 800)")

    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="로깅 레벨 (기본값: INFO)"
    )

    args = parser.parse_args()

    # 배치 크기 검증
    if args.batch_size < 1 or args.batch_size > 800:
        print("오류: 배치 크기는 1~800 사이여야 합니다.")
        sys.exit(1)

    # 배치 모니터링 실행
    result = run_batch_monitor(batch_size=args.batch_size, log_level=args.log_level)

    # 결과 출력
    if result["success"]:
        print("✅ 배치 모니터링이 성공적으로 완료되었습니다.")
        sys.exit(0)
    else:
        print("❌ 배치 모니터링 중 오류가 발생했습니다:")
        for error in result["errors"]:
            print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
