#!/usr/bin/env python3
"""
완료된 배치 결과 처리 스크립트
완료된 배치의 결과 파일을 다운로드하고 낚시성 점수를 DB에 업데이트
"""

import sys
import os
import json
import logging
from typing import List, Dict, Any, Optional

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import Settings
from src.database.operations import NewsOperations, BatchOperations
from src.models.batch_status import BatchStatus
from src.models.base import AnswerFormat
from src.utils.logging_utils import (
    setup_logger,
    log_function_start,
    log_function_end,
    log_error,
    log_batch_status,
    log_progress,
)
from src.utils.file_utils import create_temp_file, delete_file_safely
import openai

logger = setup_logger(__name__)


class BatchProcessor:
    """배치 결과 처리기"""

    def __init__(self):
        openai.api_key = Settings.OPENAI_API_KEY
        self.client = openai.OpenAI()

    def download_batch_results(self, batch_id: str, output_file_id: str) -> Optional[str]:
        """배치 결과 파일 다운로드"""
        try:
            # OpenAI에서 결과 파일 다운로드
            file_response = self.client.files.content(output_file_id)

            # 임시 파일에 저장
            temp_file = create_temp_file(suffix=".jsonl", prefix=f"batch_output_{batch_id}_")

            with open(temp_file, "wb") as f:
                f.write(file_response.content)

            logger.info(f"📥 Downloaded batch results: {output_file_id} -> {temp_file}")
            return temp_file

        except Exception as e:
            log_error(logger, e, f"downloading batch results: {batch_id}")
            return None

    def parse_batch_response(self, response_line: str) -> Optional[Dict[str, Any]]:
        """배치 응답 라인 파싱"""
        try:
            response_data = json.loads(response_line.strip())

            # 기본 정보 추출
            custom_id = response_data.get("custom_id", "")
            news_id = custom_id.replace("news_", "") if custom_id.startswith("news_") else None

            if not news_id:
                logger.warning(f"⚠️ Invalid custom_id: {custom_id}")
                return None

            # 응답 내용 확인
            response_obj = response_data.get("response")
            if not response_obj:
                logger.warning(f"⚠️ No response object for news_id: {news_id}")
                return None

            # 에러 확인
            if response_obj.get("status_code") != 200:
                error_msg = response_obj.get("body", {}).get("error", {}).get("message", "Unknown error")
                logger.warning(f"⚠️ API error for news_id {news_id}: {error_msg}")
                return None

            # 메시지 내용 추출
            choices = response_obj.get("body", {}).get("choices", [])
            if not choices:
                logger.warning(f"⚠️ No choices for news_id: {news_id}")
                return None

            message_content = choices[0].get("message", {}).get("content", "")
            if not message_content:
                logger.warning(f"⚠️ No message content for news_id: {news_id}")
                return None

            # JSON 응답 파싱
            try:
                ai_response = json.loads(message_content)

                # AnswerFormat 검증
                answer = AnswerFormat(
                    clickbait_score=ai_response["clickbait_score"], reasoning=ai_response["reasoning"]
                )

                return {"id": int(news_id), "clickbait_score": answer.clickbait_score, "reasoning": answer.reasoning}

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"⚠️ Failed to parse AI response for news_id {news_id}: {str(e)}")
                logger.debug(f"Raw content: {message_content}")
                return None

        except Exception as e:
            log_error(logger, e, f"parsing batch response line")
            return None

    def process_batch_file(self, file_path: str) -> List[Dict[str, Any]]:
        """배치 결과 파일 처리"""
        try:
            score_updates = []

            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            logger.info(f"📊 Processing {len(lines)} batch responses...")

            for i, line in enumerate(lines):
                if not line.strip():
                    continue

                result = self.parse_batch_response(line)
                if result:
                    score_updates.append(result)

                # 진행률 로그 (100개마다)
                if (i + 1) % 100 == 0:
                    log_progress(logger, i + 1, len(lines), "Processing responses")

            success_rate = len(score_updates) / len(lines) * 100 if lines else 0
            logger.info(f"✅ Successfully processed {len(score_updates)}/{len(lines)} responses ({success_rate:.1f}%)")

            return score_updates

        except Exception as e:
            log_error(logger, e, f"processing batch file: {file_path}")
            return []

    def process_completed_batch(self, batch_info: Dict[str, Any]) -> bool:
        """완료된 배치 처리"""
        try:
            batch_id = batch_info["batch_id"]
            output_file_id = batch_info["output_file_id"]

            log_batch_status(logger, batch_id, "processing", "Downloading results")

            # 결과 파일 다운로드
            result_file = self.download_batch_results(batch_id, output_file_id)
            if not result_file:
                return False

            try:
                # 결과 파일 처리
                score_updates = self.process_batch_file(result_file)

                if not score_updates:
                    logger.warning(f"⚠️ No valid score updates from batch {batch_id}")
                    return False

                # 데이터베이스 업데이트
                log_batch_status(logger, batch_id, "updating", f"Updating {len(score_updates)} scores")

                updated_count = NewsOperations.update_clickbait_scores(score_updates)

                # 배치 상태 업데이트 (완료로 표시하고 처리된 수 기록)
                BatchOperations.update_batch_status(
                    batch_id, BatchStatus.COMPLETED.value, processed_count=updated_count
                )

                log_batch_status(logger, batch_id, "completed", f"Updated {updated_count} articles")

                return True

            finally:
                # 임시 파일 정리
                delete_file_safely(result_file)

        except Exception as e:
            log_error(logger, e, f"processing completed batch: {batch_info.get('batch_id', 'Unknown')}")

            # 에러 발생 시 배치 상태를 실패로 업데이트
            try:
                BatchOperations.update_batch_status(
                    batch_info["batch_id"], BatchStatus.FAILED.value, error_message=str(e)
                )
            except:
                pass

            return False

    def get_batch_processing_stats(self) -> Dict[str, int]:
        """배치 처리 통계"""
        try:
            # 완료된 배치 수
            completed_batches = BatchOperations.get_completed_batches()

            # 전체 통계
            stats = NewsOperations.get_news_stats()

            return {
                "completed_batches_to_process": len(completed_batches),
                "total_news": stats.get("total", 0),
                "processed_news": stats.get("processed", 0),
                "unprocessed_news": stats.get("unprocessed", 0),
            }

        except Exception as e:
            log_error(logger, e, "getting batch processing stats")
            return {}


def main():
    """메인 함수"""
    try:
        log_function_start(logger, "process_completed_batches")

        # 환경변수 검증
        Settings.validate_required_env_vars()

        processor = BatchProcessor()

        # 초기 통계
        initial_stats = processor.get_batch_processing_stats()
        logger.info(f"📊 Initial stats: {initial_stats}")

        # 완료된 배치 조회
        completed_batches = BatchOperations.get_completed_batches()

        if not completed_batches:
            logger.info("✅ No completed batches to process")
            log_function_end(logger, "process_completed_batches")
            return

        logger.info(f"📋 Found {len(completed_batches)} completed batches to process")

        # 각 배치 처리
        success_count = 0
        for i, batch_info in enumerate(completed_batches):
            batch_id = batch_info["batch_id"]

            try:
                logger.info(f"🔄 Processing batch {i + 1}/{len(completed_batches)}: {batch_id}")

                success = processor.process_completed_batch(batch_info)
                if success:
                    success_count += 1
                    logger.info(f"✅ Successfully processed batch: {batch_id}")
                else:
                    logger.warning(f"⚠️ Failed to process batch: {batch_id}")

            except Exception as e:
                log_error(logger, e, f"processing batch: {batch_id}")
                continue

        # 최종 통계
        final_stats = processor.get_batch_processing_stats()
        logger.info(f"📊 Processing Summary:")
        logger.info(f"   Batches processed: {success_count}/{len(completed_batches)}")
        logger.info(f"   Final stats: {final_stats}")

        log_function_end(logger, "process_completed_batches")

    except Exception as e:
        log_error(logger, e, "main process_completed_batches function")
        sys.exit(1)


if __name__ == "__main__":
    main()
