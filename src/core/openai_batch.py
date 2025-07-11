import openai
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..config.settings import Settings
from ..config.prompts import create_batch_request
from ..models.base import AnswerFormat
from ..models.batch_status import BatchStatus, BatchProgress, BatchSummary
from ..utils.logging_utils import log_api_call, log_error, log_batch_status
from ..utils.file_utils import create_temp_file, write_jsonl_file, read_jsonl_file, delete_file_safely

logger = logging.getLogger(__name__)


class OpenAIBatchProcessor:
    """OpenAI 배치 API 처리기"""

    def __init__(self):
        openai.api_key = Settings.OPENAI_API_KEY
        self.client = openai.OpenAI()

    def create_batch_input_file(self, news_data: List[Dict[str, Any]]) -> Optional[str]:
        """배치 입력 파일 생성"""
        try:
            if not news_data:
                logger.warning("No news data provided for batch processing")
                return None

            # 배치 요청 생성
            batch_requests = []
            for news in news_data:
                request = create_batch_request(news_id=str(news["id"]), title=news["title"], content=news["content"])
                batch_requests.append(request)

            # 임시 파일 생성 및 저장
            temp_file = create_temp_file(suffix=".jsonl", prefix="batch_input_")
            written_count = write_jsonl_file(batch_requests, temp_file)

            logger.info(f"📝 Created batch input file with {written_count} requests: {temp_file}")
            return temp_file

        except Exception as e:
            log_error(logger, e, "creating batch input file")
            return None

    def upload_batch_file(self, file_path: str) -> Optional[str]:
        """배치 파일을 OpenAI에 업로드"""
        try:
            log_api_call(logger, "OpenAI Files", "/files")

            with open(file_path, "rb") as file:
                file_obj = self.client.files.create(file=file, purpose="batch")

            logger.info(f"📤 Uploaded batch file: {file_obj.id}")
            return file_obj.id

        except Exception as e:
            log_error(logger, e, f"uploading batch file: {file_path}")
            return None

    def create_batch_job(self, input_file_id: str) -> Optional[str]:
        """배치 작업 생성"""
        try:
            log_api_call(logger, "OpenAI Batches", "/batches")

            batch_obj = self.client.batches.create(
                input_file_id=input_file_id, endpoint="/v1/chat/completions", completion_window="24h"
            )

            log_batch_status(logger, batch_obj.id, batch_obj.status, "Created")
            return batch_obj.id

        except Exception as e:
            log_error(logger, e, f"creating batch job with input file: {input_file_id}")
            return None

    def get_batch_status(self, batch_id: str) -> Optional[BatchSummary]:
        """배치 상태 조회"""
        try:
            log_api_call(logger, "OpenAI Batches", f"/batches/{batch_id}")

            batch_obj = self.client.batches.retrieve(batch_id)

            # 진행률 정보 생성
            progress = BatchProgress(
                total_requests=batch_obj.request_counts.total,
                completed_requests=batch_obj.request_counts.completed,
                failed_requests=batch_obj.request_counts.failed,
            )

            # 배치 요약 생성
            summary = BatchSummary(
                batch_id=batch_obj.id,
                status=BatchStatus(batch_obj.status),
                progress=progress,
                created_at=batch_obj.created_at,
                completed_at=batch_obj.completed_at,
                error_count=batch_obj.request_counts.failed,
            )

            return summary

        except Exception as e:
            log_error(logger, e, f"getting batch status: {batch_id}")
            return None

    def download_batch_results(self, batch_id: str, output_file_id: str) -> Optional[str]:
        """배치 결과 다운로드"""
        try:
            log_api_call(logger, "OpenAI Files", f"/files/{output_file_id}/content")

            # 결과 파일 다운로드
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

    def parse_batch_results(self, results_file: str) -> List[Dict[str, Any]]:
        """배치 결과 파일 파싱"""
        try:
            results = []

            with open(results_file, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        result = self._parse_single_result(line)
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.warning(f"Failed to parse line {line_num}: {str(e)}")
                        continue

            success_rate = len(results) / line_num * 100 if line_num > 0 else 0
            logger.info(f"✅ Parsed {len(results)}/{line_num} batch results ({success_rate:.1f}% success)")

            return results

        except Exception as e:
            log_error(logger, e, f"parsing batch results file: {results_file}")
            return []

    def _parse_single_result(self, result_line: str) -> Optional[Dict[str, Any]]:
        """단일 결과 라인 파싱"""
        try:
            result_data = json.loads(result_line)

            # 기본 정보 추출
            custom_id = result_data.get("custom_id", "")
            news_id = custom_id.replace("news_", "") if custom_id.startswith("news_") else None

            if not news_id:
                logger.warning(f"Invalid custom_id: {custom_id}")
                return None

            # 응답 확인
            response = result_data.get("response")
            if not response or response.get("status_code") != 200:
                logger.warning(f"Failed response for news_id {news_id}")
                return None

            # 메시지 내용 추출
            choices = response.get("body", {}).get("choices", [])
            if not choices:
                logger.warning(f"No choices for news_id {news_id}")
                return None

            message_content = choices[0].get("message", {}).get("content", "")
            if not message_content:
                logger.warning(f"No message content for news_id {news_id}")
                return None

            # AI 응답 파싱
            ai_response = json.loads(message_content)

            # AnswerFormat 검증
            answer = AnswerFormat(clickbait_score=ai_response["clickbait_score"], reasoning=ai_response["reasoning"])

            return {"id": int(news_id), "clickbait_score": answer.clickbait_score, "reasoning": answer.reasoning}

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to parse result: {str(e)}")
            return None

    def process_batch_workflow(self, news_data: List[Dict[str, Any]]) -> Optional[str]:
        """완전한 배치 워크플로우 처리 (파일 생성 -> 업로드 -> 배치 생성)"""
        try:
            # 1. 입력 파일 생성
            input_file = self.create_batch_input_file(news_data)
            if not input_file:
                return None

            try:
                # 2. 파일 업로드
                input_file_id = self.upload_batch_file(input_file)
                if not input_file_id:
                    return None

                # 3. 배치 작업 생성
                batch_id = self.create_batch_job(input_file_id)
                if not batch_id:
                    return None

                logger.info(f"🚀 Successfully created batch workflow: {batch_id}")
                return batch_id

            finally:
                # 임시 파일 정리
                delete_file_safely(input_file)

        except Exception as e:
            log_error(logger, e, "processing complete batch workflow")
            return None

    def cancel_batch(self, batch_id: str) -> bool:
        """배치 작업 취소"""
        try:
            log_api_call(logger, "OpenAI Batches", f"/batches/{batch_id}/cancel")

            self.client.batches.cancel(batch_id)
            log_batch_status(logger, batch_id, "cancelled", "Manually cancelled")

            return True

        except Exception as e:
            log_error(logger, e, f"cancelling batch: {batch_id}")
            return False

    def list_batches(self, limit: int = 20) -> List[BatchSummary]:
        """배치 목록 조회"""
        try:
            log_api_call(logger, "OpenAI Batches", f"/batches?limit={limit}")

            batches = self.client.batches.list(limit=limit)
            summaries = []

            for batch_obj in batches.data:
                progress = BatchProgress(
                    total_requests=batch_obj.request_counts.total,
                    completed_requests=batch_obj.request_counts.completed,
                    failed_requests=batch_obj.request_counts.failed,
                )

                summary = BatchSummary(
                    batch_id=batch_obj.id,
                    status=BatchStatus(batch_obj.status),
                    progress=progress,
                    created_at=batch_obj.created_at,
                    completed_at=batch_obj.completed_at,
                    error_count=batch_obj.request_counts.failed,
                )

                summaries.append(summary)

            return summaries

        except Exception as e:
            log_error(logger, e, "listing batches")
            return []
