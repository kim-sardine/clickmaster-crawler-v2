"""
OpenAI Batch API 처리 모듈
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from .openai_client import OpenAIClient
from .prompt_generator import PromptGenerator
from .bulk_updater import BulkUpdater
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class BatchProcessor:
    """배치 처리 핵심 로직"""

    def __init__(
        self, supabase, openai_client: OpenAIClient, prompt_generator: PromptGenerator, bulk_updater: BulkUpdater
    ):
        """
        Args:
            supabase: Supabase 클라이언트
            openai_client: OpenAI 클라이언트
            prompt_generator: 프롬프트 생성기
            bulk_updater: 벌크 업데이터
        """
        self.supabase = supabase
        self.openai_client = openai_client
        self.prompt_generator = prompt_generator
        self.bulk_updater = bulk_updater

    def check_active_batch(self) -> Optional[Dict[str, Any]]:
        """
        현재 실행 중인 배치 확인 (생성 시간 기준 가장 오래된 것)

        Returns:
            활성 배치 정보 또는 None
        """
        logger.info("Checking for active batches")

        try:
            response = (
                self.supabase.client.table("batch")
                .select("*")
                .eq("status", "in_progress")
                .order("created_at", desc=False)  # 가장 오래된 것부터
                .execute()
            )

            if response.data:
                # 여러 배치가 있는 경우 경고 로그
                if len(response.data) > 1:
                    logger.warning(f"Found {len(response.data)} active batches! Processing oldest first.")
                    for i, batch in enumerate(response.data):
                        logger.warning(
                            f"  Batch {i + 1}: {batch['batch_id']} (created: {batch.get('created_at', 'unknown')})"
                        )

                active_batch = response.data[0]
                logger.info(f"Processing active batch: {active_batch['batch_id']}")
                return active_batch
            else:
                logger.info("No active batch found")
                return None

        except Exception as e:
            logger.error(f"Failed to check active batch: {e}")
            return None

    def get_all_active_batches(self) -> List[Dict[str, Any]]:
        """
        모든 활성 배치 조회 (모니터링/디버깅용)

        Returns:
            활성 배치 리스트
        """
        logger.info("Retrieving all active batches")

        try:
            response = (
                self.supabase.client.table("batch")
                .select("*")
                .eq("status", "in_progress")
                .order("created_at", desc=False)
                .execute()
            )

            batches = response.data
            logger.info(f"Found {len(batches)} active batches")

            for i, batch in enumerate(batches):
                logger.info(f"  Batch {i + 1}: {batch['batch_id']} (created: {batch.get('created_at', 'unknown')})")

            return batches

        except Exception as e:
            logger.error(f"Failed to retrieve active batches: {e}")
            return []

    def get_pending_articles(self, limit: int = 800) -> List[Dict[str, Any]]:
        """
        미처리 Article 데이터 조회

        Args:
            limit: 최대 조회 개수

        Returns:
            미처리 Article 리스트
        """
        logger.info(f"Retrieving pending articles (limit: {limit})")

        try:
            response = (
                self.supabase.client.table("articles")
                .select("*")
                .is_("clickbait_score", "null")
                .order("created_at", desc=False)
                .limit(limit)
                .execute()
            )

            articles = response.data
            logger.info(f"Found {len(articles)} pending articles")
            return articles

        except Exception as e:
            logger.error(f"Failed to retrieve pending articles: {e}")
            return []

    def create_batch_request(self, articles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        배치 요청 생성

        Args:
            articles: Article 데이터 리스트

        Returns:
            생성된 배치 정보 또는 None
        """
        if not articles:
            logger.warning("No articles to process")
            return None

        logger.info(f"Creating batch request for {len(articles)} articles")

        try:
            # 배치 요청 생성
            batch_requests = self.prompt_generator.generate_batch_requests(articles)

            if not batch_requests:
                logger.warning("No valid batch requests generated")
                return None

            # OpenAI 배치 생성
            batch = self.openai_client.create_batch(batch_requests)

            batch_info = {
                "id": batch["id"] if isinstance(batch, dict) else batch.id,
                "status": batch["status"] if isinstance(batch, dict) else batch.status,
                "created_at": datetime.now().isoformat(),
            }

            batch_id = batch["id"] if isinstance(batch, dict) else batch.id
            logger.info(f"Batch created successfully: {batch_id}")
            return batch_info

        except Exception as e:
            logger.error(f"Failed to create batch request: {e}")
            return None

    def process_batch_results(self, batch_id: str) -> bool:
        """
        배치 결과 처리 (멱등성 최적화 포함)

        Args:
            batch_id: 배치 ID

        Returns:
            성공 여부
        """
        logger.info(f"Processing batch results: {batch_id} (with idempotency optimization)")

        try:
            # 1단계: 배치 상태 확인 - 이미 완료된 경우 스킵
            existing_batch = self._get_batch_info(batch_id)
            if existing_batch and existing_batch.get("status") == "completed":
                logger.info(f"Batch {batch_id} is already completed - operation is idempotent")
                return True

            # 2단계: 배치 결과 다운로드 (캐싱 고려)
            logger.info("Downloading batch results from OpenAI")
            results = self._get_cached_or_download_results(batch_id)

            if not results:
                logger.error("No results found in batch - batch may be incomplete or corrupted")
                return False

            logger.info(f"Downloaded {len(results)} results, starting parsing...")

            # 3단계: 결과 파싱 (파싱 에러 시 exception 발생)
            updates = self._parse_batch_results(results)

            if not updates:
                logger.error("No valid updates parsed from results - this should not happen in strict mode")
                return False

            logger.info(f"Successfully parsed {len(updates)} updates, starting bulk update...")

            # 4단계: 멱등성을 고려한 벌크 업데이트 수행
            success = self.bulk_updater.bulk_update_articles(updates)

            if success:
                logger.info(f"Successfully processed batch {batch_id} with idempotency guarantees")
                return True
            else:
                logger.error("Bulk update failed - database operation error")
                return False

        except Exception as e:
            error_message = str(e)

            # 파싱 에러와 다른 에러를 구분
            if "Batch parsing failed:" in error_message:
                logger.error(f"Batch processing failed due to parsing errors: {e}")
                logger.error("This indicates issues with OpenAI response format or content validation")
            elif "No results found" in error_message:
                logger.error(f"Batch processing failed - no results available: {e}")
                logger.error("This may indicate OpenAI batch completion issues")
            else:
                logger.error(f"Batch processing failed due to unexpected error: {e}")
                logger.error("This may indicate system/network issues")

            return False

    def _parse_batch_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        배치 결과 파싱 (파싱 에러 시 전체 프로세스 실패)

        Args:
            results: OpenAI 배치 결과

        Returns:
            파싱된 업데이트 데이터

        Raises:
            Exception: 파싱 에러 발생 시 전체 프로세스 실패
        """
        logger.info(f"Parsing {len(results)} batch results (strict mode: any parsing error will fail entire process)")

        updates = []
        parsing_errors = []

        for i, result in enumerate(results, 1):
            try:
                # Article ID 추출
                custom_id = result.get("custom_id", "")
                if not custom_id.startswith("article_"):
                    error_msg = f"Result {i}: Invalid custom_id format: {custom_id}"
                    logger.error(error_msg)
                    parsing_errors.append(error_msg)
                    continue

                article_id = custom_id.replace("article_", "")

                # OpenAI 응답 추출
                response_body = result.get("response", {}).get("body", {})
                choices = response_body.get("choices", [])

                if not choices:
                    error_msg = f"Result {i} (article {article_id}): No choices in response"
                    logger.error(error_msg)
                    parsing_errors.append(error_msg)
                    continue

                message_content = choices[0].get("message", {}).get("content", "")

                if not message_content:
                    error_msg = f"Result {i} (article {article_id}): Empty message content"
                    logger.error(error_msg)
                    parsing_errors.append(error_msg)
                    continue

                # PromptGenerator의 validate_clickbait_response 사용
                validated_data = self.prompt_generator.validate_clickbait_response(message_content)

                if validated_data:
                    # UUID는 문자열 형태로 사용
                    update = {
                        "id": article_id,
                        "clickbait_score": validated_data["clickbait_score"],
                        "clickbait_explanation": validated_data["clickbait_explanation"],
                    }
                    updates.append(update)
                    logger.debug(f"Result {i} (article {article_id}): Successfully parsed")
                else:
                    error_msg = f"Result {i} (article {article_id}): Invalid response data - validation failed"
                    logger.error(error_msg)
                    parsing_errors.append(error_msg)

            except Exception as e:
                error_msg = f"Result {i}: Critical parsing error: {e}"
                logger.error(error_msg)
                parsing_errors.append(error_msg)

        # 파싱 에러가 하나라도 있으면 전체 프로세스 실패
        if parsing_errors:
            total_errors = len(parsing_errors)
            error_summary = f"Batch parsing failed: {total_errors} errors out of {len(results)} results"
            logger.error(error_summary)
            logger.error("Parsing errors details:")
            for error in parsing_errors:
                logger.error(f"  - {error}")

            # 전체 프로세스 실패
            raise Exception(f"{error_summary}. First error: {parsing_errors[0]}")

        logger.info(f"Successfully parsed all {len(updates)} results without errors")
        return updates

    def save_batch_info_to_database(self, batch_info: Dict[str, Any], article_count: int) -> Optional[Dict[str, Any]]:
        """
        배치 정보를 데이터베이스에 저장 (원자적 처리로 중복 방지)

        Args:
            batch_info: 배치 정보
            article_count: 처리할 Article 수

        Returns:
            저장된 데이터 또는 None
        """
        batch_id = batch_info["id"]
        logger.info(f"Saving batch info to database: {batch_id}")

        try:
            # 1단계: 이미 존재하는 배치인지 확인 (중복 방지)
            existing_check = (
                self.supabase.client.table("batch").select("id, batch_id, status").eq("batch_id", batch_id).execute()
            )

            if existing_check.data:
                existing_batch = existing_check.data[0]
                logger.warning(f"Batch already exists: {batch_id} (status: {existing_batch['status']})")
                return existing_batch

            # 2단계: 다른 in_progress 배치 존재 여부 확인 (동시성 제어)
            active_check = (
                self.supabase.client.table("batch")
                .select("id, batch_id, status, created_at")
                .eq("status", "in_progress")
                .execute()
            )

            if active_check.data:
                logger.warning(f"Another batch is already in progress: {active_check.data[0]['batch_id']}")
                logger.warning("Skipping batch creation to prevent concurrent processing")
                return None

            # 3단계: 배치 정보 삽입 (원자적 처리)
            batch_data = {
                "batch_id": batch_id,
                "status": "in_progress",
                "article_count": article_count,
                "created_at": batch_info.get("created_at", datetime.now().isoformat()),
            }

            response = self.supabase.client.table("batch").insert(batch_data).execute()

            if response.data:
                logger.info(f"Batch info saved successfully with concurrency control")
                return response.data[0]
            else:
                logger.error("Failed to save batch info - no data returned")
                return None

        except Exception as e:
            error_msg = str(e)

            # 동시성 위반 에러 처리 (unique constraint violation)
            if "23505" in error_msg or "duplicate key value" in error_msg:
                logger.warning(f"Batch {batch_id} was already created by another instance")

                # 이미 생성된 배치 정보 조회
                try:
                    existing = self.supabase.client.table("batch").select("*").eq("batch_id", batch_id).execute()

                    if existing.data:
                        logger.info("Returning existing batch info created by concurrent instance")
                        return existing.data[0]

                except Exception as lookup_error:
                    logger.error(f"Failed to lookup existing batch: {lookup_error}")

            logger.error(f"Failed to save batch info: {e}")
            return None

    def update_batch_status(
        self, batch_id: str, status: str, error_message: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        배치 상태 업데이트 (멱등성 보장)

        Args:
            batch_id: 배치 ID
            status: 새로운 상태
            error_message: 에러 메시지 (선택사항)

        Returns:
            업데이트된 데이터 또는 None
        """
        try:
            # 1단계: 현재 배치 상태 확인 (멱등성 체크)
            current_batch = self._get_batch_info(batch_id)

            if not current_batch:
                logger.warning(f"Batch {batch_id} not found for status update")
                return None

            current_status = current_batch.get("status")

            # 멱등성 체크: 이미 원하는 상태인 경우
            if current_status == status:
                logger.info(f"Batch {batch_id} is already in '{status}' status - operation is idempotent")
                return current_batch

            # 상태 전이 유효성 검증
            if not self._is_valid_status_transition(current_status, status):
                logger.warning(f"Invalid status transition for batch {batch_id}: {current_status} -> {status}")
                return None

            # 2단계: 상태 업데이트 실행
            update_data = {
                "status": status,
                "updated_at": datetime.now().isoformat(),
            }

            # 완료 관련 상태인 경우 완료 시간 기록
            if status in ["completed", "failed", "cancelled"]:
                update_data["completed_at"] = datetime.now().isoformat()

            if error_message:
                update_data["error_message"] = error_message

            response = self.supabase.client.table("batch").update(update_data).eq("batch_id", batch_id).execute()

            if response.data:
                logger.info(f"Batch status updated successfully: {batch_id} ({current_status} -> {status})")
                return response.data[0]
            else:
                logger.error("Failed to update batch status - no data returned")
                return None

        except Exception as e:
            logger.error(f"Failed to update batch status: {e}")
            return None

    def _is_valid_status_transition(self, current_status: str, new_status: str) -> bool:
        """
        배치 상태 전이 유효성 검증

        Args:
            current_status: 현재 상태
            new_status: 새로운 상태

        Returns:
            유효한 전이인지 여부
        """
        # 유효한 상태 전이 규칙 정의
        valid_transitions = {
            "in_progress": ["completed", "failed", "cancelled"],
            "completed": ["completed"],  # 완료된 상태는 다시 완료로만 가능 (멱등성)
            "failed": ["failed", "in_progress"],  # 실패한 배치는 재시도 가능
            "cancelled": ["cancelled"],  # 취소된 상태는 다시 취소로만 가능 (멱등성)
        }

        allowed_next_statuses = valid_transitions.get(current_status, [])
        is_valid = new_status in allowed_next_statuses

        if not is_valid:
            logger.debug(f"Status transition validation: {current_status} -> {new_status} = {is_valid}")

        return is_valid

    def check_batch_completion(self, batch_id: str) -> Optional[str]:
        """
        OpenAI 배치 완료 상태 확인

        Args:
            batch_id: 배치 ID

        Returns:
            배치 상태 또는 None
        """
        try:
            batch = self.openai_client.get_batch_status(batch_id)
            logger.debug(f"Batch {batch_id} status: {batch.status}")
            return batch.status

        except Exception as e:
            logger.error(f"Failed to check batch completion: {e}")
            return None

    def _get_batch_info(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """
        배치 정보 조회

        Args:
            batch_id: 배치 ID

        Returns:
            배치 정보 또는 None
        """
        try:
            response = self.supabase.client.table("batch").select("*").eq("batch_id", batch_id).execute()

            if response.data:
                return response.data[0]
            return None

        except Exception as e:
            logger.error(f"Failed to get batch info: {e}")
            return None

    def _get_cached_or_download_results(self, batch_id: str) -> List[Dict[str, Any]]:
        """
        캐시된 결과가 있으면 사용하고, 없으면 OpenAI에서 다운로드

        Args:
            batch_id: 배치 ID

        Returns:
            배치 결과 리스트
        """
        # TODO: 향후 Redis나 파일 시스템 캐싱 구현 가능
        # 현재는 매번 다운로드하지만, 로그로 재실행 상황을 명확히 표시

        try:
            # 재실행 감지 로깅
            batch_info = self._get_batch_info(batch_id)
            if batch_info:
                created_at = batch_info.get("created_at", "unknown")
                logger.info(f"Re-processing batch created at: {created_at}")
                logger.info("Note: Results will be re-downloaded (caching not implemented yet)")

            # OpenAI에서 결과 다운로드
            return self.openai_client.get_batch_results(batch_id)

        except Exception as e:
            logger.error(f"Failed to get batch results: {e}")
            return []
