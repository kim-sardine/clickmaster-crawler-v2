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
        현재 실행 중인 배치 확인

        Returns:
            활성 배치 정보 또는 None
        """
        logger.info("Checking for active batches")

        try:
            response = self.supabase.client.table("batch").select("*").eq("status", "in_progress").execute()

            if response.data:
                active_batch = response.data[0]
                logger.info(f"Found active batch: {active_batch['batch_id']}")
                return active_batch
            else:
                logger.info("No active batch found")
                return None

        except Exception as e:
            logger.error(f"Failed to check active batch: {e}")
            return None

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
        배치 결과 처리

        Args:
            batch_id: 배치 ID

        Returns:
            성공 여부
        """
        logger.info(f"Processing batch results: {batch_id}")

        try:
            # 배치 결과 다운로드
            results = self.openai_client.get_batch_results(batch_id)

            if not results:
                logger.warning("No results found in batch")
                return False

            # 결과 파싱 (이미 validate_clickbait_response에서 검증됨)
            updates = self._parse_batch_results(results)

            if not updates:
                logger.warning("No valid updates parsed from results")
                return False

            # 벌크 업데이트 수행
            success = self.bulk_updater.bulk_update_articles(updates)

            if success:
                logger.info(f"Successfully processed {len(updates)} articles")
                return True
            else:
                logger.error("Bulk update failed")
                return False

        except Exception as e:
            logger.error(f"Failed to process batch results: {e}")
            return False

    def _parse_batch_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        배치 결과 파싱

        Args:
            results: OpenAI 배치 결과

        Returns:
            파싱된 업데이트 데이터
        """
        logger.info(f"Parsing {len(results)} batch results")

        updates = []

        for result in results:
            try:
                # Article ID 추출
                custom_id = result.get("custom_id", "")
                if not custom_id.startswith("article_"):
                    logger.warning(f"Invalid custom_id format: {custom_id}")
                    continue

                article_id = custom_id.replace("article_", "")

                # OpenAI 응답 추출
                response_body = result.get("response", {}).get("body", {})
                choices = response_body.get("choices", [])

                if not choices:
                    logger.warning(f"No choices in response for article {article_id}")
                    continue

                message_content = choices[0].get("message", {}).get("content", "")

                if not message_content:
                    logger.warning(f"Empty message content for article {article_id}")
                    continue

                # PromptGenerator의 validate_clickbait_response 사용
                validated_data = self.prompt_generator.validate_clickbait_response(message_content)

                if validated_data:
                    try:
                        article_id_int = int(article_id)
                        update = {
                            "id": article_id_int,
                            "clickbait_score": validated_data["clickbait_score"],
                            "clickbait_explanation": validated_data["clickbait_explanation"],
                        }
                        updates.append(update)
                    except ValueError as e:
                        logger.error(f"Invalid article ID {article_id}: {e}")
                else:
                    logger.warning(f"Invalid response data for article {article_id}")

            except Exception as e:
                logger.error(f"Failed to parse result: {e}")
                continue

        logger.info(f"Successfully parsed {len(updates)} updates")
        return updates

    def save_batch_info_to_database(self, batch_info: Dict[str, Any], article_count: int) -> Optional[Dict[str, Any]]:
        """
        배치 정보를 데이터베이스에 저장

        Args:
            batch_info: 배치 정보
            article_count: 처리할 Article 수

        Returns:
            저장된 데이터 또는 None
        """
        logger.info(f"Saving batch info to database: {batch_info['id']}")

        try:
            batch_data = {
                "batch_id": batch_info["id"],
                "status": "in_progress",
                "article_count": article_count,
                "created_at": batch_info.get("created_at", datetime.now().isoformat()),
            }

            response = self.supabase.client.table("batch").insert(batch_data).execute()

            if response.data:
                logger.info("Batch info saved successfully")
                return response.data[0]
            else:
                logger.error("Failed to save batch info")
                return None

        except Exception as e:
            logger.error(f"Failed to save batch info: {e}")
            return None

    def update_batch_status(
        self, batch_id: str, status: str, error_message: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        배치 상태 업데이트

        Args:
            batch_id: 배치 ID
            status: 새로운 상태
            error_message: 에러 메시지 (선택사항)

        Returns:
            업데이트된 데이터 또는 None
        """
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.now().isoformat(),
            }

            if error_message:
                update_data["error_message"] = error_message

            response = self.supabase.client.table("batch").update(update_data).eq("batch_id", batch_id).execute()

            if response.data:
                logger.info(f"Batch status updated successfully: {batch_id} -> {status}")
                return response.data[0]
            else:
                logger.error("Failed to update batch status")
                return None

        except Exception as e:
            logger.error(f"Failed to update batch status: {e}")
            return None

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
