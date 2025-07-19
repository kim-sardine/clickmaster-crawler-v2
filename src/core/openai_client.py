"""
OpenAI 클라이언트 모듈
"""

import json
import tempfile
from typing import List, Dict, Any, Optional
from pathlib import Path

from openai import OpenAI
from openai.types import Batch

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class OpenAIClient:
    """OpenAI API 클라이언트 래퍼"""

    def __init__(self, api_key: str):
        """
        Args:
            api_key: OpenAI API 키
        """
        self.client = OpenAI(api_key=api_key)

    def create_batch(self, batch_requests: List[Dict[str, Any]]) -> Batch:
        """
        배치 요청 생성

        Args:
            batch_requests: 배치 요청 리스트

        Returns:
            생성된 배치 객체
        """
        logger.info(f"Creating batch with {len(batch_requests)} requests")

        # JSONL 파일 생성
        jsonl_content = "\n".join(json.dumps(req) for req in batch_requests)

        # 임시 파일로 JSONL 저장
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write(jsonl_content)
            temp_file_path = f.name

        try:
            # 파일 업로드
            with open(temp_file_path, "rb") as file:
                uploaded_file = self.client.files.create(file=file, purpose="batch")

            logger.info(f"File uploaded successfully: {uploaded_file.id}")

            # 배치 생성
            batch = self.client.batches.create(
                input_file_id=uploaded_file.id, endpoint="/v1/chat/completions", completion_window="24h"
            )

            logger.info(f"Batch created successfully: {batch.id}")
            return batch

        finally:
            # 임시 파일 정리
            Path(temp_file_path).unlink(missing_ok=True)

    def get_batch_status(self, batch_id: str) -> Batch:
        """
        배치 상태 조회

        Args:
            batch_id: 배치 ID

        Returns:
            배치 객체
        """
        logger.debug(f"Retrieving batch status: {batch_id}")
        return self.client.batches.retrieve(batch_id)

    def get_batch_results(self, batch_id: str) -> List[Dict[str, Any]]:
        """
        배치 결과 다운로드 및 파싱

        Args:
            batch_id: 배치 ID

        Returns:
            파싱된 결과 리스트
        """
        logger.info(f"Downloading batch results: {batch_id}")

        # 배치 상태 확인
        batch = self.client.batches.retrieve(batch_id)

        if batch.status != "completed":
            raise ValueError(f"Batch is not completed. Current status: {batch.status}")

        if not batch.output_file_id:
            raise ValueError("No output file available for this batch")

        # 결과 파일 다운로드
        result_content = self.client.files.content(batch.output_file_id)

        # JSONL 파싱
        results = []
        for line in result_content.content.decode("utf-8").strip().split("\n"):
            if line.strip():
                try:
                    result = json.loads(line)
                    results.append(result)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse result line: {line[:100]}... Error: {e}")
                    continue

        logger.info(f"Downloaded and parsed {len(results)} results")
        return results

    def cancel_batch(self, batch_id: str) -> Batch:
        """
        배치 취소

        Args:
            batch_id: 배치 ID

        Returns:
            취소된 배치 객체
        """
        logger.info(f"Cancelling batch: {batch_id}")
        return self.client.batches.cancel(batch_id)

    def list_batches(self, limit: Optional[int] = None) -> List[Batch]:
        """
        배치 목록 조회

        Args:
            limit: 최대 조회 개수

        Returns:
            배치 리스트
        """
        logger.debug(f"Listing batches (limit: {limit})")

        batches = []
        for batch in self.client.batches.list(limit=limit):
            batches.append(batch)

        return batches
