"""
OpenAI Batch API 처리 모듈 테스트
"""

import pytest
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.core.batch_processor import BatchProcessor
from src.core.openai_client import OpenAIClient
from src.core.prompt_generator import PromptGenerator
from src.core.bulk_updater import BulkUpdater


class TestBatchProcessor:
    """배치 처리기 테스트"""

    @pytest.fixture
    def mock_supabase(self):
        """Supabase 클라이언트 모킹"""
        mock = Mock()
        return mock

    @pytest.fixture
    def mock_openai_client(self):
        """OpenAI 클라이언트 모킹"""
        mock = Mock(spec=OpenAIClient)
        return mock

    @pytest.fixture
    def mock_prompt_generator(self):
        """프롬프트 생성기 모킹"""
        mock = Mock(spec=PromptGenerator)
        return mock

    @pytest.fixture
    def mock_bulk_updater(self):
        """벌크 업데이터 모킹"""
        mock = Mock(spec=BulkUpdater)
        return mock

    @pytest.fixture
    def batch_processor(self, mock_supabase, mock_openai_client, mock_prompt_generator, mock_bulk_updater):
        """배치 처리기 인스턴스"""
        return BatchProcessor(
            supabase=mock_supabase,
            openai_client=mock_openai_client,
            prompt_generator=mock_prompt_generator,
            bulk_updater=mock_bulk_updater,
        )

    def test_check_active_batch_returns_none_when_no_active_batch(self, batch_processor, mock_supabase):
        """활성 배치가 없을 때 None을 반환하는지 테스트"""
        # Given
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value.data = []

        # When
        result = batch_processor.check_active_batch()

        # Then
        assert result is None
        mock_supabase.table.assert_called_with("batch")

    def test_check_active_batch_returns_batch_when_active_exists(self, batch_processor, mock_supabase):
        """활성 배치가 있을 때 배치 정보를 반환하는지 테스트"""
        # Given
        active_batch = {"id": 1, "batch_id": "batch_123", "status": "in_progress", "article_count": 500}
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [active_batch]

        # When
        result = batch_processor.check_active_batch()

        # Then
        assert result == active_batch

    def test_get_pending_articles_returns_correct_data(self, batch_processor, mock_supabase):
        """미처리 Article 데이터를 올바르게 반환하는지 테스트"""
        # Given
        pending_articles = [
            {"id": 1, "title": "Test Title 1", "content": "Test Content 1"},
            {"id": 2, "title": "Test Title 2", "content": "Test Content 2"},
        ]
        mock_supabase.table.return_value.select.return_value.is_.return_value.order.return_value.limit.return_value.execute.return_value.data = pending_articles

        # When
        result = batch_processor.get_pending_articles(limit=800)

        # Then
        assert result == pending_articles
        assert len(result) == 2
        mock_supabase.table.assert_called_with("Article")

    def test_create_batch_request_calls_openai_correctly(
        self, batch_processor, mock_openai_client, mock_prompt_generator
    ):
        """배치 요청 생성이 올바르게 호출되는지 테스트"""
        # Given
        articles = [{"id": 1, "title": "Test Title", "content": "Test Content"}]
        mock_batch_requests = [
            {
                "custom_id": "article_1",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {"model": "gpt-4o-mini", "messages": []},
            }
        ]
        mock_prompt_generator.generate_batch_requests.return_value = mock_batch_requests
        mock_openai_client.create_batch.return_value = {"id": "batch_123", "status": "validating"}

        # Pre-check이 성공하도록 Mock 설정
        with patch.object(batch_processor, "_pre_check_batch_creation", return_value=True):
            # When
            result = batch_processor.create_batch_request(articles)

            # Then
            assert result["id"] == "batch_123"
            mock_prompt_generator.generate_batch_requests.assert_called_once_with(articles)
            mock_openai_client.create_batch.assert_called_once_with(mock_batch_requests)

    def test_process_batch_results_handles_valid_responses(
        self, batch_processor, mock_openai_client, mock_bulk_updater
    ):
        """배치 결과 처리가 올바르게 작동하는지 테스트"""
        # Given
        batch_id = "batch_123"
        mock_results = [
            {
                "custom_id": "article_1",
                "response": {
                    "body": {
                        "choices": [
                            {
                                "message": {
                                    "content": '{"clickbait_score": 85, "clickbait_explanation": "과도한 호기심 유발"}'
                                }
                            }
                        ]
                    }
                },
            }
        ]
        mock_openai_client.get_batch_results.return_value = mock_results
        mock_bulk_updater.bulk_update_articles.return_value = True

        # PromptGenerator의 validate_clickbait_response Mock 설정
        with patch.object(batch_processor.prompt_generator, "validate_clickbait_response") as mock_validate:
            mock_validate.return_value = {"clickbait_score": 85, "clickbait_explanation": "과도한 호기심 유발"}

            # When
            success = batch_processor.process_batch_results(batch_id)

            # Then
            assert success is True
            mock_openai_client.get_batch_results.assert_called_once_with(batch_id)
            mock_bulk_updater.bulk_update_articles.assert_called_once()
            mock_validate.assert_called_once()

    def test_process_batch_results_handles_invalid_json(self, batch_processor, mock_openai_client, mock_bulk_updater):
        """배치 결과에 잘못된 JSON이 있을 때 처리하는지 테스트"""
        # Given
        batch_id = "batch_123"
        mock_results = [
            {
                "custom_id": "article_1",
                "response": {"body": {"choices": [{"message": {"content": "Invalid JSON content"}}]}},
            }
        ]
        mock_openai_client.get_batch_results.return_value = mock_results

        # PromptGenerator의 validate_clickbait_response Mock 설정 (유효하지 않은 JSON)
        with patch.object(batch_processor.prompt_generator, "validate_clickbait_response") as mock_validate:
            mock_validate.return_value = None  # 유효하지 않은 응답

            # When
            success = batch_processor.process_batch_results(batch_id)

            # Then
            # 잘못된 JSON만 있으면 valid updates가 없어서 False가 반환됨
            assert success is False
            # bulk_update_articles는 호출되지 않아야 함 (유효한 데이터가 없으므로)
            mock_bulk_updater.bulk_update_articles.assert_not_called()
            mock_validate.assert_called_once()

    def test_save_batch_info_to_database(self, batch_processor, mock_supabase):
        """배치 정보 데이터베이스 저장 테스트"""
        # Given
        batch_info = {"id": "batch_123", "status": "validating"}
        article_count = 500

        # Supabase response Mock 설정
        mock_response = Mock()
        mock_response.data = [{"id": 1}]
        mock_supabase.table.return_value.insert.return_value.execute.return_value = mock_response

        # When
        result = batch_processor.save_batch_info_to_database(batch_info, article_count)

        # Then
        assert result is not None
        assert result["id"] == 1
        mock_supabase.table.assert_called_with("batch")
        mock_supabase.table.return_value.insert.assert_called_once()
        mock_supabase.table.return_value.insert.return_value.execute.assert_called_once()

    def test_update_batch_status(self, batch_processor, mock_supabase):
        """배치 상태 업데이트 테스트"""
        # Given
        batch_id = "batch_123"
        status = "completed"

        # Supabase response Mock 설정
        mock_response = Mock()
        mock_response.data = [{"id": 1, "status": "completed"}]
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = mock_response

        # When
        result = batch_processor.update_batch_status(batch_id, status)

        # Then
        assert result is not None
        assert result["status"] == "completed"
        mock_supabase.table.assert_called_with("batch")
        mock_supabase.table.return_value.update.assert_called_once()
        mock_supabase.table.return_value.update.return_value.eq.assert_called_once_with("batch_id", batch_id)


class TestOpenAIClient:
    """OpenAI 클라이언트 테스트"""

    @pytest.fixture
    def openai_client(self):
        """OpenAI 클라이언트 인스턴스"""
        return OpenAIClient(api_key="test_key")

    def test_create_batch_uploads_file_and_creates_batch(self, openai_client):
        """배치 생성이 파일 업로드와 배치 생성을 올바르게 수행하는지 테스트"""
        # Given
        batch_requests = [
            {
                "custom_id": "article_1",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": "test prompt"}],
                    "response_format": {"type": "json_object"},
                },
            }
        ]

        # OpenAI 클라이언트 Mock 설정
        with patch.object(openai_client, "client") as mock_client:
            mock_file = Mock()
            mock_file.id = "file_123"
            mock_client.files.create.return_value = mock_file

            mock_batch = Mock()
            mock_batch.id = "batch_123"
            mock_batch.status = "validating"
            mock_client.batches.create.return_value = mock_batch

            # When
            result = openai_client.create_batch(batch_requests)

            # Then
            assert result.id == "batch_123"
            mock_client.files.create.assert_called_once()
            mock_client.batches.create.assert_called_once()

    def test_get_batch_status(self, openai_client):
        """배치 상태 조회 테스트"""
        # Given
        batch_id = "batch_123"

        with patch.object(openai_client, "client") as mock_client:
            mock_batch = Mock()
            mock_batch.status = "completed"
            mock_client.batches.retrieve.return_value = mock_batch

            # When
            result = openai_client.get_batch_status(batch_id)

            # Then
            assert result.status == "completed"
            mock_client.batches.retrieve.assert_called_once_with(batch_id)

    def test_get_batch_results_downloads_and_parses_results(self, openai_client):
        """배치 결과 다운로드 및 파싱 테스트"""
        # Given
        batch_id = "batch_123"

        with patch.object(openai_client, "client") as mock_client:
            mock_batch = Mock()
            mock_batch.output_file_id = "file_456"
            mock_batch.status = "completed"
            mock_client.batches.retrieve.return_value = mock_batch

            # JSONL 형태의 결과 데이터
            jsonl_content = '{"custom_id": "article_1", "response": {"body": {"choices": [{"message": {"content": "{\\"clickbait_score\\": 85, \\"clickbait_explanation\\": \\"test\\"}"}}]}}}\n'
            mock_response = Mock()
            mock_response.content = jsonl_content.encode()
            mock_client.files.content.return_value = mock_response

            # When
            results = openai_client.get_batch_results(batch_id)

            # Then
            assert len(results) == 1
            assert results[0]["custom_id"] == "article_1"
            mock_client.batches.retrieve.assert_called_once_with(batch_id)
            mock_client.files.content.assert_called_once_with("file_456")


class TestPromptGenerator:
    """프롬프트 생성기 테스트"""

    @pytest.fixture
    def prompt_generator(self):
        """프롬프트 생성기 인스턴스"""
        return PromptGenerator()

    def test_generate_clickbait_prompt(self, prompt_generator):
        """클릭베이트 프롬프트 생성 테스트"""
        # Given
        title = "충격! 이것 때문에 모든 것이 바뀐다"
        content = "이것은 테스트 내용입니다. " * 50

        # When
        prompt = prompt_generator.generate_clickbait_prompt(title, content)

        # Then
        assert title in prompt
        assert content[:1000] in prompt
        assert "클릭베이트 정도를 0-100점으로 평가" in prompt

    def test_generate_batch_requests(self, prompt_generator):
        """배치 요청 생성 테스트"""
        # Given
        articles = [
            {"id": 1, "title": "테스트 제목 1", "content": "테스트 내용 1"},
            {"id": 2, "title": "테스트 제목 2", "content": "테스트 내용 2"},
        ]

        # When
        requests = prompt_generator.generate_batch_requests(articles)

        # Then
        assert len(requests) == 2
        assert requests[0]["custom_id"] == "article_1"
        assert requests[1]["custom_id"] == "article_2"
        assert requests[0]["body"]["model"] == "gpt-4o-mini"
        assert requests[0]["body"]["response_format"]["type"] == "json_schema"
        assert requests[0]["body"]["response_format"]["json_schema"]["strict"] is True

    def test_validate_clickbait_response_valid(self, prompt_generator):
        """유효한 클릭베이트 응답 검증 테스트"""
        # Given
        valid_response = '{"clickbait_score": 75, "clickbait_explanation": "과장된 표현 사용"}'

        # When
        result = prompt_generator.validate_clickbait_response(valid_response)

        # Then
        assert result is not None
        assert result["clickbait_score"] == 75
        assert result["clickbait_explanation"] == "과장된 표현 사용"

    def test_validate_clickbait_response_invalid_json(self, prompt_generator):
        """잘못된 JSON 형식 응답 검증 테스트"""
        # Given
        invalid_response = "Invalid JSON content"

        # When
        result = prompt_generator.validate_clickbait_response(invalid_response)

        # Then
        assert result is None

    def test_validate_clickbait_response_missing_fields(self, prompt_generator):
        """필수 필드 누락 응답 검증 테스트"""
        # Given
        incomplete_response = '{"clickbait_score": 75}'

        # When
        result = prompt_generator.validate_clickbait_response(incomplete_response)

        # Then
        assert result is None

    def test_validate_clickbait_response_invalid_score(self, prompt_generator):
        """잘못된 점수 범위 응답 검증 테스트"""
        # Given
        invalid_score_response = '{"clickbait_score": 150, "clickbait_explanation": "설명"}'

        # When
        result = prompt_generator.validate_clickbait_response(invalid_score_response)

        # Then
        assert result is None


class TestBulkUpdater:
    """벌크 업데이터 테스트"""

    @pytest.fixture
    def mock_supabase(self):
        """Supabase 클라이언트 모킹"""
        return Mock()

    @pytest.fixture
    def bulk_updater(self, mock_supabase):
        """벌크 업데이터 인스턴스"""
        return BulkUpdater(supabase=mock_supabase)

    def test_bulk_update_articles_success(self, bulk_updater, mock_supabase):
        """Article 벌크 업데이트 성공 테스트"""
        # Given
        updates = [
            {"id": 1, "clickbait_score": 85, "clickbait_explanation": "Test 1"},
            {"id": 2, "clickbait_score": 42, "clickbait_explanation": "Test 2"},
        ]
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = {"data": updates}

        # When
        result = bulk_updater.bulk_update_articles(updates)

        # Then
        assert result is True
        mock_supabase.table.assert_called_with("Article")
        mock_supabase.table.return_value.upsert.assert_called_with(updates)

    def test_bulk_update_articles_handles_errors(self, bulk_updater, mock_supabase):
        """Article 벌크 업데이트 에러 처리 테스트"""
        # Given
        updates = [{"id": 1, "clickbait_score": 85, "clickbait_explanation": "Test 1"}]
        mock_supabase.table.return_value.upsert.return_value.execute.side_effect = Exception("Database error")

        # individual update도 실패하도록 설정
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.side_effect = Exception(
            "Individual update error"
        )

        # When
        result = bulk_updater.bulk_update_articles(updates)

        # Then
        # 모든 업데이트가 실패했으므로 False를 반환해야 함
        assert result is False

    def test_bulk_update_splits_large_batches(self, bulk_updater, mock_supabase):
        """큰 배치를 적절히 분할하는지 테스트"""
        # Given
        large_updates = [
            {"id": i, "clickbait_score": 50, "clickbait_explanation": f"Test {i}"}
            for i in range(1200)  # 배치 크기 한도 초과
        ]
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = {"data": []}

        # When
        result = bulk_updater.bulk_update_articles(large_updates, batch_size=500)

        # Then
        assert result is True
        # 1200개 데이터는 500씩 3번에 나누어 처리되어야 함
        assert mock_supabase.table.return_value.upsert.call_count == 3
