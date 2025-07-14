"""
데이터베이스 연산 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.database.supabase_client import SupabaseClient, get_supabase_client
from src.database.operations import DatabaseOperations
from src.models.article import Article, Journalist


class TestSupabaseClient:
    """SupabaseClient 테스트"""

    def test_client_initialization_with_env_vars(self):
        """환경변수로 클라이언트 초기화 테스트"""
        with patch.dict("os.environ", {"SUPABASE_URL": "https://test.supabase.co", "SUPABASE_KEY": "test-key"}):
            client = SupabaseClient()
            assert client.url == "https://test.supabase.co"
            assert client.key == "test-key"

    def test_client_initialization_with_params(self):
        """파라미터로 클라이언트 초기화 테스트"""
        client = SupabaseClient(url="https://test.supabase.co", key="test-key")
        assert client.url == "https://test.supabase.co"
        assert client.key == "test-key"

    def test_client_initialization_missing_env_vars(self):
        """환경변수 누락 시 오류 테스트"""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="SUPABASE_URL과 SUPABASE_KEY 환경변수가 필요합니다"):
                SupabaseClient()

    @patch("src.database.supabase_client.create_client")
    def test_client_property_lazy_initialization(self, mock_create_client):
        """클라이언트 지연 초기화 테스트"""
        mock_client = Mock()
        mock_create_client.return_value = mock_client

        client = SupabaseClient(url="https://test.supabase.co", key="test-key")

        # 첫 번째 접근
        result1 = client.client
        assert result1 == mock_client
        mock_create_client.assert_called_once_with("https://test.supabase.co", "test-key")

        # 두 번째 접근 (캐시된 인스턴스 사용)
        result2 = client.client
        assert result2 == mock_client
        # create_client는 한 번만 호출되어야 함
        assert mock_create_client.call_count == 1

    @patch("src.database.supabase_client.create_client")
    def test_connection_test_success(self, mock_create_client):
        """연결 테스트 성공"""
        mock_client = Mock()
        mock_table = Mock()
        mock_select = Mock()
        mock_limit = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.limit.return_value = mock_limit
        mock_limit.execute.return_value = Mock(data=[])

        mock_create_client.return_value = mock_client

        client = SupabaseClient(url="https://test.supabase.co", key="test-key")

        assert client.test_connection() == True

    @patch("src.database.supabase_client.create_client")
    def test_connection_test_failure(self, mock_create_client):
        """연결 테스트 실패"""
        mock_client = Mock()
        mock_client.table.side_effect = Exception("Connection failed")
        mock_create_client.return_value = mock_client

        client = SupabaseClient(url="https://test.supabase.co", key="test-key")

        assert client.test_connection() == False


class TestDatabaseOperations:
    """DatabaseOperations 테스트"""

    @pytest.fixture
    def mock_client(self):
        """Mock Supabase 클라이언트 픽스처"""
        with patch("src.database.operations.get_supabase_client") as mock_get_client:
            mock_supabase_client = Mock()
            mock_client = Mock()
            mock_supabase_client.client = mock_client
            mock_get_client.return_value = mock_supabase_client
            yield mock_client

    def test_get_or_create_journalist_existing(self, mock_client):
        """기존 기자 조회 테스트"""
        # Mock 설정
        mock_table = Mock()
        mock_select = Mock()
        mock_eq1 = Mock()
        mock_eq2 = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq1
        mock_eq1.eq.return_value = mock_eq2
        mock_eq2.execute.return_value = Mock(data=[{"id": "journalist-123", "name": "홍길동", "publisher": "조선일보"}])

        db_ops = DatabaseOperations()
        result = db_ops.get_or_create_journalist("홍길동", "조선일보")

        assert result["id"] == "journalist-123"
        assert result["name"] == "홍길동"
        assert result["publisher"] == "조선일보"

    def test_get_or_create_journalist_new(self, mock_client):
        """새 기자 생성 테스트"""
        # 기존 기자 조회 Mock (빈 결과)
        mock_table = Mock()
        mock_select = Mock()
        mock_eq1 = Mock()
        mock_eq2 = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq1
        mock_eq1.eq.return_value = mock_eq2
        mock_eq2.execute.return_value = Mock(data=[])

        # 새 기자 생성 Mock
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_insert.execute.return_value = Mock(
            data=[{"id": "journalist-456", "name": "김철수", "publisher": "중앙일보"}]
        )

        db_ops = DatabaseOperations()
        result = db_ops.get_or_create_journalist("김철수", "중앙일보", "uuid123")

        assert result["id"] == "journalist-456"
        assert result["name"] == "김철수"
        assert result["publisher"] == "중앙일보"

    def test_check_duplicate_article_exists(self, mock_client):
        """중복 기사 존재 테스트"""
        mock_table = Mock()
        mock_select = Mock()
        mock_eq = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq
        mock_eq.execute.return_value = Mock(data=[{"id": "article-123"}])

        db_ops = DatabaseOperations()
        result = db_ops.check_duplicate_article("https://n.news.naver.com/article/023/0003123456")

        assert result == True

    def test_check_duplicate_article_not_exists(self, mock_client):
        """중복 기사 미존재 테스트"""
        mock_table = Mock()
        mock_select = Mock()
        mock_eq = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq
        mock_eq.execute.return_value = Mock(data=[])

        db_ops = DatabaseOperations()
        result = db_ops.check_duplicate_article("https://n.news.naver.com/article/023/0003123456")

        assert result == False

    def test_insert_article_success(self, mock_client):
        """기사 삽입 성공 테스트"""
        # get_or_create_journalist Mock
        with patch.object(DatabaseOperations, "get_or_create_journalist") as mock_get_journalist:
            mock_get_journalist.return_value = {"id": "journalist-123"}

            # insert Mock
            mock_table = Mock()
            mock_insert = Mock()

            mock_client.table.return_value = mock_table
            mock_table.insert.return_value = mock_insert
            mock_insert.execute.return_value = Mock(data=[{"id": "article-123", "title": "테스트 기사 제목입니다"}])

            db_ops = DatabaseOperations()

            article = Article(
                title="테스트 기사 제목입니다",
                content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 넘겼을 것입니다.",
                journalist_name="홍길동",
                publisher="조선일보",
                published_at=datetime.now(),
                naver_url="https://n.news.naver.com/article/023/0003123456",
            )

            result = db_ops.insert_article(article)

            assert result["id"] == "article-123"
            assert result["title"] == "테스트 기사 제목입니다"

    def test_get_unprocessed_articles(self, mock_client):
        """미처리 기사 조회 테스트"""
        mock_table = Mock()
        mock_select = Mock()
        mock_is = Mock()
        mock_limit = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.is_.return_value = mock_is
        mock_is.limit.return_value = mock_limit
        mock_limit.execute.return_value = Mock(
            data=[{"id": "article-1", "title": "기사 1"}, {"id": "article-2", "title": "기사 2"}]
        )

        db_ops = DatabaseOperations()
        result = db_ops.get_unprocessed_articles(limit=100)

        assert len(result) == 2
        assert result[0]["id"] == "article-1"
        assert result[1]["id"] == "article-2"

    def test_update_article_score_success(self, mock_client):
        """기사 점수 업데이트 성공 테스트"""
        mock_table = Mock()
        mock_update = Mock()
        mock_eq = Mock()

        mock_client.table.return_value = mock_table
        mock_table.update.return_value = mock_update
        mock_update.eq.return_value = mock_eq
        mock_eq.execute.return_value = Mock(data=[{"id": "article-123"}])

        db_ops = DatabaseOperations()
        result = db_ops.update_article_score("article-123", 75, "중간 수준의 낚시성 제목")

        assert result == True
