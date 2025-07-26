"""
데이터베이스 연산 테스트
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.database.supabase_client import SupabaseClient
from src.database.operations import DatabaseOperations
from src.models.article import Article


class TestSupabaseClient:
    """SupabaseClient 테스트"""

    def test_client_initialization_with_env_vars(self):
        """환경변수로 클라이언트 초기화 테스트"""
        with patch.dict(
            "os.environ", {"SUPABASE_URL": "https://test.supabase.co", "SUPABASE_SERVICE_ROLE_KEY": "test-key"}
        ):
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
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("src.database.supabase_client.load_dotenv"),
        ):  # load_dotenv() 호출 무효화
            with pytest.raises(ValueError, match="SUPABASE_URL과 SUPABASE_SERVICE_ROLE_KEY 환경변수가 필요합니다"):
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

        assert client.test_connection() is True

    @patch("src.database.supabase_client.create_client")
    def test_connection_test_failure(self, mock_create_client):
        """연결 테스트 실패"""
        mock_client = Mock()
        mock_client.table.side_effect = Exception("Connection failed")
        mock_create_client.return_value = mock_client

        client = SupabaseClient(url="https://test.supabase.co", key="test-key")

        assert client.test_connection() is False


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

    def test_get_or_create_journalist_anonymous(self, mock_client):
        """익명 기자 생성 테스트"""
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
            data=[{"id": "journalist-anonymous", "name": "익명기자_조선일보", "publisher": "조선일보"}]
        )

        db_ops = DatabaseOperations()
        result = db_ops.get_or_create_journalist("익명", "조선일보")

        # 익명 기자명이 언론사별로 정규화되는지 확인
        assert result["id"] == "journalist-anonymous"
        assert result["name"] == "익명기자_조선일보"
        assert result["publisher"] == "조선일보"

    def test_get_or_create_journalist_normalize_name(self, mock_client):
        """기자명 정규화 테스트"""
        test_cases = [
            ("", "익명기자_중앙일보"),
            ("   ", "익명기자_중앙일보"),
            ("기자", "익명기자_중앙일보"),
            ("  홍길동  ", "홍길동"),  # 공백 제거
        ]

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

        db_ops = DatabaseOperations()

        for input_name, expected_name in test_cases:
            mock_insert.execute.return_value = Mock(
                data=[{"id": f"journalist-{expected_name}", "name": expected_name, "publisher": "중앙일보"}]
            )

            result = db_ops.get_or_create_journalist(input_name, "중앙일보")
            assert result["name"] == expected_name

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

        assert result is True

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

        assert result is False

    def test_check_duplicate_articles_batch_mixed(self, mock_client):
        """배치 중복 체크 테스트 - 일부 중복"""
        # Mock 설정 - 기존 기사들
        mock_table = Mock()
        mock_select = Mock()
        mock_in = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.in_.return_value = mock_in
        mock_in.execute.return_value = Mock(
            data=[
                {"naver_url": "https://n.news.naver.com/article/023/0003123456"},
                {"naver_url": "https://n.news.naver.com/article/421/0007123456"},
            ]
        )

        db_ops = DatabaseOperations()
        urls = [
            "https://n.news.naver.com/article/023/0003123456",  # 중복
            "https://n.news.naver.com/article/421/0007123456",  # 중복
            "https://n.news.naver.com/article/999/0001234567",  # 신규
        ]
        result = db_ops.check_duplicate_articles_batch(urls)

        expected = {
            "https://n.news.naver.com/article/023/0003123456": True,  # 중복
            "https://n.news.naver.com/article/421/0007123456": True,  # 중복
            "https://n.news.naver.com/article/999/0001234567": False,  # 신규
        }
        assert result == expected

    def test_check_duplicate_articles_batch_all_new(self, mock_client):
        """배치 중복 체크 테스트 - 모두 신규"""
        # Mock 설정 - 기존 기사 없음
        mock_table = Mock()
        mock_select = Mock()
        mock_in = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.in_.return_value = mock_in
        mock_in.execute.return_value = Mock(data=[])

        db_ops = DatabaseOperations()
        urls = [
            "https://n.news.naver.com/article/023/0003123456",
            "https://n.news.naver.com/article/421/0007123456",
        ]
        result = db_ops.check_duplicate_articles_batch(urls)

        expected = {
            "https://n.news.naver.com/article/023/0003123456": False,
            "https://n.news.naver.com/article/421/0007123456": False,
        }
        assert result == expected

    def test_check_duplicate_articles_batch_empty(self, mock_client):
        """배치 중복 체크 테스트 - 빈 리스트"""
        db_ops = DatabaseOperations()
        result = db_ops.check_duplicate_articles_batch([])
        assert result == {}

    def test_check_duplicate_articles_batch_error(self, mock_client):
        """배치 중복 체크 에러 테스트"""
        # Mock 설정 - 에러 발생
        mock_client.table.side_effect = Exception("Database error")

        db_ops = DatabaseOperations()
        urls = ["https://n.news.naver.com/article/023/0003123456"]
        result = db_ops.check_duplicate_articles_batch(urls)

        # 에러 시 모든 URL을 신규로 처리
        expected = {"https://n.news.naver.com/article/023/0003123456": False}
        assert result == expected

    def test_get_or_create_journalists_batch_mixed(self, mock_client):
        """배치 기자 조회/생성 테스트 - 일부 기존, 일부 신규"""
        # Mock 설정 - 기존 기자 조회
        mock_table = Mock()
        mock_select = Mock()
        mock_eq_name = Mock()
        mock_eq_publisher = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq_name
        mock_eq_name.eq.return_value = mock_eq_publisher

        # 첫 번째 기자는 기존, 두 번째는 없음, 세 번째는 기존
        mock_eq_publisher.execute.side_effect = [
            Mock(data=[{"id": "journalist-1", "name": "기자A", "publisher": "언론사1"}]),  # 기존
            Mock(data=[]),  # 없음
            Mock(data=[{"id": "journalist-3", "name": "기자C", "publisher": "언론사2"}]),  # 기존
        ]

        # 새 기자 배치 생성 Mock
        mock_insert = Mock()
        mock_table.insert.return_value = mock_insert
        mock_insert.execute.return_value = Mock(data=[{"id": "journalist-2", "name": "기자B", "publisher": "언론사1"}])

        db_ops = DatabaseOperations()
        journalist_specs = [
            ("기자A", "언론사1"),  # 기존
            ("기자B", "언론사1"),  # 신규
            ("기자C", "언론사2"),  # 기존
        ]

        result = db_ops.get_or_create_journalists_batch(journalist_specs)

        # 모든 기자가 결과에 포함되어야 함
        assert len(result) == 3
        assert "기자A_언론사1" in result
        assert "기자B_언론사1" in result
        assert "기자C_언론사2" in result

        # 배치 생성이 한 번 호출되어야 함 (기자B만)
        mock_table.insert.assert_called_once()

    def test_get_or_create_journalists_batch_all_existing(self, mock_client):
        """배치 기자 조회/생성 테스트 - 모두 기존"""
        # Mock 설정 - 모든 기자가 기존에 존재
        mock_table = Mock()
        mock_select = Mock()
        mock_eq_name = Mock()
        mock_eq_publisher = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq_name
        mock_eq_name.eq.return_value = mock_eq_publisher

        mock_eq_publisher.execute.side_effect = [
            Mock(data=[{"id": "journalist-1", "name": "기자A", "publisher": "언론사1"}]),
            Mock(data=[{"id": "journalist-2", "name": "기자B", "publisher": "언론사2"}]),
        ]

        db_ops = DatabaseOperations()
        journalist_specs = [("기자A", "언론사1"), ("기자B", "언론사2")]

        result = db_ops.get_or_create_journalists_batch(journalist_specs)

        assert len(result) == 2
        assert "기자A_언론사1" in result
        assert "기자B_언론사2" in result

        # 새 기자 생성이 호출되지 않아야 함
        mock_table.insert.assert_not_called()

    def test_get_or_create_journalists_batch_all_new(self, mock_client):
        """배치 기자 조회/생성 테스트 - 모두 신규"""
        # Mock 설정 - 기존 기자 없음
        mock_table = Mock()
        mock_select = Mock()
        mock_eq_name = Mock()
        mock_eq_publisher = Mock()
        mock_insert = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq_name
        mock_eq_name.eq.return_value = mock_eq_publisher
        mock_table.insert.return_value = mock_insert

        # 기존 기자 조회 - 모두 없음
        mock_eq_publisher.execute.return_value = Mock(data=[])

        # 배치 생성 성공
        mock_insert.execute.return_value = Mock(
            data=[
                {"id": "journalist-1", "name": "기자A", "publisher": "언론사1"},
                {"id": "journalist-2", "name": "기자B", "publisher": "언론사2"},
            ]
        )

        db_ops = DatabaseOperations()
        journalist_specs = [("기자A", "언론사1"), ("기자B", "언론사2")]

        result = db_ops.get_or_create_journalists_batch(journalist_specs)

        assert len(result) == 2
        assert "기자A_언론사1" in result
        assert "기자B_언론사2" in result

        # 배치 생성이 한 번 호출되어야 함
        mock_table.insert.assert_called_once()

    def test_get_or_create_journalists_batch_anonymous_normalization(self, mock_client):
        """배치 기자 조회/생성 테스트 - 익명 기자 정규화"""
        # Mock 설정
        mock_table = Mock()
        mock_select = Mock()
        mock_eq_name = Mock()
        mock_eq_publisher = Mock()
        mock_insert = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq_name
        mock_eq_name.eq.return_value = mock_eq_publisher
        mock_table.insert.return_value = mock_insert

        # 기존 기자 없음
        mock_eq_publisher.execute.return_value = Mock(data=[])

        # 배치 생성 성공
        mock_insert.execute.return_value = Mock(
            data=[
                {"id": "journalist-1", "name": "익명기자_언론사1", "publisher": "언론사1"},
                {"id": "journalist-2", "name": "익명기자_언론사2", "publisher": "언론사2"},
            ]
        )

        db_ops = DatabaseOperations()
        journalist_specs = [
            ("익명", "언론사1"),  # 익명 -> 익명기자_언론사1
            ("", "언론사2"),  # 빈 문자열 -> 익명기자_언론사2
        ]

        result = db_ops.get_or_create_journalists_batch(journalist_specs)

        assert len(result) == 2
        assert "익명기자_언론사1_언론사1" in result
        assert "익명기자_언론사2_언론사2" in result

    def test_get_or_create_journalists_batch_empty(self, mock_client):
        """배치 기자 조회/생성 테스트 - 빈 리스트"""
        db_ops = DatabaseOperations()
        result = db_ops.get_or_create_journalists_batch([])
        assert result == {}

    def test_get_or_create_journalists_batch_error_handling(self, mock_client):
        """배치 기자 조회/생성 에러 테스트"""
        # Mock 설정 - 에러 발생
        mock_client.table.side_effect = Exception("Database error")

        db_ops = DatabaseOperations()
        journalist_specs = [("기자A", "언론사1")]

        result = db_ops.get_or_create_journalists_batch(journalist_specs)

        # 에러 시 빈 딕셔너리 반환
        assert result == {}

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

        assert result is True

    def test_bulk_insert_articles_with_caching(self, mock_client):
        """기자 캐싱이 포함된 배치 삽입 테스트"""
        # get_or_create_journalists_batch Mock
        with patch.object(DatabaseOperations, "get_or_create_journalists_batch") as mock_batch_journalist:
            # 배치 기자 처리 결과
            mock_batch_journalist.return_value = {
                "홍길동_조선일보": {"id": "journalist-1", "name": "홍길동", "publisher": "조선일보"},
                "김철수_중앙일보": {"id": "journalist-2", "name": "김철수", "publisher": "중앙일보"},
            }

            # 배치 삽입 Mock 설정 (한 번의 호출로 모든 기사 삽입)
            mock_table = Mock()
            mock_insert = Mock()

            mock_client.table.return_value = mock_table
            mock_table.insert.return_value = mock_insert
            mock_insert.execute.return_value = Mock(
                data=[
                    {"id": "article-1", "title": "기사 1", "journalist_id": "journalist-1"},
                    {"id": "article-2", "title": "기사 2", "journalist_id": "journalist-2"},
                    {"id": "article-3", "title": "기사 3", "journalist_id": "journalist-1"},
                ]
            )

            db_ops = DatabaseOperations()

            articles = [
                Article(
                    title="테스트 기사 제목1입니다",
                    content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                    journalist_name="홍길동",
                    publisher="조선일보",
                    published_at=datetime.now(),
                    naver_url="https://n.news.naver.com/article/023/0003123456",
                ),
                Article(
                    title="테스트 기사 제목2입니다",
                    content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                    journalist_name="김철수",
                    publisher="중앙일보",
                    published_at=datetime.now(),
                    naver_url="https://n.news.naver.com/article/001/0014123456",
                ),
                Article(
                    title="테스트 기사 제목3입니다",
                    content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                    journalist_name="홍길동",  # 같은 기자 (캐시 활용)
                    publisher="조선일보",
                    published_at=datetime.now(),
                    naver_url="https://n.news.naver.com/article/023/0003123457",
                ),
            ]

            result = db_ops.bulk_insert_articles(articles)

            # 결과 검증
            assert len(result) == 3
            assert result[0]["id"] == "article-1"
            assert result[1]["id"] == "article-2"
            assert result[2]["id"] == "article-3"

            # 배치 기자 처리가 한 번만 호출되었는지 확인
            mock_batch_journalist.assert_called_once()

            # 배치 기자 처리에 전달된 고유 기자 조합 확인
            batch_call_args = mock_batch_journalist.call_args[0][0]  # 첫 번째 인수
            assert len(batch_call_args) == 2  # 고유 기자 조합 수: 홍길동+조선일보, 김철수+중앙일보
            assert ("홍길동", "조선일보") in batch_call_args
            assert ("김철수", "중앙일보") in batch_call_args

            # 배치 삽입이 한 번만 호출되었는지 확인
            mock_table.insert.assert_called_once()

            # 배치 삽입에 전달된 데이터 검증
            insert_call_args = mock_table.insert.call_args[0][0]  # 첫 번째 인수
            assert len(insert_call_args) == 3  # 3개 기사 데이터

            # 각 기사에 올바른 기자 ID가 설정되었는지 확인
            assert insert_call_args[0]["journalist_id"] == "journalist-1"  # 홍길동
            assert insert_call_args[1]["journalist_id"] == "journalist-2"  # 김철수
            assert insert_call_args[2]["journalist_id"] == "journalist-1"  # 홍길동 (배치 처리로 동일 ID)

    def test_bulk_insert_articles_empty_list(self, mock_client):
        """빈 기사 리스트 배치 삽입 테스트"""
        db_ops = DatabaseOperations()
        result = db_ops.bulk_insert_articles([])

        assert result == []

    def test_bulk_insert_articles_partial_failure(self, mock_client):
        """배치 삽입 실패 시 개별 삽입 폴백 테스트"""
        # get_or_create_journalists_batch Mock
        with patch.object(DatabaseOperations, "get_or_create_journalists_batch") as mock_batch_journalist:
            mock_batch_journalist.return_value = {
                "홍길동_조선일보": {"id": "journalist-1", "name": "홍길동", "publisher": "조선일보"}
            }

            # 배치 삽입 Mock 설정 - 첫 번째 시도는 실패
            mock_table = Mock()
            mock_insert = Mock()

            mock_client.table.return_value = mock_table
            mock_table.insert.return_value = mock_insert

            # 배치 삽입은 실패하고, 폴백에서 개별 처리
            mock_insert.execute.side_effect = [
                Exception("배치 삽입 실패"),  # 첫 번째 배치 삽입 실패
                Mock(data=[{"id": "article-1", "title": "기사 1"}]),  # 개별 삽입 1번째 성공
                Exception("개별 삽입 실패"),  # 개별 삽입 2번째 실패
                Mock(data=[{"id": "article-3", "title": "기사 3"}]),  # 개별 삽입 3번째 성공
            ]

            # 폴백에서 사용할 개별 기자 조회/생성 Mock (이제는 호출되지 않아야 함)
            with patch.object(DatabaseOperations, "get_or_create_journalist") as mock_individual_journalist:
                mock_individual_journalist.return_value = {
                    "id": "journalist-1",
                    "name": "홍길동",
                    "publisher": "조선일보",
                }

                db_ops = DatabaseOperations()

                articles = [
                    Article(
                        title="테스트 기사 제목1입니다",
                        content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                        journalist_name="홍길동",
                        publisher="조선일보",
                        published_at=datetime.now(),
                        naver_url="https://n.news.naver.com/article/023/0003123456",
                    ),
                    Article(
                        title="테스트 기사 제목2입니다",
                        content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                        journalist_name="홍길동",
                        publisher="조선일보",
                        published_at=datetime.now(),
                        naver_url="https://n.news.naver.com/article/023/0003123457",
                    ),
                    Article(
                        title="테스트 기사 제목3입니다",
                        content="이것은 테스트 기사 내용입니다. 최소 100자 이상이어야 하므로 더 길게 작성해보겠습니다. 충분히 긴 내용이 되도록 추가 텍스트를 넣어보겠습니다. 이제 100자를 충분히 넘겼을 것입니다. 더 추가해보겠습니다.",
                        journalist_name="홍길동",
                        publisher="조선일보",
                        published_at=datetime.now(),
                        naver_url="https://n.news.naver.com/article/023/0003123458",
                    ),
                ]

                result = db_ops.bulk_insert_articles(articles)

                # 배치 삽입 실패 후 폴백으로 2개 성공
                assert len(result) == 2
                assert result[0]["id"] == "article-1"
                assert result[1]["id"] == "article-3"

                # 배치 기자 처리가 호출되었는지 확인
                mock_batch_journalist.assert_called_once()

                # 개별 기자 조회가 호출되지 않았는지 확인 (캐시 재사용으로 인해)
                assert mock_individual_journalist.call_count == 0, "기자 캐시 재사용으로 개별 조회가 발생하지 않아야 함"

    def test_fix_inconsistent_stats_no_issues(self, mock_client):
        """통계 불일치가 없는 경우 테스트"""
        # Mock 설정
        mock_table = Mock()
        mock_client.table.return_value = mock_table

        # 기자 조회 Mock
        mock_journalists_select = Mock()
        mock_articles_select = Mock()
        mock_eq = Mock()

        # table() 호출에 따른 분기
        def table_side_effect(table_name):
            if table_name == "journalists":
                return Mock(select=Mock(return_value=mock_journalists_select))
            elif table_name == "articles":
                return Mock(select=Mock(return_value=Mock(eq=Mock(return_value=mock_eq))))
            return mock_table

        mock_client.table.side_effect = table_side_effect

        # 기자 데이터
        mock_journalists_select.execute.return_value = Mock(
            data=[
                {
                    "id": "journalist-1",
                    "name": "홍길동",
                    "publisher": "조선일보",
                    "article_count": 2,
                    "avg_clickbait_score": 50.0,
                    "max_score": 75,
                }
            ]
        )

        # 해당 기자의 기사 데이터 (평균 50, 최대 75)
        mock_eq.execute.return_value = Mock(data=[{"clickbait_score": 25}, {"clickbait_score": 75}])

        db_ops = DatabaseOperations()
        result = db_ops.fix_inconsistent_stats()

        assert result["fixed"] == 0
        assert result["total_checked"] == 1
        assert result["total_inconsistent"] == 0

    def test_fix_inconsistent_stats_with_issues(self, mock_client):
        """통계 불일치가 있는 경우 테스트"""
        # Mock 설정
        mock_table = Mock()
        mock_client.table.return_value = mock_table

        # 기자 조회 Mock
        mock_journalists_select = Mock()
        mock_articles_select = Mock()
        mock_eq = Mock()

        # table() 호출에 따른 분기
        def table_side_effect(table_name):
            if table_name == "journalists":
                return Mock(select=Mock(return_value=mock_journalists_select))
            elif table_name == "articles":
                return Mock(select=Mock(return_value=Mock(eq=Mock(return_value=mock_eq))))
            return mock_table

        mock_client.table.side_effect = table_side_effect

        # 기자 데이터 (잘못된 통계)
        mock_journalists_select.execute.return_value = Mock(
            data=[
                {
                    "id": "journalist-1",
                    "name": "홍길동",
                    "publisher": "조선일보",
                    "article_count": 1,  # 실제는 2개
                    "avg_clickbait_score": 30.0,  # 실제는 50.0
                    "max_score": 50,  # 실제는 75
                }
            ]
        )

        # 해당 기자의 실제 기사 데이터
        mock_eq.execute.return_value = Mock(data=[{"clickbait_score": 25}, {"clickbait_score": 75}])

        # update_journalist_stats_manual Mock
        with patch.object(DatabaseOperations, "update_journalist_stats_manual") as mock_update:
            mock_update.return_value = True

            db_ops = DatabaseOperations()
            result = db_ops.fix_inconsistent_stats()

            assert result["fixed"] == 1
            assert result["total_checked"] == 1
            assert result["total_inconsistent"] == 1
            mock_update.assert_called_once_with("journalist-1")

    def test_fix_inconsistent_stats_empty_database(self, mock_client):
        """기자가 없는 경우 테스트"""
        # Mock 설정
        mock_table = Mock()
        mock_select = Mock()

        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.execute.return_value = Mock(data=[])

        db_ops = DatabaseOperations()
        result = db_ops.fix_inconsistent_stats()

        assert result["fixed"] == 0
        assert result["total_checked"] == 0

    def test_avg_clickbait_score_calculation_less_than_10_articles(self, mock_client):
        """기사 수가 10개 미만일 때 avg_clickbait_score 계산 테스트"""
        # Mock 설정
        mock_articles_table = Mock()
        mock_journalists_table = Mock()
        mock_select = Mock()
        mock_eq = Mock()
        mock_update = Mock()
        mock_update_eq = Mock()

        # table() 호출에 따른 분기
        def table_side_effect(table_name):
            if table_name == "articles":
                return mock_articles_table
            elif table_name == "journalists":
                return mock_journalists_table
            return Mock()

        mock_client.table.side_effect = table_side_effect

        # Articles 테이블 Mock (기사 조회)
        mock_articles_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq

        # Journalists 테이블 Mock (기자 업데이트)
        mock_journalists_table.update.return_value = mock_update
        mock_update.eq.return_value = mock_update_eq

        # 5개 기사의 clickbait_score: [90, 80, 70, 60, 50]
        # 모든 기사의 평균: (90+80+70+60+50)/5 = 70.0
        mock_articles_result = Mock()
        mock_articles_result.data = [
            {"clickbait_score": 90},
            {"clickbait_score": 80},
            {"clickbait_score": 70},
            {"clickbait_score": 60},
            {"clickbait_score": 50},
        ]
        mock_eq.execute.return_value = mock_articles_result

        mock_update_result = Mock()
        mock_update_result.data = [{"id": "test-journalist"}]
        mock_update_eq.execute.return_value = mock_update_result

        db_ops = DatabaseOperations()
        result = db_ops.update_journalist_stats_manual("test-journalist")

        assert result is True

        # update 호출 인자 확인
        call_args = mock_journalists_table.update.call_args[0][0]
        assert call_args["article_count"] == 5
        assert call_args["avg_clickbait_score"] == 70.0  # 모든 5개 기사의 평균
        assert call_args["max_score"] == 90

    def test_avg_clickbait_score_calculation_more_than_10_articles(self, mock_client):
        """기사 수가 10개 이상일 때 avg_clickbait_score 계산 테스트 (상위 10개 평균)"""
        # Mock 설정
        mock_articles_table = Mock()
        mock_journalists_table = Mock()
        mock_select = Mock()
        mock_eq = Mock()
        mock_update = Mock()
        mock_update_eq = Mock()

        # table() 호출에 따른 분기
        def table_side_effect(table_name):
            if table_name == "articles":
                return mock_articles_table
            elif table_name == "journalists":
                return mock_journalists_table
            return Mock()

        mock_client.table.side_effect = table_side_effect

        # Articles 테이블 Mock (기사 조회)
        mock_articles_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq

        # Journalists 테이블 Mock (기자 업데이트)
        mock_journalists_table.update.return_value = mock_update
        mock_update.eq.return_value = mock_update_eq

        # 15개 기사의 clickbait_score: [95,90,85,80,75,70,65,60,55,50,45,40,35,30,25]
        # 상위 10개의 평균: (95+90+85+80+75+70+65+60+55+50)/10 = 72.5
        mock_articles_result = Mock()
        mock_articles_result.data = [
            {"clickbait_score": 95},
            {"clickbait_score": 90},
            {"clickbait_score": 85},
            {"clickbait_score": 80},
            {"clickbait_score": 75},
            {"clickbait_score": 70},
            {"clickbait_score": 65},
            {"clickbait_score": 60},
            {"clickbait_score": 55},
            {"clickbait_score": 50},
            {"clickbait_score": 45},
            {"clickbait_score": 40},
            {"clickbait_score": 35},
            {"clickbait_score": 30},
            {"clickbait_score": 25},
        ]
        mock_eq.execute.return_value = mock_articles_result

        mock_update_result = Mock()
        mock_update_result.data = [{"id": "test-journalist"}]
        mock_update_eq.execute.return_value = mock_update_result

        db_ops = DatabaseOperations()
        result = db_ops.update_journalist_stats_manual("test-journalist")

        assert result is True

        # update 호출 인자 확인
        call_args = mock_journalists_table.update.call_args[0][0]
        assert call_args["article_count"] == 15
        assert call_args["avg_clickbait_score"] == 72.5  # 상위 10개 기사의 평균
        assert call_args["max_score"] == 95

    def test_avg_clickbait_score_calculation_with_null_scores(self, mock_client):
        """null clickbait_score가 포함된 경우 테스트"""
        # Mock 설정
        mock_articles_table = Mock()
        mock_journalists_table = Mock()
        mock_select = Mock()
        mock_eq = Mock()
        mock_update = Mock()
        mock_update_eq = Mock()

        # table() 호출에 따른 분기
        def table_side_effect(table_name):
            if table_name == "articles":
                return mock_articles_table
            elif table_name == "journalists":
                return mock_journalists_table
            return Mock()

        mock_client.table.side_effect = table_side_effect

        # Articles 테이블 Mock (기사 조회)
        mock_articles_table.select.return_value = mock_select
        mock_select.eq.return_value = mock_eq

        # Journalists 테이블 Mock (기자 업데이트)
        mock_journalists_table.update.return_value = mock_update
        mock_update.eq.return_value = mock_update_eq

        # 12개 기사 중 3개는 null, 9개는 점수 있음
        # 점수 있는 9개: [95,90,85,80,75,70,65,60,55] - 모든 점수 사용 (10개 미만)
        # 평균: (95+90+85+80+75+70+65+60+55)/9 = 75.0
        mock_articles_result = Mock()
        mock_articles_result.data = [
            {"clickbait_score": 95},
            {"clickbait_score": 90},
            {"clickbait_score": 85},
            {"clickbait_score": 80},
            {"clickbait_score": 75},
            {"clickbait_score": 70},
            {"clickbait_score": 65},
            {"clickbait_score": 60},
            {"clickbait_score": 55},
            {"clickbait_score": None},
            {"clickbait_score": None},
            {"clickbait_score": None},
        ]
        mock_eq.execute.return_value = mock_articles_result

        mock_update_result = Mock()
        mock_update_result.data = [{"id": "test-journalist"}]
        mock_update_eq.execute.return_value = mock_update_result

        db_ops = DatabaseOperations()
        result = db_ops.update_journalist_stats_manual("test-journalist")

        assert result is True

        # update 호출 인자 확인
        call_args = mock_journalists_table.update.call_args[0][0]
        assert call_args["article_count"] == 12  # 전체 기사 수 (null 포함)
        assert call_args["avg_clickbait_score"] == 75.0  # 점수 있는 9개 기사의 평균
        assert call_args["max_score"] == 95
