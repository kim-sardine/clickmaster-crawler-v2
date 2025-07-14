"""
Supabase 클라이언트 설정
"""

import os
from typing import Optional
from supabase import create_client, Client
from dotenv import load_dotenv


class SupabaseClient:
    """Supabase 클라이언트 래퍼"""

    def __init__(self, url: Optional[str] = None, key: Optional[str] = None):
        """
        Supabase 클라이언트 초기화

        Args:
            url: Supabase URL (환경변수에서 가져옴)
            key: Supabase 서비스 키 (환경변수에서 가져옴)
        """
        load_dotenv()

        self.url = url or os.environ.get("SUPABASE_URL")
        self.key = key or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL과 SUPABASE_SERVICE_ROLE_KEY 환경변수가 필요합니다")

        self._client: Optional[Client] = None

    @property
    def client(self) -> Client:
        """Supabase 클라이언트 반환 (지연 초기화)"""
        if self._client is None:
            self._client = create_client(self.url, self.key)
        return self._client

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            # 간단한 쿼리로 연결 테스트
            result = self.client.table("articles").select("id").limit(1).execute()
            return True
        except Exception:
            return False


# 전역 인스턴스
_supabase_client = None


def get_supabase_client() -> SupabaseClient:
    """Supabase 클라이언트 싱글톤 인스턴스 반환"""
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseClient()
    return _supabase_client
