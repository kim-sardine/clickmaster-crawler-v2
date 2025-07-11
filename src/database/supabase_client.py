import os
from supabase import create_client, Client
from typing import Optional
import logging

from ..config.settings import Settings

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Supabase 클라이언트 래퍼"""

    _instance: Optional["SupabaseClient"] = None
    _client: Optional[Client] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._initialize_client()

    def _initialize_client(self):
        """Supabase 클라이언트 초기화"""
        try:
            Settings.validate_required_env_vars()

            self._client = create_client(Settings.SUPABASE_URL, Settings.SUPABASE_KEY)

            logger.info("✅ Supabase client initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {str(e)}")
            raise

    @property
    def client(self) -> Client:
        """Supabase 클라이언트 반환"""
        if self._client is None:
            self._initialize_client()
        return self._client

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            # 간단한 쿼리로 연결 테스트
            result = self.client.table("articles").select("id").limit(1).execute()
            logger.info("✅ Supabase connection test successful")
            return True
        except Exception as e:
            logger.error(f"❌ Supabase connection test failed: {str(e)}")
            return False

    def get_table_info(self, table_name: str) -> dict:
        """테이블 정보 조회"""
        try:
            result = self.client.table(table_name).select("*").limit(1).execute()
            return {"table_name": table_name, "connection_ok": True, "sample_count": len(result.data)}
        except Exception as e:
            return {"table_name": table_name, "connection_ok": False, "error": str(e)}


# 전역 인스턴스
supabase_client = SupabaseClient()
