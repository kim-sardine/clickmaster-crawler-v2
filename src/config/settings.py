"""
애플리케이션 설정
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """애플리케이션 설정 클래스"""

    # Supabase 설정
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    # Naver API 설정
    NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
    NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

    # OpenAI 설정 (향후 사용)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # 크롤링 설정
    DEFAULT_KEYWORDS = [
        "논란",
        "충격",
        "경악",
        "발칵",
        "최근 한 온라인 커뮤니티",
    ]
    CRAWL_DELAY_SECONDS = 0.5

    # 로깅 설정
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @classmethod
    def validate(cls) -> bool:
        """필수 설정 검증"""
        required_settings = [
            cls.SUPABASE_URL,
            cls.SUPABASE_SERVICE_ROLE_KEY,
            cls.NAVER_CLIENT_ID,
            cls.NAVER_CLIENT_SECRET,
        ]

        return all(setting is not None for setting in required_settings)


# 전역 설정 인스턴스
settings = Settings()
