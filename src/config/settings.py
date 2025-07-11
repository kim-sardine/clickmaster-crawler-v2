import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()


class Settings:
    """프로젝트 설정"""

    # API 설정
    NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
    NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # Supabase 설정
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # 크롤링 설정
    MIN_TITLE_LENGTH = 9
    MIN_CONTENT_LENGTH = 100
    MAX_CONTENT_LENGTH = 700
    NAVER_DISPLAY_COUNT = 100  # 한번에 가져올 뉴스 수
    MAX_NEWS_PER_KEYWORD = 1000  # 키워드당 최대 뉴스 수

    # 배치 설정
    BATCH_SIZE = 100  # OpenAI 배치 요청당 아이템 수
    BATCH_TIMEOUT_HOURS = 24  # 배치 완료 대기 시간

    # 크롤링 지연 설정
    REQUEST_DELAY = 1.0  # 요청 간격 (초)
    MAX_RETRIES = 3  # 최대 재시도 횟수

    @classmethod
    def validate_required_env_vars(cls):
        """필수 환경변수 검증"""
        required_vars = ["NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET", "OPENAI_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]

        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)

        if missing_vars:
            raise ValueError(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    @classmethod
    def get_headers(cls):
        """네이버 API 헤더 생성"""
        return {
            "X-Naver-Client-Id": cls.NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": cls.NAVER_CLIENT_SECRET,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
