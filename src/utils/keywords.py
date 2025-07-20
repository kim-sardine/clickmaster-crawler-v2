"""
키워드 관련 상수 및 함수 모듈
"""

import os
import requests
import re
from typing import List

from src.utils.logging_utils import log_func, get_logger
from src.config.settings import settings

logger = get_logger(__name__)

# SerpApi Google Trends API URL
SERPAPI_TRENDS_URL = "https://serpapi.com/search"

# 한국어와 영어 문자 패턴 (한글, 영문자, 숫자, 공백, 일반적인 기호만 허용)
VALID_CHARS_PATTERN = re.compile(r"^[가-힣a-zA-Z0-9\s\.\,\'\"\-\+\?\!\/\(\)\[\]\{\}\:\;\&\%\@\#\*\^\$\_\=\~\`\\]+$")

# 사용할 최대 키워드 수
MAX_KEYWORDS = 5


@log_func
def get_google_trends_keywords() -> List[str]:
    """
    SerpApi의 Google Trends Trending Now API를 통해 한국의 실시간 인기 검색어를 가져옵니다.

    환경 변수 'SERP_API_KEY'에 API 키를 설정해야 합니다.

    Returns:
        인기 검색어 키워드 리스트

    Raises:
        SystemExit: API 호출 실패 시 프로세스 종료
    """
    # SerpApi API 키 확인
    api_key = os.getenv("SERP_API_KEY")
    if not api_key:
        logger.error("SerpApi API key not found. Set environment variable 'SERP_API_KEY'")
        logger.error("Process terminating due to missing API key")
        raise SystemExit(1)

    try:
        # API 요청 파라미터 설정 - 한국(KR)의 실시간 트렌드
        params = {
            "api_key": api_key,
            "geo": "KR",
            "hl": "ko",  # 한국어로 결과 요청
            "hours": 48,  # 2일간의 트렌드
            "only_active": True,  # 활성 트렌드만 가져오기
            "engine": "google_trends_trending_now",
            "no_cache": True,
        }

        # API 요청
        response = requests.get(SERPAPI_TRENDS_URL, params=params, timeout=10)
        response.raise_for_status()

        # API 응답 처리
        response_data = response.json()
        if "trending_searches" in response_data:
            # 검색어 추출 - 모든 트렌드 쿼리 추출
            keywords = []
            for trend in response_data["trending_searches"]:
                if "query" in trend:
                    query = trend["query"]
                    # 한국어와 영어 이외의 문자가 포함된 검색어 필터링
                    if VALID_CHARS_PATTERN.match(query):
                        keywords.append(query)
                    else:
                        logger.info(f"Filtered out keyword with non-Korean/English characters: {query}")
            if not keywords:
                logger.error("No valid trending searches found from Google Trends")
                logger.error("Process terminating due to empty keyword list")
                raise SystemExit(1)

            # 최대 20개의 키워드만 사용
            keywords = keywords[:MAX_KEYWORDS]
            logger.info(f"Retrieved {len(keywords)} keywords from Google Trends")

            # 기본 키워드 추가하고 중복 제거
            keywords.extend(settings.DEFAULT_KEYWORDS)
            keywords = list(set(keywords))

            logger.info(f"Retrieved {len(keywords)} keywords from Google Trends")
            return keywords
        else:
            logger.error("Invalid response format from SerpApi")
            logger.error("Process terminating due to invalid API response")
            raise SystemExit(1)

    except requests.RequestException as e:
        logger.error(f"Error fetching Google Trends from SerpApi: {str(e)}")
        logger.error("Process terminating due to API request failure")
        raise SystemExit(1)
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing SerpApi response: {str(e)}")
        logger.error("Process terminating due to response parsing error")
        raise SystemExit(1)


@log_func
def get_combined_keywords() -> List[str]:
    """
    기본 키워드와 Google Trends 키워드를 결합하여 반환합니다.

    Returns:
        결합된 키워드 리스트

    Raises:
        SystemExit: Google Trends API 호출 실패 시 프로세스 종료
    """
    # Google Trends 키워드 가져오기 (이미 DEFAULT_KEYWORDS 포함)
    keywords = get_google_trends_keywords()
    logger.info(f"Final keyword list contains {len(keywords)} keywords")
    return keywords
