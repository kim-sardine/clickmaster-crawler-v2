"""
텍스트 처리 유틸리티 모듈
"""

from typing import Tuple
from urllib.parse import urlparse


def normalize_journalist_info(name: str, publisher: str) -> Tuple[str, str]:
    """
    기자명과 언론사명 정규화

    Args:
        name: 기자명
        publisher: 언론사명

    Returns:
        정규화된 (기자명, 언론사명) 튜플
    """
    # 이름과 언론사 정규화 (None 및 비문자 입력 방어)
    name = "" if name is None else str(name).strip()
    publisher = "" if publisher is None else str(publisher).strip()

    # 언론사 최소 길이 보정
    if len(publisher) < 2:
        publisher = "네이버뉴스"

    # 익명/무효 기자명 처리 - 각 언론사별로 별도의 익명 기자 생성
    invalid_name_tokens = {"", " ", "익명", "기자", "사용자", "-", "_"}
    if len(name) < 2 or name in invalid_name_tokens:
        name = f"익명기자_{publisher}"

    return name, publisher


def normalize_naver_url(url: str) -> str:
    """
    네이버 뉴스 URL을 표준 형태로 정규화

    규칙:
    - 쿼리스트링/프래그먼트 제거
    - '/mnews/article/' → '/article/'로 통일
    - 필요 시 말미 슬래시 제거

    Args:
        url: 원본 URL

    Returns:
        정규화된 URL
    """
    if not isinstance(url, str):
        return ""

    # 공백 제거
    url = url.strip()
    if not url:
        return ""

    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path

    # '/mnews/article/' → '/article/'
    path = path.replace("/mnews/article/", "/article/")

    # 말미 슬래시 제거 (단, 루트 제외)
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]

    # 표준 조합 (쿼리/프래그먼트 제거)
    normalized = f"{scheme}://{netloc}{path}"
    return normalized
