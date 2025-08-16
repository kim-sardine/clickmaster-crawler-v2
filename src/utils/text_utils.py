"""
텍스트 처리 유틸리티 모듈
"""

from typing import Tuple


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
