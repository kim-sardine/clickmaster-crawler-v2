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
    # 이름과 언론사 정규화
    name = name.strip()
    publisher = publisher.strip()

    # 익명 기자 처리 - 각 언론사별로 별도의 익명 기자 생성
    if name in ["익명", "기자", "", " "]:
        name = f"익명기자_{publisher}"

    return name, publisher
