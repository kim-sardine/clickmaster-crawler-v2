"""
로깅 유틸리티 모듈 - 프로젝트 전체 로깅 설정 통일
"""

import logging
import functools
from typing import Callable, Any, Optional

# 전역 로깅 설정 상태 추적
_logging_configured = False


def setup_logging(level: str = "INFO", format_string: Optional[str] = None) -> None:
    """
    전역 로깅 설정 - 프로젝트 전체에서 한 번만 호출되도록 함

    Args:
        level: 로깅 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: 로그 포맷 문자열
    """
    global _logging_configured

    # 이미 설정된 경우 중복 설정 방지
    if _logging_configured:
        return

    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 루트 로거 설정
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(),  # 콘솔 출력만
        ],
        force=True,  # 기존 설정 덮어쓰기
    )

    _logging_configured = True


def get_logger(name: str) -> logging.Logger:
    """
    로거 인스턴스 반환

    Args:
        name: 로거 이름 (보통 __name__ 사용)

    Returns:
        로거 인스턴스
    """
    return logging.getLogger(name)


def log_func(func: Callable) -> Callable:
    """
    함수 실행 로그를 남기는 데코레이터

    Args:
        func: 데코레이트할 함수

    Returns:
        래핑된 함수
    """
    logger = get_logger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        func_name = func.__name__
        logger.debug(f"Starting function: {func_name}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Function {func_name} completed successfully")
            return result
        except Exception as e:
            logger.error(f"Function {func_name} failed with error: {str(e)}")
            raise

    return wrapper
