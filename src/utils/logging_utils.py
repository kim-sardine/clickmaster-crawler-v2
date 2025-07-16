"""
로깅 유틸리티 모듈
"""

import logging
import functools
from typing import Callable, Any

# 기본 로거
logger = logging.getLogger(__name__)


def log_func(func: Callable) -> Callable:
    """
    함수 실행 로그를 남기는 데코레이터

    Args:
        func: 데코레이트할 함수

    Returns:
        래핑된 함수
    """

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
