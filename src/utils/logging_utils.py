import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """로거 설정"""
    logger = logging.getLogger(name)

    # 이미 설정된 로거는 반환
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, log_level.upper()))

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def log_function_start(logger: logging.Logger, function_name: str, **kwargs):
    """함수 시작 로그"""
    params = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    logger.info(f"🚀 Starting {function_name}({params})")


def log_function_end(logger: logging.Logger, function_name: str, duration: float = None):
    """함수 종료 로그"""
    if duration:
        logger.info(f"✅ Completed {function_name} in {duration:.2f}s")
    else:
        logger.info(f"✅ Completed {function_name}")


def log_error(logger: logging.Logger, error: Exception, context: str = ""):
    """에러 로그"""
    error_msg = f"❌ Error in {context}: {str(error)}" if context else f"❌ Error: {str(error)}"
    logger.error(error_msg, exc_info=True)


def log_progress(logger: logging.Logger, current: int, total: int, task: str = "Processing"):
    """진행률 로그"""
    percentage = (current / total) * 100
    logger.info(f"📊 {task}: {current}/{total} ({percentage:.1f}%)")


def log_batch_status(logger: logging.Logger, batch_id: str, status: str, details: str = ""):
    """배치 상태 로그"""
    status_emoji = {"pending": "⏳", "in_progress": "🔄", "completed": "✅", "failed": "❌", "cancelled": "⏹️"}.get(
        status, "📋"
    )

    message = f"{status_emoji} Batch {batch_id}: {status}"
    if details:
        message += f" - {details}"
    logger.info(message)


def log_news_stats(logger: logging.Logger, collected: int, processed: int, errors: int):
    """뉴스 처리 통계 로그"""
    logger.info(f"📈 News Stats - Collected: {collected}, Processed: {processed}, Errors: {errors}")


def log_api_call(logger: logging.Logger, api_name: str, endpoint: str, status_code: int = None):
    """API 호출 로그"""
    if status_code:
        logger.info(f"🌐 API Call to {api_name} ({endpoint}) - Status: {status_code}")
    else:
        logger.info(f"🌐 API Call to {api_name} ({endpoint})")
