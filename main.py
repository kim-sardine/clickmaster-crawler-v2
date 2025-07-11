#!/usr/bin/env python3
"""
Clickmaster Crawler - 메인 엔트리포인트
로컬 실행을 위한 메인 스크립트
"""

import sys
import os
import argparse
import logging

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config.settings import Settings
from src.utils.logging_utils import setup_logger
from scripts.crawl_news import main as crawl_news_main
from scripts.monitor_batches import main as monitor_batches_main
from scripts.process_completed_batches import main as process_completed_batches_main
from scripts.process_naksi_king import main as process_naksi_king_main

logger = setup_logger(__name__)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Clickmaster Crawler - 네이버 뉴스 낚시성 분석 도구")
    parser.add_argument("command", choices=["crawl", "monitor", "process", "naksi-king"], help="실행할 명령")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="로그 레벨")

    args = parser.parse_args()

    # 로그 레벨 설정
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        logger.info(f"🚀 Starting Clickmaster Crawler - Command: {args.command}")

        # 환경변수 검증
        Settings.validate_required_env_vars()

        if args.command == "crawl":
            logger.info("📰 Starting news crawling...")
            crawl_news_main()
        elif args.command == "monitor":
            logger.info("🔍 Starting batch monitoring...")
            monitor_batches_main()
        elif args.command == "process":
            logger.info("⚙️ Starting batch processing...")
            process_completed_batches_main()
        elif args.command == "naksi-king":
            logger.info("👑 Starting Naksi King analysis...")
            process_naksi_king_main()

        logger.info("✅ Command completed successfully")

    except KeyboardInterrupt:
        logger.info("⏹️ Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Command failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
