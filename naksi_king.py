#!/usr/bin/env python3
"""
Naksi King - 특별 뉴스 처리 엔트리포인트
낚시성 뉴스 분석 및 리포트 생성을 위한 스크립트
"""

import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.process_naksi_king import main

if __name__ == "__main__":
    main()
