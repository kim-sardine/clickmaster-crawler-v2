#!/usr/bin/env python3
"""
특별 뉴스 처리 스크립트 (Naksi King)
특정 조건의 뉴스나 특별한 처리가 필요한 뉴스를 위한 스크립트
"""

import sys
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import Settings
from src.config.keywords import get_keywords_by_category
from src.database.operations import NewsOperations, BatchOperations
from src.models.batch_status import BatchStatus
from src.utils.logging_utils import setup_logger, log_function_start, log_function_end, log_error, log_news_stats
from src.utils.date_utils import get_kst_now, get_date_range_for_days_ago
from src.utils.text_utils import detect_clickbait_patterns

logger = setup_logger(__name__)


class NaksiKingProcessor:
    """특별 뉴스 처리기 (낚시왕 전용)"""

    def __init__(self):
        self.high_score_threshold = 8.0  # 높은 낚시성 점수 기준
        self.special_keywords = [
            "충격",
            "깜짝",
            "대박",
            "실화",
            "놀라운",
            "믿을 수 없는",
            "경악",
            "화제",
            "논란",
            "폭로",
            "고백",
            "비밀",
        ]

    def find_high_clickbait_news(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """높은 낚시성 점수의 뉴스 찾기"""
        try:
            # 최근 N일간의 높은 점수 뉴스 조회
            start_date, end_date = get_date_range_for_days_ago(days_back)

            # Supabase에서 높은 점수 뉴스 조회 (임시로 직접 쿼리)
            from src.database.supabase_client import supabase_client

            result = (
                supabase_client.client.table("articles")
                .select("*")
                .gte("clickbait_score", self.high_score_threshold)
                .gte("published_date", start_date)
                .lte("published_date", end_date)
                .order("clickbait_score", desc=True)
                .execute()
            )

            high_score_news = result.data
            logger.info(
                f"🎯 Found {len(high_score_news)} high clickbait articles (score >= {self.high_score_threshold})"
            )

            return high_score_news

        except Exception as e:
            log_error(logger, e, "finding high clickbait news")
            return []

    def analyze_clickbait_patterns(self, news_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """낚시성 패턴 분석"""
        try:
            pattern_stats = {}
            keyword_stats = {}
            source_stats = {}
            score_distribution = {"8.0-8.5": 0, "8.5-9.0": 0, "9.0-9.5": 0, "9.5-10.0": 0}

            for news in news_list:
                title = news.get("title", "")
                source = news.get("source", "Unknown")
                score = news.get("clickbait_score", 0)

                # 패턴 분석
                patterns = detect_clickbait_patterns(title)
                for pattern in patterns:
                    pattern_stats[pattern] = pattern_stats.get(pattern, 0) + 1

                # 키워드 분석
                for keyword in self.special_keywords:
                    if keyword in title:
                        keyword_stats[keyword] = keyword_stats.get(keyword, 0) + 1

                # 출처별 통계
                source_stats[source] = source_stats.get(source, 0) + 1

                # 점수 분포
                if 8.0 <= score < 8.5:
                    score_distribution["8.0-8.5"] += 1
                elif 8.5 <= score < 9.0:
                    score_distribution["8.5-9.0"] += 1
                elif 9.0 <= score < 9.5:
                    score_distribution["9.0-9.5"] += 1
                elif 9.5 <= score <= 10.0:
                    score_distribution["9.5-10.0"] += 1

            analysis = {
                "total_analyzed": len(news_list),
                "pattern_stats": pattern_stats,
                "keyword_stats": keyword_stats,
                "source_stats": source_stats,
                "score_distribution": score_distribution,
                "analysis_date": get_kst_now().isoformat(),
            }

            return analysis

        except Exception as e:
            log_error(logger, e, "analyzing clickbait patterns")
            return {}

    def generate_naksi_king_report(self, analysis: Dict[str, Any]) -> str:
        """낚시왕 리포트 생성"""
        try:
            report_lines = []
            report_lines.append("🎣 === NAKSI KING REPORT ===")
            report_lines.append(f"📅 Analysis Date: {analysis.get('analysis_date', 'Unknown')}")
            report_lines.append(f"📊 Total High-Clickbait Articles: {analysis.get('total_analyzed', 0)}")
            report_lines.append("")

            # 점수 분포
            report_lines.append("📈 Score Distribution:")
            score_dist = analysis.get("score_distribution", {})
            for score_range, count in score_dist.items():
                if count > 0:
                    report_lines.append(f"   {score_range}: {count} articles")
            report_lines.append("")

            # 상위 패턴
            report_lines.append("🎯 Top Clickbait Patterns:")
            pattern_stats = analysis.get("pattern_stats", {})
            sorted_patterns = sorted(pattern_stats.items(), key=lambda x: x[1], reverse=True)
            for pattern, count in sorted_patterns[:10]:
                report_lines.append(f"   {pattern}: {count}")
            report_lines.append("")

            # 상위 키워드
            report_lines.append("🔥 Top Clickbait Keywords:")
            keyword_stats = analysis.get("keyword_stats", {})
            sorted_keywords = sorted(keyword_stats.items(), key=lambda x: x[1], reverse=True)
            for keyword, count in sorted_keywords[:10]:
                report_lines.append(f"   {keyword}: {count}")
            report_lines.append("")

            # 상위 출처
            report_lines.append("📰 Top Sources:")
            source_stats = analysis.get("source_stats", {})
            sorted_sources = sorted(source_stats.items(), key=lambda x: x[1], reverse=True)
            for source, count in sorted_sources[:10]:
                report_lines.append(f"   {source}: {count}")

            report_lines.append("\n🎣 === END OF REPORT ===")

            return "\n".join(report_lines)

        except Exception as e:
            log_error(logger, e, "generating naksi king report")
            return "Failed to generate report"

    def find_potential_naksi_king_candidates(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """낚시왕 후보 찾기 (점수 9.5 이상)"""
        try:
            candidates = []

            for news in news_list:
                score = news.get("clickbait_score", 0)
                if score >= 9.5:
                    # 추가 정보 포함
                    candidate = {
                        "id": news.get("id"),
                        "title": news.get("title"),
                        "source": news.get("source"),
                        "clickbait_score": score,
                        "reasoning": news.get("reasoning"),
                        "published_date": news.get("published_date"),
                        "url": news.get("url"),
                    }
                    candidates.append(candidate)

            # 점수 순으로 정렬
            candidates.sort(key=lambda x: x["clickbait_score"], reverse=True)

            logger.info(f"👑 Found {len(candidates)} Naksi King candidates (score >= 9.5)")

            return candidates

        except Exception as e:
            log_error(logger, e, "finding naksi king candidates")
            return []

    def update_naksi_king_status(self, candidates: List[Dict[str, Any]]) -> int:
        """낚시왕 상태 업데이트 (향후 확장용)"""
        try:
            # 현재는 로그만 남기고, 향후 별도 테이블이나 플래그 추가 가능
            updated_count = 0

            for candidate in candidates:
                logger.info(f"👑 NAKSI KING: {candidate['title']} (Score: {candidate['clickbait_score']})")
                updated_count += 1

            return updated_count

        except Exception as e:
            log_error(logger, e, "updating naksi king status")
            return 0


def main():
    """메인 함수"""
    try:
        log_function_start(logger, "process_naksi_king")

        # 환경변수 검증
        Settings.validate_required_env_vars()

        processor = NaksiKingProcessor()

        # 1. 높은 낚시성 점수의 뉴스 찾기
        logger.info("🔍 Finding high clickbait news...")
        high_score_news = processor.find_high_clickbait_news(days_back=7)

        if not high_score_news:
            logger.info("✅ No high clickbait news found in the last 7 days")
            log_function_end(logger, "process_naksi_king")
            return

        # 2. 낚시성 패턴 분석
        logger.info("📊 Analyzing clickbait patterns...")
        analysis = processor.analyze_clickbait_patterns(high_score_news)

        # 3. 리포트 생성 및 출력
        logger.info("📝 Generating Naksi King report...")
        report = processor.generate_naksi_king_report(analysis)

        # 리포트 출력
        print("\n" + "=" * 60)
        print(report)
        print("=" * 60 + "\n")

        # 4. 낚시왕 후보 찾기
        logger.info("👑 Finding Naksi King candidates...")
        candidates = processor.find_potential_naksi_king_candidates(high_score_news)

        if candidates:
            logger.info(f"👑 Top 5 Naksi King Candidates:")
            for i, candidate in enumerate(candidates[:5], 1):
                logger.info(f"   {i}. {candidate['title'][:50]}... (Score: {candidate['clickbait_score']})")

        # 5. 상태 업데이트
        updated_count = processor.update_naksi_king_status(candidates)

        # 6. 최종 통계
        stats = NewsOperations.get_news_stats()
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   High clickbait articles: {len(high_score_news)}")
        logger.info(f"   Naksi King candidates: {len(candidates)}")
        logger.info(f"   Total news in DB: {stats}")

        log_function_end(logger, "process_naksi_king")

    except Exception as e:
        log_error(logger, e, "main process_naksi_king function")
        sys.exit(1)


if __name__ == "__main__":
    main()
