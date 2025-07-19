#!/usr/bin/env python3
"""
기자 통계 동기화 스크립트

GitHub workflow에서 정기적으로 실행되어 기자 통계를 업데이트하고 검증합니다.
"""

import sys
import argparse
from datetime import datetime

import traceback

from src.config.settings import settings
from src.database.operations import DatabaseOperations
from src.utils.logging_utils import setup_logging, get_logger


def validate_environment() -> bool:
    """환경 변수 검증"""
    if not settings.validate():
        return False

    required_vars = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]
    logger = get_logger(__name__)

    for var in required_vars:
        if not hasattr(settings, var) or not getattr(settings, var):
            logger.error(f"필수 환경변수가 설정되지 않았습니다: {var}")
            return False

    return True


def run_stats_sync(fix_inconsistencies: bool = True, full_update: bool = False) -> dict:
    """
    기자 통계 동기화 실행

    Args:
        fix_inconsistencies: 통계 불일치 수정 여부
        full_update: 전체 기자 통계 강제 업데이트 여부

    Returns:
        실행 결과 딕셔너리
    """
    logger = get_logger(__name__)
    db_ops = DatabaseOperations()

    result = {"start_time": datetime.now().isoformat(), "success": False, "summary": {}, "actions": [], "errors": []}

    try:
        # 1. 현재 통계 요약 확인
        logger.info("=== 기자 통계 동기화 시작 ===")

        summary = db_ops.get_journalist_stats_summary()
        result["summary"]["before"] = summary

        logger.info(
            f"현재 상태 - 기자: {summary.get('total_journalists', 0)}명, "
            f"활성 기자: {summary.get('active_journalists', 0)}명, "
            f"점수 있는 기자: {summary.get('scored_journalists', 0)}명"
        )
        logger.info(
            f"기사: {summary.get('total_articles', 0)}개, "
            f"분석 완료: {summary.get('scored_articles', 0)}개, "
            f"대기 중: {summary.get('pending_articles', 0)}개"
        )

        # 2. 통계 불일치 감지 및 수정
        if fix_inconsistencies:
            logger.info("--- 통계 불일치 감지 및 수정 ---")
            inconsistency_result = db_ops.fix_inconsistent_stats()
            result["actions"].append({"action": "fix_inconsistencies", "result": inconsistency_result})

            if inconsistency_result.get("error"):
                logger.error(f"통계 불일치 수정 중 오류: {inconsistency_result['error']}")
                result["errors"].append(f"Inconsistency fix error: {inconsistency_result['error']}")
            else:
                logger.info(f"통계 불일치 수정 완료: {inconsistency_result.get('fixed', 0)}건")

        # 3. 전체 통계 업데이트 (옵션)
        if full_update:
            logger.info("--- 전체 기자 통계 강제 업데이트 ---")
            update_result = db_ops.update_all_journalist_stats()
            result["actions"].append({"action": "full_update", "result": update_result})

            if update_result.get("error"):
                logger.error(f"전체 통계 업데이트 중 오류: {update_result['error']}")
                result["errors"].append(f"Full update error: {update_result['error']}")
            else:
                logger.info(
                    f"전체 통계 업데이트 완료: {update_result.get('success', 0)}/{update_result.get('total', 0)}"
                )

        # 4. 최종 통계 요약 확인
        final_summary = db_ops.get_journalist_stats_summary()
        result["summary"]["after"] = final_summary

        logger.info(
            f"최종 상태 - 기자: {final_summary.get('total_journalists', 0)}명, "
            f"활성 기자: {final_summary.get('active_journalists', 0)}명, "
            f"점수 있는 기자: {final_summary.get('scored_journalists', 0)}명"
        )

        # 5. 성공 여부 판단
        has_errors = len(result["errors"]) > 0
        has_actions = len(result["actions"]) > 0

        if has_errors:
            logger.warning(f"일부 오류가 발생했지만 동기화 완료 (오류 {len(result['errors'])}건)")
            result["success"] = True  # 부분 성공
        elif has_actions:
            logger.info("기자 통계 동기화 성공적으로 완료")
            result["success"] = True
        else:
            logger.info("동기화할 내용이 없어 건너뜀")
            result["success"] = True

        result["end_time"] = datetime.now().isoformat()
        logger.info("=== 기자 통계 동기화 완료 ===")

        return result

    except Exception as e:
        error_msg = f"기자 통계 동기화 중 예상치 못한 오류: {e}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())

        result["errors"].append(error_msg)
        result["success"] = False
        result["end_time"] = datetime.now().isoformat()

        return result


def print_result_summary(result: dict):
    """실행 결과 요약 출력"""
    logger = get_logger(__name__)

    logger.info("\n" + "=" * 60)
    logger.info("📊 기자 통계 동기화 결과 요약")
    logger.info("=" * 60)

    # 실행 시간
    start_time = datetime.fromisoformat(result["start_time"])
    end_time = datetime.fromisoformat(result["end_time"])
    duration = end_time - start_time

    print(f"🕐 실행 시간: {duration.total_seconds():.2f}초")
    print(f"✅ 성공 여부: {'성공' if result['success'] else '실패'}")

    # 실행한 작업들
    if result["actions"]:
        print(f"\n📋 실행된 작업: {len(result['actions'])}개")
        for action in result["actions"]:
            action_name = action["action"]
            action_result = action["result"]

            if action_name == "fix_inconsistencies":
                fixed = action_result.get("fixed", 0)
                total = action_result.get("total_inconsistent", 0)
                print(f"  - 통계 불일치 수정: {fixed}/{total}건")

            elif action_name == "full_update":
                success = action_result.get("success", 0)
                total = action_result.get("total", 0)
                print(f"  - 전체 통계 업데이트: {success}/{total}명")
    else:
        print("\n📋 실행된 작업: 없음 (동기화 필요 없음)")

    # 오류 정보
    if result["errors"]:
        print(f"\n❌ 오류: {len(result['errors'])}건")
        for i, error in enumerate(result["errors"], 1):
            print(f"  {i}. {error}")

    # 통계 변화
    before = result["summary"].get("before", {})
    after = result["summary"].get("after", {})

    if before and after:
        print("\n📈 통계 변화:")

        fields = [
            ("total_journalists", "총 기자 수"),
            ("active_journalists", "활성 기자 수"),
            ("scored_journalists", "점수 있는 기자 수"),
            ("total_articles", "총 기사 수"),
            ("scored_articles", "분석 완료 기사 수"),
            ("pending_articles", "대기 중 기사 수"),
        ]

        for field, name in fields:
            before_val = before.get(field, 0)
            after_val = after.get(field, 0)
            change = after_val - before_val

            if change != 0:
                change_str = f"({change:+d})" if change != 0 else ""
                print(f"  - {name}: {before_val} → {after_val} {change_str}")
            else:
                print(f"  - {name}: {after_val}")

    print("=" * 60)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="기자 통계 동기화 스크립트")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="로그 레벨")
    parser.add_argument(
        "--fix-inconsistencies", action="store_true", default=True, help="통계 불일치 감지 및 수정 (기본값: True)"
    )
    parser.add_argument("--no-fix-inconsistencies", action="store_true", help="통계 불일치 수정 비활성화")
    parser.add_argument("--full-update", action="store_true", help="모든 기자 통계 강제 업데이트")
    parser.add_argument("--quiet", action="store_true", help="요약 출력 생략")

    args = parser.parse_args()

    # 로깅 설정
    setup_logging(args.log_level)
    logger = get_logger(__name__)

    try:
        # 환경 변수 검증
        if not validate_environment():
            logger.error("환경 설정이 올바르지 않습니다")
            sys.exit(1)

        # 불일치 수정 옵션 처리
        fix_inconsistencies = args.fix_inconsistencies and not args.no_fix_inconsistencies

        # 동기화 실행
        result = run_stats_sync(fix_inconsistencies=fix_inconsistencies, full_update=args.full_update)

        # 결과 출력
        if not args.quiet:
            print_result_summary(result)

        # 종료 코드 설정
        if result["success"]:
            logger.info("기자 통계 동기화가 성공적으로 완료되었습니다")
            sys.exit(0)
        else:
            logger.error("기자 통계 동기화가 실패했습니다")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단되었습니다")
        sys.exit(1)
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
