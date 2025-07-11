from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
import re


def get_kst_now() -> datetime:
    """현재 한국 시간 반환"""
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst)


def get_yesterday_date_range():
    """어제 날짜 범위 반환 (시작일, 종료일)"""
    now = get_kst_now()
    yesterday = now - timedelta(days=1)

    start_date = yesterday.strftime("%Y%m%d")
    end_date = yesterday.strftime("%Y%m%d")

    return start_date, end_date


def get_date_range_for_days_ago(days_ago: int):
    """N일 전 날짜 범위 반환"""
    now = get_kst_now()
    target_date = now - timedelta(days=days_ago)

    start_date = target_date.strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")

    return start_date, end_date


def parse_naver_date(date_string: str) -> datetime:
    """네이버 날짜 문자열을 datetime으로 변환"""
    try:
        # 네이버 날짜 형식: "Thu, 11 Jul 2024 14:30:00 +0900"
        return date_parser.parse(date_string)
    except Exception:
        # 파싱 실패 시 현재 시간 반환
        return get_kst_now()


def format_datetime_for_db(dt: datetime) -> str:
    """데이터베이스용 datetime 문자열 생성"""
    return dt.isoformat()


def is_within_date_range(target_date: datetime, start_date: str, end_date: str) -> bool:
    """날짜가 지정된 범위 내에 있는지 확인"""
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d") + timedelta(days=1)  # 하루 종료까지

        # timezone 정보 제거하고 비교
        target_date_naive = target_date.replace(tzinfo=None)

        return start_dt <= target_date_naive < end_dt
    except Exception:
        return False


def calculate_age_in_hours(created_at: datetime) -> float:
    """생성 시간으로부터 경과 시간(시간 단위) 계산"""
    now = get_kst_now()
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone(timedelta(hours=9)))

    delta = now - created_at
    return delta.total_seconds() / 3600


def format_duration(seconds: float) -> str:
    """초를 읽기 쉬운 시간 형식으로 변환"""
    if seconds < 60:
        return f"{seconds:.1f}초"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}분"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}시간"


def get_current_timestamp() -> str:
    """현재 타임스탬프 문자열 반환 (파일명용)"""
    return get_kst_now().strftime("%Y%m%d_%H%M%S")


def extract_date_from_title(title: str) -> str:
    """제목에서 날짜 추출 (YYYY-MM-DD 형식)"""
    # 날짜 패턴 매칭
    date_patterns = [
        r"(\d{4})-(\d{1,2})-(\d{1,2})",
        r"(\d{4})\.(\d{1,2})\.(\d{1,2})",
        r"(\d{4})/(\d{1,2})/(\d{1,2})",
        r"(\d{1,2})/(\d{1,2})/(\d{4})",
        r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
    ]

    for pattern in date_patterns:
        match = re.search(pattern, title)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                # 년도가 마지막에 오는 경우 처리
                if len(groups[0]) == 4:  # YYYY-MM-DD
                    year, month, day = groups
                else:  # MM-DD-YYYY or DD-MM-YYYY
                    if int(groups[0]) > 12:  # DD-MM-YYYY
                        day, month, year = groups
                    else:  # MM-DD-YYYY
                        month, day, year = groups

                try:
                    # 날짜 유효성 검증
                    date_obj = datetime(int(year), int(month), int(day))
                    return date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue

    return get_kst_now().strftime("%Y-%m-%d")
