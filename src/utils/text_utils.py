import re
from typing import List, Optional
from bs4 import BeautifulSoup


def clean_html(text: str) -> str:
    """HTML 태그 제거 및 텍스트 정리"""
    if not text:
        return ""

    # BeautifulSoup으로 HTML 태그 제거
    soup = BeautifulSoup(text, "html.parser")
    cleaned_text = soup.get_text()

    # 특수 HTML 엔티티 처리
    cleaned_text = cleaned_text.replace("&nbsp;", " ")
    cleaned_text = cleaned_text.replace("&amp;", "&")
    cleaned_text = cleaned_text.replace("&lt;", "<")
    cleaned_text = cleaned_text.replace("&gt;", ">")
    cleaned_text = cleaned_text.replace("&quot;", '"')
    cleaned_text = cleaned_text.replace("&#39;", "'")

    return cleaned_text


def normalize_whitespace(text: str) -> str:
    """공백 문자 정규화"""
    if not text:
        return ""

    # 연속된 공백을 하나로 합치기
    text = re.sub(r"\s+", " ", text)

    # 앞뒤 공백 제거
    return text.strip()


def remove_special_chars(text: str, keep_korean: bool = True) -> str:
    """특수문자 제거"""
    if not text:
        return ""

    if keep_korean:
        # 한글, 영문, 숫자, 기본 문장부호만 유지
        text = re.sub(r"[^\w\s가-힣.,!?\'\"():-]", "", text)
    else:
        # 영문, 숫자, 기본 문장부호만 유지
        text = re.sub(r"[^\w\s.,!?\'\"():-]", "", text)

    return normalize_whitespace(text)


def extract_main_content(text: str, max_length: int = 700) -> str:
    """메인 콘텐츠 추출 및 길이 제한"""
    if not text:
        return ""

    # HTML 정리
    cleaned = clean_html(text)

    # 공백 정규화
    normalized = normalize_whitespace(cleaned)

    # 길이 제한
    if len(normalized) > max_length:
        # 문장 단위로 자르기 시도
        sentences = normalized.split(".")
        result = ""
        for sentence in sentences:
            if len(result + sentence + ".") <= max_length:
                result += sentence + "."
            else:
                break

        # 문장 단위로 자르지 못했다면 단순 자르기
        if len(result) < max_length * 0.8:  # 80% 이상 채워졌을 때만 문장 단위 적용
            result = normalized[:max_length]

        return result.strip()

    return normalized


def is_valid_title(title: str, min_length: int = 9) -> bool:
    """제목 유효성 검사"""
    if not title:
        return False

    cleaned_title = normalize_whitespace(clean_html(title))

    # 최소 길이 확인
    if len(cleaned_title) < min_length:
        return False

    # 의미있는 내용이 있는지 확인 (한글 또는 영문 포함)
    if not re.search(r"[가-힣a-zA-Z]", cleaned_title):
        return False

    return True


def is_valid_content(content: str, min_length: int = 100, max_length: int = 700) -> bool:
    """본문 유효성 검사"""
    if not content:
        return False

    cleaned_content = extract_main_content(content, max_length)

    # 길이 확인
    if len(cleaned_content) < min_length:
        return False

    # 의미있는 내용이 있는지 확인
    if not re.search(r"[가-힣a-zA-Z]", cleaned_content):
        return False

    return True


def extract_keywords_from_text(text: str) -> List[str]:
    """텍스트에서 키워드 추출 (간단한 버전)"""
    if not text:
        return []

    # 한글 단어만 추출 (2글자 이상)
    korean_words = re.findall(r"[가-힣]{2,}", text)

    # 중복 제거 및 빈도순 정렬 (간단히 set으로 중복만 제거)
    unique_words = list(set(korean_words))

    return unique_words[:10]  # 상위 10개만 반환


def sanitize_filename(text: str, max_length: int = 100) -> str:
    """파일명으로 사용 가능한 문자열로 변환"""
    if not text:
        return "untitled"

    # HTML 태그 및 특수문자 제거
    cleaned = clean_html(text)
    cleaned = re.sub(r'[<>:"/\\|?*]', "", cleaned)
    cleaned = normalize_whitespace(cleaned)

    # 길이 제한
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length]

    return cleaned if cleaned else "untitled"


def detect_clickbait_patterns(title: str) -> List[str]:
    """낚시성 패턴 감지"""
    patterns = []
    title_lower = title.lower()

    # 자극적 표현
    clickbait_words = ["충격", "깜짝", "놀라운", "믿을 수 없는", "경악", "대박", "실화"]
    for word in clickbait_words:
        if word in title:
            patterns.append(f"자극적 표현: {word}")

    # 숫자를 이용한 어그로
    number_patterns = re.findall(r"\d+", title)
    if len(number_patterns) >= 2:
        patterns.append("과도한 숫자 사용")

    # 질문형 제목
    if "?" in title:
        patterns.append("질문형 제목")

    # 과도한 감탄사
    exclamation_count = title.count("!")
    if exclamation_count >= 2:
        patterns.append(f"과도한 감탄사 ({exclamation_count}개)")

    return patterns
