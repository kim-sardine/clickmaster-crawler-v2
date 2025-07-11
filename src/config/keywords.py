"""사전 정의된 뉴스 검색 키워드"""

# 정치 관련 키워드
POLITICS_KEYWORDS = [
    "국정감사",
    "국회",
    "정당",
    "선거",
    "정치인",
    "대통령",
    "총리",
    "장관",
    "국정원",
    "검찰",
    "법무부",
    "외교부",
    "국방부",
    "기획재정부",
    "정치",
    "정부",
    "여당",
    "야당",
    "국정",
]

# 경제 관련 키워드
ECONOMY_KEYWORDS = [
    "주식",
    "부동산",
    "금리",
    "환율",
    "증시",
    "경제",
    "투자",
    "기업",
    "취업",
    "일자리",
    "임금",
    "물가",
    "인플레이션",
    "코스피",
    "코스닥",
    "재정",
    "예산",
    "세금",
    "경기",
    "성장률",
]

# 사회 관련 키워드
SOCIETY_KEYWORDS = [
    "교육",
    "의료",
    "복지",
    "범죄",
    "사건",
    "재판",
    "판결",
    "대법원",
    "헌법재판소",
    "사회",
    "문화",
    "종교",
    "환경",
    "날씨",
    "기후",
    "인권",
    "여성",
    "청소년",
    "노인",
    "장애인",
]

# 국제 관련 키워드
INTERNATIONAL_KEYWORDS = [
    "미국",
    "중국",
    "일본",
    "러시아",
    "유럽",
    "북한",
    "외교",
    "무역",
    "국제",
    "세계",
    "전쟁",
    "평화",
    "협상",
    "회담",
    "정상회담",
    "유엔",
    "WHO",
    "올림픽",
    "월드컵",
    "국제기구",
]

# 연예 관련 키워드
ENTERTAINMENT_KEYWORDS = [
    "연예인",
    "아이돌",
    "드라마",
    "영화",
    "음악",
    "예능",
    "방송",
    "TV",
    "케이팝",
    "한류",
    "배우",
    "가수",
    "스타",
    "셀럽",
    "연예계",
    "콘서트",
    "공연",
    "페스티벌",
    "시상식",
    "데뷔",
]

# 스포츠 관련 키워드
SPORTS_KEYWORDS = [
    "축구",
    "야구",
    "농구",
    "배구",
    "골프",
    "테니스",
    "수영",
    "육상",
    "체조",
    "태권도",
    "스포츠",
    "올림픽",
    "월드컵",
    "프로야구",
    "K리그",
    "선수",
    "감독",
    "경기",
    "우승",
    "메달",
]

# 기술 관련 키워드
TECHNOLOGY_KEYWORDS = [
    "AI",
    "인공지능",
    "로봇",
    "자동차",
    "전기차",
    "반도체",
    "스마트폰",
    "컴퓨터",
    "인터넷",
    "게임",
    "기술",
    "과학",
    "연구",
    "개발",
    "혁신",
    "IT",
    "소프트웨어",
    "하드웨어",
    "앱",
    "플랫폼",
]

# 라이프스타일 관련 키워드
LIFESTYLE_KEYWORDS = [
    "건강",
    "다이어트",
    "운동",
    "요리",
    "맛집",
    "여행",
    "패션",
    "뷰티",
    "육아",
    "결혼",
    "라이프스타일",
    "취미",
    "펜션",
    "카페",
    "레스토랑",
    "쇼핑",
    "할인",
    "세일",
    "이벤트",
    "프로모션",
]

# 전체 키워드 리스트
ALL_KEYWORDS = (
    POLITICS_KEYWORDS
    + ECONOMY_KEYWORDS
    + SOCIETY_KEYWORDS
    + INTERNATIONAL_KEYWORDS
    + ENTERTAINMENT_KEYWORDS
    + SPORTS_KEYWORDS
    + TECHNOLOGY_KEYWORDS
    + LIFESTYLE_KEYWORDS
)

# 카테고리별 키워드 딕셔너리
KEYWORD_CATEGORIES = {
    "politics": POLITICS_KEYWORDS,
    "economy": ECONOMY_KEYWORDS,
    "society": SOCIETY_KEYWORDS,
    "international": INTERNATIONAL_KEYWORDS,
    "entertainment": ENTERTAINMENT_KEYWORDS,
    "sports": SPORTS_KEYWORDS,
    "technology": TECHNOLOGY_KEYWORDS,
    "lifestyle": LIFESTYLE_KEYWORDS,
}


def get_keywords_by_category(category: str = None):
    """카테고리별 키워드 반환"""
    if category is None:
        return ALL_KEYWORDS
    return KEYWORD_CATEGORIES.get(category, [])
