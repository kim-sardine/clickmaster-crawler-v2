# 클릭마스터 크롤러 (Clickmaster Crawler)

네이버 뉴스를 크롤링하여 각 뉴스의 **Clickbait 정도를 0~100 정수 값으로 측정**하고, 판단 근거와 함께 Supabase 데이터베이스에 저장하는 시스템입니다.

## 🎯 주요 기능

- 네이버 뉴스 API를 통한 자동 뉴스 수집
- HTML 태그 및 엔티티 자동 처리
- 기사 중복 검사 및 필터링
- Supabase 데이터베이스 자동 저장
- 데이터 검증 및 에러 핸들링

## 📋 데이터 검증 규칙

수정된 검증 규칙:
- **제목**: 최소 9자 이상
- **본문**: 최대 700자까지 저장
- **URL**: 네이버 뉴스 URL만 허용
- **낚시 점수**: 0-100 범위 내 정수값

## 🛠️ 기술 스택

- **Python 3.9+** - 메인 개발 언어
- **Supabase** - 메인 데이터베이스 (PostgreSQL)
- **Naver Open API** - 뉴스 검색 API
- **Requests/BeautifulSoup** - 웹 크롤링
- **Pytest** - 테스트 프레임워크

## 📦 설치 및 설정

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env` 파일을 생성하고 다음 내용을 입력:

```env
# Supabase Configuration
SUPABASE_URL=your-supabase-url
SUPABASE_KEY=your-supabase-service-role-key

# Naver API Configuration
NAVER_CLIENT_ID=your-naver-client-id
NAVER_CLIENT_SECRET=your-naver-client-secret
```

### 3. 데이터베이스 스키마 설정

Supabase에서 다음 테이블을 생성:

```sql
-- 기자 테이블
CREATE TABLE journalists (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  naver_uuid TEXT,
  publisher TEXT NOT NULL,
  article_count INTEGER DEFAULT 0,
  average_score DECIMAL(5,2) DEFAULT 0.00,
  max_score INTEGER DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 기사 테이블
CREATE TABLE articles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  journalist_id UUID NOT NULL REFERENCES journalists(id),
  publisher TEXT NOT NULL,
  clickbait_score INTEGER CHECK (clickbait_score >= 0 AND clickbait_score <= 100),
  score_explanation TEXT,
  published_at TIMESTAMP WITH TIME ZONE NOT NULL,
  naver_url TEXT UNIQUE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## 🚀 사용법

### 기본 실행

```bash
python main.py
```

### 스크립트 실행

```bash
# 기본 키워드로 크롤링
python scripts/crawl_news.py

# 특정 키워드로 크롤링
python scripts/crawl_news.py --keywords 충격 공포 반전

# 키워드당 최대 기사 수 설정
python scripts/crawl_news.py --max-per-keyword 30

# 테스트 모드 (실제 저장 안함)
python scripts/crawl_news.py --dry-run
```

## 🧪 테스트

### 전체 테스트 실행

```bash
python -m pytest tests/ -v
```

### 개별 테스트 실행

```bash
# 모델 테스트
python -m pytest tests/test_models.py -v

# 데이터베이스 테스트
python -m pytest tests/test_database.py -v

# 크롤러 테스트
python -m pytest tests/test_crawler.py -v
```

## 📁 프로젝트 구조

```
clickmaster-crawler/
├── src/
│   ├── models/          # 데이터 모델
│   │   └── article.py   # Article, Journalist 모델
│   ├── database/        # 데이터베이스 연산
│   │   ├── supabase_client.py
│   │   └── operations.py
│   ├── crawlers/        # 크롤링 로직
│   │   └── naver_crawler.py
│   └── config/          # 설정 파일
│       └── settings.py
├── scripts/             # 실행 스크립트
│   └── crawl_news.py
├── tests/               # 테스트 코드
│   ├── test_models.py
│   ├── test_database.py
│   └── test_crawler.py
├── logs/                # 로그 파일
├── main.py              # 메인 실행 파일
└── requirements.txt     # 의존성 목록
```

## 🔍 데이터 플로우

1. **뉴스 수집**: 네이버 뉴스 API를 통해 키워드별 뉴스 검색
2. **데이터 처리**: HTML 태그/엔티티 제거, 내용 길이 검증
3. **중복 제거**: 네이버 URL 기준 중복 기사 필터링
4. **기자 관리**: 기자 정보 자동 생성/조회
5. **데이터 저장**: Supabase 데이터베이스에 안전하게 저장

## 🎨 개발 원칙

- **TDD (Test-Driven Development)**: 테스트 우선 개발
- **SOLID 원칙**: 객체지향 설계 원칙 준수
- **Clean Architecture**: 계층 분리 및 의존성 관리
- **모듈화**: 기능별 독립적인 모듈 설계

## 📊 로깅

모든 중요한 작업은 로그로 기록됩니다:

```
logs/
├── main_20240115.log           # 메인 실행 로그
├── crawl_news_20240115.log     # 크롤링 로그
└── ...
```

## 🔧 설정 옵션

`src/config/settings.py`에서 다음 옵션을 조정할 수 있습니다:

- `DEFAULT_KEYWORDS`: 기본 검색 키워드
- `MAX_ARTICLES_PER_KEYWORD`: 키워드당 최대 기사 수
- `CRAWL_DELAY_SECONDS`: 크롤링 간격
- `LOG_LEVEL`: 로그 레벨

## 🚨 주의사항

- 네이버 API 사용량 제한을 준수하세요
- 크롤링 간격을 적절히 설정하여 서버 부하를 방지하세요
- 환경 변수를 안전하게 관리하세요
- 정기적으로 로그를 확인하고 정리하세요

## 🤝 기여하기

1. Fork the repository
2. Create a feature branch
3. Write tests for new features
4. Ensure all tests pass
5. Submit a pull request

## 📄 라이선스

MIT License 