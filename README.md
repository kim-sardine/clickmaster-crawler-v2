# Clickmaster Crawler V2

네이버 뉴스 기사의 클릭베이트(낚시성 제목) 정도를 AI로 분석하는 크롤러 시스템입니다.

## 📋 프로젝트 소개

이 프로젝트는 네이버 뉴스를 크롤링하여 각 기사의 클릭베이트 점수(0-100)를 측정하고, 그 결과를 데이터베이스에 저장합니다. OpenAI의 GPT 모델을 활용하여 기사 제목과 내용을 분석하고, 객관적인 점수와 판단 근거를 제공합니다.

## 🚀 주요 기능

- **자동화된 뉴스 크롤링**: 네이버 뉴스 API를 통한 체계적인 데이터 수집
- **AI 기반 분석**: OpenAI Batch API를 활용한 효율적인 대량 처리
- **데이터베이스 저장**: Supabase를 통한 안정적인 데이터 관리
- **통계 분석**: 기자별, 언론사별 클릭베이트 경향 분석
- **GitHub Actions 자동화**: 정기적인 크롤링 및 모니터링

## 🛠 기술 스택

- **Language**: Python 3.12+
- **Database**: Supabase (PostgreSQL)
- **AI**: OpenAI GPT API (Batch Processing)
- **Crawling**: Naver News API, BeautifulSoup4
- **Automation**: GitHub Actions
- **Testing**: pytest

## 📁 프로젝트 구조

```
clickmaster-crawler-v2/
├── src/
│   ├── crawlers/          # 크롤링 로직
│   ├── database/          # 데이터베이스 연산
│   ├── core/              # 핵심 비즈니스 로직
│   ├── models/            # 데이터 모델
│   ├── config/            # 설정 관리
│   └── utils/             # 유틸리티 함수
├── scripts/               # 실행 스크립트
├── tests/                 # 테스트 코드
└── .github/workflows/     # GitHub Actions 워크플로우
```

## 🔧 설치 및 실행

### 1. 저장소 클론
```bash
git clone https://github.com/kim-sardine/clickmaster-crawler-v2.git
cd clickmaster-crawler-v2
```

### 2. 가상환경 설정
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 3. 의존성 설치
```bash
pip install -r requirements.txt
```

### 4. 환경 변수 설정
`.env` 파일을 생성하고 다음 환경 변수를 설정합니다:

```env
# Supabase
SUPABASE_URL=your-supabase-url
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Naver API
NAVER_CLIENT_ID=your-naver-client-id
NAVER_CLIENT_SECRET=your-naver-client-secret

# OpenAI
OPENAI_API_KEY=your-openai-api-key

# Optional: Google Trends via SerpAPI
SERP_API_KEY=your-serp-api-key
```

### 5. 뉴스 크롤링 실행
```bash
python -m scripts.crawl_news --date 2024-01-01
```

### 6. OpenAI 배치 모니터링
```bash
python -m scripts.openai_batch_monitor --batch-size 800
```

## 🧪 테스트

```bash
# 모든 테스트 실행
pytest

# 특정 테스트 파일 실행
pytest tests/test_crawler.py

# 커버리지 리포트
pytest --cov=src tests/
```

## 📊 데이터 스키마

### Articles 테이블
- `id`: 고유 식별자
- `title`: 기사 제목
- `content`: 기사 본문
- `clickbait_score`: AI가 측정한 클릭베이트 점수 (0-100)
- `score_explanation`: 점수 판단 근거
- `published_at`: 기사 발행일
- `naver_url`: 네이버 뉴스 URL

### Journalists 테이블
- `id`: 고유 식별자
- `name`: 기자명
- `publisher`: 언론사
- `article_count`: 작성 기사 수
- `average_score`: 평균 클릭베이트 점수

## 🤖 GitHub Actions

### Daily News Crawling
- 매일 한국시간 오전 6시에 자동 실행
- 전날 뉴스를 수집하여 분석

### Hourly Batch Monitor
- 매시간 실행되어 OpenAI 배치 작업 모니터링
- 완료된 배치 결과를 자동으로 처리

## 🤝 기여하기

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## ⚠️ 면책 조항

- 이 프로젝트는 교육 및 연구 목적으로 제작되었습니다
- 크롤링 시 네이버의 이용 약관과 robots.txt를 준수합니다
- 수집된 데이터의 저작권은 원 저작권자에게 있습니다

## 📧 문의

프로젝트에 대한 문의사항은 [Issues](https://github.com/kim-sardine/clickmaster-crawler-v2/issues)를 통해 제출해주세요.
