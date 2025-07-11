# Clickmaster Crawler

네이버 뉴스 기사의 낚시성 제목을 자동으로 분석하는 시스템입니다.

## 🔥 주요 기능

- **자동 뉴스 수집**: 네이버 검색 API를 통한 키워드 기반 뉴스 크롤링
- **AI 기반 분석**: OpenAI GPT를 이용한 낚시성 제목 분석 (0-10점 스코어링)
- **배치 처리**: OpenAI Batch API를 이용한 대량 분석 처리
- **자동화**: GitHub Actions을 통한 무료 스케줄링
- **데이터 저장**: Supabase를 이용한 클라우드 데이터베이스

## 📋 워크플로우

### 1. 뉴스 수집 (매일 오전 6시)
- 사전 정의된 키워드로 네이버 뉴스 검색
- 전날 발행된 뉴스만 필터링
- 중복 제거 및 유효성 검사
- Supabase articles 테이블에 저장

### 2. 배치 처리 (매시간)
- 미처리 뉴스 조회
- OpenAI Batch API로 낚시성 분석 요청
- 배치 상태 모니터링

### 3. 결과 처리 (매시간)
- 완료된 배치 결과 다운로드
- 낚시성 점수 및 분석 근거 추출
- 데이터베이스 업데이트

### 4. 특별 분석 (주기적)
- 높은 낚시성 점수 뉴스 분석
- 패턴 및 통계 리포트 생성
- "낚시왕" 후보 선정

## 🚀 설치 및 설정

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정
`.env` 파일을 생성하고 다음 값들을 설정하세요:

```env
# Naver Search API
NAVER_CLIENT_ID=your_naver_client_id
NAVER_CLIENT_SECRET=your_naver_client_secret

# OpenAI API
OPENAI_API_KEY=your_openai_api_key

# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key
```

### 3. API 키 발급

#### Naver Search API
1. [네이버 개발자 센터](https://developers.naver.com/) 접속
2. 검색 API 서비스 신청
3. Client ID와 Client Secret 발급

#### OpenAI API
1. [OpenAI Platform](https://platform.openai.com/) 접속
2. API 키 발급
3. 배치 API 사용을 위한 크레딧 충전

#### Supabase
1. [Supabase](https://supabase.com/) 프로젝트 생성
2. 데이터베이스 URL과 anon key 확인
3. 필요한 테이블 생성 (articles, batch_jobs)

## 🖥️ 로컬 실행

### 개별 명령 실행
```bash
# 뉴스 크롤링
python main.py crawl

# 배치 모니터링
python main.py monitor

# 배치 결과 처리
python main.py process

# 낚시왕 분석
python main.py naksi-king
# 또는
python naksi_king.py
```

### 스크립트 직접 실행
```bash
# 개별 스크립트 실행
python scripts/crawl_news.py
python scripts/monitor_batches.py
python scripts/process_completed_batches.py
python scripts/process_naksi_king.py
```

## 🏗️ 프로젝트 구조

```
clickmaster-crawler/
├── .github/workflows/          # GitHub Actions 워크플로우
├── src/                        # 소스 코드
│   ├── crawlers/              # 크롤링 로직
│   ├── core/                  # 핵심 처리 로직
│   ├── database/              # 데이터베이스 조작
│   ├── models/                # 데이터 모델
│   ├── utils/                 # 유틸리티 함수
│   └── config/                # 설정 및 프롬프트
├── scripts/                   # 실행 스크립트
│   ├── crawl_news.py         # 뉴스 크롤링
│   ├── monitor_batches.py    # 배치 모니터링
│   ├── process_completed_batches.py  # 배치 결과 처리
│   └── process_naksi_king.py # 특별 분석
├── main.py                   # 메인 엔트리포인트
├── naksi_king.py            # 낚시왕 분석 엔트리포인트
└── requirements.txt         # Python 의존성
```

## 📊 데이터베이스 스키마

### articles 테이블
```sql
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content TEXT NOT NULL,
    url VARCHAR(1000) UNIQUE NOT NULL,
    published_date DATE NOT NULL,
    source VARCHAR(100),
    author VARCHAR(100),
    clickbait_score DECIMAL(3,1),
    reasoning TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### batch_jobs 테이블
```sql
CREATE TABLE batch_jobs (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL,
    input_file_id VARCHAR(100),
    output_file_id VARCHAR(100),
    total_count INTEGER DEFAULT 0,
    processed_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);
```

## ⚙️ 설정 사항

### 크롤링 설정
- 최소 제목 길이: 9자
- 최소 본문 길이: 100자
- 최대 본문 길이: 700자
- 키워드당 최대 뉴스 수: 1,000개
- API 호출 간격: 1초

### 배치 설정
- 배치 크기: 100개
- 배치 타임아웃: 24시간
- 최대 재시도: 3회

## 🤖 GitHub Actions

### 워크플로우
1. **daily-crawler.yml**: 매일 오전 6시 뉴스 크롤링
2. **batch-processor.yml**: 매시간 배치 처리 및 모니터링

### Secrets 설정
GitHub Repository Settings > Secrets에서 다음 값들을 설정하세요:
- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`
- `OPENAI_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_KEY`

## 📈 분석 결과

### 낚시성 점수 기준
- **0-2점**: 정확하고 객관적인 제목
- **3-4점**: 약간의 자극적 표현
- **5-6점**: 과장되었지만 완전히 거짓은 아님
- **7-8점**: 상당히 과장되고 오해 유발 가능
- **9-10점**: 매우 자극적이고 거짓 정보

### 분석 지표
- 제목과 본문 내용의 일치도
- 감정적/자극적 표현 사용 빈도
- 과장 표현 및 어그로성 요소
- 독자 오해 유발 가능성

## 🔧 개발 및 기여

### 로그 레벨 설정
```bash
python main.py crawl --log-level DEBUG
```

### 테스트
```bash
# 개발 중 테스트 실행
python -m pytest tests/
```

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 🤝 기여 방법

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📞 문의

프로젝트에 대한 문의사항이나 버그 리포트는 GitHub Issues를 이용해 주세요.

---

**Clickmaster Crawler** - 낚시성 뉴스를 잡는 현명한 선택 🎣 