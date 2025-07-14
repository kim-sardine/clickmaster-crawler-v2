# 스크립트 사용법

클릭마스터 크롤러 프로젝트의 독립 실행 스크립트 모음입니다.

## 📋 스크립트 목록

### 1. `crawl_news.py` - 네이버 뉴스 크롤링
네이버 뉴스를 키워드별로 크롤링하여 Supabase 데이터베이스에 저장합니다.

```bash
# 기본 실행 (필수: --date 파라미터)
python scripts/crawl_news.py --date 2024-01-15

# 키워드 지정
python scripts/crawl_news.py --date 2024-01-15 --keywords "AI" "머신러닝" "딥러닝"

# 기사 수 제한
python scripts/crawl_news.py --date 2024-01-15 --max-per-keyword 50

# 테스트 모드 (저장하지 않음)
python scripts/crawl_news.py --date 2024-01-15 --dry-run
```

### 2. `sync_journalist_stats.py` - 기자 통계 동기화
기자별 통계를 동기화하고 불일치를 수정합니다.

```bash
# 기본 실행 (통계 불일치 수정)
python scripts/sync_journalist_stats.py

# 전체 기자 통계 강제 업데이트
python scripts/sync_journalist_stats.py --full-update

# 통계 불일치 수정 비활성화
python scripts/sync_journalist_stats.py --no-fix-inconsistencies

# 조용한 모드 (요약 출력 생략)
python scripts/sync_journalist_stats.py --quiet

# 디버그 모드
python scripts/sync_journalist_stats.py --log-level DEBUG
```

## 🚀 기자 통계 동기화 스크립트 상세

### 기능
- **통계 불일치 감지 및 수정**: 실제 기사 수와 저장된 통계 간 차이 자동 수정
- **전체 통계 재계산**: 모든 기자의 `article_count`, `avg_clickbait_score`, `max_score` 업데이트
- **상세 로깅**: 모든 작업 과정을 상세히 기록
- **에러 핸들링**: 개별 기자 업데이트 실패 시에도 전체 작업 계속 진행

### 실행 조건
1. **환경 변수 설정**:
   ```bash
   export SUPABASE_URL="your-supabase-url"
   export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
   export SUPABASE_ANON_KEY="your-anon-key"
   ```

2. **Python 의존성**:
   ```bash
   pip install -r requirements.txt
   ```

### 옵션 설명

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--log-level` | 로그 레벨 (DEBUG, INFO, WARNING, ERROR) | INFO |
| `--fix-inconsistencies` | 통계 불일치 자동 수정 | True |
| `--no-fix-inconsistencies` | 통계 불일치 수정 비활성화 | False |
| `--full-update` | 모든 기자 통계 강제 업데이트 | False |
| `--quiet` | 요약 출력 생략 | False |

### 출력 예시

```
===============================================================
📊 기자 통계 동기화 결과 요약
===============================================================
🕐 실행 시간: 12.34초
✅ 성공 여부: 성공

📋 실행된 작업: 2개
  - 통계 불일치 수정: 3/5건
  - 전체 통계 업데이트: 25/25명

📈 통계 변화:
  - 총 기자 수: 25
  - 활성 기자 수: 20 → 22 (+2)
  - 점수 있는 기자 수: 15 → 18 (+3)
  - 총 기사 수: 67
  - 분석 완료 기사 수: 18
  - 대기 중 기사 수: 49
===============================================================
```

## 🤖 GitHub Actions 자동화

### 자동 실행 스케줄
- **매일 3회**: 오전 9시, 오후 3시, 오후 9시 (KST)
- **주간 전체 업데이트**: 매주 일요일 오전 2시 (KST)

### 수동 실행
GitHub Actions 탭에서 `Sync Journalist Statistics` workflow를 수동으로 실행할 수 있습니다.

옵션:
- **모든 기자 통계 강제 업데이트**: 전체 기자 통계 재계산
- **로그 레벨**: DEBUG, INFO, WARNING, ERROR 중 선택
- **통계 불일치 수정**: 불일치 감지 및 수정 활성화/비활성화

### 알림 기능
- **실패 시**: Slack `#alerts` 채널로 실패 알림
- **주간 업데이트 성공 시**: Slack `#updates` 채널로 성공 알림
- **로그 아티팩트**: 실패 시 로그 파일 자동 업로드

## 🔧 트러블슈팅

### 1. 환경 변수 오류
```
ERROR: 필수 환경변수가 설정되지 않았습니다: SUPABASE_URL
```
**해결**: `.env` 파일 또는 시스템 환경 변수에 Supabase 설정 추가

### 2. 데이터베이스 연결 실패
```
ERROR: 기자 통계 일괄 업데이트 오류: ...
```
**해결**: 
- Supabase URL과 키 확인
- 네트워크 연결 확인
- Supabase 서비스 상태 확인

### 3. 권한 오류
```
ERROR: Permission denied
```
**해결**: `SERVICE_ROLE_KEY` 사용 (ANON_KEY가 아닌)

### 4. 메모리 부족 (대량 업데이트 시)
```
ERROR: Memory limit exceeded
```
**해결**: 배치 크기 조정 또는 서버 메모리 증설

## 📊 모니터링

### 로그 파일 위치
```
logs/sync_journalist_stats_YYYYMMDD.log
```

### 주요 로그 레벨
- **INFO**: 일반적인 진행 상황
- **WARNING**: 주의 필요한 상황 (일부 실패 등)
- **ERROR**: 심각한 오류
- **DEBUG**: 상세한 디버깅 정보

### 통계 확인 쿼리
```sql
-- 기자별 통계 요약
SELECT 
  name, 
  publisher, 
  article_count, 
  avg_clickbait_score, 
  max_score,
  updated_at
FROM journalists 
WHERE article_count > 0
ORDER BY avg_clickbait_score DESC
LIMIT 10;

-- 통계 불일치 확인
SELECT 
  j.name,
  j.article_count as stored_count,
  COUNT(a.id) as actual_count,
  j.avg_clickbait_score as stored_avg,
  AVG(a.clickbait_score) as actual_avg
FROM journalists j
LEFT JOIN articles a ON j.id = a.journalist_id
GROUP BY j.id, j.name, j.article_count, j.avg_clickbait_score
HAVING j.article_count != COUNT(a.id);
```

## 🎯 성능 최적화

### 권장사항
1. **정기적인 통계 동기화**: 매일 3회 자동 실행으로 데이터 일관성 유지
2. **배치 처리**: 대량 업데이트 시 배치 단위로 처리
3. **인덱스 활용**: `journalist_id`, `clickbait_score` 인덱스 활용
4. **로그 관리**: 주기적인 로그 파일 정리

### 실행 시간 예상
- **불일치 수정**: 1-5초 (불일치 건수에 따라)
- **전체 업데이트**: 기자 수 × 0.5초 (25명 기준 약 12초)
- **대용량 데이터**: 기자 1000명 기준 약 8-10분 