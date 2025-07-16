"""
Supabase 데이터베이스 연산
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from src.models.article import Article, Journalist
from src.database.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """데이터베이스 연산 클래스"""

    def __init__(self):
        self.client = get_supabase_client()

    def get_or_create_journalist(self, name: str, publisher: str, naver_uuid: Optional[str] = None) -> Dict[str, Any]:
        """
        기자 정보 조회 또는 생성

        Args:
            name: 기자명
            publisher: 언론사
            naver_uuid: 네이버 UUID (선택)

        Returns:
            기자 정보 딕셔너리
        """
        try:
            # 이름과 언론사 정규화
            name = name.strip()
            publisher = publisher.strip()

            # 익명 기자 처리 - 각 언론사별로 별도의 익명 기자 생성
            if name in ["익명", "기자", "", " "]:
                name = f"익명기자_{publisher}"
                logger.debug(f"익명 기자명 정규화: {name}")

            # 기존 기자 조회
            existing = (
                self.client.client.table("journalists")
                .select("*")
                .eq("name", name)
                .eq("publisher", publisher)
                .execute()
            )

            if existing.data:
                journalist_info = existing.data[0]
                logger.debug(f"기존 기자 조회: {name} ({publisher}) - ID: {journalist_info['id']}")
                return journalist_info

            # 새 기자 생성
            journalist = Journalist(name=name, publisher=publisher, naver_uuid=naver_uuid)
            journalist_data = journalist.to_dict()

            result = self.client.client.table("journalists").insert(journalist_data).execute()

            if result.data:
                new_journalist = result.data[0]
                logger.info(f"🆕 새 기자 생성: {name} ({publisher}) - ID: {new_journalist['id']}")
                return new_journalist
            else:
                raise Exception("기자 생성 실패 - 응답 데이터 없음")

        except Exception as e:
            logger.error(f"기자 조회/생성 오류 [{name}, {publisher}]: {e}")
            raise

    def insert_article(self, article: Article) -> Dict[str, Any]:
        """
        기사 삽입

        Args:
            article: 기사 객체

        Returns:
            삽입된 기사 정보
        """
        try:
            # 기자 정보 조회/생성
            journalist = self.get_or_create_journalist(article.journalist_name, article.publisher)

            # 기사 데이터 준비
            article.journalist_id = journalist["id"]
            article_data = article.to_dict()

            # 기사 삽입
            result = self.client.client.table("articles").insert(article_data).execute()

            if result.data:
                logger.info(f"기사 삽입 완료: {article.title[:50]}...")
                return result.data[0]
            else:
                raise Exception("기사 삽입 실패")

        except Exception as e:
            logger.error(f"기사 삽입 오류: {e}")
            raise

    def bulk_insert_articles(self, articles: List[Article]) -> List[Dict[str, Any]]:
        """
        기사 배치 삽입 (Supabase 배치 삽입 활용)

        Args:
            articles: 기사 리스트

        Returns:
            삽입된 기사 정보 리스트
        """
        if not articles:
            logger.warning("삽입할 기사가 없습니다")
            return []

        logger.info(f"배치 삽입 시작: {len(articles)}개 기사")

        try:
            # 1단계: 모든 기자 정보를 미리 처리하고 캐싱 (순서 보장)
            journalist_cache = {}
            unique_journalists = []
            seen_journalists = set()

            # 기사 순서대로 고유한 기자들을 수집 (순서 보장)
            for article in articles:
                journalist_tuple = (article.journalist_name, article.publisher)
                if journalist_tuple not in seen_journalists:
                    unique_journalists.append(journalist_tuple)
                    seen_journalists.add(journalist_tuple)

            logger.info(f"처리할 고유 기자 수: {len(unique_journalists)}")

            # 고유 기자들에 대해 순서대로 조회/생성
            for journalist_name, publisher in unique_journalists:
                journalist_key = f"{journalist_name}_{publisher}"
                try:
                    journalist = self.get_or_create_journalist(journalist_name, publisher)
                    journalist_cache[journalist_key] = journalist
                    logger.debug(f"기자 정보 처리: {journalist_name} ({publisher}) - ID: {journalist['id']}")
                except Exception as e:
                    logger.error(f"기자 정보 처리 실패 [{journalist_name}, {publisher}]: {e}")
                    # 실패한 기자의 기사들은 제외하고 계속 진행
                    continue

            # 2단계: 기사 데이터 준비 (배치 삽입용)
            articles_data = []
            skipped_count = 0

            for article in articles:
                journalist_key = f"{article.journalist_name}_{article.publisher}"

                if journalist_key not in journalist_cache:
                    logger.warning(f"기자 정보가 없어 기사 제외: {article.title[:50]}...")
                    skipped_count += 1
                    continue

                # 기사에 기자 ID 설정
                article.journalist_id = journalist_cache[journalist_key]["id"]
                article_data = article.to_dict()
                articles_data.append(article_data)

            if not articles_data:
                logger.warning("삽입할 수 있는 기사가 없습니다")
                return []

            logger.info(f"배치 삽입 준비 완료: {len(articles_data)}개 기사 (제외: {skipped_count}개)")

            # 3단계: Supabase 배치 삽입 실행
            result = self.client.client.table("articles").insert(articles_data).execute()

            if result.data:
                inserted_count = len(result.data)
                logger.info(f"배치 삽입 완료: {inserted_count}개 기사 성공")

                # 처리된 기자 정보 로깅
                logger.info(f"처리된 기자 수: {len(journalist_cache)}명")
                for journalist_key, journalist_info in journalist_cache.items():
                    name, publisher = journalist_key.split("_", 1)
                    logger.info(f"  - {name} ({publisher}): ID {journalist_info['id']}")

                return result.data
            else:
                logger.error("배치 삽입 실패 - 응답 데이터 없음")
                return []

        except Exception as e:
            logger.error(f"배치 삽입 실행 오류: {e}")

            # 오류 발생 시 개별 삽입으로 폴백
            logger.info("개별 삽입으로 폴백 시작...")
            return self._fallback_individual_insert(articles)

    def _fallback_individual_insert(self, articles: List[Article]) -> List[Dict[str, Any]]:
        """
        배치 삽입 실패 시 개별 삽입으로 폴백

        Args:
            articles: 기사 리스트

        Returns:
            삽입된 기사 정보 리스트
        """
        journalist_cache = {}
        inserted_articles = []

        logger.warning("개별 삽입 모드로 진행합니다...")

        for i, article in enumerate(articles, 1):
            try:
                # 기자 캐시 키 생성 (이름 + 언론사)
                journalist_key = f"{article.journalist_name}_{article.publisher}"

                # 캐시에서 기자 정보 조회
                if journalist_key not in journalist_cache:
                    journalist = self.get_or_create_journalist(article.journalist_name, article.publisher)
                    journalist_cache[journalist_key] = journalist
                    logger.debug(f"기자 정보 캐싱: {article.journalist_name} ({article.publisher})")
                else:
                    journalist = journalist_cache[journalist_key]

                # 기사에 기자 ID 설정
                article.journalist_id = journalist["id"]
                article_data = article.to_dict()

                # 기사 삽입
                result = self.client.client.table("articles").insert(article_data).execute()

                if result.data:
                    inserted_articles.append(result.data[0])
                    logger.debug(f"기사 삽입 완료 ({i}/{len(articles)}): {article.title[:50]}...")
                else:
                    logger.warning(f"기사 삽입 실패 ({i}/{len(articles)}): {article.title[:50]}...")

            except Exception as e:
                logger.error(f"기사 삽입 실패 ({i}/{len(articles)}): {article.title[:50]}... - {e}")
                continue

        logger.info(f"개별 삽입 완료: {len(inserted_articles)}/{len(articles)}개 기사")
        return inserted_articles

    def check_duplicate_article(self, naver_url: str) -> bool:
        """
        중복 기사 체크

        Args:
            naver_url: 네이버 뉴스 URL

        Returns:
            중복 여부
        """
        try:
            result = self.client.client.table("articles").select("id").eq("naver_url", naver_url).execute()

            return len(result.data) > 0

        except Exception as e:
            logger.error(f"중복 체크 오류: {e}")
            return False

    def get_unprocessed_articles(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        미처리 기사 조회 (clickbait_score가 null인 기사)

        Args:
            limit: 조회 제한 수

        Returns:
            미처리 기사 리스트
        """
        try:
            result = (
                self.client.client.table("articles").select("*").is_("clickbait_score", "null").limit(limit).execute()
            )

            logger.info(f"미처리 기사 조회: {len(result.data)}개")
            return result.data

        except Exception as e:
            logger.error(f"미처리 기사 조회 오류: {e}")
            return []

    def update_article_score(self, article_id: str, clickbait_score: int, clickbait_explanation: str) -> bool:
        """
        기사 낚시 점수 업데이트

        Args:
            article_id: 기사 ID
            clickbait_score: 낚시 점수 (0-100)
            clickbait_explanation: 점수 설명

        Returns:
            업데이트 성공 여부
        """
        try:
            result = (
                self.client.client.table("articles")
                .update(
                    {
                        "clickbait_score": clickbait_score,
                        "clickbait_explanation": clickbait_explanation,
                        "updated_at": datetime.now().isoformat(),
                    }
                )
                .eq("id", article_id)
                .execute()
            )

            success = len(result.data) > 0
            if success:
                logger.info(f"기사 점수 업데이트 완료: {article_id}")

            return success

        except Exception as e:
            logger.error(f"기사 점수 업데이트 오류: {e}")
            return False

    def update_journalist_stats_manual(self, journalist_id: str) -> bool:
        """
        특정 기자의 통계 수동 업데이트

        Args:
            journalist_id: 기자 ID

        Returns:
            업데이트 성공 여부
        """
        try:
            # 해당 기자의 모든 기사 통계 계산
            result = (
                self.client.client.table("articles")
                .select("clickbait_score")
                .eq("journalist_id", journalist_id)
                .execute()
            )

            if not result.data:
                logger.warning(f"기자 ID {journalist_id}의 기사가 없습니다")
                return False

            # 통계 계산
            total_articles = len(result.data)
            scored_articles = [article for article in result.data if article["clickbait_score"] is not None]

            if scored_articles:
                scores = [article["clickbait_score"] for article in scored_articles]
                avg_score = sum(scores) / len(scores)
                max_score = max(scores)
            else:
                avg_score = 0.0
                max_score = 0

            # 기자 통계 업데이트
            update_result = (
                self.client.client.table("journalists")
                .update(
                    {
                        "article_count": total_articles,
                        "avg_clickbait_score": round(avg_score, 2),
                        "max_score": max_score,
                        "updated_at": datetime.now().isoformat(),
                    }
                )
                .eq("id", journalist_id)
                .execute()
            )

            success = len(update_result.data) > 0
            if success:
                logger.info(
                    f"기자 통계 업데이트 완료: {journalist_id} (기사수: {total_articles}, 평균: {avg_score:.2f}, 최고: {max_score})"
                )

            return success

        except Exception as e:
            logger.error(f"기자 통계 업데이트 오류: {e}")
            return False

    def update_all_journalist_stats(self) -> Dict[str, Any]:
        """
        모든 기자의 통계 일괄 업데이트

        Returns:
            업데이트 결과 딕셔너리
        """
        try:
            # 모든 기자 조회
            journalists_result = self.client.client.table("journalists").select("id, name, publisher").execute()

            if not journalists_result.data:
                logger.warning("업데이트할 기자가 없습니다")
                return {"success": 0, "failed": 0, "total": 0}

            success_count = 0
            failed_count = 0
            total_count = len(journalists_result.data)

            logger.info(f"총 {total_count}명의 기자 통계 업데이트 시작")

            for journalist in journalists_result.data:
                try:
                    if self.update_journalist_stats_manual(journalist["id"]):
                        success_count += 1
                    else:
                        failed_count += 1
                        logger.error(f"기자 통계 업데이트 실패: {journalist['name']} ({journalist['publisher']})")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"기자 통계 업데이트 예외: {journalist['name']} ({journalist['publisher']}) - {e}")

            result = {"success": success_count, "failed": failed_count, "total": total_count}

            logger.info(f"기자 통계 일괄 업데이트 완료: 성공 {success_count}/{total_count}, 실패 {failed_count}")
            return result

        except Exception as e:
            logger.error(f"기자 통계 일괄 업데이트 오류: {e}")
            return {"success": 0, "failed": 0, "total": 0, "error": str(e)}

    def get_journalist_stats_summary(self) -> Dict[str, Any]:
        """
        기자 통계 요약 정보 조회

        Returns:
            통계 요약 딕셔너리
        """
        try:
            # 기자 총 수
            journalists_result = self.client.client.table("journalists").select("id", count="exact").execute()
            total_journalists = journalists_result.count

            # 기사가 있는 기자 수
            active_journalists_result = (
                self.client.client.table("journalists").select("id", count="exact").gt("article_count", 0).execute()
            )
            active_journalists = active_journalists_result.count

            # 평균 점수가 있는 기자 수 (AI 분석 완료된 기사가 있는 기자)
            scored_journalists_result = (
                self.client.client.table("journalists")
                .select("id", count="exact")
                .gt("avg_clickbait_score", 0)
                .execute()
            )
            scored_journalists = scored_journalists_result.count

            # 전체 기사 수
            articles_result = self.client.client.table("articles").select("id", count="exact").execute()
            total_articles = articles_result.count

            # AI 분석 완료된 기사 수
            scored_articles_result = (
                self.client.client.table("articles")
                .select("id", count="exact")
                .not_.is_("clickbait_score", "null")
                .execute()
            )
            scored_articles = scored_articles_result.count

            return {
                "total_journalists": total_journalists,
                "active_journalists": active_journalists,
                "scored_journalists": scored_journalists,
                "total_articles": total_articles,
                "scored_articles": scored_articles,
                "pending_articles": total_articles - scored_articles,
            }

        except Exception as e:
            logger.error(f"통계 요약 조회 오류: {e}")
            return {}

    def fix_inconsistent_stats(self) -> Dict[str, Any]:
        """
        통계 불일치 감지 및 수정 (Supabase 호환 방식)

        Returns:
            수정 결과 딕셔너리
        """
        try:
            logger.info("통계 불일치 감지를 시작합니다...")

            # 모든 기자 정보 조회
            journalists_result = self.client.client.table("journalists").select("*").execute()
            if not journalists_result.data:
                logger.info("기자가 없습니다")
                return {"fixed": 0, "total_checked": 0}

            inconsistent_journalists = []
            total_checked = 0

            for journalist in journalists_result.data:
                total_checked += 1
                journalist_id = journalist["id"]
                stored_count = journalist.get("article_count", 0)
                stored_avg = journalist.get("avg_clickbait_score", 0.0)
                stored_max = journalist.get("max_score", 0)

                # 해당 기자의 실제 기사 통계 계산
                articles_result = (
                    self.client.client.table("articles")
                    .select("clickbait_score")
                    .eq("journalist_id", journalist_id)
                    .execute()
                )

                # 실제 값 계산
                actual_count = len(articles_result.data)
                scored_articles = [
                    article for article in articles_result.data if article["clickbait_score"] is not None
                ]

                if scored_articles:
                    scores = [article["clickbait_score"] for article in scored_articles]
                    actual_avg = sum(scores) / len(scores)
                    actual_max = max(scores)
                else:
                    actual_avg = 0.0
                    actual_max = 0

                # 불일치 감지 (소수점 2자리까지 비교)
                count_mismatch = stored_count != actual_count
                avg_mismatch = abs(stored_avg - actual_avg) > 0.01
                max_mismatch = stored_max != actual_max

                if count_mismatch or avg_mismatch or max_mismatch:
                    inconsistent_journalists.append(
                        {
                            "id": journalist_id,
                            "name": journalist["name"],
                            "publisher": journalist["publisher"],
                            "stored_count": stored_count,
                            "stored_avg": stored_avg,
                            "stored_max": stored_max,
                            "actual_count": actual_count,
                            "actual_avg": actual_avg,
                            "actual_max": actual_max,
                        }
                    )

                    logger.warning(
                        f"통계 불일치 발견: {journalist['name']} ({journalist['publisher']}) "
                        f"- 저장된 값: {stored_count}/{stored_avg:.2f}/{stored_max} "
                        f"- 실제 값: {actual_count}/{actual_avg:.2f}/{actual_max}"
                    )

            if not inconsistent_journalists:
                logger.info("통계 불일치가 발견되지 않았습니다")
                return {"fixed": 0, "total_checked": total_checked, "total_inconsistent": 0}

            # 불일치 수정
            fixed_count = 0
            for journalist in inconsistent_journalists:
                try:
                    if self.update_journalist_stats_manual(journalist["id"]):
                        fixed_count += 1
                        logger.info(f"수정 완료: {journalist['name']} ({journalist['publisher']})")
                    else:
                        logger.error(f"수정 실패: {journalist['name']} ({journalist['publisher']})")
                except Exception as e:
                    logger.error(f"수정 중 오류 [{journalist['name']}]: {e}")

            result = {
                "fixed": fixed_count,
                "total_inconsistent": len(inconsistent_journalists),
                "total_checked": total_checked,
            }

            logger.info(f"통계 불일치 수정 완료: {fixed_count}/{len(inconsistent_journalists)}건")
            return result

        except Exception as e:
            logger.error(f"통계 불일치 수정 오류: {e}")
            return {"fixed": 0, "total_checked": 0, "error": str(e)}
