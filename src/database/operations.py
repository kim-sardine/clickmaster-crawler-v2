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
            # 기존 기자 조회
            existing = (
                self.client.client.table("journalists")
                .select("*")
                .eq("name", name)
                .eq("publisher", publisher)
                .execute()
            )

            if existing.data:
                logger.info(f"기존 기자 조회: {name} ({publisher})")
                return existing.data[0]

            # 새 기자 생성
            journalist = Journalist(name=name, publisher=publisher, naver_uuid=naver_uuid)

            result = self.client.client.table("journalists").insert(journalist.to_dict()).execute()

            if result.data:
                logger.info(f"새 기자 생성: {name} ({publisher})")
                return result.data[0]
            else:
                raise Exception("기자 생성 실패")

        except Exception as e:
            logger.error(f"기자 조회/생성 오류: {e}")
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
        기사 배치 삽입

        Args:
            articles: 기사 리스트

        Returns:
            삽입된 기사 정보 리스트
        """
        inserted_articles = []

        for article in articles:
            try:
                result = self.insert_article(article)
                inserted_articles.append(result)
            except Exception as e:
                logger.error(f"기사 삽입 실패: {article.title[:50]}... - {e}")
                continue

        logger.info(f"배치 삽입 완료: {len(inserted_articles)}/{len(articles)}")
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

    def update_article_score(self, article_id: str, clickbait_score: int, score_explanation: str) -> bool:
        """
        기사 낚시 점수 업데이트

        Args:
            article_id: 기사 ID
            clickbait_score: 낚시 점수 (0-100)
            score_explanation: 점수 설명

        Returns:
            업데이트 성공 여부
        """
        try:
            result = (
                self.client.client.table("articles")
                .update(
                    {
                        "clickbait_score": clickbait_score,
                        "score_explanation": score_explanation,
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
