"""
프롬프트 생성 모듈
"""

import json
import logging
from typing import List, Dict, Any

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# 클릭베이트 평가 JSON Schema
CLICKBAIT_EVALUATION_SCHEMA = {
    "type": "object",
    "properties": {
        "clickbait_score": {"type": "integer", "minimum": 0, "maximum": 100, "description": "클릭베이트 점수 (0-100)"},
        "clickbait_explanation": {"type": "string", "description": "클릭베이트 점수 판단 근거"},
    },
    "required": ["clickbait_score", "clickbait_explanation"],
    "additionalProperties": False,
}


class PromptGenerator:
    """OpenAI API 프롬프트 생성기"""

    def __init__(self):
        """초기화"""
        pass

    def generate_clickbait_prompt(self, title: str, content: str) -> str:
        """
        클릭베이트 평가를 위한 프롬프트 생성

        Args:
            title: 뉴스 제목
            content: 뉴스 내용

        Returns:
            클릭베이트 평가 프롬프트
        """
        prompt = f"""다음 뉴스 기사를 분석하여 클릭베이트 정도를 0-100점으로 평가해주세요.

**클릭베이트 평가 기준:**
- 0-20점: 객관적이고 정확한 제목
- 21-40점: 약간의 흥미 유발 요소 있음
- 41-60점: 호기심을 자극하는 표현 사용
- 61-80점: 과장되거나 선정적인 표현
- 81-100점: 극도로 선정적이거나 미끼성 제목

**평가 요소:**
1. 제목의 과장성
2. 감정적 자극 정도
3. 내용과 제목의 일치성
4. 호기심 갭(Curiosity Gap) 활용도
5. 선정적 표현 사용

**뉴스 제목:** {title}

**뉴스 내용:** {content[:1000]}{"..." if len(content) > 1000 else ""}

위 기준에 따라 클릭베이트 점수와 판단 근거를 제공해주세요."""

        return prompt

    def generate_batch_requests(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Article 목록을 OpenAI Batch API 요청 형식으로 변환

        Args:
            articles: Article 딕셔너리 목록

        Returns:
            OpenAI Batch API 요청 목록
        """
        batch_requests = []

        for article in articles:
            prompt = self.generate_clickbait_prompt(article["title"], article["content"])

            request = {
                "custom_id": f"article_{article['id']}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {
                            "role": "system",
                            "content": "당신은 뉴스 기사의 클릭베이트 정도를 평가하는 전문가입니다. 객관적이고 일관된 기준으로 평가해주세요.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "clickbait_evaluation",
                            "strict": True,
                            "schema": CLICKBAIT_EVALUATION_SCHEMA,
                        },
                    },
                },
            }

            batch_requests.append(request)

        logger.info(f"Generated {len(batch_requests)} batch requests")
        return batch_requests

    def validate_clickbait_response(self, response_content: str) -> Dict[str, Any]:
        """
        클릭베이트 응답 검증

        Args:
            response_content: OpenAI 응답 내용

        Returns:
            검증된 클릭베이트 데이터 또는 None
        """
        try:
            data = json.loads(response_content)

            # 필수 필드 확인
            if "clickbait_score" not in data or "clickbait_explanation" not in data:
                logger.warning(f"Missing required fields in response: {data}")
                return None

            # 점수 범위 검증
            score = data["clickbait_score"]
            if not isinstance(score, int) or score < 0 or score > 100:
                logger.warning(f"Invalid clickbait_score: {score}")
                return None

            # 설명 검증
            explanation = data["clickbait_explanation"]
            if not isinstance(explanation, str) or len(explanation.strip()) == 0:
                logger.warning(f"Invalid clickbait_explanation: {explanation}")
                return None

            return {
                "clickbait_score": score,
                "clickbait_explanation": explanation.strip(),
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}, content: {response_content}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error validating response: {e}")
            return None
