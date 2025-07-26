"""
프롬프트 생성 모듈
"""

import json
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
        prompt = f"""다음 단계를 따라 분석을 진행해주세요:

1. 뉴스 제목 분석:
   - 제목에 사용된 단어와 표현을 면밀히 분석하세요.
   - 제목에 모호하거나 오해의 소지가 있는 표현이 있는지 살펴보세요.
   - 제목이 독자의 감정을 과도하게 자극하는지 검토하세요.
   - 제목에 과장되거나 왜곡된 표현이 있는지 확인하세요.
   - "충격", "경악", "발칵" 등 감정적 반응을 유도하는 과장된 표현이 있는지 확인하세요.
   - "이것", "저것", "그것" 등 모호한 대명사를 사용해 호기심을 유발하는지 확인하세요.

2. 본문 내용 분석:
   - 제공된 본문 내용을 주의 깊게 읽으세요.
   - 핵심 정보와 주요 메시지를 파악하세요.
   - 본문 내용은 최대 700자까지만 제공됩니다.

3. 클릭베이트 의도 분석:
   - 제목과 본문 내용 사이에 의도적인 불일치가 있는지 확인하세요.
   - 제목이 내용을 과장하거나 왜곡하여 독자를 오도하고 있는지 살펴보세요.
   - 제목이 클릭을 유도하기 위해 의도적으로 본문의 정보를 왜곡하거나 숨기고 있는지 판단하세요.
   - 제목이 기사의 실제 본문 내용보다 더 자극적이거나 중요한 것처럼 보이게 하는지 분석하세요.
   - 기사의 소재 자체가 자극적인 경우 클릭베이트로 오인될 수 있습니다. 이때는 제목과 내용의 일치 여부를 주의깊게 확인하세요.

4. 클릭베이트 정도 평가:
   - 앞선 분석을 바탕으로 기사 제목의 클릭베이트 정도를 0-100 사이의 정수로 평가하세요.
   - 평가 기준:
     * 0-20: 전혀 클릭베이트 의도가 없음. 제목이 본문 내용을 정확하게 반영함.
     * 21-40: 약간의 클릭베이트 의도가 있음. 제목이 본문 내용을 약간 과장함.
     * 41-60: 중간 정도의 클릭베이트 의도가 있음. 제목이 본문 내용을 상당히 과장함.
     * 61-80: 강한 클릭베이트 의도가 있음. 제목이 본문 내용을 크게 왜곡함.
     * 81-100: 매우 강한 클릭베이트 의도가 있음. 제목이 본문 내용과 거의 무관하거나 완전히 왜곡함.
   - 평가 이유를 구체적으로 설명하세요.

5. 최종 검토:
   - 평가가 객관적이고 공정한지 다시 한 번 확인하세요.
   - 본문 내용은 일부만 제공되었다는 한계를 고려하여 평가에 반영했는지 확인하세요.
   - 제목의 낚시성 정도를 정확하게 반영하는 점수를 부여했는지 확인하세요.

대표적인 클릭베이트 기사 제목의 예시는 다음과 같습니다:
- 영화, 드라마 등 작품 속 캐릭터의 상황을 배우의 실제 상황으로 오인하게 하는 제목
- 대상을 직접 언급하지 않고 간접적인 방식으로 언급해 독자의 궁금증을 불러일으키는 제목
- 앞말/중간말/뒷말을 잘라내서 다른 의미로 받아들일 여지로 만드는 제목
- "충격", "경악", "발칵" 등 감정적 반응을 유도하는 과장된 표현을 사용하는 제목
- "이것", "저것", "그것" 등 모호한 대명사를 사용해 호기심을 유발하는 제목

**뉴스 제목:** {title}

**뉴스 내용:** {content[:700]}{"..." if len(content) > 700 else ""}"""

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
                            "content": "당신은 뉴스의 제목과 내용을 분석하여 클릭베이트 여부를 판단하는 전문가입니다. 주어진 뉴스 제목과 본문 내용을 분석하여 해당 기사의 제목이 클릭베이트인지 여부를 판단하고, 그 정도를 0에서 100 사이의 정수로 평가해주세요. 객관적이고 일관된 기준으로 평가해주세요.",
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
