"""OpenAI API 프롬프트 템플릿"""

CLICKBAIT_ANALYSIS_PROMPT = """
당신은 뉴스 기사의 낚시성 제목을 분석하는 전문가입니다.

다음 뉴스 기사의 제목과 본문을 분석하여 낚시성 정도를 0~10점으로 평가해 주세요.

평가 기준:
- 0-2점: 정확하고 객관적인 제목
- 3-4점: 약간의 자극적 표현이 있지만 본문과 일치
- 5-6점: 과장되었지만 완전히 거짓은 아님
- 7-8점: 상당히 과장되고 오해를 유발할 수 있음
- 9-10점: 매우 자극적이고 본문과 다르거나 거짓

고려사항:
1. 제목이 본문 내용을 정확히 반영하는가?
2. 과도한 감정적 표현이나 자극적 단어가 사용되었는가?
3. 독자를 오해하게 만들 수 있는 표현이 있는가?
4. "충격", "깜짝", "놀라운", "믿을 수 없는" 등의 과장 표현이 남용되었는가?

제목: {title}

본문: {content}

응답은 반드시 다음 JSON 형식으로만 답변해 주세요:
{{
    "clickbait_score": [0~10 사이의 숫자],
    "reasoning": "[평가 근거를 한국어로 2-3문장으로 설명]"
}}
"""

BATCH_SYSTEM_MESSAGE = """
당신은 뉴스 기사의 낚시성 제목을 분석하는 전문가입니다. 
제공된 뉴스 기사의 제목과 본문을 분석하여 낚시성 정도를 0~10점으로 평가하고, 
그 근거를 명확히 설명해 주세요.

응답은 반드시 JSON 형식으로만 제공해야 합니다.
"""


def create_analysis_message(title: str, content: str) -> str:
    """분석용 메시지 생성"""
    return CLICKBAIT_ANALYSIS_PROMPT.format(title=title, content=content)


def create_batch_request(news_id: str, title: str, content: str) -> dict:
    """배치 요청 아이템 생성"""
    return {
        "custom_id": f"news_{news_id}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": BATCH_SYSTEM_MESSAGE},
                {"role": "user", "content": create_analysis_message(title, content)},
            ],
            "max_tokens": 200,
            "temperature": 0.1,
        },
    }
