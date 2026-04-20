"""
gemini_fetcher.py
Gemini Flash 2.0 + Google Search Grounding을 사용하여
기관 수요예측 경쟁률을 실시간 검색·파싱하는 모듈

동작 방식:
  - Gemini 2.0 Flash 의 googleSearch 도구(grounding)를 활성화
  - "종목명 기관 수요예측 경쟁률" 쿼리로 Google 검색 결과를 기반으로 답변 생성
  - 응답 텍스트에서 경쟁률 숫자를 정규식으로 추출
  - 상장일자가 확정된 종목에 대해서만 호출 (수요예측 완료 보장)

호출 흐름:
  notion_handler.query_pages_with_listing_date()
      → 상장일자 있음 & 경쟁률 없음인 페이지 목록
  gemini_fetcher.fetch_competition_rate(종목명)
      → Gemini API 호출 → float 반환 또는 None
  notion_handler.update_competition_rate(page_id, rate)
      → Notion 페이지 경쟁률 필드 업데이트
"""

import re
import time
import logging
import requests

import config

logger = logging.getLogger(__name__)


# ── 프롬프트 ─────────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
다음 한국 IPO 종목의 기관 수요예측 경쟁률을 알려주세요.

종목명: {name}

요청사항:
- 기관 수요예측 경쟁률(대 1 기준)을 숫자로만 답해주세요.
- 예시 형식: 1234.56
- 경쟁률 정보를 찾을 수 없으면 "없음"이라고만 답해주세요.
- 설명, 단위, 부가 문구 없이 숫자 또는 "없음"만 출력하세요.
"""


# ── 내부 헬퍼 ────────────────────────────────────────────────

def _call_gemini(prompt: str) -> str | None:
    """
    Gemini Flash 2.0 API 호출 (Google Search Grounding 활성화).

    Returns
    -------
    str | None  모델 응답 텍스트 또는 None(오류 시)
    """
    if not config.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY 환경 변수가 설정되지 않았습니다.")
        return None

    url = (
        f"{config.GEMINI_BASE_URL}/{config.GEMINI_MODEL}"
        f":generateContent?key={config.GEMINI_API_KEY}"
    )

    body = {
        "contents": [
            {
                "parts": [{"text": prompt}]
            }
        ],
        "tools": [
            {
                # Google Search Grounding — 실시간 검색 결과 기반 답변
                "googleSearch": {}
            }
        ],
        "generationConfig": {
            "temperature":    0.0,   # 결정론적 답변 (숫자 추출이 목적)
            "maxOutputTokens": 64,   # 숫자 1개만 필요하므로 짧게
        },
    }

    try:
        resp = requests.post(url, json=body, timeout=config.REQUEST_TIMEOUT_SEC)

        # 429 Rate Limit 처리
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"Gemini Rate Limit → {retry_after}초 대기 후 재시도")
            time.sleep(retry_after)
            resp = requests.post(url, json=body, timeout=config.REQUEST_TIMEOUT_SEC)

        resp.raise_for_status()
        data = resp.json()

        # 응답 텍스트 추출
        candidates = data.get("candidates", [])
        if not candidates:
            logger.warning("Gemini 응답에 candidates 없음")
            return None

        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            logger.warning("Gemini 응답에 parts 없음")
            return None

        return parts[0].get("text", "").strip()

    except requests.HTTPError as e:
        logger.error(f"Gemini HTTP 오류: {e.response.status_code} | {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Gemini API 호출 실패: {e}")
        return None


def _parse_rate(text: str) -> float | None:
    """
    Gemini 응답 텍스트에서 경쟁률 숫자 추출.

    허용 형식:
      "1234.56"  "1,234.56"  "1234"  "약 1234.5"  "1234.5 : 1"
    거부:
      "없음"  "N/A"  ""  (빈 응답)
    """
    if not text:
        return None

    # "없음" 류 응답 처리
    if re.search(r"없음|N/A|미확인|정보\s*없|찾을\s*수\s*없", text, re.IGNORECASE):
        return None

    # 숫자 추출 (쉼표 포함 형식 허용)
    m = re.search(r"(\d[\d,]*\.?\d*)", text)
    if not m:
        return None

    try:
        rate = float(m.group(1).replace(",", ""))
        # 합리적 범위 검증: 1 초과 ~ 9999 이하
        if 1.0 < rate <= 9999.0:
            return rate
        logger.warning(f"경쟁률 범위 이상: {rate} (원문: '{text}')")
        return None
    except ValueError:
        return None


# ── 공개 API ─────────────────────────────────────────────────

def fetch_competition_rate(name: str) -> float | None:
    """
    특정 종목의 기관 수요예측 경쟁률을 Gemini Flash 2.0으로 조회.

    Parameters
    ----------
    name : str
        Notion DB에 저장된 종목명 (예: "코스모로보틱스")

    Returns
    -------
    float | None
        경쟁률 (예: 1234.5) 또는 조회 불가 시 None
    """
    if not name:
        return None

    prompt = _PROMPT_TEMPLATE.format(name=name)
    logger.info(f"[Gemini] '{name}' 기관 수요예측 경쟁률 조회 중...")

    raw = _call_gemini(prompt)
    if raw is None:
        logger.warning(f"[Gemini] '{name}' API 응답 없음")
        return None

    logger.debug(f"[Gemini] '{name}' 원시 응답: '{raw}'")

    rate = _parse_rate(raw)
    if rate is not None:
        logger.info(f"[Gemini] '{name}' 경쟁률 확인: {rate}:1")
    else:
        logger.info(f"[Gemini] '{name}' 경쟁률 미확인 (수요예측 전 또는 정보 없음)")

    # Rate Limit 대기
    time.sleep(config.GEMINI_DELAY_SEC)
    return rate
