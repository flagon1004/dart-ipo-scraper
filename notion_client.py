"""
parser.py
스크래핑 원시 데이터 정제 모듈

- 날짜 포맷 정규화 (YYYY-MM-DD)
- 공모가 숫자 변환
- 종목명 공백/특수문자 정리
- 중복 제거 (접수번호 + 종목명 통합 캐시)
  규칙:
    1) 동일 접수번호 → 즉시 skip
    2) 동일 종목명   → 접수번호 유무에 관계없이 skip
       (접수번호가 있는 건이 먼저 등록됐어도, 이후 같은 종목명이 접수번호 없이 오면 skip)
"""

import re
import logging

logger = logging.getLogger(__name__)


# ── 내부 헬퍼 ────────────────────────────────────────────────

def _normalize_date(raw: str) -> str | None:
    """
    날짜 문자열을 ISO 포맷(YYYY-MM-DD)으로 정규화.
    이미 '-' 구분자인 경우 그대로 반환, 점('.') 구분자 변환.
    변환 불가 시 None 반환.
    """
    if not raw:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    converted = raw.replace(".", "-")
    if re.match(r"^\d{4}-\d{2}-\d{2}$", converted):
        return converted
    logger.warning(f"날짜 변환 불가: '{raw}'")
    return None


def _normalize_amount(raw: str) -> int | None:
    """
    금액 문자열을 정수로 변환.
    예) "25,000원" → 25000
    """
    if not raw:
        return None
    cleaned = re.sub(r"[^\d]", "", raw)
    if cleaned:
        return int(cleaned)
    logger.warning(f"금액 변환 불가: '{raw}'")
    return None


def _normalize_name(raw: str) -> str:
    """종목명 정제: 앞뒤 공백 제거, 연속 공백 단일화"""
    return re.sub(r"\s+", " ", raw.strip())


# ── 공개 API ─────────────────────────────────────────────────

def clean_and_filter(raw_list: list[dict]) -> list[dict]:
    """
    scraper.fetch_ipo_board() 결과를 받아 정제된 리스트 반환.

    중복 제거 규칙 (두 캐시를 동시에 운용):
      seen_rcp  : 처리된 접수번호 집합
      seen_name : 처리된 종목명 집합  ← 접수번호 유무에 무관하게 항상 등록

      흐름:
        ① rcp_no가 있고 seen_rcp에 존재 → skip (접수번호 완전 중복)
        ② name이 seen_name에 존재       → skip (동일 종목명 중복)
        ③ 통과 → seen_rcp(rcp_no 있을 때), seen_name 모두 등록

    이 방식으로 아래 케이스를 모두 방지:
      - 같은 접수번호가 두 번 등장
      - 같은 종목이 접수번호 있는 건 + 없는 건으로 중복 등장
      - 같은 종목이 접수번호 없이 두 번 등장

    Parameters
    ----------
    raw_list : list[dict]   scraper.py 에서 반환된 원시 데이터

    Returns
    -------
    list[dict]   정제·중복제거된 데이터
    """
    cleaned: list[dict] = []
    seen_rcp: set[str]  = set()   # 처리된 접수번호
    seen_name: set[str] = set()   # 처리된 종목명 (항상 등록)

    for item in raw_list:
        try:
            name = _normalize_name(item.get("종목명", ""))
            if not name:
                logger.warning(f"종목명 없음, 건너뜀: {item}")
                continue

            rcp_no    = item.get("접수번호", "").strip()
            sub_end   = _normalize_date(item.get("청약종료일", ""))
            sub_start = _normalize_date(item.get("청약시작일", ""))
            rcp_date  = _normalize_date(item.get("접수일자", ""))

            # ── 중복 체크 1: 접수번호 ─────────────────────────
            if rcp_no and rcp_no in seen_rcp:
                logger.debug(f"[중복-접수번호] {name} ({rcp_no}) 건너뜀")
                continue

            # ── 중복 체크 2: 종목명 (접수번호 유무 무관) ───────
            if name in seen_name:
                logger.debug(f"[중복-종목명] {name} 건너뜀")
                continue

            # ── 통과: 양쪽 캐시 모두 등록 ─────────────────────
            if rcp_no:
                seen_rcp.add(rcp_no)
            seen_name.add(name)

            cleaned.append({
                "종목명":     name,
                "접수번호":   rcp_no,
                "보고서명":   item.get("보고서명", ""),
                "증권종류":   item.get("증권종류", "보통주"),
                "청약시작일": sub_start,
                "청약종료일": sub_end,
                "접수일자":   rcp_date,
                # 경쟁률·상장일자·공모가·주관사는 후속 단계에서 채움
                "경쟁률":   None,
                "상장일자": None,
                "공모가":   None,
                "주관사":   "",
            })

        except Exception as e:
            logger.error(f"정제 중 오류: {e} | 항목: {item}")

    logger.info(f"parser: {len(raw_list)}건 입력 → {len(cleaned)}건 정제 완료")
    return cleaned
