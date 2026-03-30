"""
parser.py - 데이터 정제 모듈

날짜 파싱, 금액 정규화, 보통주 필터링, Notion 페이로드 변환
"""

import re
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# 보통주 키워드 (증권종류 컬럼 필터)
COMMON_STOCK_KEYWORDS = ["보통주", "기명식보통주", "보통"]


def clean_and_filter(
    raw_items: list[dict],
    증권종류: str = "보통주",
) -> list[dict]:
    """
    DART 스크래퍼 원시 데이터를 정제 후 보통주만 반환

    Args:
        raw_items: scraper.py가 반환한 원시 딕셔너리 리스트
        증권종류: 필터 키워드 (기본: "보통주")

    Returns:
        정제된 IPO 정보 리스트
    """
    cleaned = []
    for item in raw_items:
        secu = item.get("증권종류", "")
        if not _is_common_stock(secu):
            logger.debug(f"[스킵] {item.get('종목명')} | 증권종류={secu}")
            continue

        parsed = _parse_item(item)
        if parsed:
            cleaned.append(parsed)

    logger.info(f"필터링 결과: 원본 {len(raw_items)}건 → 보통주 {len(cleaned)}건")
    return cleaned


def _is_common_stock(secu_text: str) -> bool:
    """증권종류 텍스트가 보통주 해당 여부"""
    for kw in COMMON_STOCK_KEYWORDS:
        if kw in secu_text:
            return True
    return False


def _parse_item(raw: dict) -> Optional[dict]:
    """
    단일 항목 정제

    Returns:
        정제된 딕셔너리 또는 None (파싱 실패 시)
    """
    try:
        종목명   = raw.get("종목명", "").strip()
        청약일   = raw.get("청약일", "")
        접수일자 = raw.get("접수일자", "")
        접수번호 = raw.get("접수번호", "")

        if not 종목명:
            return None

        # 청약 시작/종료일 파싱: "2026.04.15~2026.04.16" or "2026-04-15~2026-04-16"
        sub_start, sub_end = _parse_subscription_dates(청약일)

        # 접수일자 파싱
        rcept_date = _parse_date_str(접수일자)

        result = {
            "종목명":     종목명,
            "청약시작일": sub_start,    # YYYY-MM-DD or None
            "청약종료일": sub_end,      # YYYY-MM-DD or None (Notion에 이 값만 저장)
            "청약기한":   sub_end,      # Notion 필드명과 맞춤
            "접수일자":   rcept_date,
            "접수번호":   접수번호,
            # 아래는 상세 조회 후 보강될 필드
            "공모가":     raw.get("공모가_raw"),    # int or None
            "주관사":     raw.get("주관사", ""),
            "상장일자":   raw.get("상장예정일_raw"),  # YYYY-MM-DD or None
            "경쟁률":     None,  # 38커뮤에서 보강
        }
        return result

    except Exception as e:
        logger.warning(f"항목 파싱 오류 ({raw.get('종목명')}): {e}")
        return None


def _parse_subscription_dates(date_str: str) -> tuple[Optional[str], Optional[str]]:
    """
    청약일 문자열에서 시작/종료일 파싱

    입력 예시:
      "2026.04.15 ~ 2026.04.16"
      "2026-04-15~2026-04-16"
      "2026.04.15"
      "04.15~04.16" (연도 없는 경우)
    """
    if not date_str or not date_str.strip():
        return None, None

    # 구분자로 분리
    parts = re.split(r"~|∼|－|—", date_str)
    start_raw = parts[0].strip() if len(parts) >= 1 else ""
    end_raw   = parts[1].strip() if len(parts) >= 2 else start_raw

    start = _parse_date_str(start_raw)
    end   = _parse_date_str(end_raw)

    # 연도 없는 경우 현재 연도 보완
    if start is None:
        start = _parse_date_str_no_year(start_raw)
    if end is None:
        end = _parse_date_str_no_year(end_raw)

    return start, end


def _parse_date_str(s: str) -> Optional[str]:
    """
    다양한 날짜 포맷을 YYYY-MM-DD 로 변환

    지원 포맷: YYYYMMDD, YYYY.MM.DD, YYYY-MM-DD, YYYY/MM/DD
    """
    if not s:
        return None
    s = s.strip()
    patterns = [
        (r"(\d{4})\.(\d{1,2})\.(\d{1,2})", "{0}-{1}-{2}"),
        (r"(\d{4})-(\d{1,2})-(\d{1,2})",   "{0}-{1}-{2}"),
        (r"(\d{4})/(\d{1,2})/(\d{1,2})",   "{0}-{1}-{2}"),
        (r"(\d{4})(\d{2})(\d{2})",          "{0}-{1}-{2}"),
    ]
    for pattern, fmt in patterns:
        m = re.search(pattern, s)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            try:
                dt = datetime(int(y), int(mo), int(d))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _parse_date_str_no_year(s: str) -> Optional[str]:
    """연도 없는 MM.DD 형태 처리 - 현재 연도 부여"""
    m = re.search(r"(\d{1,2})\.(\d{1,2})", s)
    if m:
        year = datetime.today().year
        mo, d = m.group(1).zfill(2), m.group(2).zfill(2)
        try:
            dt = datetime(year, int(mo), int(d))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def normalize_amount(value: str) -> Optional[int]:
    """
    금액 문자열 → 정수 변환

    예: "25,000원" → 25000
        "2만5천원" → 변환 불가 → None
        "25000" → 25000
    """
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    if digits:
        try:
            return int(digits)
        except ValueError:
            pass
    return None


def normalize_rate(value: str) -> Optional[float]:
    """
    경쟁률 문자열 → float 변환

    예: "1,523.50 : 1" → 1523.5
        "1523.5" → 1523.5
        "1,523" → 1523.0
    """
    if not value:
        return None
    # 숫자와 소수점만 추출
    cleaned = re.sub(r"[^\d.]", "", str(value).split(":")[0].strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def merge_detail(base: dict, detail: dict) -> dict:
    """
    기본 정보(base)에 상세 정보(detail)를 병합

    detail의 값이 None이 아닌 경우에만 덮어씀
    """
    merged = dict(base)
    for key, val in detail.items():
        if val is not None and val != "":
            merged[key] = val
    return merged
