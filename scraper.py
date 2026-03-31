"""
scraper.py
DART 공모게시판(dsac005) HTML 파싱 모듈

필터 조건 (두 조건 모두 충족해야 추출):
  1. <tr> 내부에서 title="기타법인" 속성이 존재하는 행
     → <tr title="기타법인"> 또는 <td title="기타법인"> 또는 <img alt="기타법인">
  2. <tr> 내부 증권종류 td 텍스트가 "보통주" 인 행
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup, Tag

import config

logger = logging.getLogger(__name__)


# ── 내부 헬퍼 ────────────────────────────────────────────────

def _get_session() -> requests.Session:
    """공통 세션 생성 (헤더 고정)"""
    s = requests.Session()
    s.headers.update(config.DART_HEADERS)
    return s


def _has_corp_type(tr: Tag, corp_type: str) -> bool:
    """
    <tr> 행에 특정 법인 구분이 존재하는지 다중 방법으로 확인.

    확인 순서:
      1) <tr title="기타법인">
      2) <tr> 내 <td title="기타법인">
      3) <tr> 내 <img alt="기타법인">
      4) <tr> 내 첫 번째 <td> 텍스트가 "기" 인 경우 (약자 표시)
    """
    # 1) tr 자체 title
    if tr.get("title", "") == corp_type:
        return True

    # 2) td title 속성
    for td in tr.find_all("td"):
        if td.get("title", "") == corp_type:
            return True

    # 3) img alt 속성
    for img in tr.find_all("img"):
        if img.get("alt", "") == corp_type:
            return True

    # 4) 약자 텍스트 폴백 ("기타법인" → "기")
    abbr_map = {
        "기타법인":    "기",
        "유가증권시장": "유",
        "코스닥시장":  "코",
        "코넥스시장":  "넥",
    }
    abbr = abbr_map.get(corp_type)
    if abbr:
        tds = tr.find_all("td")
        if tds:
            # 두 번째 td(발행회사명 컬럼)의 텍스트 앞 글자 확인
            corp_td_text = tds[1].get_text(strip=True) if len(tds) > 1 else ""
            if corp_td_text.startswith(abbr + " ") or corp_td_text.startswith(abbr):
                return True

    return False


def _has_sec_type(tr: Tag, sec_type: str) -> bool:
    """
    <tr> 행의 증권종류 컬럼(td[3])이 sec_type 인지 확인.

    확인 순서:
      1) td[3] title 속성
      2) td[3] 텍스트 포함 여부
    """
    tds = tr.find_all("td")
    if len(tds) < 4:
        return False

    sec_td = tds[3]

    # 1) title 속성
    if sec_type in sec_td.get("title", ""):
        return True

    # 2) 텍스트 직접 포함
    sec_text = sec_td.get_text(strip=True)
    if sec_type in sec_text:
        return True

    return False


def _parse_row(tr: Tag) -> dict | None:
    """
    단일 <tr> 행을 파싱하여 dict 반환.
    파싱 실패 시 None 반환.

    컬럼 순서: 번호 | 발행회사명 | 보고서명 | 증권종류 | 청약일 | 접수일자
    """
    tds = tr.find_all("td")
    if len(tds) < 6:
        return None

    try:
        # ── 발행회사명 ──
        corp_td = tds[1]
        # 회사명 링크 텍스트만 추출 (약자 접두사 "기 ", "코 " 제거)
        corp_name_raw = corp_td.get_text(strip=True)
        # "기 코스모로보틱스IR" → "코스모로보틱스" 형태 처리
        # 반드시 "약자 + 공백" 패턴일 때만 약자 제거 (예: "기 채비" → "채비")
        # 공백 없으면 종목명 일부로 간주 (예: "코스모로보틱스" 는 그대로 유지)
        corp_name = re.sub(r"^[유코넥기] ", "", corp_name_raw)
        corp_name = re.sub(r"\s*IR\s*$", "", corp_name).strip()
        # a 태그 텍스트가 더 정확
        a_tag = corp_td.find("a")
        if a_tag:
            corp_name = a_tag.get_text(strip=True)

        # ── 보고서명 & 접수번호 ──
        report_td = tds[2]
        report_a  = report_td.find("a")
        report_name = report_a.get_text(strip=True) if report_a else report_td.get_text(strip=True)
        rcp_no = ""
        if report_a and report_a.get("href"):
            m = re.search(r"rcpNo=(\d+)", report_a["href"])
            if m:
                rcp_no = m.group(1)

        # ── 증권종류 ──
        sec_type = tds[3].get_text(strip=True)
        # 복수 종류인 경우 첫 번째만 (예: "보통주, 우선주" → "보통주")
        sec_type = sec_type.split(",")[0].strip()

        # ── 청약일 파싱: "2026.04.08~2026.04.09" ──
        date_raw  = tds[4].get_text(strip=True)
        sub_start = ""
        sub_end   = ""
        if "~" in date_raw:
            parts     = date_raw.split("~")
            sub_start = parts[0].strip().replace(".", "-")
            sub_end   = parts[1].strip().replace(".", "-")
        else:
            sub_end = date_raw.replace(".", "-")

        # ── 접수일자 ──
        rcp_date = tds[5].get_text(strip=True).replace(".", "-")

        return {
            "종목명":     corp_name,
            "접수번호":   rcp_no,
            "보고서명":   report_name,
            "증권종류":   sec_type,
            "청약시작일": sub_start,
            "청약종료일": sub_end,
            "접수일자":   rcp_date,
        }

    except Exception as e:
        logger.warning(f"행 파싱 실패: {e} | HTML: {str(tr)[:200]}")
        return None


def _fetch_page(page_no: int, session: requests.Session) -> BeautifulSoup | None:
    """단일 페이지 HTML 요청 → BeautifulSoup 반환"""
    params = {**config.DART_DEFAULT_PARAMS, "pageNo": str(page_no)}
    try:
        resp = session.get(
            config.DART_SEARCH_URL,
            params=params,
            timeout=config.REQUEST_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        logger.error(f"페이지 {page_no} 요청 실패: {e}")
        return None


def _get_total_pages(soup: BeautifulSoup) -> int:
    """
    페이지 정보 텍스트 "[1/3] [총 17건]" 에서 총 페이지 수 추출.
    추출 실패 시 1 반환.
    """
    text = soup.get_text()
    m = re.search(r"\[(\d+)/(\d+)\]", text)
    if m:
        return int(m.group(2))
    return 1


# ── 공개 API ─────────────────────────────────────────────────

def fetch_ipo_board() -> list[dict]:
    """
    DART 공모게시판 전체 페이지를 순회하며
    [기타법인 + 보통주] 조건에 맞는 항목만 파싱하여 반환.

    Returns
    -------
    list[dict]  각 항목: {종목명, 접수번호, 보고서명, 증권종류,
                          청약시작일, 청약종료일, 접수일자}
    """
    session   = _get_session()
    results   = []
    page_no   = 1

    logger.info("DART 공모게시판 스크래핑 시작")

    while True:
        logger.info(f"  → 페이지 {page_no} 요청 중...")
        soup = _fetch_page(page_no, session)

        if soup is None:
            logger.error(f"페이지 {page_no} 파싱 불가 → 중단")
            break

        # 총 페이지 수 파악 (첫 페이지에서만)
        if page_no == 1:
            total_pages = _get_total_pages(soup)
            logger.info(f"  총 {total_pages} 페이지")

        # tbody → tr 순회
        tbody = soup.find("tbody")
        if not tbody:
            # tbody 없으면 table > tr 전체에서 헤더 제외
            table = soup.find("table")
            all_rows = table.find_all("tr")[1:] if table else []
        else:
            all_rows = tbody.find_all("tr")

        page_count = 0
        for tr in all_rows:
            # ── 핵심 필터: 기타법인 AND 보통주 ──
            is_other_corp = _has_corp_type(tr, config.FILTER_CORP_TYPE)
            is_ordinary   = _has_sec_type(tr, config.FILTER_SEC_TYPE)

            if not (is_other_corp and is_ordinary):
                logger.debug(
                    f"  [SKIP] 기타법인={is_other_corp}, 보통주={is_ordinary} "
                    f"| {tr.get_text(strip=True)[:60]}"
                )
                continue

            row = _parse_row(tr)
            if row:
                results.append(row)
                page_count += 1
                logger.info(
                    f"  [추출] {row['종목명']} | 청약종료={row['청약종료일']} "
                    f"| 접수번호={row['접수번호']}"
                )

        logger.info(f"  페이지 {page_no}: {page_count}건 추출")

        if page_no >= total_pages:
            break

        page_no += 1
        time.sleep(config.REQUEST_DELAY_SEC)

    logger.info(f"DART 스크래핑 완료: 총 {len(results)}건 (기타법인+보통주)")
    return results
