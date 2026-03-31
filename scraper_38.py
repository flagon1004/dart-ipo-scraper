"""
scraper_38.py
38커뮤니케이션(38.co.kr)에서 기관 수요예측 경쟁률 수집 모듈

수집 흐름:
  ① 공모주 목록 페이지에서 종목별 상세 링크 추출
  ② 상세 페이지에서 기관 수요예측 경쟁률 파싱
  ③ 종목명 유사도 매칭 (정확 일치 → 포함 관계 순)
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

_SESSION: requests.Session | None = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(config.IPO38_HEADERS)
    return _SESSION


def _fetch(url: str) -> BeautifulSoup | None:
    """URL 요청 → BeautifulSoup 반환. 실패 시 None."""
    try:
        resp = _get_session().get(url, timeout=config.REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        resp.encoding = "euc-kr"   # 38커뮤니케이션은 EUC-KR 인코딩
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        logger.error(f"38커뮤 요청 실패: {url} | {e}")
        return None


def _normalize(name: str) -> str:
    """공백·특수문자 제거 후 소문자 변환 (비교용)"""
    return re.sub(r"[\s\W]", "", name).lower()


def _extract_rate(soup: BeautifulSoup) -> float | None:
    """
    상세 페이지 HTML에서 기관 수요예측 경쟁률 추출.

    38커뮤 상세 페이지 패턴:
      "수요예측경쟁률 : 1234.56 : 1" 또는 테이블 셀 내 숫자
    """
    text = soup.get_text()

    # 패턴 1: "경쟁률" 키워드 근처 숫자
    m = re.search(r"경쟁률[^\d]*(\d[\d,]*\.?\d*)\s*[:대]?\s*1", text)
    if m:
        rate_str = m.group(1).replace(",", "")
        try:
            return float(rate_str)
        except ValueError:
            pass

    # 패턴 2: "수요예측" 키워드 근처 숫자
    m = re.search(r"수요예측[^\d]{0,20}(\d[\d,]*\.?\d*)", text)
    if m:
        rate_str = m.group(1).replace(",", "")
        try:
            rate = float(rate_str)
            if 1 < rate < 99999:   # 합리적 범위 내
                return rate
        except ValueError:
            pass

    return None


def _get_ipo_links() -> list[dict]:
    """
    38커뮤 공모주 목록 페이지에서 종목명·상세URL 추출.

    Returns: [{"name": str, "url": str}, ...]
    """
    soup = _fetch(config.IPO38_IPO_URL)
    if not soup:
        return []

    items = []
    # 38커뮤 공모주 목록 테이블 파싱
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # 공모주 상세 링크 패턴: /html/fund/ipo_detail.htm?code=...
        if "ipo_detail" in href or "ipoInfo" in href:
            name = a.get_text(strip=True)
            if name and len(name) > 1:
                full_url = href if href.startswith("http") else config.IPO38_BASE_URL + "/" + href.lstrip("/")
                items.append({"name": name, "url": full_url})

    logger.info(f"38커뮤 공모주 목록: {len(items)}건")
    return items


def fetch_demand_forecast_rate(target_name: str) -> float | None:
    """
    특정 종목의 기관 수요예측 경쟁률 반환.

    Parameters
    ----------
    target_name : str
        Notion DB에 저장된 종목명

    Returns
    -------
    float | None  경쟁률 (예: 1234.5) 또는 미확인 시 None
    """
    links = _get_ipo_links()
    if not links:
        logger.warning("38커뮤 공모주 목록 조회 실패")
        return None

    norm_target = _normalize(target_name)

    # 종목명 매칭 (정확 일치 우선, 포함 관계 폴백)
    matched_url = None
    for item in links:
        norm_item = _normalize(item["name"])
        if norm_item == norm_target:
            matched_url = item["url"]
            break

    if not matched_url:
        for item in links:
            norm_item = _normalize(item["name"])
            if norm_target in norm_item or norm_item in norm_target:
                matched_url = item["url"]
                logger.debug(f"38커뮤 부분 매칭: '{target_name}' ≈ '{item['name']}'")
                break

    if not matched_url:
        logger.info(f"38커뮤 종목 미발견: {target_name}")
        return None

    time.sleep(config.REQUEST_DELAY_SEC)
    detail_soup = _fetch(matched_url)
    if not detail_soup:
        return None

    rate = _extract_rate(detail_soup)
    if rate:
        logger.info(f"38커뮤 경쟁률 확인: {target_name} = {rate}:1")
    else:
        logger.info(f"38커뮤 경쟁률 미확인: {target_name} (수요예측 전 또는 미게시)")

    return rate
