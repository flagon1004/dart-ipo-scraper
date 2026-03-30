"""
scraper_38.py - 38커뮤니케이션(38.co.kr) 기관 수요예측 경쟁률 스크래퍼

38커뮤니케이션은 기관 수요예측 결과를 가장 빠르게 업데이트하는 사이트입니다.
공모 일정 페이지에서 종목명으로 검색 후 수요예측 경쟁률을 파싱합니다.
"""

import re
import time
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)


class Com38Scraper:
    """38커뮤니케이션 수요예측 경쟁률 스크래퍼"""

    # 기관 수요예측 경쟁률이 포함된 페이지 URL
    IPO_SCHEDULE_URL = "https://www.38.co.kr/html/fund/index.htm?o=k"
    IPO_DETAIL_BASE  = "https://www.38.co.kr/html/fund/"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(config.COM38_HEADERS)

    def fetch_demand_forecast_rate(self, 종목명: str) -> Optional[float]:
        """
        38커뮤니케이션에서 종목명으로 기관 수요예측 경쟁률 검색

        Args:
            종목명: 검색할 종목명 (예: "에이피알")

        Returns:
            기관 수요예측 경쟁률 (float) 또는 None
        """
        logger.info(f"[38커뮤] 수요예측 경쟁률 조회: {종목명}")

        # 1단계: 공모 일정 페이지에서 종목 링크 탐색
        detail_url = self._find_stock_detail_url(종목명)
        if not detail_url:
            logger.info(f"[38커뮤] 종목 미발견: {종목명}")
            return None

        time.sleep(config.REQUEST_DELAY_38)

        # 2단계: 상세 페이지에서 수요예측 경쟁률 파싱
        rate = self._parse_demand_rate(detail_url, 종목명)
        if rate is not None:
            logger.info(f"[38커뮤] {종목명} 수요예측 경쟁률: {rate}:1")
        else:
            logger.info(f"[38커뮤] {종목명} 수요예측 경쟁률 미확인 (수요예측 전이거나 데이터 없음)")

        return rate

    def _find_stock_detail_url(self, 종목명: str) -> Optional[str]:
        """공모 일정 목록 페이지에서 종목 상세 URL 탐색"""
        # 38커뮤니케이션 공모주 검색 URL
        search_urls = [
            "https://www.38.co.kr/html/fund/index.htm?o=k",
            "https://www.38.co.kr/html/fund/index.htm?o=d",
        ]

        for url in search_urls:
            try:
                resp = self.session.get(url, timeout=15)
                resp.raise_for_status()
                # EUC-KR 인코딩 처리
                resp.encoding = "euc-kr"
                soup = BeautifulSoup(resp.text, "lxml")

                detail_url = self._search_in_page(soup, 종목명)
                if detail_url:
                    return detail_url

                time.sleep(config.REQUEST_DELAY_38)

            except requests.RequestException as e:
                logger.warning(f"[38커뮤] 목록 페이지 접근 실패 ({url}): {e}")

        return None

    def _search_in_page(self, soup: BeautifulSoup, 종목명: str) -> Optional[str]:
        """
        페이지 내 링크 중 종목명이 포함된 것 탐색

        38커뮤니케이션의 공모주 목록은 <table> 내 <a> 태그로 구성됩니다.
        """
        # 종목명의 핵심 키워드 추출 (주식회사, (주) 등 제거)
        keyword = re.sub(r"\(주\)|주식회사|㈜|\s+", "", 종목명).strip()
        if len(keyword) < 2:
            keyword = 종목명.strip()

        links = soup.find_all("a", href=True)
        for link in links:
            link_text = link.get_text(strip=True)
            href = link["href"]

            # 종목명이 링크 텍스트에 포함되고, fund 관련 URL인 경우
            if keyword in link_text and ("fund" in href or "ipo" in href.lower()):
                # 절대 URL로 변환
                if href.startswith("http"):
                    return href
                elif href.startswith("/"):
                    return f"https://www.38.co.kr{href}"
                else:
                    return f"{self.IPO_DETAIL_BASE}{href}"

        return None

    def _parse_demand_rate(self, detail_url: str, 종목명: str) -> Optional[float]:
        """
        상세 페이지에서 기관 수요예측 경쟁률 파싱

        38커뮤니케이션 상세 페이지의 테이블 구조:
        - "기관경쟁률" 또는 "수요예측 경쟁률" 레이블 옆에 경쟁률 값이 있음
        """
        try:
            resp = self.session.get(detail_url, timeout=15)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.warning(f"[38커뮤] 상세 페이지 접근 실패 ({detail_url}): {e}")
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        # 경쟁률 관련 키워드
        rate_keywords = [
            "기관경쟁률", "수요예측경쟁률", "기관수요예측", "경쟁률",
            "수요예측 경쟁률", "기관 경쟁률"
        ]

        # 방법 1: 테이블에서 키워드 옆 셀 파싱
        tables = soup.find_all("table")
        for tbl in tables:
            rows = tbl.find_all("tr")
            for row in rows:
                cells = row.find_all(["th", "td"])
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True).replace(" ", "")
                    for kw in rate_keywords:
                        if kw.replace(" ", "") in cell_text:
                            # 바로 옆 셀에서 숫자 추출
                            if i + 1 < len(cells):
                                next_cell = cells[i + 1].get_text(strip=True)
                                rate = self._extract_rate(next_cell)
                                if rate is not None:
                                    return rate
                            # 같은 셀 내에서 숫자 추출 시도
                            rate = self._extract_rate(cell_text)
                            if rate is not None:
                                return rate

        # 방법 2: 전체 텍스트에서 패턴 매칭
        full_text = soup.get_text()
        for kw in rate_keywords:
            pattern = rf"{kw}\s*[:\s]\s*([\d,]+\.?\d*)\s*(?::?\s*1)?"
            m = re.search(pattern, full_text.replace(" ", ""))
            if m:
                rate = self._extract_rate(m.group(1))
                if rate is not None:
                    return rate

        return None

    def _extract_rate(self, text: str) -> Optional[float]:
        """
        텍스트에서 경쟁률 숫자 추출

        예: "1,523.50:1" → 1523.5
            "1523배" → 1523.0
            "미실시" → None
        """
        if not text or any(kw in text for kw in ["미실시", "미정", "-", "N/A", "없음"]):
            return None

        # 숫자 패턴 추출 (콜론 앞부분만)
        cleaned = text.split(":")[0].split("대")[0].strip()
        cleaned = re.sub(r"[^\d.]", "", cleaned)

        try:
            val = float(cleaned)
            # 합리적 범위 체크 (1 ~ 10,000)
            if 1.0 <= val <= 10000.0:
                return val
        except ValueError:
            pass

        return None

    def fetch_all_current_ipos(self) -> list[dict]:
        """
        38커뮤니케이션 전체 공모주 일정 및 수요예측 결과 수집

        Returns:
            종목명, 경쟁률 등이 담긴 딕셔너리 리스트
        """
        logger.info("[38커뮤] 전체 공모주 목록 수집 시작")
        results = []

        try:
            resp = self.session.get(self.IPO_SCHEDULE_URL, timeout=15)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            soup = BeautifulSoup(resp.text, "lxml")
            results = self._parse_schedule_table(soup)
        except requests.RequestException as e:
            logger.error(f"[38커뮤] 목록 수집 실패: {e}")

        logger.info(f"[38커뮤] 총 {len(results)}건 수집")
        return results

    def _parse_schedule_table(self, soup: BeautifulSoup) -> list[dict]:
        """38커뮤니케이션 공모 일정 테이블 파싱"""
        items = []
        tables = soup.find_all("table")

        for tbl in tables:
            headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
            if not any("종목" in h or "기업" in h or "회사" in h for h in headers):
                continue

            rows = tbl.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                item = {}
                for i, col in enumerate(cols):
                    text = col.get_text(strip=True)
                    if i < len(headers):
                        item[headers[i]] = text

                    # 링크 추출
                    a = col.find("a")
                    if a and ("fund" in a.get("href", "") or "ipo" in a.get("href", "").lower()):
                        item["_detail_url"] = a["href"]
                        if not item["_detail_url"].startswith("http"):
                            item["_detail_url"] = f"https://www.38.co.kr{item['_detail_url']}"

                if item:
                    items.append(item)

        return items
