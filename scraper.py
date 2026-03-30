"""
scraper.py - DART 공모게시판 HTTP 요청 + HTML 파싱

DART 공모게시판(dsac005)은 POST 기반 AJAX 엔드포인트를 사용합니다.
응답은 HTML 테이블 형태로 반환됩니다.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)


class DartScraper:
    """DART 공모게시판 스크래퍼"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(config.DART_HEADERS)
        self._init_session()

    def _init_session(self):
        """DART 메인 페이지 접속으로 세션/쿠키 초기화"""
        try:
            resp = self.session.get(config.DART_BOARD_URL, timeout=15)
            resp.raise_for_status()
            logger.info(f"DART 세션 초기화 완료 (status={resp.status_code})")
        except requests.RequestException as e:
            logger.warning(f"DART 세션 초기화 실패 (계속 진행): {e}")

    def fetch_ipo_board(
        self,
        secu_type: str = config.DART_SECU_TYPE_COMMON,
        days_back: int = config.DART_SEARCH_DAYS,
    ) -> list[dict]:
        """
        DART 공모게시판에서 공모주 목록 수집

        Args:
            secu_type: 증권 종류 코드 (010=보통주)
            days_back: 오늘 기준 며칠 전까지 조회할지

        Returns:
            공모주 기본 정보 딕셔너리 리스트
        """
        today = datetime.today()
        start_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
        end_date   = (today + timedelta(days=days_back)).strftime("%Y%m%d")

        all_items: list[dict] = []
        page = 1

        logger.info(
            f"DART 공모게시판 조회 시작 | secuType={secu_type} "
            f"| 기간: {start_date}~{end_date}"
        )

        while True:
            payload = {
                "selectDate":  "",
                "startDate":   start_date,
                "endDate":     end_date,
                "secuType":    secu_type,
                "corpName":    "",
                "pageIndex":   str(page),
                "maxResults":  str(config.DART_PAGE_SIZE),
                "orderByField":"d",   # 청약종료일 기준 정렬
                "orderByType": "D",   # 내림차순
            }

            try:
                resp = self.session.post(
                    config.DART_SEARCH_URL,
                    data=payload,
                    timeout=20,
                )
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"DART 요청 실패 (page={page}): {e}")
                break

            items, has_next = self._parse_board_html(resp.text)
            all_items.extend(items)
            logger.info(f"  page {page}: {len(items)}건 수집")

            if not has_next or not items:
                break

            page += 1
            time.sleep(config.REQUEST_DELAY_DART)

        logger.info(f"DART 공모게시판 총 {len(all_items)}건 수집 완료")
        return all_items

    def _parse_board_html(self, html: str) -> tuple[list[dict], bool]:
        """
        공모게시판 HTML 테이블 파싱

        Returns:
            (items_list, has_next_page)
        """
        soup = BeautifulSoup(html, "lxml")
        items = []

        # 결과 테이블 탐색
        table = soup.find("table")
        if not table:
            logger.debug("테이블 없음 (마지막 페이지이거나 결과 없음)")
            return [], False

        rows = table.find_all("tr")
        if len(rows) <= 1:
            return [], False

        for row in rows[1:]:  # 헤더 제외
            cols = row.find_all("td")
            if not cols:
                continue

            # 조회 결과 없음 메시지 처리
            if "조회 결과가 없습니다" in row.get_text():
                return [], False

            try:
                # 컬럼 순서: 번호 | 발행회사명 | 보고서명 | 증권의종류 | 청약일 | 접수일자
                num       = cols[0].get_text(strip=True)
                corp_name = cols[1].get_text(strip=True)
                report_nm = cols[2].get_text(strip=True)
                secu_nm   = cols[3].get_text(strip=True)
                sub_date  = cols[4].get_text(strip=True)   # "청약시작일~청약종료일"
                rcept_dt  = cols[5].get_text(strip=True)   # 접수일자

                # 보고서 링크에서 접수번호(rcept_no) 추출
                link_tag  = cols[2].find("a")
                rcept_no  = ""
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    # href 예: javascript:viewReport('20260325000123')
                    import re
                    m = re.search(r"'(\d+)'", href)
                    if m:
                        rcept_no = m.group(1)

                item = {
                    "번호":     num,
                    "종목명":   corp_name,
                    "보고서명": report_nm,
                    "증권종류": secu_nm,
                    "청약일":   sub_date,   # 정제는 parser.py에서
                    "접수일자": rcept_dt,
                    "접수번호": rcept_no,
                }
                items.append(item)

            except (IndexError, AttributeError) as e:
                logger.debug(f"행 파싱 오류 (건너뜀): {e} | {row.get_text()[:80]}")
                continue

        # 다음 페이지 존재 여부: 수집 건수가 페이지 크기와 같으면 다음 페이지 있을 수 있음
        has_next = (len(items) == config.DART_PAGE_SIZE)
        return items, has_next

    def fetch_report_detail(self, rcept_no: str) -> dict:
        """
        공모 보고서 상세 페이지에서 추가 정보 수집
        (공모가, 주관사, 상장예정일 등)

        Args:
            rcept_no: 접수번호

        Returns:
            추가 정보 딕셔너리
        """
        detail_url = f"{config.DART_BASE_URL}/viewer/main.do?rcpNo={rcept_no}"
        extra: dict = {}

        try:
            resp = self.session.get(detail_url, timeout=20)
            resp.raise_for_status()
            extra = self._parse_report_detail(resp.text, rcept_no)
        except requests.RequestException as e:
            logger.warning(f"보고서 상세 조회 실패 (rcept_no={rcept_no}): {e}")

        time.sleep(config.REQUEST_DELAY_DART)
        return extra

    def _parse_report_detail(self, html: str, rcept_no: str) -> dict:
        """
        증권신고서 상세 페이지 파싱
        공모게시판 목록 API가 JSON을 반환하는 경우를 위한 폴백 파서도 포함
        """
        soup = BeautifulSoup(html, "lxml")
        result = {}

        # iframe src에서 실제 문서 URL 추출 시도
        iframe = soup.find("iframe", id="contents")
        if iframe and iframe.get("src"):
            doc_url = config.DART_BASE_URL + iframe["src"]
            try:
                resp2 = self.session.get(doc_url, timeout=20)
                soup  = BeautifulSoup(resp2.text, "lxml")
            except Exception as e:
                logger.debug(f"iframe 문서 접근 실패: {e}")

        # 테이블에서 공모가, 주관사, 상장예정일 탐색
        tables = soup.find_all("table")
        for tbl in tables:
            for row in tbl.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                text  = " ".join(cells)

                if "모집가액" in text or "공모가" in text:
                    # 금액 컬럼 추출 (숫자만)
                    for cell in cells:
                        import re
                        nums = re.findall(r"[\d,]+", cell)
                        if nums:
                            try:
                                result["공모가_raw"] = int(nums[0].replace(",", ""))
                                break
                            except ValueError:
                                pass

                if "주관" in text and "증권" in text:
                    for cell in cells:
                        if "증권" in cell and len(cell) > 2:
                            result.setdefault("주관사", cell)

                if "상장예정일" in text or "상장일" in text:
                    for cell in cells:
                        import re
                        m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", cell)
                        if m:
                            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
                            result.setdefault("상장예정일_raw", f"{y}-{mo}-{d}")

        logger.debug(f"보고서 상세 파싱 결과 (rcept_no={rcept_no}): {result}")
        return result
