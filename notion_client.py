"""
notion_client.py - Notion API 연동 모듈

Notion Internal Integration을 통해 공모주 DB를 조회·생성·업데이트합니다.
API 버전: 2022-06-28
Rate Limit: 3 req/sec → 요청 간 0.35초 sleep
"""

import time
import logging
from typing import Optional

import requests

import config

logger = logging.getLogger(__name__)


class NotionClient:
    """Notion API 클라이언트"""

    BASE_URL = "https://api.notion.com/v1"

    def __init__(self):
        if not config.NOTION_API_KEY:
            raise ValueError("NOTION_API_KEY 환경 변수가 설정되지 않았습니다.")
        if not config.NOTION_DB_ID:
            raise ValueError("NOTION_DB_ID 환경 변수가 설정되지 않았습니다.")

        self.headers = {
            "Authorization":  f"Bearer {config.NOTION_API_KEY}",
            "Content-Type":   "application/json",
            "Notion-Version": config.NOTION_VERSION,
        }
        self.db_id = config.NOTION_DB_ID

    # ── 조회 ──────────────────────────────────────────────────────────────

    def find_page_by_name(self, 종목명: str) -> Optional[dict]:
        """
        종목명으로 기존 페이지 조회

        Args:
            종목명: 검색할 종목명

        Returns:
            페이지 딕셔너리 또는 None
        """
        payload = {
            "filter": {
                "property": config.NOTION_FIELDS["종목명"],
                "title":    {"equals": 종목명},
            }
        }
        result = self._query_db(payload)
        pages = result.get("results", [])
        return pages[0] if pages else None

    def find_page_by_rcept_no(self, rcept_no: str) -> Optional[dict]:
        """접수번호로 기존 페이지 조회 (더 정확한 중복 체크)"""
        if not rcept_no:
            return None
        payload = {
            "filter": {
                "property": config.NOTION_FIELDS["접수번호"],
                "rich_text": {"equals": rcept_no},
            }
        }
        result = self._query_db(payload)
        pages = result.get("results", [])
        return pages[0] if pages else None

    def query_pending_competition(self) -> list[dict]:
        """경쟁률이 비어있는 페이지 조회 (수요예측 미수집 건)"""
        payload = {
            "filter": {
                "property": config.NOTION_FIELDS["경쟁률"],
                "number":   {"is_empty": True},
            }
        }
        result = self._query_db(payload)
        pages  = result.get("results", [])
        logger.info(f"경쟁률 미수집 페이지: {len(pages)}건")

        # 편의를 위해 종목명 + id만 추출
        return [
            {
                "id":    p["id"],
                "종목명": self._get_title(p),
            }
            for p in pages
        ]

    def _query_db(self, payload: dict) -> dict:
        """Notion DB 쿼리 실행"""
        url = f"{self.BASE_URL}/databases/{self.db_id}/query"
        self._rate_limit()
        resp = requests.post(url, json=payload, headers=self.headers, timeout=15)
        self._check_response(resp, "DB 쿼리")
        return resp.json()

    # ── 생성 ──────────────────────────────────────────────────────────────

    def create_ipo_page(self, data: dict) -> Optional[str]:
        """
        신규 공모주 Notion 페이지 생성

        Args:
            data: parser.py가 반환한 정제된 IPO 딕셔너리

        Returns:
            생성된 페이지 ID 또는 None
        """
        properties = self._build_properties(data)
        payload = {
            "parent":     {"database_id": self.db_id},
            "properties": properties,
        }

        url = f"{self.BASE_URL}/pages"
        self._rate_limit()
        resp = requests.post(url, json=payload, headers=self.headers, timeout=15)
        self._check_response(resp, f"페이지 생성 ({data.get('종목명')})")

        page_id = resp.json().get("id")
        logger.info(f"[Notion] 신규 등록: {data.get('종목명')} (id={page_id})")
        return page_id

    # ── 업데이트 ───────────────────────────────────────────────────────────

    def update_competition_rate(self, page_id: str, 경쟁률: float) -> bool:
        """기관 수요예측 경쟁률 업데이트"""
        return self._update_page(
            page_id,
            {config.NOTION_FIELDS["경쟁률"]: {"number": 경쟁률}},
            label="경쟁률 갱신",
        )

    def update_listing_date(self, page_id: str, 상장일자: str) -> bool:
        """상장일자 업데이트"""
        return self._update_page(
            page_id,
            {config.NOTION_FIELDS["상장일자"]: {"date": {"start": 상장일자}}},
            label="상장일자 갱신",
        )

    def update_ipo_price(self, page_id: str, 공모가: int) -> bool:
        """확정 공모가 업데이트"""
        return self._update_page(
            page_id,
            {config.NOTION_FIELDS["공모가"]: {"number": 공모가}},
            label="공모가 갱신",
        )

    def update_if_changed(self, existing_page: dict, new_data: dict) -> bool:
        """
        기존 페이지와 새 데이터를 비교하여 변경사항만 업데이트

        Args:
            existing_page: Notion API가 반환한 페이지 객체
            new_data: 새로 수집된 데이터

        Returns:
            업데이트 여부
        """
        page_id    = existing_page["id"]
        종목명    = new_data.get("종목명", "")
        props      = existing_page.get("properties", {})
        updates    = {}

        # 공모가 변경 체크 (희망공모가 → 확정공모가)
        existing_price = self._get_number(props, config.NOTION_FIELDS["공모가"])
        new_price      = new_data.get("공모가")
        if new_price and new_price != existing_price:
            updates[config.NOTION_FIELDS["공모가"]] = {"number": new_price}
            logger.info(f"[Notion] {종목명} 공모가 변경: {existing_price} → {new_price}")

        # 상장일자 변경 체크
        existing_date = self._get_date(props, config.NOTION_FIELDS["상장일자"])
        new_date      = new_data.get("상장일자")
        if new_date and new_date != existing_date:
            updates[config.NOTION_FIELDS["상장일자"]] = {"date": {"start": new_date}}
            logger.info(f"[Notion] {종목명} 상장일자 변경: {existing_date} → {new_date}")

        if not updates:
            logger.debug(f"[Notion] {종목명} 변경사항 없음 (스킵)")
            return False

        return self._update_page(page_id, updates, label=f"{종목명} 정보 갱신")

    def _update_page(self, page_id: str, properties: dict, label: str = "") -> bool:
        """Notion 페이지 속성 업데이트"""
        url = f"{self.BASE_URL}/pages/{page_id}"
        self._rate_limit()
        resp = requests.patch(
            url,
            json={"properties": properties},
            headers=self.headers,
            timeout=15,
        )
        self._check_response(resp, f"페이지 업데이트 ({label})")
        return resp.status_code == 200

    # ── 속성 빌더 ──────────────────────────────────────────────────────────

    def _build_properties(self, data: dict) -> dict:
        """IPO 데이터를 Notion 속성 딕셔너리로 변환"""
        F = config.NOTION_FIELDS
        props: dict = {}

        # 종목명 (Title)
        종목명 = data.get("종목명", "")
        if 종목명:
            props[F["종목명"]] = {
                "title": [{"text": {"content": 종목명}}]
            }

        # 청약기한 (Date) - 청약 종료일만
        청약기한 = data.get("청약기한") or data.get("청약종료일")
        if 청약기한:
            props[F["청약기한"]] = {"date": {"start": 청약기한}}

        # 공모가 (Number)
        공모가 = data.get("공모가")
        if 공모가 is not None:
            props[F["공모가"]] = {"number": int(공모가)}

        # 경쟁률 (Number)
        경쟁률 = data.get("경쟁률")
        if 경쟁률 is not None:
            props[F["경쟁률"]] = {"number": float(경쟁률)}

        # 상장일자 (Date)
        상장일자 = data.get("상장일자")
        if 상장일자:
            props[F["상장일자"]] = {"date": {"start": 상장일자}}

        # 주관사 (Rich Text)
        주관사 = data.get("주관사", "")
        if 주관사:
            props[F["주관사"]] = {
                "rich_text": [{"text": {"content": 주관사[:2000]}}]
            }

        # 접수번호 (Rich Text) - 중복 체크용
        접수번호 = data.get("접수번호", "")
        if 접수번호:
            props[F["접수번호"]] = {
                "rich_text": [{"text": {"content": 접수번호}}]
            }

        return props

    # ── 유틸리티 ───────────────────────────────────────────────────────────

    def _rate_limit(self):
        """Notion API Rate Limit 준수 (3 req/sec)"""
        time.sleep(config.NOTION_RATE_LIMIT)

    def _check_response(self, resp: requests.Response, context: str = ""):
        """응답 상태 코드 확인 및 로깅"""
        if resp.status_code not in (200, 201):
            logger.error(
                f"Notion API 오류 [{context}] "
                f"status={resp.status_code} | {resp.text[:300]}"
            )
            resp.raise_for_status()

    def _get_title(self, page: dict) -> str:
        """페이지에서 Title 속성 텍스트 추출"""
        field = config.NOTION_FIELDS["종목명"]
        try:
            return page["properties"][field]["title"][0]["plain_text"]
        except (KeyError, IndexError):
            return ""

    def _get_number(self, props: dict, field_name: str) -> Optional[float]:
        """Number 속성 값 추출"""
        try:
            return props[field_name]["number"]
        except (KeyError, TypeError):
            return None

    def _get_date(self, props: dict, field_name: str) -> Optional[str]:
        """Date 속성 시작일 추출"""
        try:
            return props[field_name]["date"]["start"]
        except (KeyError, TypeError):
            return None
