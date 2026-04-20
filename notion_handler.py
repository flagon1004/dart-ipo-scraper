"""
notion_handler.py
Notion API v2022-06-28 연동 모듈

중복 방지 전략 (3단계):
  1차 — 접수번호(rcpNo) 기준 DB 쿼리 → 완전 일치 시 skip
  2차 — 종목명 기준 DB 쿼리 → 같은 종목명이 이미 있으면 skip 또는 update
  3차 — 로컬 캐시(seen set) → API 호출 없이 빠른 중복 판단
"""

import time
import logging
import requests

import config

logger = logging.getLogger(__name__)


# ── 내부 헬퍼 ────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "Authorization":  f"Bearer {config.NOTION_API_KEY}",
        "Notion-Version": config.NOTION_VERSION,
        "Content-Type":   "application/json",
    }


def _rate_limit():
    """Notion API Rate Limit 준수 (3 req/s)"""
    time.sleep(config.NOTION_RATE_LIMIT)


def _safe_request(method: str, url: str, **kwargs) -> dict | None:
    """
    requests 호출 래퍼 — HTTP 오류 시 로그 후 None 반환
    """
    try:
        resp = getattr(requests, method)(url, headers=_headers(), timeout=15, **kwargs)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2))
            logger.warning(f"Notion Rate Limit → {retry_after}초 대기")
            time.sleep(retry_after)
            resp = getattr(requests, method)(url, headers=_headers(), timeout=15, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        logger.error(f"Notion HTTP 오류: {e} | {url}")
        return None
    except Exception as e:
        logger.error(f"Notion 요청 실패: {e}")
        return None


# ── 조회 ─────────────────────────────────────────────────────

def find_by_rcp_no(rcp_no: str) -> dict | None:
    """
    접수번호(rcpNo)로 기존 Notion 페이지 조회.
    중복 방지 1차 키: 접수번호는 DART 고유값이므로 완전한 중복 판단 가능.
    """
    if not rcp_no:
        return None

    url  = f"{config.NOTION_BASE_URL}/databases/{config.NOTION_DB_ID}/query"
    body = {
        "filter": {
            "property": config.NOTION_FIELD["접수번호"],
            "rich_text": {"equals": rcp_no}
        }
    }
    data = _safe_request("post", url, json=body)
    _rate_limit()

    if data and data.get("results"):
        page = data["results"][0]
        logger.debug(f"[중복-접수번호] 기존 페이지 발견: {rcp_no} → {page['id']}")
        return page
    return None


def find_by_name(name: str) -> dict | None:
    """
    종목명으로 기존 Notion 페이지 조회.
    중복 방지 2차 키: 접수번호 없는 경우 폴백.

    Returns: 가장 최근 등록된 페이지 (results[0]) 또는 None
    """
    if not name:
        return None

    url  = f"{config.NOTION_BASE_URL}/databases/{config.NOTION_DB_ID}/query"
    body = {
        "filter": {
            "property": config.NOTION_FIELD["종목명"],
            "title": {"equals": name}
        }
    }
    data = _safe_request("post", url, json=body)
    _rate_limit()

    if data and data.get("results"):
        page = data["results"][0]
        logger.debug(f"[중복-종목명] 기존 페이지 발견: {name} → {page['id']}")
        return page
    return None


def query_pending_competition() -> list[dict]:
    """
    경쟁률(기관 수요예측)이 비어 있는 Notion 페이지 목록 반환.
    38커뮤니케이션 보강 대상 식별용.
    """
    url  = f"{config.NOTION_BASE_URL}/databases/{config.NOTION_DB_ID}/query"
    body = {
        "filter": {
            "property": config.NOTION_FIELD["경쟁률"],
            "number": {"is_empty": True}
        }
    }
    data = _safe_request("post", url, json=body)
    _rate_limit()

    if not data:
        return []

    results = []
    for page in data.get("results", []):
        props = page.get("properties", {})
        name_prop = props.get(config.NOTION_FIELD["종목명"], {})
        title_list = name_prop.get("title", [])
        name = title_list[0]["text"]["content"] if title_list else ""
        results.append({"id": page["id"], "종목명": name})

    return results


def query_pages_with_listing_date() -> list[dict]:
    """
    상장일자가 존재하고 경쟁률이 비어 있는 Notion 페이지 목록 반환.

    조건:
      - 상장일자(Date) : is_not_empty   → 상장일자가 확정된 종목
      - 경쟁률(Number) : is_empty       → 아직 경쟁률 미수집 종목

    상장일자가 확정됐다는 것은 수요예측이 완료됐음을 의미하므로
    Gemini 검색으로 경쟁률을 확보할 수 있는 상태임.
    """
    url  = f"{config.NOTION_BASE_URL}/databases/{config.NOTION_DB_ID}/query"
    body = {
        "filter": {
            "and": [
                {
                    "property": config.NOTION_FIELD["상장일자"],
                    "date": {"is_not_empty": True}
                },
                {
                    "property": config.NOTION_FIELD["경쟁률"],
                    "number": {"is_empty": True}
                },
            ]
        }
    }
    data = _safe_request("post", url, json=body)
    _rate_limit()

    if not data:
        return []

    results = []
    for page in data.get("results", []):
        props      = page.get("properties", {})
        name_prop  = props.get(config.NOTION_FIELD["종목명"], {})
        title_list = name_prop.get("title", [])
        name       = title_list[0]["text"]["content"] if title_list else ""

        # 상장일자도 함께 추출 (로그용)
        listing_prop = props.get(config.NOTION_FIELD["상장일자"], {})
        listing_date = (listing_prop.get("date") or {}).get("start", "")

        results.append({
            "id":     page["id"],
            "종목명": name,
            "상장일자": listing_date,
        })
        logger.debug(f"경쟁률 보강 대상: {name} (상장일자={listing_date})")

    logger.info(f"경쟁률 보강 대상: {len(results)}건 (상장일자 有 & 경쟁률 無)")
    return results


# ── 생성 / 업데이트 ──────────────────────────────────────────

def _build_properties(data: dict) -> dict:
    """
    dict → Notion properties 포맷 변환.
    None 값 필드는 포함하지 않음 (불필요한 null 덮어쓰기 방지).
    """
    props = {}

    # 종목명 (Title)
    if data.get("종목명"):
        props[config.NOTION_FIELD["종목명"]] = {
            "title": [{"text": {"content": data["종목명"]}}]
        }

    # 청약기한 (Date - 종료일 단일)
    if data.get("청약종료일"):
        props[config.NOTION_FIELD["청약기한"]] = {
            "date": {"start": data["청약종료일"]}
        }

    # 공모가 (Number)
    if data.get("공모가") is not None:
        props[config.NOTION_FIELD["공모가"]] = {"number": data["공모가"]}

    # 경쟁률 (Number)
    if data.get("경쟁률") is not None:
        props[config.NOTION_FIELD["경쟁률"]] = {"number": data["경쟁률"]}

    # 상장일자 (Date)
    if data.get("상장일자"):
        props[config.NOTION_FIELD["상장일자"]] = {
            "date": {"start": data["상장일자"]}
        }

    # 주관사 (Rich Text)
    if data.get("주관사"):
        props[config.NOTION_FIELD["주관사"]] = {
            "rich_text": [{"text": {"content": data["주관사"]}}]
        }

    # 접수번호 (Rich Text - 중복 체크 키)
    if data.get("접수번호"):
        props[config.NOTION_FIELD["접수번호"]] = {
            "rich_text": [{"text": {"content": data["접수번호"]}}]
        }

    return props


def create_page(data: dict) -> str | None:
    """
    신규 Notion 페이지 생성.

    Returns: 생성된 page_id 또는 None
    """
    url  = f"{config.NOTION_BASE_URL}/pages"
    body = {
        "parent":     {"database_id": config.NOTION_DB_ID},
        "properties": _build_properties(data),
    }
    result = _safe_request("post", url, json=body)
    _rate_limit()

    if result:
        page_id = result.get("id", "")
        logger.info(f"[Notion 생성] {data.get('종목명')} | page_id={page_id}")
        return page_id
    return None


def update_page(page_id: str, data: dict) -> bool:
    """
    기존 Notion 페이지 업데이트.
    data 에 포함된 필드만 덮어씀 (None 필드 무시).

    Returns: 성공 여부
    """
    url  = f"{config.NOTION_BASE_URL}/pages/{page_id}"
    body = {"properties": _build_properties(data)}
    result = _safe_request("patch", url, json=body)
    _rate_limit()

    if result:
        logger.info(f"[Notion 업데이트] page_id={page_id} | 변경 필드: {list(data.keys())}")
        return True
    return False


def update_competition_rate(page_id: str, rate: float) -> bool:
    """기관 수요예측 경쟁률 업데이트 전용 헬퍼"""
    return update_page(page_id, {"경쟁률": rate})


def update_listing_date(page_id: str, listing_date: str) -> bool:
    """상장일자 업데이트 전용 헬퍼"""
    return update_page(page_id, {"상장일자": listing_date})


# ── 핵심: 중복 체크 후 생성/업데이트 결정 ───────────────────

def upsert_ipo(data: dict, local_cache: set[str]) -> str:
    """
    중복 방지 3단계를 거쳐 신규 등록 또는 업데이트 결정.

    중복 판단 우선순위:
      1단계: 로컬 캐시(local_cache) → 동일 실행 내 중복 방지 (API 호출 없이 빠름)
      2단계: Notion DB 접수번호 쿼리 → 영구적 중복 방지 (가장 신뢰도 높음)
      3단계: Notion DB 종목명 쿼리 → 접수번호 없는 경우 폴백

    동일 데이터 판단 기준:
      - 신규: Notion에 해당 접수번호/종목명이 없음 → pages.create
      - 변경: 이미 존재하나 공모가·청약기한 등이 달라짐 → pages.update
      - 중복: 접수번호 + 청약종료일 모두 동일 → skip

    Parameters
    ----------
    data        : parser.clean_and_filter 반환 항목
    local_cache : 이번 실행에서 이미 처리한 접수번호 집합 (변이됨)

    Returns
    -------
    str  "created" | "updated" | "skipped"
    """
    name   = data.get("종목명", "")
    rcp_no = data.get("접수번호", "")

    # ── 1단계: 로컬 캐시 ────────────────────────────────────
    cache_key = rcp_no if rcp_no else name
    if cache_key in local_cache:
        logger.info(f"[SKIP-캐시] {name} ({rcp_no})")
        return "skipped"

    # ── 2단계: 접수번호 기준 Notion 쿼리 ───────────────────
    existing = None
    if rcp_no:
        existing = find_by_rcp_no(rcp_no)

    # ── 3단계: 종목명 기준 폴백 쿼리 ───────────────────────
    if existing is None:
        existing = find_by_name(name)

    # ── 신규 / 변경 / 중복 분기 ─────────────────────────────
    if existing is None:
        # 신규 등록
        page_id = create_page(data)
        if page_id:
            local_cache.add(cache_key)
            return "created"
        return "skipped"   # 생성 실패

    # 기존 페이지 존재 → 변경 여부 확인
    existing_props = existing.get("properties", {})

    def _get_text(prop_name: str) -> str:
        p = existing_props.get(prop_name, {})
        # rich_text
        rt = p.get("rich_text", [])
        if rt:
            return rt[0].get("text", {}).get("content", "")
        # title
        tl = p.get("title", [])
        if tl:
            return tl[0].get("text", {}).get("content", "")
        return ""

    def _get_date(prop_name: str) -> str:
        p = existing_props.get(prop_name, {})
        d = p.get("date") or {}
        return d.get("start", "")

    def _get_number(prop_name: str) -> float | None:
        p = existing_props.get(prop_name, {})
        return p.get("number")

    # 변경 감지: 접수번호·청약종료일·공모가가 동일하면 완전 중복으로 skip
    same_rcp  = (_get_text(config.NOTION_FIELD["접수번호"]) == rcp_no)
    same_date = (_get_date(config.NOTION_FIELD["청약기한"]) == (data.get("청약종료일") or ""))
    same_price = (_get_number(config.NOTION_FIELD["공모가"]) == data.get("공모가"))

    if same_rcp and same_date and same_price:
        logger.info(f"[SKIP-완전동일] {name} ({rcp_no})")
        local_cache.add(cache_key)
        return "skipped"

    # 변경사항 있음 → 업데이트 (변경된 필드만 전달)
    update_fields: dict = {}
    if not same_date and data.get("청약종료일"):
        update_fields["청약종료일"] = data["청약종료일"]
    if not same_price and data.get("공모가") is not None:
        update_fields["공모가"] = data["공모가"]
    if data.get("경쟁률") is not None and _get_number(config.NOTION_FIELD["경쟁률"]) is None:
        update_fields["경쟁률"] = data["경쟁률"]
    if data.get("상장일자") and not _get_date(config.NOTION_FIELD["상장일자"]):
        update_fields["상장일자"] = data["상장일자"]

    if update_fields:
        update_page(existing["id"], update_fields)
        local_cache.add(cache_key)
        return "updated"

    # 변경사항 없음 (접수번호만 다르거나 마이너 차이)
    logger.info(f"[SKIP-변경없음] {name}")
    local_cache.add(cache_key)
    return "skipped"

