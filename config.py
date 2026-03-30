"""
config.py - 전체 파이프라인 설정값
환경 변수 우선, 없으면 기본값 사용
"""

import os

# ── Notion ────────────────────────────────────────────────
NOTION_API_KEY: str = os.environ.get("NOTION_API_KEY", "")
NOTION_DB_ID:   str = os.environ.get("NOTION_DB_ID",   "")
NOTION_VERSION: str = "2022-06-28"

# ── DART 공모게시판 ────────────────────────────────────────
DART_BASE_URL   = "https://dart.fss.or.kr"
DART_BOARD_URL  = f"{DART_BASE_URL}/dsac005/main.do"
DART_SEARCH_URL = f"{DART_BASE_URL}/dsac005/search.ax"

# 보통주 필터 (secuType 파라미터값)
# 010=보통주, 020=우선주, 030=신주인수권, 040=전환사채, 050=신주인수권부사채,
# 060=교환사채, 070=파생결합증권, 080=기타, 010+020=지분증권 전체
DART_SECU_TYPE_COMMON = "010"   # 보통주

# 조회 기간 (최근 N일) - 청약 기간 기준
DART_SEARCH_DAYS = 90           # 오늘 기준 앞뒤 90일

# 페이지당 결과 수
DART_PAGE_SIZE = 100

# ── 38커뮤니케이션 ─────────────────────────────────────────
COM38_BASE_URL   = "https://www.38.co.kr"
COM38_IPO_URL    = f"{COM38_BASE_URL}/ipo/schedule/schedule.aspx"
COM38_SEARCH_URL = f"{COM38_BASE_URL}/ipo/schedule/search.aspx"

# ── HTTP 공통 헤더 ─────────────────────────────────────────
COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

DART_HEADERS = {
    **COMMON_HEADERS,
    "Referer": DART_BOARD_URL,
    "Origin":  DART_BASE_URL,
    "Accept":  "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

COM38_HEADERS = {
    **COMMON_HEADERS,
    "Referer": COM38_BASE_URL,
    "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── 요청 간격 (초) ─────────────────────────────────────────
REQUEST_DELAY_DART   = 3    # DART 요청 간 딜레이
REQUEST_DELAY_38     = 2    # 38커뮤 요청 간 딜레이
NOTION_RATE_LIMIT    = 0.35 # Notion API: 3 req/sec 제한 준수

# ── 파일 경로 ──────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = os.path.join(BASE_DIR, "data")
LOG_DIR       = os.path.join(BASE_DIR, "logs")
LAST_RUN_FILE = os.path.join(DATA_DIR, "last_run.json")
LOG_FILE      = os.path.join(LOG_DIR,  "scraper.log")

# ── Notion 필드명 (DB 속성명과 정확히 일치해야 함) ──────────
NOTION_FIELDS = {
    "종목명":   "종목명",
    "청약기한": "청약기한",
    "공모가":   "공모가",
    "경쟁률":   "경쟁률",
    "상장일자": "상장일자",
    "주관사":   "주관사",
    "접수번호": "접수번호",   # 중복 체크용 보조 필드 (Rich Text)
}
