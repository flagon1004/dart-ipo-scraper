"""
config.py
전체 파이프라인 설정값 (URL, 헤더, 상수 등)
환경 변수 기반 시크릿은 GitHub Secrets → os.environ 에서 로드
"""

import os

# ── DART 공모게시판 ──────────────────────────────────────────
DART_BASE_URL   = "https://dart.fss.or.kr"
DART_SEARCH_URL = "https://dart.fss.or.kr/dsac005/search.ax"

DART_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://dart.fss.or.kr/dsac005/main.do",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

# 기본 조회 파라미터 (GET)
# selectDate 미지정 시 최근 데이터 반환
DART_DEFAULT_PARAMS = {
    "selectDate": "",          # 빈값 = 전체 기간
    "pageNo":     "1",
    "maxResults": "100",       # 한 번에 최대 100건
    "securities": "",          # 빈값 = 전체 증권종류
}

# ── 필터 조건 ────────────────────────────────────────────────
# <tr> 또는 내부 <td>/<img> 의 title/alt 속성값
FILTER_CORP_TYPE   = "기타법인"   # 법인 구분 필터
FILTER_SEC_TYPE    = "보통주"     # 증권 종류 필터

# ── 38커뮤니케이션 ───────────────────────────────────────────
IPO38_BASE_URL    = "https://www.38.co.kr"
IPO38_IPO_URL     = "https://www.38.co.kr/html/fund/index.htm?o=kd"
IPO38_HEADERS     = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.38.co.kr/",
}

# ── Notion ───────────────────────────────────────────────────
NOTION_API_KEY = os.environ.get("NOTION_API_KEY", "")
NOTION_DB_ID   = os.environ.get("NOTION_DB_ID", "")
NOTION_VERSION = "2022-06-28"
NOTION_BASE_URL = "https://api.notion.com/v1"

# Notion 필드명 (DB 스키마와 정확히 일치해야 함)
NOTION_FIELD = {
    "종목명":   "종목명",
    "청약기한": "청약기한",
    "공모가":   "공모가",
    "경쟁률":   "경쟁률",
    "상장일자": "상장일자",
    "주관사":   "주관사",
    "접수번호": "접수번호",   # 중복 체크용 유니크 키
}

# ── 요청 제어 ────────────────────────────────────────────────
REQUEST_DELAY_SEC   = 2.0    # 요청 간 대기 (초)
REQUEST_TIMEOUT_SEC = 20     # 타임아웃
NOTION_RATE_LIMIT   = 0.35   # Notion API 3 req/s 제한 → 0.35초 간격

# ── 로깅 ────────────────────────────────────────────────────
LOG_DIR      = "logs"
LOG_FILENAME = "dart_ipo_scraper.log"
