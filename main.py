"""
main.py - DART IPO 자동 수집 파이프라인 엔트리포인트

실행 흐름:
  STEP 1: DART 공모게시판 스크래핑 (보통주 필터)
  STEP 2: Notion DB 동기화 (신규 등록 / 변경 업데이트)
  STEP 3: 38커뮤니케이션 기관 수요예측 경쟁률 보강
  STEP 4: 실행 결과 저장
"""

import json
import logging
import os
import sys
import time
from datetime import datetime

import config
from scraper    import DartScraper
from scraper_38 import Com38Scraper
from parser     import clean_and_filter, merge_detail
from notion_client import NotionClient

# ── 로깅 설정 ──────────────────────────────────────────────────────────────

os.makedirs(config.LOG_DIR,  exist_ok=True)
os.makedirs(config.DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── 실행 상태 저장/로드 ────────────────────────────────────────────────────

def load_last_run() -> dict:
    """마지막 실행 상태 로드"""
    if os.path.exists(config.LAST_RUN_FILE):
        try:
            with open(config.LAST_RUN_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_last_run(status: dict):
    """실행 결과 저장"""
    status["last_run_at"] = datetime.now().isoformat()
    try:
        with open(config.LAST_RUN_FILE, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logger.warning(f"last_run.json 저장 실패: {e}")


# ── 메인 파이프라인 ────────────────────────────────────────────────────────

def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("DART IPO 자동 수집 파이프라인 시작")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 환경 변수 체크
    if not config.NOTION_API_KEY or not config.NOTION_DB_ID:
        logger.error("NOTION_API_KEY 또는 NOTION_DB_ID 환경 변수가 없습니다.")
        sys.exit(1)

    stats = {
        "new":     0,   # 신규 등록 건수
        "updated": 0,   # 업데이트 건수
        "rate":    0,   # 경쟁률 보강 건수
        "errors":  0,   # 오류 건수
    }

    # ── STEP 1: DART 공모게시판 스크래핑 ──────────────────────────────────
    logger.info("\n[STEP 1] DART 공모게시판 스크래핑")
    try:
        dart_scraper = DartScraper()
        raw_data     = dart_scraper.fetch_ipo_board()
        ipo_list     = clean_and_filter(raw_data, 증권종류="보통주")
    except Exception as e:
        logger.error(f"DART 스크래핑 실패: {e}", exc_info=True)
        stats["errors"] += 1
        ipo_list = []

    logger.info(f"수집된 보통주 IPO: {len(ipo_list)}건")

    # 상세 페이지에서 공모가/주관사 보강
    for item in ipo_list:
        rcept_no = item.get("접수번호", "")
        if rcept_no:
            try:
                detail = dart_scraper.fetch_report_detail(rcept_no)
                item.update({k: v for k, v in detail.items() if v is not None})
            except Exception as e:
                logger.warning(f"상세 조회 실패 ({item.get('종목명')}): {e}")

    # ── STEP 2: Notion DB 동기화 ──────────────────────────────────────────
    logger.info("\n[STEP 2] Notion DB 동기화")
    try:
        notion = NotionClient()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    for item in ipo_list:
        종목명   = item.get("종목명", "")
        접수번호 = item.get("접수번호", "")

        try:
            # 중복 체크: 접수번호 우선, 없으면 종목명
            existing = None
            if 접수번호:
                existing = notion.find_page_by_rcept_no(접수번호)
            if not existing:
                existing = notion.find_page_by_name(종목명)

            if not existing:
                # 신규 등록
                notion.create_ipo_page(item)
                logger.info(f"  [신규] {종목명}")
                stats["new"] += 1
            else:
                # 변경 업데이트
                changed = notion.update_if_changed(existing, item)
                if changed:
                    logger.info(f"  [갱신] {종목명}")
                    stats["updated"] += 1

        except Exception as e:
            logger.error(f"  [오류] {종목명}: {e}", exc_info=True)
            stats["errors"] += 1

    # ── STEP 3: 기관 수요예측 경쟁률 보강 (38커뮤니케이션) ──────────────────
    logger.info("\n[STEP 3] 기관 수요예측 경쟁률 보강 (38커뮤니케이션)")
    try:
        pending = notion.query_pending_competition()
        scraper38 = Com38Scraper()

        for page in pending:
            종목명 = page["종목명"]
            page_id = page["id"]

            if not 종목명:
                continue

            try:
                rate = scraper38.fetch_demand_forecast_rate(종목명)
                if rate is not None:
                    notion.update_competition_rate(page_id, rate)
                    logger.info(f"  [경쟁률] {종목명}: {rate}:1")
                    stats["rate"] += 1
                else:
                    logger.debug(f"  [경쟁률 미확인] {종목명} (수요예측 전)")
            except Exception as e:
                logger.warning(f"  [경쟁률 오류] {종목명}: {e}")

    except Exception as e:
        logger.error(f"경쟁률 보강 단계 실패: {e}", exc_info=True)
        stats["errors"] += 1

    # ── STEP 4: 실행 결과 저장 ────────────────────────────────────────────
    elapsed = round(time.time() - start_time, 1)
    stats["elapsed_sec"] = elapsed

    logger.info("\n" + "=" * 60)
    logger.info("파이프라인 완료")
    logger.info(f"  신규 등록: {stats['new']}건")
    logger.info(f"  정보 갱신: {stats['updated']}건")
    logger.info(f"  경쟁률 보강: {stats['rate']}건")
    logger.info(f"  오류: {stats['errors']}건")
    logger.info(f"  소요 시간: {elapsed}초")
    logger.info("=" * 60)

    save_last_run(stats)

    # 오류가 있으면 비정상 종료 (GitHub Actions에서 실패 감지용)
    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
