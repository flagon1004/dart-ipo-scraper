"""
main.py
DART IPO 스크래퍼 엔트리포인트

실행 순서:
  STEP 1: DART 공모게시판 스크래핑 (기타법인 + 보통주 필터)
  STEP 2: 데이터 정제
  STEP 3: Notion DB 동기화 (3단계 중복 방지 upsert)
  STEP 4: 기관 수요예측 경쟁률 보강 (38커뮤니케이션)
  STEP 5: 실행 결과 저장
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import config
import scraper
import parser
import scraper_38
import notion_client


# ── 로깅 설정 ────────────────────────────────────────────────

def _setup_logging():
    Path(config.LOG_DIR).mkdir(exist_ok=True)
    log_path = Path(config.LOG_DIR) / config.LOG_FILENAME

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 파일 핸들러
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    # 콘솔 핸들러
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)


logger = logging.getLogger(__name__)


# ── 실행 상태 저장 ───────────────────────────────────────────

def _save_run_status(stats: dict):
    path = Path("data/last_run.json")
    path.parent.mkdir(exist_ok=True)
    stats["timestamp"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    logger.info(f"실행 결과 저장: {path}")


# ── 환경 변수 검증 ───────────────────────────────────────────

def _validate_env() -> bool:
    missing = []
    if not config.NOTION_API_KEY:
        missing.append("NOTION_API_KEY")
    if not config.NOTION_DB_ID:
        missing.append("NOTION_DB_ID")
    if missing:
        logger.error(f"필수 환경 변수 누락: {', '.join(missing)}")
        return False
    return True


# ── 메인 파이프라인 ──────────────────────────────────────────

def main():
    _setup_logging()
    logger.info("=" * 60)
    logger.info("DART IPO 스크래퍼 시작")
    logger.info("=" * 60)

    # 환경 변수 검증
    if not _validate_env():
        raise SystemExit(1)

    stats = {
        "dart_scraped":  0,
        "dart_filtered": 0,
        "notion_created": 0,
        "notion_updated": 0,
        "notion_skipped": 0,
        "competition_updated": 0,
        "errors": [],
    }

    # ── STEP 1: DART 공모게시판 스크래핑 ─────────────────────
    logger.info("\n[STEP 1] DART 공모게시판 스크래핑")
    logger.info("필터: 기타법인(title='기타법인') AND 보통주(title='보통주')")
    try:
        raw_list = scraper.fetch_ipo_board()
        stats["dart_scraped"] = len(raw_list)
        logger.info(f"스크래핑 완료: {len(raw_list)}건 (필터 적용 후)")
    except Exception as e:
        logger.error(f"STEP 1 실패: {e}")
        stats["errors"].append(f"STEP1: {e}")
        _save_run_status(stats)
        raise

    # ── STEP 2: 데이터 정제 ──────────────────────────────────
    logger.info("\n[STEP 2] 데이터 정제")
    try:
        ipo_list = parser.clean_and_filter(raw_list)
        stats["dart_filtered"] = len(ipo_list)
        logger.info(f"정제 완료: {len(ipo_list)}건")
    except Exception as e:
        logger.error(f"STEP 2 실패: {e}")
        stats["errors"].append(f"STEP2: {e}")
        _save_run_status(stats)
        raise

    # ── STEP 3: Notion DB 동기화 ─────────────────────────────
    logger.info("\n[STEP 3] Notion DB 동기화 (중복 방지 upsert)")
    logger.info(
        "중복 판단 기준:\n"
        "  1단계: 로컬 캐시 (동일 실행 내 중복)\n"
        "  2단계: Notion 접수번호 쿼리 (영구 중복)\n"
        "  3단계: Notion 종목명 쿼리 (접수번호 없는 경우 폴백)"
    )

    # 로컬 캐시: 이번 실행에서 처리한 접수번호/종목명 집합
    local_cache: set[str] = set()

    for item in ipo_list:
        try:
            result = notion_client.upsert_ipo(item, local_cache)
            if result == "created":
                stats["notion_created"] += 1
            elif result == "updated":
                stats["notion_updated"] += 1
            else:
                stats["notion_skipped"] += 1
        except Exception as e:
            name = item.get("종목명", "?")
            logger.error(f"Notion upsert 실패: {name} | {e}")
            stats["errors"].append(f"STEP3-{name}: {e}")

    logger.info(
        f"Notion 동기화 완료: "
        f"신규={stats['notion_created']}, "
        f"업데이트={stats['notion_updated']}, "
        f"중복skip={stats['notion_skipped']}"
    )

    # ── STEP 4: 경쟁률 보강 (38커뮤니케이션) ────────────────
    logger.info("\n[STEP 4] 기관 수요예측 경쟁률 보강 (38커뮤니케이션)")
    try:
        # 경쟁률이 비어 있는 페이지 목록 조회
        pending = notion_client.query_pending_competition()
        logger.info(f"경쟁률 미수집 종목: {len(pending)}건")

        for page in pending:
            name    = page.get("종목명", "")
            page_id = page.get("id", "")
            if not name or not page_id:
                continue

            rate = scraper_38.fetch_demand_forecast_rate(name)
            if rate is not None:
                ok = notion_client.update_competition_rate(page_id, rate)
                if ok:
                    stats["competition_updated"] += 1
                    logger.info(f"[경쟁률 갱신] {name}: {rate}:1")
            else:
                logger.debug(f"[경쟁률 미확인] {name}: 수요예측 전 또는 정보 없음")

    except Exception as e:
        logger.error(f"STEP 4 실패: {e}")
        stats["errors"].append(f"STEP4: {e}")

    # ── STEP 5: 실행 결과 저장 ───────────────────────────────
    logger.info("\n[STEP 5] 실행 결과 저장")
    _save_run_status(stats)

    # ── 최종 요약 ─────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("실행 완료 요약")
    logger.info(f"  DART 수집: {stats['dart_scraped']}건")
    logger.info(f"  정제 후:   {stats['dart_filtered']}건")
    logger.info(f"  Notion 신규:    {stats['notion_created']}건")
    logger.info(f"  Notion 업데이트: {stats['notion_updated']}건")
    logger.info(f"  Notion 중복skip: {stats['notion_skipped']}건")
    logger.info(f"  경쟁률 갱신:    {stats['competition_updated']}건")
    if stats["errors"]:
        logger.warning(f"  오류 {len(stats['errors'])}건: {stats['errors']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
