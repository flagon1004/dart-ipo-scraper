# Notion DB 초기 셋업 가이드

## 1. Notion DB 생성

1. Notion에서 새 페이지 생성 → "데이터베이스 > 테이블" 선택
2. 아래 표를 참고하여 속성(Property) 추가

| 속성 이름 | 타입        | 설명                          |
|----------|-------------|-------------------------------|
| 종목명   | 제목(Title) | 기본 제목 필드 (변경 불필요)    |
| 청약기한 | 날짜(Date)  | 청약 종료일                    |
| 공모가   | 숫자(Number) | 형식: 숫자 (₩ 설정 선택)       |
| 경쟁률   | 숫자(Number) | 기관 수요예측 경쟁률            |
| 상장일자 | 날짜(Date)  | 상장 예정/확정일                |
| 주관사   | 텍스트(Text) | 대표 주관 증권사                |
| 접수번호 | 텍스트(Text) | DART 접수번호 (중복 방지용)      |

> ⚠️ 속성 이름은 config.py의 NOTION_FIELDS와 정확히 일치해야 합니다.

## 2. DB ID 확인

Notion DB URL 형식:
```
https://www.notion.so/{워크스페이스명}/{32자리-hex-ID}?v=...
```
`?v=` 앞의 32자리 문자열이 `NOTION_DB_ID`입니다.

예시:
```
https://www.notion.so/myworkspace/1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d?v=...
                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                   이 부분이 DB ID
```

## 3. Notion Integration 생성

1. https://www.notion.so/my-integrations 접속
2. "새 통합" 클릭
3. 이름: `DART IPO Scraper`
4. 연결된 워크스페이스: DB가 있는 워크스페이스 선택
5. 기능 설정:
   - ✅ 콘텐츠 읽기
   - ✅ 콘텐츠 업데이트
   - ✅ 콘텐츠 삽입
6. "제출" → 시크릿 키 복사 (한 번만 표시되므로 반드시 저장)

## 4. DB에 Integration 연결

1. Notion DB 페이지 열기
2. 오른쪽 상단 `...` 클릭 → "연결" → `DART IPO Scraper` 선택
3. "액세스 허용" 클릭

## 5. GitHub Secrets 등록

GitHub 저장소 → Settings → Secrets and variables → Actions → "New repository secret"

| Secret 이름      | 값                              |
|------------------|---------------------------------|
| `NOTION_API_KEY` | Notion Integration 시크릿 토큰   |
| `NOTION_DB_ID`   | Notion DB의 32자리 ID            |
