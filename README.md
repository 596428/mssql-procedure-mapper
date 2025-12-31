# MSSQL Procedure Mapper

MSSQL 프로시저를 분석하여 테이블/컬럼 매핑 정보를 추출하는 도구입니다.
C# → Java 마이그레이션 시 테이블/컬럼 명칭 변환을 지원합니다.

## 주요 기능

- **프로시저 분석**: Gemini API를 활용한 SQL 프로시저 자동 분석
- **테이블/컬럼 매핑**: Oracle DB에서 기존↔신규 테이블/컬럼 매핑 정보 조회
- **SQL Parser**: sqlglot 기반 테이블 추출 (with nolock 힌트, #임시테이블 지원)
- **결과 출력**: Excel (.xlsx) 및 CSV 형식으로 결과 저장

## 설치

```bash
pip install -r requirements.txt
```

## 설정

1. `mapper/config.example.py`를 `mapper/config.py`로 복사
2. Oracle 접속 정보 수정
3. `.env` 파일에 Gemini API 키 설정:

```
GEMINI_API_KEY_1=your_api_key_here
GEMINI_API_KEY_2=your_backup_key_here
```

## 사용법

1. `input.txt`에 분석할 MSSQL 프로시저 붙여넣기
2. 실행:

```bash
python -m mapper.main
```

3. 결과 확인:
   - `output/excel/` - Excel 파일
   - `output/csv/` - CSV 파일 (입력/출력 분리)

## 출력 형식

### 테이블 정보
| 관련테이블 한글명 | 관련테이블 영문명 | 매핑여부 |
|------------------|------------------|----------|
| 방문일 | TN_HCM_VISTDY | O |

### 항목 정보
| 테이블 한글명 | 테이블 영문명 | 항목 한글명 | 항목 영문명 | 유형 | 길이 | PK | FK |
|--------------|--------------|------------|------------|------|------|----|----|
| 방문일 | TN_HCM_VISTDY | 센터코드 | CNTR_CD | CHAR | 2 | Y | |

## 프로젝트 구조

```
mapper/
├── main.py           # 메인 실행 파일
├── gemini_analyzer.py # Gemini API 분석기
├── sql_parser.py     # SQL 파서 (테이블 추출)
├── oracle_mapper.py  # Oracle DB 매핑 조회
├── excel_writer.py   # Excel/CSV 출력
└── config.py         # 설정 (gitignore)
```

## 제한사항

- EXEC로 호출되는 하위 프로시저 내부는 분석되지 않음
- 매우 긴 프로시저(4만자 이상)는 분석 품질이 저하될 수 있음
- SELECT * 구문은 Oracle 매핑 테이블 기준으로 확장됨
