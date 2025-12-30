import json
import time
import google.generativeai as genai
from typing import Optional
from dataclasses import dataclass, field
from .config import GEMINI_API_KEY, GEMINI_API_KEYS
from .sql_parser import SQLParser


@dataclass
class TableUsage:
    """테이블 사용 정보"""
    name: str
    alias: str = ""
    columns: list = field(default_factory=list)
    is_derived: bool = False  # Derived Table (인라인 뷰) 여부


@dataclass
class ColumnMapping:
    """컬럼 매핑 정보"""
    table: str
    column: str
    parameter: str = ""  # 연관된 파라미터 (입력 컬럼용)
    is_derived: bool = False  # Derived Table 컬럼 여부


@dataclass
class AnalysisResult:
    """프로시저 분석 결과"""
    description: str = ""  # 프로시저 설명
    parameters: list = field(default_factory=list)  # 프로시저 파라미터 목록
    input_columns: list = field(default_factory=list)  # WHERE 조건의 입력 컬럼
    output_columns: list = field(default_factory=list)  # SELECT 절의 출력 컬럼
    tables: list = field(default_factory=list)  # 사용된 테이블 목록
    raw_response: str = ""
    response_time: float = 0.0  # 응답 시간 (초)


# ============================================================
# Phase 1: 프로시저 분석 프롬프트 (자연어 분석에 집중)
# ============================================================
PHASE1_ANALYSIS_PROMPT = """
당신은 MSSQL 프로시저/쿼리 분석 전문가입니다.
SQL 파서가 추출한 기본 정보를 참고하여, SQL 코드를 정밀 분석해주세요.

## SQL 파서 전처리 결과 (참고용):
{parser_hint}

## 분석 순서 (이 순서대로 작성):

### 1. 프로시저 설명 (description)
- 이 프로시저가 무엇을 하는지 설명
- 다음 내용을 포함: 어떤 테이블에서, 어떤 조건으로, 어떤 데이터를 조회/처리하는지
- **IF/ELSE 분기가 있는 경우**: 각 분기 조건과 해당 분기에서 수행하는 작업을 설명
  - 예: "IF @SaupID < 0일 경우 전체 목록 조회, 그렇지 않으면 특정 사업장만 조회"
- 한국어로 3-5문장 정도로 작성

### 2. 파라미터 목록
- 프로시저 선언부의 모든 @파라미터를 나열
- 각 파라미터의 타입과 기본값(있으면)

### 3. 사용 테이블 목록 (파서 결과 보완/수정)
- FROM, JOIN에 사용된 모든 테이블 나열
- 각 테이블의 별칭(alias) 명시
- 임시테이블(#으로 시작)은 [임시]로 표시
- 파서가 놓친 테이블이 있으면 추가

### 4. IF/ELSE 분기 분석
- 각 분기의 조건과 해당 분기에서 실행되는 SELECT문 설명
- 분기가 없으면 "분기 없음" 명시

### 5. 입력 컬럼 (WHERE 조건)
- WHERE 절에서 @파라미터와 비교되는 컬럼들
- 형식: "테이블명.컬럼명 ← @파라미터명"

### 6. 출력 컬럼 (SELECT 절) ★가장 중요★
- 모든 SELECT문의 출력 컬럼을 빠짐없이 나열
- IF/ELSE 분기가 있으면 각 분기별로 구분하여 나열
- 형식: "테이블명.컬럼명" (별칭 A, B 대신 원본 테이블명 사용)
- SELECT * 또는 A.* 형태는 "테이블명.*"로 기록
- CASE, ISNULL 등에 포함된 컬럼도 각각 기록

#### ★ 서브쿼리/임시테이블 컬럼 규칙 (필수):

**1) 서브쿼리 → 원본 테이블 추적**:
- 서브쿼리 결과(별칭)가 아닌, **원본 테이블의 실제 컬럼명**까지 추적
- 예시:
  ```sql
  SELECT CntDr FROM (SELECT DamdangDr, COUNT(*) AS CntDr FROM NBOGUN_Saupja GROUP BY DamdangDr) Dr
  ```
  → 잘못: "CntDr" (서브쿼리 별칭)
  → 올바름: "NBOGUN_Saupja.DamdangDr" (원본 테이블.컬럼)
- 집계함수(COUNT, SUM 등)의 결과는 GROUP BY 컬럼을 기록

**2) 임시테이블(#으로 시작) → 그대로 유지**:
- SELECT INTO로 생성된 임시테이블 컬럼은 **임시테이블명.컬럼명** 그대로 기록
- 예시: `#staff_temp.VisitCnt` → 그대로 "#staff_temp.VisitCnt"
- 임시테이블은 [임시] 또는 is_derived: true로 표시

## 주의사항:
- dbo. 등 스키마 접두사는 제거
- 테이블 별칭(A, B 등) 대신 원본 테이블명 사용
- 서브쿼리 별칭(CntDr, VisitCnt 등)이 아닌 원본 컬럼명(DamdangDr, Visitor 등) 사용
- 파서 결과를 기반으로 하되, SQL을 직접 분석하여 누락된 항목 보완

## 분석할 SQL:
"""

# ============================================================
# Phase 2: JSON 변환 프롬프트
# ============================================================
PHASE2_JSON_PROMPT = """
아래의 프로시저 분석 결과를 JSON 형식으로 변환해주세요.

## 입력된 분석 결과:
{analysis_text}

## 변환 규칙:
1. description: 프로시저 설명을 그대로 전달 (요약하지 말 것)
2. parameters: 파라미터 목록을 배열로 변환
3. input_columns: 입력 컬럼을 배열로 변환 (테이블, 컬럼, 파라미터)
4. output_columns: 출력 컬럼을 배열로 변환 (테이블, 컬럼)
   - 임시테이블/인라인뷰는 is_derived: true
   - 일반 테이블은 is_derived: false
5. tables: 사용 테이블을 배열로 변환
   - 임시테이블/인라인뷰는 is_derived: true
6. 중복된 (테이블, 컬럼) 쌍은 제거

## 출력 형식 (순수 JSON만, 마크다운 없이):
{{
  "description": "프로시저 설명",
  "parameters": [{{"name": "@Param", "type": "CHAR(10)"}}],
  "input_columns": [{{"table": "테이블명", "column": "컬럼명", "parameter": "@파라미터"}}],
  "output_columns": [{{"table": "테이블명", "column": "컬럼명", "is_derived": false}}],
  "tables": [{{"name": "테이블명", "alias": "A", "is_derived": false}}]
}}
"""


class GeminiAnalyzer:
    """Gemini API를 사용한 프로시저 분석기"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or GEMINI_API_KEY
        self.api_keys = GEMINI_API_KEYS if GEMINI_API_KEYS else [self.api_key]
        self.current_key_index = 0

        if not self.api_key:
            raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다.")

        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-3-pro-preview')

    def _switch_api_key(self):
        """API 키 전환 (429 에러 대응)"""
        if len(self.api_keys) > 1:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            new_key = self.api_keys[self.current_key_index]
            genai.configure(api_key=new_key)
            return True
        return False

    def analyze(self, procedure_text: str) -> AnalysisResult:
        """프로시저 텍스트 분석 (3-phase 방식: 파서 → 분석 → JSON)"""
        try:
            start_time = time.time()

            # ========================================
            # Phase 0: SQL 파서 전처리
            # ========================================
            print("  [Phase 0] SQL 파서 전처리 중...")
            parser = SQLParser()
            parse_result = parser.parse(procedure_text)
            parser_hint = parser.to_structured_text(parse_result)
            print(f"  [Phase 0] 완료 (테이블 {len(parse_result.all_tables)}개, 컬럼 {len(parse_result.all_select_columns)}개 추출)")

            # ========================================
            # Phase 1: 프로시저 분석 (자연어 출력)
            # ========================================
            phase1_prompt = PHASE1_ANALYSIS_PROMPT.format(parser_hint=parser_hint) + procedure_text

            print("  [Phase 1] 프로시저 분석 중...")
            phase1_response = self.model.generate_content(
                phase1_prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.7,
                ),
                request_options={"timeout": 120}
            )
            analysis_text = phase1_response.text.strip()
            phase1_time = time.time() - start_time
            print(f"  [Phase 1] 완료 ({phase1_time:.2f}초)")

            # ========================================
            # Phase 2: JSON 변환
            # ========================================
            phase2_prompt = PHASE2_JSON_PROMPT.format(analysis_text=analysis_text)

            print("  [Phase 2] JSON 변환 중...")
            phase2_start = time.time()
            phase2_response = self.model.generate_content(
                phase2_prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.3,  # JSON 변환은 약간 낮은 temperature
                ),
                request_options={"timeout": 120}
            )
            json_text = phase2_response.text.strip()
            phase2_time = time.time() - phase2_start
            print(f"  [Phase 2] 완료 ({phase2_time:.2f}초)")

            elapsed_time = time.time() - start_time

            # JSON 파싱
            result = self._parse_response(json_text)
            result.raw_response = f"=== Phase 1 분석 결과 ===\n{analysis_text}\n\n=== Phase 2 JSON 변환 ===\n{json_text}"
            result.response_time = elapsed_time
            return result

        except Exception as e:
            if "429" in str(e) and self._switch_api_key():
                # 키 전환 후 재시도
                return self.analyze(procedure_text)
            raise

    def _parse_response(self, response_text: str) -> AnalysisResult:
        """Gemini 응답 파싱"""
        # 마크다운 코드블록 제거
        text = response_text
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        text = text.strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류: {e}")
            print(f"원본 응답: {response_text[:500]}")
            return AnalysisResult(raw_response=response_text)

        # 결과 구성
        result = AnalysisResult()

        # 프로시저 설명
        result.description = data.get("description", "")

        # 파라미터
        result.parameters = data.get("parameters", [])

        # 입력 컬럼 (WHERE 조건)
        for col in data.get("input_columns", []):
            result.input_columns.append(ColumnMapping(
                table=col.get("table", ""),
                column=col.get("column", ""),
                parameter=col.get("parameter", ""),
                is_derived=col.get("is_derived", False)
            ))

        # 출력 컬럼 (SELECT 절)
        for col in data.get("output_columns", []):
            result.output_columns.append(ColumnMapping(
                table=col.get("table", ""),
                column=col.get("column", ""),
                is_derived=col.get("is_derived", False)
            ))

        # 테이블 정보
        for t in data.get("tables", []):
            result.tables.append(TableUsage(
                name=t.get("name", ""),
                alias=t.get("alias", ""),
                is_derived=t.get("is_derived", False)
            ))

        return result


def test_analyzer():
    """분석기 테스트"""
    sample = """
    CREATE proc [dbo].[UP_Test]
    (@VisitDate CHAR(10), @Code VARCHAR(3) = '')
    AS
    SELECT A.Code, A.Name, B.SDate, B.EDate
    FROM CODE_Sangtae A
    INNER JOIN CODE_SangtaeJido B ON A.Code = B.Code
    WHERE SDate <= @VisitDate AND EDate >= @VisitDate
      AND (@Code = '' OR A.Code = @Code)
    """

    try:
        analyzer = GeminiAnalyzer()
        result = analyzer.analyze(sample)

        print("=== 분석 결과 ===")
        print(f"응답 시간: {result.response_time:.2f}초")
        print(f"파라미터: {result.parameters}")
        print(f"\n입력 컬럼 ({len(result.input_columns)}개):")
        for col in result.input_columns:
            print(f"  - {col.table}.{col.column} ← {col.parameter}")
        print(f"\n출력 컬럼 ({len(result.output_columns)}개):")
        for col in result.output_columns:
            print(f"  - {col.table}.{col.column}")
        print(f"\n테이블 ({len(result.tables)}개):")
        for t in result.tables:
            print(f"  - {t.name} ({t.alias})")

    except Exception as e:
        print(f"분석 실패: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_analyzer()
