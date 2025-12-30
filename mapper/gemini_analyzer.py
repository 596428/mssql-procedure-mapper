import json
import time
import google.generativeai as genai
from typing import Optional
from dataclasses import dataclass, field
from .config import GEMINI_API_KEY, GEMINI_API_KEYS


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


ANALYSIS_PROMPT = """
당신은 MSSQL 프로시저/쿼리 분석 전문가입니다.
주어진 SQL 코드를 분석하여 입력값과 출력값을 구분해주세요.

## 분석 규칙:

### 프로시저 설명 (description)
- 이 프로시저가 무엇을 하는지 설명
- 다음 내용을 포함: 어떤 테이블에서, 어떤 조건으로, 어떤 데이터를 조회/처리하는지
- **IF/ELSE 분기가 있는 경우**: 각 분기 조건과 해당 분기에서 수행하는 작업을 설명
  - 예: "IF @SaupID < 0일 경우 전체 목록 조회, 그렇지 않으면 특정 사업장만 조회"
- 한국어로 3-5문장 정도로 작성

### 입력값 (input_columns)
- 메인 쿼리와 서브쿼리의 WHERE 절에서 프로시저 파라미터(@xxx)와 비교되는 컬럼들
- 예: `WHERE SDate <= @VisitDate` → SDate는 @VisitDate의 입력 컬럼
- 예: `WHERE A.Code = @Code` → Code는 @Code의 입력 컬럼

### 출력값 (output_columns)
- SELECT 절에 나오는 모든 컬럼들
- **중요**: 서브쿼리 결과(AS alias)는 서브쿼리 내부의 원본 테이블과 컬럼을 추적해서 기록
  - 예: `(SELECT ',' + K.Keyword FROM CODE_JidoKeyword K ...) AS Keyword`
    → table: "CODE_JidoKeyword", column: "Keyword"
  - 예: `(SELECT ',' + K1.Name FROM TableA K2 INNER JOIN TableB K1 ...) AS Pyeongga`
    → table: "TableB", column: "Name" (SELECT에서 실제 사용된 컬럼의 테이블)

### 테이블
- FROM, JOIN 절의 모든 테이블 (메인쿼리 + 서브쿼리 전부)
- 별칭(alias)이 있으면 함께 기록
- **Derived Table (인라인 뷰) 구분**:
  - `(SELECT ... UNION ...) AS A` 형태의 서브쿼리로 생성된 가상 테이블은 `is_derived: true`로 표시
  - 실제 물리적 테이블은 `is_derived: false` 또는 생략

## 출력 형식 (JSON):
```json
{
  "description": "프로시저 설명 (어떤 테이블에서, 어떤 조건으로, 어떤 데이터를 가져오는지, IF/ELSE 분기 조건 포함)",
  "parameters": [
    {"name": "@ParamName", "type": "CHAR(10)"}
  ],
  "input_columns": [
    {"table": "테이블명", "column": "컬럼명", "parameter": "@연관파라미터"}
  ],
  "output_columns": [
    {"table": "테이블명", "column": "컬럼명", "is_derived": false}
  ],
  "tables": [
    {"name": "테이블명", "alias": "별칭", "is_derived": false},
    {"name": "DerivedTable", "alias": "A", "is_derived": true}
  ]
}
```

## 중요:
- 반드시 유효한 JSON만 출력하세요
- 마크다운 코드블록 없이 순수 JSON만 출력하세요
- 테이블명에서 스키마(dbo.)는 제거하세요
- output_columns의 table은 반드시 원본 테이블명을 사용 (별칭 A, B가 아닌 실제 테이블명)
- 서브쿼리의 SELECT 컬럼도 원본 테이블까지 추적하여 기록
- 계산식(CASE, ISNULL 등)은 관련 컬럼들을 각각 별도로 기록
- Derived Table(인라인 뷰)의 컬럼은 output_columns에서 is_derived: true로 표시

## 분석할 SQL:
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
        self.model = genai.GenerativeModel('gemini-2.0-flash')

    def _switch_api_key(self):
        """API 키 전환 (429 에러 대응)"""
        if len(self.api_keys) > 1:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            new_key = self.api_keys[self.current_key_index]
            genai.configure(api_key=new_key)
            return True
        return False

    def analyze(self, procedure_text: str) -> AnalysisResult:
        """프로시저 텍스트 분석"""
        prompt = ANALYSIS_PROMPT + procedure_text

        try:
            start_time = time.time()
            # 타임아웃 설정 (60초)
            response = self.model.generate_content(
                prompt,
                request_options={"timeout": 60}
            )
            elapsed_time = time.time() - start_time

            raw_text = response.text.strip()

            # JSON 파싱
            result = self._parse_response(raw_text)
            result.raw_response = raw_text
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
