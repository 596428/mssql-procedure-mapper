"""
SQL Parser 전처리 모듈
- sqlglot을 사용하여 MSSQL 프로시저를 파싱
- 테이블, 컬럼, 파라미터 정보를 구조화하여 추출
"""

import re
import sqlglot
from sqlglot import exp
from dataclasses import dataclass, field
from typing import List, Dict, Set, Tuple, Optional


@dataclass
class ParsedTable:
    """파싱된 테이블 정보"""
    name: str
    alias: str = ""
    is_temp: bool = False  # 임시테이블 (#으로 시작)
    branch: str = ""  # 소속 분기 (IF @Gubun = '1' 등)


@dataclass
class ParsedColumn:
    """파싱된 컬럼 정보"""
    table: str  # 테이블명 또는 별칭
    column: str
    is_select: bool = False  # SELECT 절 컬럼 여부
    is_where: bool = False   # WHERE 절 컬럼 여부
    parameter: str = ""      # 연관 파라미터 (@xxx)
    branch: str = ""         # 소속 분기


@dataclass
class ParsedBranch:
    """IF/ELSE 분기 정보"""
    condition: str  # 예: "@Gubun = '1'"
    tables: List[ParsedTable] = field(default_factory=list)
    select_columns: List[ParsedColumn] = field(default_factory=list)
    where_columns: List[ParsedColumn] = field(default_factory=list)


@dataclass
class ParseResult:
    """전체 파싱 결과"""
    procedure_name: str = ""
    parameters: List[Dict] = field(default_factory=list)
    branches: List[ParsedBranch] = field(default_factory=list)
    all_tables: List[ParsedTable] = field(default_factory=list)
    all_select_columns: List[ParsedColumn] = field(default_factory=list)
    all_where_columns: List[ParsedColumn] = field(default_factory=list)
    alias_map: Dict[str, str] = field(default_factory=dict)  # 별칭 → 테이블명


class SQLParser:
    """MSSQL 프로시저 파서"""

    def __init__(self):
        self.alias_map = {}  # 별칭 → 원본 테이블명 매핑

    def parse(self, sql_text: str) -> ParseResult:
        """프로시저 전체 파싱"""
        result = ParseResult()

        # 1. 프로시저명 추출
        result.procedure_name = self._extract_proc_name(sql_text)

        # 2. 파라미터 추출
        result.parameters = self._extract_parameters(sql_text)

        # 3. IF/ELSE 분기 분리
        branches = self._split_branches(sql_text)

        # 4. 각 분기별 파싱
        for branch_condition, branch_sql in branches:
            branch = self._parse_branch(branch_condition, branch_sql)
            result.branches.append(branch)

            # 전체 목록에도 추가
            result.all_tables.extend(branch.tables)
            result.all_select_columns.extend(branch.select_columns)
            result.all_where_columns.extend(branch.where_columns)

        # 5. 별칭 맵 저장
        result.alias_map = self.alias_map.copy()

        return result

    def _extract_proc_name(self, sql: str) -> str:
        """프로시저명 추출"""
        match = re.search(r'CREATE\s+PROC(?:EDURE)?\s+\[?(\w+)\]?\.\[?(\w+)\]?', sql, re.IGNORECASE)
        if match:
            return match.group(2)
        return ""

    def _extract_parameters(self, sql: str) -> List[Dict]:
        """파라미터 목록 추출"""
        params = []
        # 프로시저 선언부에서 파라미터 추출
        param_pattern = r'@(\w+)\s+([\w\(\),\s]+?)(?:\s*=\s*([^\s,\)]+))?(?=\s*,|\s*\)|$)'

        # AS 이전 부분에서만 검색
        header_match = re.search(r'(CREATE\s+PROC.*?)\bAS\b', sql, re.IGNORECASE | re.DOTALL)
        if header_match:
            header = header_match.group(1)
            for match in re.finditer(param_pattern, header):
                params.append({
                    "name": f"@{match.group(1)}",
                    "type": match.group(2).strip(),
                    "default": match.group(3) if match.group(3) else ""
                })
        return params

    def _split_branches(self, sql: str) -> List[Tuple[str, str]]:
        """IF/ELSE 분기 분리"""
        branches = []

        # IF @Gubun = 'X' 패턴으로 분리
        pattern = r"(?:ELSE\s+)?IF\s+(@\w+)\s*=\s*'?(\w+)'?"

        parts = re.split(pattern, sql, flags=re.IGNORECASE)

        if len(parts) == 1:
            # 분기 없음 - 전체를 하나의 분기로
            branches.append(("", sql))
        else:
            # 첫 부분 (프로시저 헤더)
            i = 1
            while i < len(parts):
                if i + 2 < len(parts):
                    param = parts[i]      # @Gubun
                    value = parts[i + 1]  # '1', '2' 등
                    body = parts[i + 2]   # 해당 분기 SQL
                    condition = f"{param} = '{value}'"
                    branches.append((condition, body))
                i += 3

        # 분기가 없으면 전체 SQL 반환
        if not branches:
            branches.append(("", sql))

        return branches

    def _parse_branch(self, condition: str, sql: str) -> ParsedBranch:
        """단일 분기 파싱"""
        branch = ParsedBranch(condition=condition)

        # SQL 전처리 (힌트 제거 등)
        sql = self._preprocess_sql(sql)

        # SELECT 문 추출
        select_statements = self._extract_select_statements(sql)

        for select_sql in select_statements:
            # sqlglot으로 파싱 시도
            try:
                parsed = sqlglot.parse_one(select_sql, dialect="tsql")

                # 테이블 추출
                tables = self._extract_tables_from_ast(parsed, condition)
                branch.tables.extend(tables)

                # SELECT 컬럼 추출
                select_cols = self._extract_select_columns_from_ast(parsed, condition)
                branch.select_columns.extend(select_cols)

                # WHERE 컬럼 추출
                where_cols = self._extract_where_columns_from_ast(parsed, condition)
                branch.where_columns.extend(where_cols)

            except Exception as e:
                # sqlglot 파싱 실패시 정규식 폴백
                tables = self._extract_tables_regex(select_sql, condition)
                branch.tables.extend(tables)

                select_cols = self._extract_select_columns_regex(select_sql, condition)
                branch.select_columns.extend(select_cols)

        return branch

    def _extract_select_statements(self, sql: str) -> List[str]:
        """SQL에서 SELECT 문 추출"""
        statements = []

        # 간단한 SELECT 문 추출 (중첩 고려 안함)
        pattern = r'\bSELECT\b.*?(?=\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bIF\b|\bELSE\b|\bEND\b|$)'

        for match in re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL):
            stmt = match.group(0).strip()
            if stmt:
                statements.append(stmt)

        return statements if statements else [sql]

    def _preprocess_sql(self, sql: str) -> str:
        """SQL 전처리 - MSSQL 힌트 제거 등"""
        # with (nolock), with (readuncommitted) 등 테이블 힌트 제거
        sql = re.sub(r'\s+with\s*\([^)]+\)', '', sql, flags=re.IGNORECASE)
        return sql

    def _extract_tables_from_ast(self, ast, branch: str) -> List[ParsedTable]:
        """AST에서 테이블 추출"""
        tables = []

        for table in ast.find_all(exp.Table):
            name = table.name
            alias = table.alias if table.alias else ""

            # dbo. 제거
            name = re.sub(r'^dbo\.', '', name, flags=re.IGNORECASE)

            # SQL 키워드가 alias로 잡힌 경우 제외
            if alias and alias.upper() in ('WHERE', 'WITH', 'ON', 'AND', 'OR', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS'):
                alias = ""

            is_temp = name.startswith('#')

            tables.append(ParsedTable(
                name=name,
                alias=alias,
                is_temp=is_temp,
                branch=branch
            ))

            # 별칭 맵 업데이트
            if alias:
                self.alias_map[alias] = name

        return tables

    def _extract_select_columns_from_ast(self, ast, branch: str) -> List[ParsedColumn]:
        """AST에서 SELECT 컬럼 추출"""
        columns = []

        # SELECT 절의 컬럼들
        for select in ast.find_all(exp.Select):
            for expr in select.expressions:
                cols = self._extract_column_refs(expr, branch, is_select=True)
                columns.extend(cols)

        return columns

    def _extract_where_columns_from_ast(self, ast, branch: str) -> List[ParsedColumn]:
        """AST에서 WHERE 컬럼 추출"""
        columns = []

        for where in ast.find_all(exp.Where):
            # 파라미터(@xxx)와 비교되는 컬럼 찾기
            for eq in where.find_all(exp.EQ):
                left = eq.left
                right = eq.right

                param = None
                col_expr = None

                # @파라미터 찾기
                if isinstance(right, exp.Parameter) or (hasattr(right, 'name') and str(right).startswith('@')):
                    param = str(right)
                    col_expr = left
                elif isinstance(left, exp.Parameter) or (hasattr(left, 'name') and str(left).startswith('@')):
                    param = str(left)
                    col_expr = right

                if param and col_expr:
                    cols = self._extract_column_refs(col_expr, branch, is_where=True, parameter=param)
                    columns.extend(cols)

        return columns

    def _extract_column_refs(self, expr, branch: str, is_select=False, is_where=False, parameter="") -> List[ParsedColumn]:
        """표현식에서 컬럼 참조 추출"""
        columns = []

        if isinstance(expr, exp.Column):
            table = expr.table if expr.table else ""
            column = expr.name

            # 별칭을 원본 테이블명으로 변환
            if table in self.alias_map:
                table = self.alias_map[table]

            columns.append(ParsedColumn(
                table=table,
                column=column,
                is_select=is_select,
                is_where=is_where,
                parameter=parameter,
                branch=branch
            ))

        elif isinstance(expr, exp.Star):
            # SELECT * 또는 A.*
            table = expr.table if hasattr(expr, 'table') and expr.table else ""
            if table in self.alias_map:
                table = self.alias_map[table]

            columns.append(ParsedColumn(
                table=table,
                column="*",
                is_select=is_select,
                branch=branch
            ))

        else:
            # 재귀적으로 하위 표현식 탐색
            for child in expr.iter_expressions() if hasattr(expr, 'iter_expressions') else []:
                columns.extend(self._extract_column_refs(child, branch, is_select, is_where, parameter))

        return columns

    def _extract_tables_regex(self, sql: str, branch: str) -> List[ParsedTable]:
        """정규식으로 테이블 추출 (폴백)"""
        tables = []

        # FROM/JOIN 패턴 - #임시테이블 포함
        pattern = r'(?:FROM|JOIN)\s+\[?(#?\w+)\]?(?:\s+(?:AS\s+)?([A-Za-z]\w*))?'

        for match in re.finditer(pattern, sql, re.IGNORECASE):
            name = match.group(1)
            alias = match.group(2) if match.group(2) else ""

            # dbo. 제거
            name = re.sub(r'^dbo\.', '', name, flags=re.IGNORECASE)

            # SQL 키워드가 alias로 잡힌 경우 제외
            if alias and alias.upper() in ('WHERE', 'WITH', 'ON', 'AND', 'OR', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS'):
                alias = ""

            tables.append(ParsedTable(
                name=name,
                alias=alias,
                is_temp=name.startswith('#'),
                branch=branch
            ))

            if alias:
                self.alias_map[alias] = name

        return tables

    def _extract_select_columns_regex(self, sql: str, branch: str) -> List[ParsedColumn]:
        """정규식으로 SELECT 컬럼 추출 (폴백)"""
        columns = []

        # SELECT 절 추출
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns

        select_clause = select_match.group(1)

        # A.Column 또는 Column 패턴
        pattern = r'(\w+)\.(\w+|\*)'

        for match in re.finditer(pattern, select_clause):
            table = match.group(1)
            column = match.group(2)

            # 별칭을 원본 테이블명으로 변환
            if table in self.alias_map:
                table = self.alias_map[table]

            columns.append(ParsedColumn(
                table=table,
                column=column,
                is_select=True,
                branch=branch
            ))

        return columns

    def to_structured_text(self, result: ParseResult) -> str:
        """파싱 결과를 구조화된 텍스트로 변환 (LLM 입력용)"""
        lines = []

        lines.append("## SQL 파서 전처리 결과")
        lines.append("")

        # 프로시저명
        if result.procedure_name:
            lines.append(f"### 프로시저명: {result.procedure_name}")
            lines.append("")

        # 파라미터
        if result.parameters:
            lines.append("### 파라미터")
            for p in result.parameters:
                default = f" = {p['default']}" if p.get('default') else ""
                lines.append(f"- {p['name']}: {p['type']}{default}")
            lines.append("")

        # 테이블 목록 (중복 제거)
        unique_tables = {}
        for t in result.all_tables:
            key = t.name
            if key not in unique_tables:
                unique_tables[key] = t

        if unique_tables:
            lines.append("### 사용 테이블")
            for name, t in unique_tables.items():
                temp_mark = " [임시테이블]" if t.is_temp else ""
                alias_info = f" (별칭: {t.alias})" if t.alias else ""
                lines.append(f"- {name}{alias_info}{temp_mark}")
            lines.append("")

        # 분기별 SELECT 컬럼
        if result.branches:
            lines.append("### 분기별 출력 컬럼 (SELECT)")
            for branch in result.branches:
                if branch.condition:
                    lines.append(f"\n**[{branch.condition}]**")
                else:
                    lines.append("\n**[기본]**")

                seen = set()
                for col in branch.select_columns:
                    key = (col.table, col.column)
                    if key not in seen:
                        seen.add(key)
                        table = col.table if col.table else "?"
                        lines.append(f"- {table}.{col.column}")
            lines.append("")

        # WHERE 조건 컬럼
        where_cols = [c for c in result.all_where_columns if c.parameter]
        if where_cols:
            lines.append("### 입력 컬럼 (WHERE 조건)")
            seen = set()
            for col in where_cols:
                key = (col.table, col.column, col.parameter)
                if key not in seen:
                    seen.add(key)
                    table = col.table if col.table else "?"
                    lines.append(f"- {table}.{col.column} ← {col.parameter}")
            lines.append("")

        return "\n".join(lines)


def test_parser():
    """파서 테스트"""
    with open("input.txt", "r", encoding="utf-8") as f:
        sql = f.read()

    parser = SQLParser()
    result = parser.parse(sql)

    print("=== 파싱 결과 ===")
    print(f"프로시저명: {result.procedure_name}")
    print(f"파라미터: {len(result.parameters)}개")
    print(f"분기 수: {len(result.branches)}개")
    print(f"테이블 수: {len(result.all_tables)}개")
    print(f"SELECT 컬럼: {len(result.all_select_columns)}개")
    print(f"WHERE 컬럼: {len(result.all_where_columns)}개")

    print("\n=== 구조화된 텍스트 ===")
    print(parser.to_structured_text(result))


if __name__ == "__main__":
    test_parser()
