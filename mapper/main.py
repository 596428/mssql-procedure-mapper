#!/usr/bin/env python3
"""
프로시저 테이블/컬럼 매핑 도구
C# → Java 마이그레이션용 테이블/컬럼 명칭 변환
"""
import argparse
import re
import sys
from pathlib import Path
from typing import List, Tuple, Optional

from .gemini_analyzer import GeminiAnalyzer, AnalysisResult
from .oracle_mapper import OracleMapper, TableInfo, ColumnInfo
from .excel_writer import ExcelWriter


def extract_procedure_name(sql_text: str) -> Optional[str]:
    """프로시저 이름 추출 (정규식)"""
    # CREATE PROC [dbo].[프로시저명] 또는 CREATE PROCEDURE dbo.프로시저명
    pattern = r'CREATE\s+PROC(?:EDURE)?\s+(?:\[?\w+\]?\.)?\[?(\w+)\]?'
    match = re.search(pattern, sql_text, re.IGNORECASE)
    return match.group(1) if match else None


def read_procedure_file(filepath: str) -> str:
    """프로시저 파일 읽기"""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")

    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def process_mapping(
    analysis: AnalysisResult,
    mapper: OracleMapper
) -> Tuple[List[TableInfo], List[ColumnInfo], List[TableInfo], List[ColumnInfo]]:
    """분석 결과를 매핑 정보로 변환"""

    input_tables = []
    input_columns = []
    output_tables = []
    output_columns = []

    # 테이블 별칭 → 원본 테이블명 매핑
    alias_to_table = {}
    # Derived Table 여부 추적
    derived_tables = set()
    for t in analysis.tables:
        if t.alias:
            alias_to_table[t.alias] = t.name
        alias_to_table[t.name] = t.name
        if t.is_derived:
            derived_tables.add(t.name)
            if t.alias:
                derived_tables.add(t.alias)

    # 입력 컬럼 처리 (WHERE 조건에서 파라미터와 비교되는 컬럼들)
    input_table_set = set()
    input_derived_set = set()  # Derived Table 별칭 추적
    for in_col in analysis.input_columns:
        tbl = in_col.table
        col = in_col.column

        # 별칭으로 원본 테이블 찾기
        original_table = alias_to_table.get(tbl, tbl)

        # Derived Table 여부 확인
        is_derived = in_col.is_derived or tbl in derived_tables or original_table in derived_tables

        if is_derived:
            # Derived Table (인라인 뷰)는 별도 처리
            alias = tbl if tbl in derived_tables else original_table
            col_info = mapper.create_derived_column_info(alias, col)
            input_derived_set.add(alias)
        else:
            col_info = mapper.get_column_info(original_table, col)
            if not col_info:
                col_info = mapper.create_unmapped_column_info(original_table, col)
            input_table_set.add(original_table)

        # 중복 체크 후 추가
        exists = any(
            c.table_eng == col_info.table_eng and c.col_eng == col_info.col_eng
            for c in input_columns
        )
        if not exists:
            input_columns.append(col_info)

    # 입력 테이블 정보 추가
    for table_name in input_table_set:
        table_info = mapper.get_table_info(table_name)
        if not table_info:
            table_info = mapper.create_unmapped_table_info(table_name)
        input_tables.append(table_info)

    # Derived Table 정보 추가
    for alias in input_derived_set:
        input_tables.append(mapper.create_derived_table_info(alias))

    # 출력 컬럼 처리 (SELECT 절의 컬럼들)
    output_table_set = set()
    output_derived_set = set()  # Derived Table 별칭 추적
    for out_col in analysis.output_columns:
        tbl = out_col.table
        col = out_col.column

        # 별칭으로 원본 테이블 찾기
        original_table = alias_to_table.get(tbl, tbl)

        # Derived Table 여부 확인
        is_derived = out_col.is_derived or tbl in derived_tables or original_table in derived_tables

        if is_derived:
            # Derived Table (인라인 뷰)는 별도 처리
            alias = tbl if tbl in derived_tables else original_table
            col_info = mapper.create_derived_column_info(alias, col)
            output_derived_set.add(alias)
        else:
            col_info = mapper.get_column_info(original_table, col)
            if not col_info:
                col_info = mapper.create_unmapped_column_info(original_table, col)
            output_table_set.add(original_table)

        # 중복 체크 후 추가
        exists = any(
            c.table_eng == col_info.table_eng and c.col_eng == col_info.col_eng
            for c in output_columns
        )
        if not exists:
            output_columns.append(col_info)

    # 출력 테이블 정보 추가
    for table_name in output_table_set:
        table_info = mapper.get_table_info(table_name)
        if not table_info:
            table_info = mapper.create_unmapped_table_info(table_name)
        output_tables.append(table_info)

    # Derived Table 정보 추가
    for alias in output_derived_set:
        output_tables.append(mapper.create_derived_table_info(alias))

    return input_tables, input_columns, output_tables, output_columns


def run(input_file: str, output_file: str):
    """메인 실행"""
    print(f"=== 프로시저 매핑 도구 ===")
    print(f"입력: {input_file}")
    print()

    # 1. 프로시저 파일 읽기
    print("[1/5] 프로시저 파일 읽기...")
    procedure_text = read_procedure_file(input_file)
    print(f"  - 읽은 텍스트 길이: {len(procedure_text)} 자")

    # 프로시저 이름 추출
    proc_name = extract_procedure_name(procedure_text)
    if proc_name:
        print(f"  - 프로시저 이름: {proc_name}")
    else:
        print("  - 프로시저 이름: (추출 실패)")
        proc_name = "Unknown"

    # 출력 경로 설정 (output/excel, output/csv)
    project_root = Path(__file__).parent.parent
    excel_dir = project_root / "output" / "excel"
    csv_dir = project_root / "output" / "csv"
    excel_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    # 파일명 생성
    excel_file = excel_dir / f"output_{proc_name}.xlsx"
    csv_input_file = csv_dir / f"{proc_name}_입력.csv"
    csv_output_file = csv_dir / f"{proc_name}_출력.csv"

    print(f"출력 경로: {excel_dir}")

    # 2. Gemini API로 분석
    print("[2/5] Gemini API로 프로시저 분석 중...")
    analyzer = GeminiAnalyzer()
    analysis = analyzer.analyze(procedure_text)
    print(f"  - 응답 시간: {analysis.response_time:.2f}초")
    print(f"  - 발견된 테이블: {len(analysis.tables)}개")
    print(f"  - 발견된 파라미터: {[p.get('name', p) if isinstance(p, dict) else p for p in analysis.parameters]}")
    print(f"  - 입력 컬럼 (WHERE): {len(analysis.input_columns)}개")
    print(f"  - 출력 컬럼 (SELECT): {len(analysis.output_columns)}개")
    for t in analysis.tables:
        print(f"    > {t.name} ({t.alias})")

    # 3. Description 출력
    print("[3/5] 프로시저 설명:")
    if analysis.description:
        print(f"  {analysis.description[:200]}..." if len(analysis.description) > 200 else f"  {analysis.description}")
    else:
        print("  (설명 없음)")

    # 4. Oracle DB에서 매핑 조회
    print("[4/5] Oracle DB에서 매핑 정보 조회 중...")
    with OracleMapper() as mapper:
        input_tables, input_columns, output_tables, output_columns = process_mapping(
            analysis, mapper
        )

    # 매핑 통계
    unmapped_tables = set()
    for t in input_tables + output_tables:
        if not t.is_mapped:
            unmapped_tables.add(t.table_eng)
    for c in input_columns + output_columns:
        if not c.is_mapped:
            unmapped_tables.add(c.table_eng)

    print(f"  - 입력 테이블: {len(set((t.table_eng, t.table_kor) for t in input_tables))}개")
    print(f"  - 입력 컬럼: {len(input_columns)}개")
    print(f"  - 출력 테이블: {len(set((t.table_eng, t.table_kor) for t in output_tables))}개")
    print(f"  - 출력 컬럼: {len(output_columns)}개")
    if unmapped_tables:
        print(f"  - 매핑 없는 테이블/뷰: {len(unmapped_tables)}개")
        for t in sorted(unmapped_tables):
            print(f"    ! {t}")

    # 5. Excel/CSV 출력
    print("[5/5] Excel/CSV 파일 생성 중...")
    writer = ExcelWriter()
    writer.create_description_sheet(proc_name, analysis.description, analysis.parameters)
    writer.create_sheet("입력", input_tables, input_columns)
    writer.create_sheet("출력", output_tables, output_columns)
    writer.save(str(excel_file))

    # CSV 출력
    writer.save_csv(str(csv_input_file), input_tables, input_columns, "입력")
    writer.save_csv(str(csv_output_file), output_tables, output_columns, "출력")

    print()
    print("=== 완료 ===")
    print(f"Excel: {excel_file}")
    print(f"CSV: {csv_input_file.name}, {csv_output_file.name}")


def main():
    parser = argparse.ArgumentParser(
        description='프로시저 테이블/컬럼 매핑 도구'
    )
    parser.add_argument(
        '--input', '-i',
        default='input.txt',
        help='입력 프로시저 파일 (기본값: input.txt)'
    )
    parser.add_argument(
        '--output', '-o',
        default='output.xlsx',
        help='출력 Excel 파일 (기본값: output.xlsx)'
    )

    args = parser.parse_args()

    try:
        run(args.input, args.output)
    except FileNotFoundError as e:
        print(f"오류: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
