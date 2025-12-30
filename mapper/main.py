#!/usr/bin/env python3
"""
프로시저 테이블/컬럼 매핑 도구
C# → Java 마이그레이션용 테이블/컬럼 명칭 변환
"""
import argparse
import re
import sys
import time
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
    """분석 결과를 매핑 정보로 변환 (배치 쿼리 최적화)"""

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

    # ========================================
    # 1단계: 필요한 모든 (테이블, 컬럼) 쌍 수집
    # ========================================
    column_requests = []  # [(table, column), ...]
    table_requests = set()  # {table_name, ...}

    input_derived_set = set()
    output_derived_set = set()

    # 입력 컬럼 요청 수집
    input_column_keys = []  # [(original_table, col, is_derived, alias_if_derived), ...]
    for in_col in analysis.input_columns:
        tbl = in_col.table
        col = in_col.column
        original_table = alias_to_table.get(tbl, tbl)
        is_derived = in_col.is_derived or tbl in derived_tables or original_table in derived_tables

        if is_derived:
            alias = tbl if tbl in derived_tables else original_table
            input_column_keys.append((original_table, col, True, alias))
            input_derived_set.add(alias)
        else:
            input_column_keys.append((original_table, col, False, None))
            column_requests.append((original_table, col))
            table_requests.add(original_table)

    # 출력 컬럼 요청 수집
    output_column_keys = []
    star_column_tables = []  # SELECT * 처리용: [(original_table, is_derived, alias), ...]

    for out_col in analysis.output_columns:
        tbl = out_col.table
        col = out_col.column
        original_table = alias_to_table.get(tbl, tbl)
        is_derived = out_col.is_derived or tbl in derived_tables or original_table in derived_tables

        if is_derived:
            alias = tbl if tbl in derived_tables else original_table
            output_column_keys.append((original_table, col, True, alias))
            output_derived_set.add(alias)
        elif col == '*':
            # SELECT * 컬럼은 별도 처리 (해당 테이블의 모든 컬럼 조회 필요)
            star_column_tables.append((original_table, False, None))
            table_requests.add(original_table)
        else:
            output_column_keys.append((original_table, col, False, None))
            column_requests.append((original_table, col))
            table_requests.add(original_table)

    # ========================================
    # 2단계: 배치 쿼리 실행 (1-2회 SQLcl 호출)
    # ========================================
    # 중복 제거
    unique_column_requests = list(set(column_requests))
    unique_table_requests = list(table_requests)

    # 배치 조회
    column_cache = mapper.get_columns_batch(unique_column_requests)
    table_cache = mapper.get_tables_batch(unique_table_requests)

    # ========================================
    # 3단계: 캐시에서 결과 조회하여 ColumnInfo 생성
    # ========================================

    # 입력 컬럼 처리
    input_table_set = set()
    for original_table, col, is_derived, alias in input_column_keys:
        if is_derived:
            col_info = mapper.create_derived_column_info(alias, col)
        else:
            # 캐시에서 조회 (키는 대문자)
            key = (original_table.upper(), col.upper())
            col_info = column_cache.get(key)
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
        key = table_name.upper()
        table_info = table_cache.get(key)
        if not table_info:
            table_info = mapper.create_unmapped_table_info(table_name)
        input_tables.append(table_info)

    # Derived Table 정보 추가
    for alias in input_derived_set:
        input_tables.append(mapper.create_derived_table_info(alias))

    # 출력 컬럼 처리
    output_table_set = set()

    # SELECT * 컬럼 확장 처리
    for original_table, is_derived, alias in star_column_tables:
        if is_derived:
            # Derived 테이블의 *는 그대로 처리
            col_info = mapper.create_derived_column_info(alias, '*')
            output_columns.append(col_info)
        else:
            # 일반 테이블: 해당 테이블의 모든 컬럼 조회
            all_cols = mapper.get_all_columns_for_table(original_table)
            if all_cols:
                for col_info in all_cols:
                    # 중복 체크 후 추가
                    exists = any(
                        c.table_eng == col_info.table_eng and c.col_eng == col_info.col_eng
                        for c in output_columns
                    )
                    if not exists:
                        output_columns.append(col_info)
                output_table_set.add(original_table)
            else:
                # 매핑 테이블에 없으면 [매핑없음] *로 추가
                col_info = mapper.create_unmapped_column_info(original_table, '*')
                output_columns.append(col_info)
                output_table_set.add(original_table)

    # 개별 컬럼 처리
    for original_table, col, is_derived, alias in output_column_keys:
        if is_derived:
            col_info = mapper.create_derived_column_info(alias, col)
        else:
            key = (original_table.upper(), col.upper())
            col_info = column_cache.get(key)
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
        key = table_name.upper()
        table_info = table_cache.get(key)
        if not table_info:
            table_info = mapper.create_unmapped_table_info(table_name)
        output_tables.append(table_info)

    # Derived Table 정보 추가
    for alias in output_derived_set:
        output_tables.append(mapper.create_derived_table_info(alias))

    return input_tables, input_columns, output_tables, output_columns


def run(input_file: str, output_file: str):
    """메인 실행"""
    total_start = time.time()
    step_times = {}

    print(f"=== 프로시저 매핑 도구 ===")
    print(f"입력: {input_file}")
    print()

    # 1. 프로시저 파일 읽기
    step_start = time.time()
    print("[1/5] 프로시저 파일 읽기...")
    procedure_text = read_procedure_file(input_file)
    print(f"  - 읽은 텍스트 길이: {len(procedure_text)} 자")
    step_times['1_파일읽기'] = time.time() - step_start

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
    step_start = time.time()
    print("[2/5] Gemini API로 프로시저 분석 중...")
    analyzer = GeminiAnalyzer()
    analysis = analyzer.analyze(procedure_text)
    step_times['2_Gemini분석'] = time.time() - step_start
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
    step_start = time.time()
    print("[4/5] Oracle DB에서 매핑 정보 조회 중...")
    with OracleMapper() as mapper:
        input_tables, input_columns, output_tables, output_columns = process_mapping(
            analysis, mapper
        )
    step_times['4_DB매핑조회'] = time.time() - step_start

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
    step_start = time.time()
    print("[5/5] Excel/CSV 파일 생성 중...")
    writer = ExcelWriter()
    writer.create_description_sheet(proc_name, analysis.description, analysis.parameters)
    writer.create_sheet("입력", input_tables, input_columns)
    writer.create_sheet("출력", output_tables, output_columns)
    writer.save(str(excel_file))

    # CSV 출력
    writer.save_csv(str(csv_input_file), input_tables, input_columns, "입력")
    writer.save_csv(str(csv_output_file), output_tables, output_columns, "출력")
    step_times['5_파일저장'] = time.time() - step_start

    # 총 실행 시간 계산
    total_time = time.time() - total_start

    print()
    print("=== 완료 ===")
    print(f"Excel: {excel_file}")
    print(f"CSV: {csv_input_file.name}, {csv_output_file.name}")
    print()
    print("=== 실행 시간 ===")
    for step, elapsed in step_times.items():
        print(f"  {step}: {elapsed:.2f}초")
    print(f"  ----------------")
    print(f"  총 실행 시간: {total_time:.2f}초")


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
