import subprocess
from typing import Optional
from dataclasses import dataclass
from .config import ORACLE_CONFIG, MAPPING_TABLE, COLUMN_MAPPING


@dataclass
class ColumnInfo:
    """컬럼 매핑 정보"""
    table_kor: str
    table_eng: str
    col_kor: str
    col_eng: str
    data_type: str
    length: str
    pk: str
    fk: str
    is_mapped: bool = True


@dataclass
class TableInfo:
    """테이블 매핑 정보"""
    table_kor: str
    table_eng: str
    is_mapped: bool = True


class OracleMapper:
    """SQLcl을 사용하여 Oracle DB에서 매핑 정보를 조회하는 클래스"""

    DELIMITER = '|||'  # 구분자 (파이프 3개로 충돌 방지)

    def __init__(self):
        self.sqlcl_path = "/home/ajh428/sqlcl/bin/sql"
        self.conn_string = f"{ORACLE_CONFIG['user']}/{ORACLE_CONFIG['password']}@{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}/{ORACLE_CONFIG['sid']}"

    def connect(self):
        pass

    def disconnect(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _execute_query(self, query: str) -> list:
        """SQLcl로 쿼리 실행 후 결과 반환"""
        sql_script = f"""
SET HEADING OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
{query}
EXIT;
"""
        try:
            result = subprocess.run(
                [self.sqlcl_path, "-S", self.conn_string],
                input=sql_script,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                print(f"SQLcl 오류: {result.stderr}")
                return []

            # 결과 파싱
            rows = []
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                if line and not line.startswith('SQL>'):
                    # 구분자로 분리
                    parts = [p.strip() for p in line.split(self.DELIMITER)]
                    rows.append(parts)

            return rows

        except subprocess.TimeoutExpired:
            print("쿼리 타임아웃")
            return []
        except Exception as e:
            print(f"쿼리 실행 오류: {e}")
            return []

    def get_table_info(self, old_table_name: str) -> Optional[TableInfo]:
        """기존 테이블명으로 신규 테이블 정보 조회"""
        cm = COLUMN_MAPPING
        d = self.DELIMITER
        query = f"""
            SELECT DISTINCT
                {cm['new_table_kor']} || '{d}' || {cm['new_table']}
            FROM {MAPPING_TABLE}
            WHERE UPPER({cm['old_table']}) = UPPER('{old_table_name}')
            AND ROWNUM = 1;
        """

        rows = self._execute_query(query)

        if rows and len(rows[0]) >= 2:
            return TableInfo(
                table_kor=rows[0][0] or '',
                table_eng=rows[0][1] or '',
                is_mapped=True
            )
        return None

    def get_column_info(self, old_table_name: str, old_column_name: str) -> Optional[ColumnInfo]:
        """기존 테이블/컬럼명으로 신규 정보 조회"""
        cm = COLUMN_MAPPING
        d = self.DELIMITER
        query = f"""
            SELECT
                {cm['new_table_kor']} || '{d}' ||
                {cm['new_table']} || '{d}' ||
                {cm['new_column_kor']} || '{d}' ||
                {cm['new_column']} || '{d}' ||
                NVL({cm['data_type']}, ' ') || '{d}' ||
                NVL(TO_CHAR({cm['length']}), ' ') || '{d}' ||
                NVL2({cm['pk']}, 'Y', ' ') || '{d}' ||
                NVL2({cm['fk']}, 'Y', ' ')
            FROM {MAPPING_TABLE}
            WHERE UPPER({cm['old_table']}) = UPPER('{old_table_name}')
              AND UPPER({cm['old_column']}) = UPPER('{old_column_name}')
            AND ROWNUM = 1;
        """

        rows = self._execute_query(query)

        if rows and len(rows[0]) >= 8:
            return ColumnInfo(
                table_kor=rows[0][0].strip(),
                table_eng=rows[0][1].strip(),
                col_kor=rows[0][2].strip(),
                col_eng=rows[0][3].strip(),
                data_type=rows[0][4].strip(),
                length=rows[0][5].strip(),
                pk=rows[0][6].strip(),
                fk=rows[0][7].strip(),
                is_mapped=True
            )
        return None

    def create_unmapped_table_info(self, old_table_name: str) -> TableInfo:
        """매핑되지 않은 테이블 정보 생성"""
        return TableInfo(
            table_kor=f"[매핑없음] {old_table_name}",
            table_eng=old_table_name,
            is_mapped=False
        )

    def create_unmapped_column_info(self, old_table_name: str, old_column_name: str) -> ColumnInfo:
        """매핑되지 않은 컬럼 정보 생성"""
        return ColumnInfo(
            table_kor=f"[매핑없음] {old_table_name}",
            table_eng=old_table_name,
            col_kor=f"[매핑없음] {old_column_name}",
            col_eng=old_column_name,
            data_type='',
            length='',
            pk='',
            fk='',
            is_mapped=False
        )

    def create_derived_table_info(self, alias: str) -> TableInfo:
        """Derived Table (인라인 뷰) 정보 생성"""
        return TableInfo(
            table_kor=f"[인라인뷰] {alias}",
            table_eng=f"[인라인뷰] {alias}",
            is_mapped=False
        )

    def create_derived_column_info(self, alias: str, column_name: str) -> ColumnInfo:
        """Derived Table (인라인 뷰) 컬럼 정보 생성"""
        return ColumnInfo(
            table_kor=f"[인라인뷰] {alias}",
            table_eng=f"[인라인뷰] {alias}",
            col_kor=column_name,
            col_eng=column_name,
            data_type='',
            length='',
            pk='',
            fk='',
            is_mapped=False
        )


def test_connection():
    """연결 테스트"""
    try:
        with OracleMapper() as mapper:
            print("SQLcl 기반 Oracle 연결 테스트...")

            # 샘플 조회
            info = mapper.get_table_info("NBOGUN_JakupSite")
            if info:
                print(f"테이블 매핑 성공: {info.table_eng} ({info.table_kor})")
            else:
                print("테이블 매핑 없음")

            # 컬럼 조회
            col_info = mapper.get_column_info("NBOGUN_JakupSite", "Center")
            if col_info:
                print(f"컬럼 매핑 성공: {col_info.col_eng} ({col_info.col_kor})")
            else:
                print("컬럼 매핑 없음")

    except Exception as e:
        print(f"연결 실패: {e}")


if __name__ == "__main__":
    test_connection()
