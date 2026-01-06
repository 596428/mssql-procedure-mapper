"""
MSSQL 프로시저 본문 조회 모듈
"""
import pymssql
from typing import Optional
from .config import MSSQL_CONFIG


class MSSQLReader:
    """MSSQL에서 프로시저 본문을 조회하는 클래스"""

    def __init__(self):
        self.config = MSSQL_CONFIG
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """MSSQL 연결"""
        self.conn = pymssql.connect(
            server=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"]
        )

    def close(self):
        """연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_procedure_definition(self, proc_name: str) -> Optional[str]:
        """
        프로시저 본문 조회

        Args:
            proc_name: 프로시저 이름 (예: UP_NBOGUN_PlanEduList)

        Returns:
            프로시저 본문 (CREATE PROC ... 전체)
            없으면 None
        """
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT m.definition
            FROM sys.procedures p
            JOIN sys.sql_modules m ON p.object_id = m.object_id
            WHERE p.name = %s
        """, (proc_name,))

        row = cursor.fetchone()
        return row[0] if row else None

    def procedure_exists(self, proc_name: str) -> bool:
        """프로시저 존재 여부 확인"""
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT COUNT(*)
            FROM sys.procedures
            WHERE name = %s
        """, (proc_name,))

        row = cursor.fetchone()
        return row[0] > 0


def get_procedure_from_db(proc_name: str) -> Optional[str]:
    """
    프로시저 이름으로 DB에서 본문 조회 (헬퍼 함수)

    Args:
        proc_name: 프로시저 이름

    Returns:
        프로시저 본문 또는 None
    """
    with MSSQLReader() as reader:
        return reader.get_procedure_definition(proc_name)


if __name__ == "__main__":
    # 테스트
    import sys

    proc_name = sys.argv[1] if len(sys.argv) > 1 else "UP_NBOGUN_PlanEduList"

    print(f"프로시저 조회: {proc_name}")
    print("-" * 50)

    with MSSQLReader() as reader:
        if reader.procedure_exists(proc_name):
            definition = reader.get_procedure_definition(proc_name)
            print(f"본문 길이: {len(definition)} 자")
            print("-" * 50)
            print(definition[:1000])
            if len(definition) > 1000:
                print(f"\n... ({len(definition) - 1000}자 생략)")
        else:
            print(f"프로시저 '{proc_name}'을(를) 찾을 수 없습니다.")
