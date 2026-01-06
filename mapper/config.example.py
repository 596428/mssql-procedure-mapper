import os
from pathlib import Path

# .env 파일 로드
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

# Oracle 접속정보 (실제 값으로 변경 필요)
ORACLE_CONFIG = {
    "host": "your-oracle-host.com",
    "port": 1521,
    "sid": "YOUR_SID",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD"
}

# MSSQL 접속정보 (프로시저 본문 조회용)
MSSQL_CONFIG = {
    "host": "your-mssql-host.com",
    "port": 1433,
    "database": "YOUR_DATABASE",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD"
}

# Gemini API 키 (환경변수에서 로드)
GEMINI_API_KEYS = [
    os.getenv("GEMINI_API_KEY_1", ""),
    os.getenv("GEMINI_API_KEY_2", ""),
]

# 유효한 API 키만 필터링
GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]

# 기본 키 (첫 번째 유효한 키)
GEMINI_API_KEY = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else os.getenv("GEMINI_API_KEY", "")

# 매핑 테이블 정보
MAPPING_TABLE = "OHIS2015_SCHEMA_COMMENT"

# 컬럼 매핑 (기존 C# → 신규 Java)
COLUMN_MAPPING = {
    "old_table": "N",      # 기존 테이블명
    "old_table_kor": "O",  # 기존 테이블 한글명
    "old_column": "P",     # 기존 컬럼명
    "old_column_kor": "Q", # 기존 컬럼 한글명
    "new_table": "D",      # 신규 테이블명
    "new_table_kor": "E",  # 신규 테이블 한글명
    "new_column": "F",     # 신규 컬럼명
    "new_column_kor": "G", # 신규 컬럼 한글명
    "data_type": "H",      # 데이터타입
    "length": "I",         # 길이
    "pk": "AI",            # PK 여부
    "fk": "AJ",            # FK 여부
}
