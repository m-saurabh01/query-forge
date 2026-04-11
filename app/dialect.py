"""Database dialect configuration for multi-database support."""


DIALECTS = {
    "mysql": {
        "name": "MySQL",
        "limit_clause": "LIMIT {n}",
        "limit_regex": r"LIMIT\s+(\d+)",
        "date_cast": "DATE({col})",
        "date_hint": "use DATE() for timestamp columns",
    },
    "db2": {
        "name": "DB2",
        "limit_clause": "FETCH FIRST {n} ROWS ONLY",
        "limit_regex": r"FETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY",
        "date_cast": "DATE({col})",
        "date_hint": "use DATE() for timestamp columns",
    },
}


def get_dialect(dialect_key: str) -> dict:
    key = dialect_key.lower()
    if key not in DIALECTS:
        raise ValueError(f"Unsupported dialect: {dialect_key}. Supported: {', '.join(DIALECTS.keys())}")
    return DIALECTS[key]


def format_limit(dialect: dict, n: int) -> str:
    return dialect["limit_clause"].format(n=n)


def format_date_cast(dialect: dict, col: str) -> str:
    return dialect["date_cast"].format(col=col)
