import logging
import re

import sqlparse

from app.config import settings
from app.dialect import get_dialect, format_limit

logger = logging.getLogger(__name__)

FORBIDDEN_KEYWORDS = {
    "DELETE", "UPDATE", "INSERT", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "REPLACE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
}

MAX_LIMIT = 100


def validate_sql(sql: str, schema: dict[str, list[str]]) -> tuple[bool, str, str]:
    """
    Validate a SQL query against the schema.

    Returns:
        (is_valid, error_message, possibly_modified_sql)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query", sql

    # Parse with sqlparse
    statements = sqlparse.parse(sql)
    if len(statements) != 1:
        return False, "Only single SQL statements are allowed", sql

    stmt = statements[0]
    stmt_type = stmt.get_type()

    # Must be SELECT
    normalized = sql.strip().upper()
    if stmt_type != "SELECT" and not normalized.startswith("SELECT"):
        return False, "Only SELECT queries are allowed", sql

    # Check for forbidden keywords
    tokens_upper = normalized.replace("(", " ").replace(")", " ").split()
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in tokens_upper:
            return False, f"Forbidden keyword detected: {keyword}", sql

    # Verify tables exist
    table_alias_map = _extract_table_aliases(sql)
    schema_tables_lower = {t.lower(): t for t in schema.keys()}
    for table in table_alias_map.values():
        if table.lower() not in schema_tables_lower:
            return False, f"Unknown table: {table}", sql

    # Verify columns exist — table-specific when alias/prefix is used
    qualified_cols, unqualified_cols = _extract_columns_with_tables(sql)
    # Check qualified columns (e.g. u.name → check "name" exists in users)
    for alias, col in qualified_cols:
        real_table = table_alias_map.get(alias.lower())
        if real_table:
            table_cols_lower = {c.lower() for c in schema.get(real_table, [])}
            if col.lower() not in table_cols_lower:
                return False, f"Unknown column: {alias}.{col} (table '{real_table}' has no column '{col}')", sql
    # Check unqualified columns against all tables
    all_columns_lower = set()
    for cols in schema.values():
        for c in cols:
            all_columns_lower.add(c.lower())
    for col in unqualified_cols:
        if col.lower() not in all_columns_lower:
            return False, f"Unknown column: {col}", sql

    # Enforce LIMIT
    sql = _enforce_limit(sql)

    return True, "", sql


def _extract_table_aliases(sql: str) -> dict[str, str]:
    """Extract table names and their aliases from FROM and JOIN clauses.

    Returns a dict mapping alias (lowercase) → real table name.
    If no alias is used, the table name maps to itself.
    """
    alias_map: dict[str, str] = {}
    # Match: FROM/JOIN table_name [AS] alias  or  FROM/JOIN table_name
    pattern = r"(?:FROM|JOIN)\s+`?(\w+)`?(?:\s+(?:AS\s+)?`?(\w+)`?)?"
    for match in re.finditer(pattern, sql, re.IGNORECASE):
        table = match.group(1)
        alias = match.group(2)
        if alias and alias.upper() not in (
            "ON", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
            "CROSS", "GROUP", "ORDER", "LIMIT", "HAVING", "UNION",
            "SET", "FETCH", "AND", "OR",
        ):
            alias_map[alias.lower()] = table
        # Also keep full table name as a self-mapping
        alias_map[table.lower()] = table
    return alias_map


def _extract_columns_with_tables(sql: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Extract columns from SELECT clause, preserving table prefixes.

    Returns:
        (qualified_cols, unqualified_cols)
        qualified_cols: list of (alias, column) tuples like ("u", "name")
        unqualified_cols: list of plain column names
    """
    match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return [], []
    select_clause = match.group(1)

    # Strip leading DISTINCT/ALL
    select_clause = re.sub(r"^\s*(DISTINCT|ALL)\s+", "", select_clause, flags=re.IGNORECASE)

    if select_clause.strip() == "*":
        return [], []

    SQL_KEYWORDS = {
        "DISTINCT", "ALL", "AS", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
        "IS", "IN", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE", "END",
        "ASC", "DESC", "ON", "BY",
    }

    qualified = []
    unqualified = []
    for part in select_clause.split(","):
        part = part.strip()
        # Skip aggregate functions, expressions, and wildcards
        if "(" in part or "*" in part:
            continue
        # Take the column reference (before AS alias)
        col_ref = re.split(r"\s+(?:AS\s+)?", part, flags=re.IGNORECASE)[0].strip()
        col_ref = col_ref.strip("`").strip('"').strip("'")
        if "." in col_ref:
            prefix, col = col_ref.rsplit(".", 1)
            prefix = prefix.strip("`").strip()
            col = col.strip("`").strip()
            if col and col.isidentifier() and col.upper() not in SQL_KEYWORDS:
                qualified.append((prefix, col))
        else:
            col = col_ref.strip()
            if col and col.isidentifier() and col.upper() not in SQL_KEYWORDS:
                unqualified.append(col)
    return qualified, unqualified


def _enforce_limit(sql: str) -> str:
    """Ensure query has a row limit <= MAX_LIMIT, dialect-aware."""
    dialect = get_dialect(settings.db_dialect)
    limit_regex = dialect["limit_regex"]

    limit_match = re.search(limit_regex, sql, re.IGNORECASE)
    if limit_match:
        current_limit = int(limit_match.group(1))
        if current_limit > MAX_LIMIT:
            sql = sql[: limit_match.start(1)] + str(MAX_LIMIT) + sql[limit_match.end(1) :]
    else:
        # Add limit clause before trailing semicolon
        limit_clause = format_limit(dialect, MAX_LIMIT)
        sql = sql.rstrip().rstrip(";").rstrip() + f" {limit_clause};"
    return sql
