import logging
import re

import sqlglot
from sqlglot import exp

from app.config import settings
from app.db.dialect import get_dialect, format_limit

logger = logging.getLogger(__name__)

FORBIDDEN_KEYWORDS = {
    "DELETE", "UPDATE", "INSERT", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "REPLACE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
}

MAX_LIMIT = 100

# Map config dialect to sqlglot dialect
_SQLGLOT_DIALECTS = {
    "mysql": "mysql",
    "db2": "db2",
}


def validate_sql(sql: str, schema: dict[str, list[str]]) -> tuple[bool, str, str]:
    """
    Validate a SQL query using AST parsing (sqlglot) + schema checks.

    Returns:
        (is_valid, error_message, possibly_modified_sql)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query", sql

    dialect = _SQLGLOT_DIALECTS.get(settings.db_dialect, "mysql")

    # Parse with sqlglot
    try:
        parsed = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.ParseError as e:
        return False, f"SQL parse error: {e}", sql

    if not parsed or len(parsed) != 1:
        return False, "Only single SQL statements are allowed", sql

    tree = parsed[0]

    if tree is None:
        return False, "Failed to parse SQL", sql

    # Must be a SELECT statement
    if not isinstance(tree, exp.Select):
        # Could be a subquery wrapper — check for any SELECT inside
        selects = list(tree.find_all(exp.Select))
        if not selects:
            return False, "Only SELECT queries are allowed", sql

    # Check for forbidden statement types via AST
    forbidden_types = (
        exp.Delete, exp.Update, exp.Insert, exp.Drop, exp.Alter, exp.Create,
    )
    for node in tree.walk():
        n = node[0] if isinstance(node, tuple) else node
        if isinstance(n, forbidden_types):
            return False, "Only SELECT queries are allowed (forbidden statement detected)", sql

    # Token-level check for edge cases the AST might not catch
    normalized = sql.strip().upper()
    tokens_upper = normalized.replace("(", " ").replace(")", " ").split()
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in tokens_upper:
            return False, f"Forbidden keyword detected: {keyword}", sql

    # Extract tables from AST
    ast_tables = set()
    for table_node in tree.find_all(exp.Table):
        table_name = table_node.name
        if table_name:
            ast_tables.add(table_name)

    # Verify tables exist in schema
    schema_tables_lower = {t.lower(): t for t in schema.keys()}
    for table in ast_tables:
        if table.lower() not in schema_tables_lower:
            return False, f"Unknown table: {table}", sql

    # Build alias map from AST
    alias_map: dict[str, str] = {}
    for table_node in tree.find_all(exp.Table):
        real_name = table_node.name
        alias = table_node.alias
        if alias:
            alias_map[alias.lower()] = real_name
        if real_name:
            alias_map[real_name.lower()] = real_name

    # Extract and verify columns from AST
    all_columns_lower = set()
    for cols in schema.values():
        for c in cols:
            all_columns_lower.add(c.lower())

    for col_node in tree.find_all(exp.Column):
        col_name = col_node.name
        table_ref = col_node.table

        if not col_name or col_name == "*":
            continue

        if table_ref:
            real_table = alias_map.get(table_ref.lower())
            if real_table:
                table_cols_lower = {c.lower() for c in schema.get(real_table, [])}
                if col_name.lower() not in table_cols_lower:
                    return (
                        False,
                        f"Unknown column: {table_ref}.{col_name} "
                        f"(table '{real_table}' has no column '{col_name}')",
                        sql,
                    )
        else:
            if col_name.lower() not in all_columns_lower:
                return False, f"Unknown column: {col_name}", sql

    # Enforce LIMIT
    sql = _enforce_limit(sql)

    return True, "", sql


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
        limit_clause = format_limit(dialect, MAX_LIMIT)
        sql = sql.rstrip().rstrip(";").rstrip() + f" {limit_clause};"
    return sql
