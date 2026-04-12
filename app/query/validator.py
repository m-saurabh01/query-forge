import logging
import re

import sqlparse

from app.config import settings
from app.db.dialect import get_dialect, format_limit

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
    # Check columns across ALL clauses (SELECT, WHERE, ON, ORDER BY, GROUP BY)
    qualified_cols, unqualified_cols = _extract_all_column_refs(sql)
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
        alias_map[table.lower()] = table
    return alias_map


def _extract_all_column_refs(sql: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Extract column references from ALL clauses of the SQL query.

    Scans SELECT, WHERE, ON, ORDER BY, GROUP BY, and HAVING clauses.

    Returns:
        (qualified_cols, unqualified_cols)
        qualified_cols: list of (alias, column) tuples like ("u", "name")
        unqualified_cols: list of plain column names
    """
    SQL_KEYWORDS = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
        "IS", "IN", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE", "END",
        "ASC", "DESC", "ON", "BY", "AS", "JOIN", "INNER", "LEFT", "RIGHT",
        "OUTER", "CROSS", "GROUP", "ORDER", "LIMIT", "HAVING", "UNION",
        "DISTINCT", "ALL", "SET", "FETCH", "FIRST", "ROWS", "ONLY",
        "EXISTS", "ANY", "SOME", "INTO", "VALUES",
    }

    # Find all potential column references: word or table.word patterns
    # This regex finds identifiers and dotted identifiers, skipping string literals
    qualified: list[tuple[str, str]] = []
    unqualified: list[str] = []

    # Remove string literals to avoid matching values like 'some_text'
    cleaned = re.sub(r"'[^']*'", "''", sql)

    # Remove content inside function calls' parentheses but keep column refs
    # Find all dotted refs: alias.column
    for match in re.finditer(r"`?(\w+)`?\s*\.\s*`?(\w+)`?", cleaned):
        prefix = match.group(1)
        col = match.group(2)
        if (prefix.upper() not in SQL_KEYWORDS
                and col.upper() not in SQL_KEYWORDS
                and col.isidentifier()
                and not col.isdigit()):
            qualified.append((prefix, col))

    # Find all standalone column refs (not preceded by a dot and not a table/keyword)
    # We look for identifiers in WHERE, ON, ORDER BY, GROUP BY, HAVING, SELECT clauses
    # that are not part of dotted refs and not keywords/table names/values

    # Extract table names and aliases to exclude them
    table_names = set()
    for match in re.finditer(r"(?:FROM|JOIN)\s+`?(\w+)`?(?:\s+(?:AS\s+)?`?(\w+)`?)?", cleaned, re.IGNORECASE):
        table_names.add(match.group(1).lower())
        if match.group(2) and match.group(2).upper() not in (
            "ON", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
            "CROSS", "GROUP", "ORDER", "LIMIT", "HAVING", "UNION",
            "SET", "FETCH", "AND", "OR",
        ):
            table_names.add(match.group(2).lower())

    # Get SELECT clause columns separately (handle comma-separated)
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", cleaned, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1)
        select_clause = re.sub(r"^\s*(DISTINCT|ALL)\s+", "", select_clause, flags=re.IGNORECASE)
        if select_clause.strip() != "*":
            for part in select_clause.split(","):
                part = part.strip()
                if "(" in part or "*" in part:
                    continue
                col_ref = re.split(r"\s+(?:AS\s+)?", part, flags=re.IGNORECASE)[0].strip()
                col_ref = col_ref.strip("`").strip('"').strip("'")
                # Skip dotted refs (already handled above)
                if "." not in col_ref:
                    col = col_ref.strip()
                    if (col and col.isidentifier()
                            and col.upper() not in SQL_KEYWORDS
                            and col.lower() not in table_names):
                        unqualified.append(col)

    # Extract columns from WHERE, HAVING, ON, ORDER BY, GROUP BY
    # Look for standalone identifiers used as column refs
    # Pattern: word that appears in comparison contexts (=, <, >, !=, LIKE, IS, etc.)
    clause_patterns = [
        r"WHERE\s+(.*?)(?=\s+(?:GROUP|ORDER|LIMIT|HAVING|FETCH)\b|\s*;?\s*$)",
        r"HAVING\s+(.*?)(?=\s+(?:ORDER|LIMIT|FETCH)\b|\s*;?\s*$)",
        r"ON\s+(.*?)(?=\s+(?:WHERE|JOIN|INNER|LEFT|RIGHT|OUTER|CROSS|GROUP|ORDER|LIMIT|HAVING|FETCH)\b|\s*;?\s*$)",
        r"ORDER\s+BY\s+(.*?)(?=\s+(?:LIMIT|FETCH)\b|\s*;?\s*$)",
        r"GROUP\s+BY\s+(.*?)(?=\s+(?:HAVING|ORDER|LIMIT|FETCH)\b|\s*;?\s*$)",
    ]

    for pattern in clause_patterns:
        for clause_match in re.finditer(pattern, cleaned, re.IGNORECASE | re.DOTALL):
            clause_text = clause_match.group(1)
            # Strip dotted refs (already handled globally) to avoid
            # partial-word matches from lookbehind/lookahead edge cases
            stripped = re.sub(r"\w+\s*\.\s*\w+", " ", clause_text)
            stripped = re.sub(r"\w+\s*\.\s*\*", " ", stripped)
            for word_match in re.finditer(r"\b(\w+)\b", stripped):
                word = word_match.group(1).strip("`")
                if (word and word.isidentifier()
                        and not word.isdigit()
                        and word.upper() not in SQL_KEYWORDS
                        and word.lower() not in table_names):
                    unqualified.append(word)

    # Deduplicate while preserving order
    seen_q = set()
    unique_qualified = []
    for item in qualified:
        key = (item[0].lower(), item[1].lower())
        if key not in seen_q:
            seen_q.add(key)
            unique_qualified.append(item)

    seen_u = set()
    unique_unqualified = []
    for col in unqualified:
        if col.lower() not in seen_u:
            seen_u.add(col.lower())
            unique_unqualified.append(col)

    return unique_qualified, unique_unqualified


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
