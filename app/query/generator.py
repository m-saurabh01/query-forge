import re


def extract_sql(text: str) -> str:
    """Extract SQL from LLM output robustly.

    Handles various formats:
    - Raw SQL output with or without SELECT
    - Markdown fenced blocks
    - Double SELECT issues
    """
    text = text.strip()

    # If model outputs markdown fences, extract from them
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text)
    if match:
        text = match.group(1).strip()

    # Find the SELECT statement in the output
    select_match = re.search(r"\bSELECT\b", text, re.IGNORECASE)
    if select_match:
        sql = text[select_match.start():]
    else:
        # No SELECT found — model likely output a continuation
        sql = "SELECT " + text

    # Fix double SELECT (e.g., "SELECT SELECT ...")
    if re.match(r"^\s*SELECT\s+SELECT\b", sql, re.IGNORECASE):
        sql = sql[sql.upper().index("SELECT", 6):]

    # Take only the first statement (up to first semicolon)
    if ";" in sql:
        sql = sql[: sql.index(";")]

    sql = sql.strip()

    # Remove any lines that don't look like SQL (comments, explanations)
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--") and not stripped.startswith("#") and not stripped.startswith("Question"):
            lines.append(line)
    sql = "\n".join(lines).strip()

    # Ensure trailing semicolon
    if not sql.endswith(";"):
        sql += ";"

    return sql
