import re


def extract_sql(text: str) -> str:
    """Extract SQL from LLM output, handling various formats.

    The prompt ends with "SELECT" so the model continues from there.
    This function prepends SELECT and cleans up the result.
    """
    sql = "SELECT " + text.strip()

    # If model outputs markdown fences, extract from them
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", sql)
    if match:
        sql = match.group(1).strip()

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
