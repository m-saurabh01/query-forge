from app.db.dialect import get_dialect, format_limit, format_date_cast


def _build_prompt_template(dialect: dict) -> str:
    db_name = dialect["name"]
    limit_example = format_limit(dialect, 50)
    date_hint = dialect["date_hint"]
    return f"""You are a SQL query generator. Convert the user question into a {db_name} SELECT query using ONLY the tables and columns listed below.

### Database Schema:
{{schema}}

### Rules:
1. ONLY use SELECT statements
2. ONLY use tables and columns from the schema above — use EXACT column names, never abbreviate or guess
3. Always add {limit_example} unless the user specifies a count/aggregate
4. Use JOINs based on the relationships shown above — always use the exact FK columns listed
5. Output ONLY the raw SQL query, no explanation, no markdown
6. NEVER use placeholders like ? or :param — always write complete queries
7. When the user asks about related data, use JOIN or subqueries (IN, EXISTS)
8. If no specific filter value is given, return all matching rows
9. For date/time filters, ALWAYS convert to ISO format: '2026-01-18' ({date_hint})
10. Always quote string and date values in single quotes
11. Pay attention to column data types — use datetime/timestamp columns for date queries, not bit/boolean columns
12. End your SQL with a semicolon
13. When user says "name", use the column marked as the name (e.g. display_name). NEVER use a column called "name" unless it exists in the schema

### Examples:
{{examples}}

### Question: {{user_query}}
### SQL: SELECT"""


def get_prompt_template(dialect_key: str) -> str:
    dialect = get_dialect(dialect_key)
    return _build_prompt_template(dialect)


def build_few_shot_examples(
    schema: dict[str, list[str]],
    relationships: list[dict],
    dialect_key: str = "mysql",
) -> str:
    """Build few-shot examples using actual schema and FK relationships."""
    dialect = get_dialect(dialect_key)
    limit_50 = format_limit(dialect, 50)
    tables = list(schema.keys())
    if not tables:
        return ""

    # Build FK lookup: (child_table, parent_table) -> (child_col, parent_col)
    fk_lookup: dict[tuple[str, str], tuple[str, str]] = {}
    for r in relationships:
        fk_lookup[(r["table"], r["referenced_table"])] = (
            r["column"],
            r["referenced_column"],
        )

    examples = []

    # Example 1: show all from first table
    t1 = tables[0]
    examples.append(
        f"Question: show all {t1}\n" f"SQL: SELECT * FROM {t1} {limit_50};"
    )

    # Example 2: count rows in first table
    examples.append(
        f"Question: count of {t1}\n"
        f"SQL: SELECT COUNT(*) AS total FROM {t1};"
    )

    # Example 3: JOIN example using actual FK relationships
    for r in relationships:
        child = r["table"]
        parent = r["referenced_table"]
        child_col = r["column"]
        parent_col = r["referenced_column"]
        if child in schema and parent in schema:
            examples.append(
                f"Question: show {child} with their {parent}\n"
                f"SQL: SELECT {child}.*, {parent}.* FROM {child} "
                f"INNER JOIN {parent} ON {child}.{child_col} = {parent}.{parent_col} {limit_50};"
            )
            break

    # Example 4: GROUP BY using actual FK relationship
    for r in relationships:
        child = r["table"]
        parent = r["referenced_table"]
        child_col = r["column"]
        if child in schema and parent in schema:
            examples.append(
                f"Question: count {child} per {parent}\n"
                f"SQL: SELECT {child_col}, COUNT(*) AS total FROM {child} GROUP BY {child_col};"
            )
            break

    # Example 5: date filtering example
    for t in tables:
        date_cols = [
            c
            for c in schema[t]
            if any(kw in c.lower() for kw in ("date", "created", "updated", "time"))
        ]
        if date_cols:
            dc = date_cols[0]
            date_expr = format_date_cast(dialect, dc)
            examples.append(
                f"Question: how many {t} on 18 Jan 2026\n"
                f"SQL: SELECT COUNT(*) AS total FROM {t} WHERE {date_expr} = '2026-01-18';"
            )
            break

    # Example 6: recent items from first table with a date-like column
    for t in tables:
        date_cols = [
            c
            for c in schema[t]
            if any(kw in c.lower() for kw in ("date", "created", "updated", "time"))
        ]
        if date_cols:
            dc = date_cols[0]
            examples.append(
                f"Question: recent {t}\n"
                f"SQL: SELECT * FROM {t} ORDER BY {dc} DESC {limit_50};"
            )
            break

    # Example 7: recipient_type filtering — domain-specific
    if "emails" in schema and "email_recipients" in schema:
        # Use actual FK if available
        join_cond = "emails.id = email_recipients.email_id"
        fk_key = ("email_recipients", "emails")
        if fk_key in fk_lookup:
            child_col, parent_col = fk_lookup[fk_key]
            join_cond = f"emails.{parent_col} = email_recipients.{child_col}"
        examples.append(
            "Question: show all emails which have BCC recipients\n"
            "SQL: SELECT DISTINCT emails.* FROM emails "
            f"INNER JOIN email_recipients ON {join_cond} "
            f"WHERE email_recipients.recipient_type = 'BCC' {limit_50};"
        )

    return "\n\n".join(examples)


def build_error_feedback_prompt(original_prompt: str, error: str) -> str:
    """Rebuild the prompt with error feedback inserted before the SQL anchor.

    The original prompt ends with '### SQL: SELECT'. We insert the error
    feedback before that anchor so the model gets context and still
    continues from SELECT.
    """
    # Strip the trailing SELECT anchor
    base = original_prompt
    if base.rstrip().endswith("SELECT"):
        base = base.rstrip()[: -len("SELECT")].rstrip()

    return (
        f"{base}\n"
        f"-- Previous attempt failed: {error}\n"
        f"-- Use ONLY exact column names from the schema above. Do NOT invent columns.\n"
        f"### SQL: SELECT"
    )
