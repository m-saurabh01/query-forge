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
2. ONLY use tables and columns from the schema above
3. Always add {limit_example} unless the user specifies a count/aggregate
4. Use JOINs based on the relationships shown above
5. Output ONLY the SQL query, no explanation
6. NEVER use placeholders like ? or :param — always write complete queries
7. When the user asks about related data, use JOIN or subqueries (IN, EXISTS)
8. If no specific filter value is given, return all matching rows
9. For date/time filters, ALWAYS convert to ISO format: '2026-01-18' ({date_hint})
10. Always quote string and date values in single quotes
11. Pay attention to column data types — use datetime/timestamp columns for date queries, not bit/boolean columns

### Examples:
{{examples}}

### Question: {{user_query}}
### SQL: SELECT"""


def get_prompt_template(dialect_key: str) -> str:
    dialect = get_dialect(dialect_key)
    return _build_prompt_template(dialect)


def build_few_shot_examples(schema: dict[str, list[str]], dialect_key: str = "mysql") -> str:
    """Build few-shot examples dynamically from actual schema tables."""
    dialect = get_dialect(dialect_key)
    limit_50 = format_limit(dialect, 50)
    tables = list(schema.keys())
    if not tables:
        return ""

    examples = []

    # Example 1: show all from first table
    t1 = tables[0]
    examples.append(
        f"Question: show all {t1}\n"
        f"SQL: SELECT * FROM {t1} {limit_50};"
    )

    # Example 2: count rows in first table
    examples.append(
        f"Question: count of {t1}\n"
        f"SQL: SELECT COUNT(*) AS total FROM {t1};"
    )

    # Example 3: if there's a second table, show a join/subquery example
    if len(tables) >= 2:
        t2 = tables[1]
        examples.append(
            f"Question: show {t1} who have {t2}\n"
            f"SQL: SELECT DISTINCT {t1}.* FROM {t1} "
            f"INNER JOIN {t2} ON {t2}.{t1[:-1] if t1.endswith('s') else t1}_id = {t1}.id {limit_50};"
        )

    # Example 4: subquery example
    if len(tables) >= 2:
        t2 = tables[1]
        examples.append(
            f"Question: count {t2} per {t1[:-1] if t1.endswith('s') else t1}\n"
            f"SQL: SELECT {t1[:-1] if t1.endswith('s') else t1}_id, COUNT(*) AS total FROM {t2} GROUP BY {t1[:-1] if t1.endswith('s') else t1}_id;"
        )

    # Example 5: date filtering example
    for t in tables:
        date_cols = [c for c in schema[t] if any(kw in c.lower() for kw in ("date", "created", "updated", "time"))]
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
        date_cols = [c for c in schema[t] if any(kw in c.lower() for kw in ("date", "created", "updated", "time"))]
        if date_cols:
            dc = date_cols[0]
            examples.append(
                f"Question: recent {t}\n"
                f"SQL: SELECT * FROM {t} ORDER BY {dc} DESC {limit_50};"
            )
            break

    # Example 7: recipient_type filtering (TO/CC/BCC) — critical for email queries
    if "emails" in schema and "email_recipients" in schema:
        examples.append(
            "Question: show all emails which have BCC recipients\n"
            "SQL: SELECT DISTINCT emails.* FROM emails "
            "INNER JOIN email_recipients ON emails.id = email_recipients.email_id "
            f"WHERE email_recipients.recipient_type = 'BCC' {limit_50};"
        )

    return "\n\n".join(examples)
