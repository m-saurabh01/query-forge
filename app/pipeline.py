import logging
import re
import time

from app.schema_loader import get_schema, get_schema_text
from app.prompts import get_prompt_template, build_few_shot_examples
from app.config import settings
from app.llm import generate
from app.sql_validator import validate_sql
from app.query_executor import execute

logger = logging.getLogger(__name__)


def _extract_sql(text: str) -> str:
    """Extract SQL from LLM output, handling various formats."""
    # The prompt ends with "SELECT" so the model continues from there
    sql = "SELECT " + text.strip()

    # If model outputs markdown fences, extract from them
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", sql)
    if match:
        sql = match.group(1).strip()

    # Take only the first statement (up to first semicolon)
    if ";" in sql:
        sql = sql[: sql.index(";")]

    # Clean up
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


async def process_query(user_query: str) -> dict:
    """
    Simplified NL-to-SQL pipeline:
    1. Build prompt with schema + few-shot examples + user query
    2. Single LLM call → SQL
    3. Validate SQL against schema
    4. Execute query
    5. Return structured response
    """
    t0 = time.time()
    schema = get_schema()
    schema_text = get_schema_text()

    if not schema:
        return {"sql": None, "data": None, "explanation": None, "error": "No database schema loaded."}

    # Build few-shot examples from actual schema
    examples = build_few_shot_examples(schema, settings.db_dialect)

    # Step 1: Direct NL → SQL via single LLM call
    try:
        prompt_template = get_prompt_template(settings.db_dialect)
        prompt = prompt_template.format(
            schema=schema_text,
            examples=examples,
            user_query=user_query,
        )
        logger.info("Generating SQL for: %s", user_query)
        raw = generate(prompt, max_tokens=256, temperature=0.1)
        sql = _extract_sql(raw)
        logger.info("Generated SQL: %s", sql)
    except Exception as e:
        logger.error("SQL generation failed: %s", e)
        return {"sql": None, "data": None, "explanation": None, "error": f"Failed to generate SQL: {e}"}

    # Step 2: Validate SQL
    is_valid, error_msg, sql = validate_sql(sql, schema)
    if not is_valid:
        logger.warning("SQL validation failed: %s | SQL: %s", error_msg, sql)
        return {"sql": sql, "data": None, "explanation": None, "error": f"SQL validation failed: {error_msg}"}

    # Step 3: Execute
    try:
        logger.info("Executing SQL")
        data = await execute(sql)
    except RuntimeError as e:
        return {"sql": sql, "data": None, "explanation": None, "error": str(e)}

    elapsed = round(time.time() - t0, 2)
    logger.info("Pipeline completed in %.2fs", elapsed)

    return {
        "sql": sql,
        "data": data,
        "explanation": f"Query returned {len(data['rows'])} row(s) in {elapsed}s.",
        "error": None,
    }

    return {
        "sql": sql,
        "data": data,
        "explanation": f"Query returned {len(data['rows'])} row(s) in {elapsed}s.",
        "error": None,
    }
