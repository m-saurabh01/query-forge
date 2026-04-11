import json
import logging
import re

from app.llm import generate
from app.prompts import SQL_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


def _extract_sql(text: str) -> str:
    """Extract SQL from LLM output, stripping markdown fences if present."""
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    # Return first line that looks like SQL, or the whole text
    for line in text.strip().splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("SELECT"):
            # Collect from this line to end
            idx = text.index(line)
            return text[idx:].strip().rstrip(";") + ";"
    return text.strip()


def generate_sql(intent: dict, schema_text: str) -> str:
    prompt = SQL_PROMPT_TEMPLATE.format(
        intent_json=json.dumps(intent, indent=2),
        schema=schema_text,
    )
    raw = generate(prompt, max_tokens=512, temperature=0.1)
    logger.debug("Raw SQL LLM output: %s", raw)
    sql = _extract_sql(raw)
    # Remove trailing semicolons for consistency, then add one
    sql = sql.rstrip(";").strip() + ";"
    return sql
