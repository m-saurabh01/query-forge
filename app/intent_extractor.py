import json
import logging
import re

from pydantic import BaseModel

from app.llm import generate
from app.prompts import INTENT_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


class IntentModel(BaseModel):
    tables: list[str] = []
    columns: list[str] = []
    filters: list[str] = []
    aggregations: list[str] = []
    sort: str = ""
    limit: int = 50


def _extract_json(text: str) -> str:
    """Extract JSON from LLM output, stripping markdown fences if present."""
    # Try to find JSON in code fences
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    # Try to find raw JSON object
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group(0).strip()
    return text.strip()


def extract_intent(user_query: str, schema_text: str) -> dict:
    prompt = INTENT_PROMPT_TEMPLATE.format(
        schema=schema_text,
        user_query=user_query,
    )
    raw = generate(prompt, max_tokens=512, temperature=0.1)
    logger.debug("Raw intent LLM output: %s", raw)

    json_str = _extract_json(raw)
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        logger.warning("Failed to parse intent JSON: %s", json_str)
        # Fallback: return a minimal intent
        return IntentModel(tables=[], columns=["*"]).model_dump()

    intent = IntentModel(**data)
    # Cap limit
    if intent.limit > 100:
        intent.limit = 100
    return intent.model_dump()
