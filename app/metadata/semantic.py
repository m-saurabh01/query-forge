import re

from app.metadata.loader import get_table_metadata, get_column_metadata, get_relationship_metadata


# ── Intent Classification ──

INTENT_PATTERNS: dict[str, str] = {
    "AGGREGATION": r"\b(count|sum|avg|average|max|min|total|how many|number of|most|least|top|bottom)\b",
    "JOIN_QUERY": r"\b(with their|along with|and their|joined|related|together)\b",
    "FILTER_QUERY": r"\b(where|named|called|from|between|before|after|on|greater|less|equal|specific|whose|which have|that have)\b",
}


def classify_intent(user_query: str, table_names: list[str] | None = None) -> str:
    """Classify the query intent using rule-based patterns.

    Returns one of: AGGREGATION, JOIN_QUERY, FILTER_QUERY, SIMPLE_SELECT.
    """
    query_lower = user_query.lower()

    # Check if multiple tables are mentioned → JOIN_QUERY
    if table_names:
        mentioned = 0
        for t in table_names:
            t_lower = t.lower()
            if t_lower in query_lower or (t_lower.endswith("s") and t_lower[:-1] in query_lower):
                mentioned += 1
        if mentioned >= 2:
            return "JOIN_QUERY"

    # Pattern-based matching (order matters — most specific first)
    for intent, pattern in INTENT_PATTERNS.items():
        if re.search(pattern, query_lower):
            return intent

    return "SIMPLE_SELECT"


# Intent → example type tags for reordering
_INTENT_EXAMPLE_PRIORITY: dict[str, list[str]] = {
    "AGGREGATION": ["count", "group", "aggregate"],
    "JOIN_QUERY": ["join", "inner join", "with their"],
    "FILTER_QUERY": ["where", "filter", "date"],
    "SIMPLE_SELECT": ["show all", "select *"],
}


def reorder_examples_by_intent(examples_text: str, intent: str) -> str:
    """Reorder few-shot examples so intent-relevant ones come first.

    Splits examples by double-newline, scores each by intent relevance,
    and returns them reordered (relevant first, others after).
    """
    if not examples_text or intent == "SIMPLE_SELECT":
        return examples_text

    priority_keywords = _INTENT_EXAMPLE_PRIORITY.get(intent, [])
    if not priority_keywords:
        return examples_text

    blocks = examples_text.split("\n\n")
    scored = []
    for block in blocks:
        block_lower = block.lower()
        score = sum(1 for kw in priority_keywords if kw in block_lower)
        scored.append((score, block))

    # Stable sort: higher score first
    scored.sort(key=lambda x: -x[0])
    return "\n\n".join(block for _, block in scored)


# ── Semantic Schema Builder ──

_MAX_SYNONYMS_PER_COLUMN = 3
_MAX_TABLE_DESC_LEN = 60


def build_semantic_schema_text(
    schema_typed: dict[str, list[tuple[str, str]]],
    relationships: list[dict],
    metadata: dict | None = None,
    max_tables: int = 10,
) -> str:
    """Build an LLM-friendly schema text enriched with metadata.

    Keeps output compact: only adds descriptions/synonyms for columns
    that have synonyms (the most useful semantic info). Plain columns
    just get name(type) to save tokens.
    """
    tables_meta = metadata.get("tables", {}) if metadata else {}
    rels_meta = metadata.get("relationships", {}) if metadata else {}

    # Cap table count
    table_items = list(schema_typed.items())[:max_tables]

    lines = []
    for table, columns in table_items:
        # Table header with short description
        t_meta = tables_meta.get(table, {})
        desc = t_meta.get("description", "")
        if desc:
            desc = desc[:_MAX_TABLE_DESC_LEN]
            lines.append(f"Table: {table} -- {desc}")
        else:
            lines.append(f"Table: {table}")

        # Columns — only annotate those with synonyms or important hints
        col_parts = []
        cols_meta = t_meta.get("columns", {})
        for name, dtype in columns:
            c_meta = cols_meta.get(name, {})
            c_syns = c_meta.get("synonyms", [])[:_MAX_SYNONYMS_PER_COLUMN]
            c_desc = c_meta.get("description", "")

            part = f"{name} ({dtype})"
            # Only add description for columns that have synonyms (semantic value)
            # Format: "display_name (varchar) -- use this when user says 'name'"
            # This tells the LLM to map user words to the ACTUAL column name
            if c_syns:
                part += f" -- use this when user says '{c_syns[0]}'"
            col_parts.append(part)

        lines.append(f"  Columns: {', '.join(col_parts)}")

    # Relationships with descriptions
    if relationships:
        lines.append("\nRelationships (use these for JOINs):")
        for r in relationships:
            rel_key = f"{r['table']}.{r['column']} -> {r['referenced_table']}.{r['referenced_column']}"
            rel_desc = ""
            r_meta = rels_meta.get(rel_key, {})
            if r_meta:
                rel_desc = r_meta.get("description", "")

            line = f"  {rel_key}"
            if rel_desc:
                line += f" — {rel_desc}"
            lines.append(line)

    return "\n".join(lines)
