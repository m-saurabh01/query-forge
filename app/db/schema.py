import logging
import re

from app.config import settings
from app.db.pool import get_pool

logger = logging.getLogger(__name__)

_schema: dict[str, list[str]] = {}
_schema_typed: dict[str, list[tuple[str, str]]] = {}
_relationships: list[dict] = []


async def load_schema():
    global _schema, _schema_typed, _relationships
    pool = get_pool()

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Load all tables and columns with data types
            await cur.execute(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s "
                "ORDER BY TABLE_NAME, ORDINAL_POSITION",
                (settings.db_name,),
            )
            rows = await cur.fetchall()
            _schema = {}
            _schema_typed = {}
            for table_name, column_name, data_type in rows:
                _schema.setdefault(table_name, []).append(column_name)
                _schema_typed.setdefault(table_name, []).append((column_name, data_type))

            # Load foreign key relationships
            await cur.execute(
                "SELECT TABLE_NAME, COLUMN_NAME, "
                "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA = %s "
                "AND REFERENCED_TABLE_NAME IS NOT NULL",
                (settings.db_name,),
            )
            fk_rows = await cur.fetchall()
            _relationships = [
                {
                    "table": r[0],
                    "column": r[1],
                    "referenced_table": r[2],
                    "referenced_column": r[3],
                }
                for r in fk_rows
            ]

    logger.info(
        "Schema loaded: %d tables, %d relationships",
        len(_schema),
        len(_relationships),
    )


async def reload_schema():
    """Reload schema from database (for cache invalidation)."""
    await load_schema()


def get_schema() -> dict[str, list[str]]:
    return _schema


def get_schema_typed() -> dict[str, list[tuple[str, str]]]:
    return _schema_typed


def get_relationships() -> list[dict]:
    return _relationships


def filter_schema_for_query(
    user_query: str,
    synonyms: dict | None = None,
) -> tuple[dict[str, list[tuple[str, str]]], list[dict]]:
    """Filter schema to tables relevant to the user's query.

    Args:
        user_query: The natural language query.
        synonyms: Optional synonym dict from metadata with keys
                  "table_synonyms" and "column_synonyms".

    Returns filtered (schema_typed, relationships).
    If no tables match, returns the full schema as fallback.
    """
    if not _schema:
        return {}, []

    query_lower = user_query.lower()
    query_words = set(re.findall(r"\w+", query_lower))

    table_syns = synonyms.get("table_synonyms", {}) if synonyms else {}
    col_syns = synonyms.get("column_synonyms", {}) if synonyms else {}

    # Generic column names that exist in many tables — skip these to avoid
    # false-positive matches that pull in every table
    _GENERIC_COLUMNS = {
        "id", "name", "type", "status", "created_at", "updated_at",
        "email", "description", "code", "is_read", "is_deleted",
        "user_id", "email_id",
    }

    matched_tables: set[str] = set()

    for table in _schema:
        table_lower = table.lower()
        # Direct table name match
        if table_lower in query_lower:
            matched_tables.add(table)
            continue
        # Singular form match (e.g., "email" matches "emails")
        if table_lower.endswith("s") and table_lower[:-1] in query_words:
            matched_tables.add(table)
            continue
        # Business term / table synonym match
        for term in table_syns.get(table, []):
            if term.lower() in query_words:
                matched_tables.add(table)
                break
        if table in matched_tables:
            continue
        # Check if any NON-GENERIC column name appears in query
        for col in _schema[table]:
            if col.lower() not in _GENERIC_COLUMNS and col.lower() in query_words:
                matched_tables.add(table)
                break
        if table in matched_tables:
            continue
        # Column synonym match
        for col in _schema[table]:
            key = f"{table}.{col}"
            for syn in col_syns.get(key, []):
                if syn.lower() in query_lower:
                    matched_tables.add(table)
                    break
            if table in matched_tables:
                break
        if table in matched_tables:
            continue
        # Check column hints for semantic matches (only if no metadata synonyms)
        if not col_syns:
            for col in _schema[table]:
                hint = _get_column_hint(col)
                if hint and any(w in hint.lower() for w in query_words if len(w) > 4):
                    matched_tables.add(table)
                    break

    # Add FK-related tables conservatively:
    # Only include a non-matched table if it directly connects two matched tables
    # (bridge table). For single-table queries, no expansion.
    directly_matched = set(matched_tables)
    for table in _schema:
        if table in directly_matched:
            continue
        # Check if this table is FK-connected to at least 2 directly matched tables
        connected_to = set()
        for r in _relationships:
            if r["table"] == table and r["referenced_table"] in directly_matched:
                connected_to.add(r["referenced_table"])
            if r["referenced_table"] == table and r["table"] in directly_matched:
                connected_to.add(r["table"])
        if len(connected_to) >= 2:
            matched_tables.add(table)

    # Fallback: if nothing matched, return full schema
    if not matched_tables:
        return _schema_typed, _relationships

    filtered_schema = {
        t: cols for t, cols in _schema_typed.items() if t in matched_tables
    }
    filtered_rels = [
        r
        for r in _relationships
        if r["table"] in matched_tables and r["referenced_table"] in matched_tables
    ]

    return filtered_schema, filtered_rels


def get_schema_text(
    schema_typed: dict[str, list[tuple[str, str]]] | None = None,
    relationships: list[dict] | None = None,
) -> str:
    """Build schema text for the LLM prompt.

    If schema_typed and relationships are provided, uses those (filtered).
    Otherwise uses the full schema.
    """
    if schema_typed is None:
        schema_typed = _schema_typed
    if relationships is None:
        relationships = _relationships

    lines = []
    for table, columns in schema_typed.items():
        lines.append(f"Table: {table}")
        col_parts = []
        for name, dtype in columns:
            hint = _get_column_hint(name)
            if hint:
                col_parts.append(f"{name} ({dtype}) -- {hint}")
            else:
                col_parts.append(f"{name} ({dtype})")
        lines.append(f"  Columns: {', '.join(col_parts)}")

    if relationships:
        lines.append("\nRelationships (use these for JOINs):")
        for r in relationships:
            lines.append(
                f"  {r['table']}.{r['column']} -> "
                f"{r['referenced_table']}.{r['referenced_column']}"
            )
    return "\n".join(lines)


# Semantic hints: map column name patterns to user-friendly descriptions.
# Helps the LLM pick the right column when the user says "name", "email", etc.
_COLUMN_HINTS = {
    # users
    "display_name": "this is the name",
    "password_hash": "internal, do not select",
    "enabled": "whether the user account is active",
    "deleted": "whether the record is soft-deleted",
    "signature": "user email signature text",
    "role": "user role, e.g. 'ADMIN', 'USER'",
    # emails
    "sender_id": "FK to users.id — the user who sent the email",
    "body_html": "email body content",
    "thread_id": "groups emails into conversation threads",
    "is_draft": "whether the email is a draft (1=draft, 0=sent)",
    "sender_deleted": "whether the sender deleted this email",
    "read_receipt_requested": "whether sender requested a read receipt",
    # email_recipients — IMPORTANT for TO/CC/BCC queries
    "recipient_type": "values: 'TO', 'CC', 'BCC' — the type of recipient",
    "is_read": "whether the recipient has read the email",
    "is_deleted": "whether the recipient deleted the email",
    "is_starred": "whether the recipient starred/flagged the email",
    "read_receipt_sent": "whether a read receipt was sent back",
    "snoozed_until": "datetime until which the email is snoozed",
    # contacts
    "contact_email": "this is the email address of the contact",
    "is_favorite": "whether the contact is marked as favorite",
    # attachments
    "original_filename": "the original file name of the attachment",
    "stored_filename": "internal storage filename",
    "mime_type": "file MIME type, e.g. 'application/pdf'",
    "size_bytes": "file size in bytes",
    # feedback
    "type": "type/category of feedback",
    # user_achievements
    "current_progress": "progress toward unlocking the achievement",
    "notified": "whether user was notified of the achievement",
}


def _get_column_hint(column_name: str) -> str | None:
    return _COLUMN_HINTS.get(column_name)
