import logging

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


def get_schema() -> dict[str, list[str]]:
    return _schema


def get_schema_text() -> str:
    lines = []
    for table, columns in _schema_typed.items():
        lines.append(f"Table: {table}")
        col_parts = []
        for name, dtype in columns:
            hint = _get_column_hint(name)
            if hint:
                col_parts.append(f"{name} ({dtype}) -- {hint}")
            else:
                col_parts.append(f"{name} ({dtype})")
        lines.append(f"  Columns: {', '.join(col_parts)}")
    if _relationships:
        lines.append("\nRelationships:")
        for r in _relationships:
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
