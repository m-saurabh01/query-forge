import json
import logging
from pathlib import Path

from app.config import settings

logger = logging.getLogger(__name__)

_metadata: dict = {}


def load_metadata() -> dict:
    """Load metadata from JSON file. Graceful fallback if missing."""
    global _metadata
    meta_path = Path(settings.metadata_path)

    if not meta_path.exists():
        logger.warning("Metadata file not found at %s — running without metadata", meta_path)
        _metadata = {}
        return _metadata

    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            _metadata = json.load(f)
        table_count = len(_metadata.get("tables", {}))
        rel_count = len(_metadata.get("relationships", {}))
        logger.info("Metadata loaded: %d tables, %d relationships", table_count, rel_count)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load metadata from %s: %s", meta_path, e)
        _metadata = {}

    return _metadata


def get_metadata() -> dict:
    return _metadata


def get_table_metadata(table_name: str) -> dict | None:
    """Get metadata for a specific table."""
    return _metadata.get("tables", {}).get(table_name)


def get_column_metadata(table_name: str, column_name: str) -> dict | None:
    """Get metadata for a specific column."""
    table_meta = get_table_metadata(table_name)
    if table_meta:
        return table_meta.get("columns", {}).get(column_name)
    return None


def get_relationship_metadata(rel_key: str) -> dict | None:
    """Get metadata for a relationship by key like 'emails.sender_id -> users.id'."""
    return _metadata.get("relationships", {}).get(rel_key)


def get_synonyms() -> dict[str, dict]:
    """Extract synonym mappings from metadata.

    Returns:
        {
            "table_synonyms": { "users": ["user", "person", "account", ...], ... },
            "column_synonyms": { "users.display_name": ["name", "username"], ... },
        }
    """
    table_synonyms: dict[str, list[str]] = {}
    column_synonyms: dict[str, list[str]] = {}

    for table_name, table_meta in _metadata.get("tables", {}).items():
        # Table business terms
        terms = table_meta.get("business_terms", [])
        if terms:
            table_synonyms[table_name] = terms

        # Column synonyms
        for col_name, col_meta in table_meta.get("columns", {}).items():
            syns = col_meta.get("synonyms", [])
            if syns:
                column_synonyms[f"{table_name}.{col_name}"] = syns

    return {
        "table_synonyms": table_synonyms,
        "column_synonyms": column_synonyms,
    }
