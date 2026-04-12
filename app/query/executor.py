import logging
import sys

from app.config import settings
from app.db.pool import execute_query

logger = logging.getLogger(__name__)


async def execute(sql: str) -> dict:
    """Execute a validated SQL query with timeout and result size control."""
    try:
        result = await execute_query(sql, timeout=settings.query_timeout_seconds)

        # Enforce max result rows
        if len(result["rows"]) > settings.max_result_rows:
            result["rows"] = result["rows"][: settings.max_result_rows]
            logger.warning("Result truncated to %d rows", settings.max_result_rows)

        # Enforce max result size
        result_size = sys.getsizeof(str(result))
        if result_size > settings.max_result_size_bytes:
            while result["rows"] and sys.getsizeof(str(result)) > settings.max_result_size_bytes:
                result["rows"] = result["rows"][: len(result["rows"]) // 2]
            logger.warning("Result trimmed to fit size limit (%d bytes)", settings.max_result_size_bytes)

        logger.info("Query executed: %d rows returned", len(result["rows"]))
        return result
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        raise RuntimeError(f"Query execution failed: {e}") from e
