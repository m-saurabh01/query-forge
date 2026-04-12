import logging

from app.db.pool import execute_query

logger = logging.getLogger(__name__)


async def execute(sql: str) -> dict:
    """Execute a validated SQL query and return results."""
    try:
        result = await execute_query(sql)
        logger.info("Query executed: %d rows returned", len(result["rows"]))
        return result
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        raise RuntimeError(f"Query execution failed: {e}") from e
