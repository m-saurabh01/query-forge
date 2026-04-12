import asyncio

import aiomysql
import logging

from app.config import settings

logger = logging.getLogger(__name__)

_pool: aiomysql.Pool | None = None


async def init_pool() -> aiomysql.Pool:
    global _pool
    _pool = await aiomysql.create_pool(
        host=settings.db_host,
        port=settings.db_port,
        db=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        minsize=1,
        maxsize=5,
        autocommit=True,
    )
    logger.info("MySQL connection pool created")
    return _pool


def get_pool() -> aiomysql.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")
    return _pool


async def execute_query(sql: str, timeout: int | None = None) -> dict:
    if timeout is None:
        timeout = settings.query_timeout_seconds
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await asyncio.wait_for(cur.execute(sql), timeout=timeout)
            except asyncio.TimeoutError:
                raise RuntimeError(f"Query execution timed out after {timeout}s")
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = await cur.fetchall()
            return {
                "columns": columns,
                "rows": [list(row) for row in rows],
            }


async def check_connection() -> bool:
    """Check if database connection is healthy."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                return True
    except Exception:
        return False


async def close_pool():
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("MySQL connection pool closed")
