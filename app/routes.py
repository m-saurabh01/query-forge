import logging
import time
import uuid
from collections import defaultdict

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import FileResponse

from app.config import settings
from app.models import QueryRequest, QueryResponse, HealthResponse, MetricsResponse
from app.query.pipeline import process_query
from app.db.pool import check_connection
from app.db.schema import get_schema, reload_schema
from app.llm.model import is_model_loaded
from app.metrics import metrics

logger = logging.getLogger(__name__)

router = APIRouter()

# ── In-memory rate limiter (per-IP, per-minute rolling window) ──────────
_rate_buckets: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(client_ip: str) -> bool:
    """Return True if the request is allowed, False if rate-limited."""
    if settings.rate_limit_rpm <= 0:
        return True  # disabled

    now = time.time()
    window = 60.0
    bucket = _rate_buckets[client_ip]

    # Prune entries older than the window
    _rate_buckets[client_ip] = [t for t in bucket if now - t < window]
    bucket = _rate_buckets[client_ip]

    if len(bucket) >= settings.rate_limit_rpm:
        return False  # over limit

    bucket.append(now)
    return True


@router.get("/")
async def root():
    return FileResponse("static/index.html")


@router.get("/health", response_model=HealthResponse)
async def health():
    db_ok = await check_connection()
    llm_ok = is_model_loaded()
    schema = get_schema()
    return HealthResponse(
        status="ok" if (db_ok and llm_ok) else "degraded",
        database="connected" if db_ok else "disconnected",
        llm="loaded" if llm_ok else "not loaded",
        schema_tables=len(schema),
    )


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics():
    return MetricsResponse(**metrics.snapshot())


@router.post("/api/query", response_model=QueryResponse)
async def query_endpoint(request: Request, body: QueryRequest):
    user_query = body.query.strip()
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])

    if not user_query:
        return QueryResponse(request_id=request_id, error="Empty query")

    # Input length guard
    if len(user_query) > settings.max_query_length:
        return QueryResponse(
            request_id=request_id,
            error=f"Query too long ({len(user_query)} chars). "
                  f"Maximum is {settings.max_query_length}.",
        )

    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded ({settings.rate_limit_rpm} req/min). Try again shortly.",
        )

    try:
        result = await process_query(user_query, request_id=request_id)
        return QueryResponse(**result)
    except Exception as e:
        logger.error("[%s] Unhandled error: %s", request_id, e, exc_info=True)
        return QueryResponse(
            request_id=request_id,
            error="An internal error occurred. Please try again.",
        )


@router.get("/dev")
async def dev_ui():
    return FileResponse("static/dev.html")


@router.post("/api/query/debug")
async def query_debug_endpoint(request: Request, body: QueryRequest):
    """Debug endpoint that returns full pipeline trace.
    Gated behind DEBUG_ENABLED config flag.
    """
    if not settings.debug_enabled:
        raise HTTPException(status_code=404, detail="Not found")

    user_query = body.query.strip()
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])

    if not user_query:
        return {"request_id": request_id, "error": "Empty query", "trace": {}}

    try:
        result = await process_query(user_query, request_id=request_id, debug=True)
        return result
    except Exception as e:
        logger.error("[%s] Debug unhandled error: %s", request_id, e, exc_info=True)
        return {
            "request_id": request_id,
            "error": f"Internal error: {e}",
            "trace": {},
        }


@router.post("/admin/reload-schema")
async def admin_reload_schema(request: Request):
    """Reload database schema without restarting the server.
    Protected by ADMIN_SECRET header if configured.
    """
    if settings.admin_secret:
        provided = request.headers.get("X-Admin-Secret", "")
        if provided != settings.admin_secret:
            raise HTTPException(status_code=403, detail="Forbidden")

    try:
        await reload_schema()
        schema = get_schema()
        return {
            "status": "ok",
            "tables_loaded": len(schema),
            "tables": list(schema.keys()),
        }
    except Exception as e:
        logger.error("Schema reload failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Reload failed: {e}")
