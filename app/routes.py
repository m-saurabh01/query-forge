import logging
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from app.models import QueryRequest, QueryResponse, HealthResponse, MetricsResponse
from app.query.pipeline import process_query
from app.db.pool import check_connection
from app.db.schema import get_schema
from app.llm.model import is_model_loaded
from app.metrics import metrics

logger = logging.getLogger(__name__)

router = APIRouter()


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
    """Debug endpoint that returns full pipeline trace."""
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
