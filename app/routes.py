from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.models import QueryRequest, QueryResponse
from app.query.pipeline import process_query

router = APIRouter()


@router.get("/")
async def root():
    return FileResponse("static/index.html")


@router.post("/api/query", response_model=QueryResponse)
async def query_endpoint(request: QueryRequest):
    user_query = request.query.strip()
    if not user_query:
        return QueryResponse(error="Empty query")
    result = await process_query(user_query)
    return result
