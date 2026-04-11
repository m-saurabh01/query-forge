import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.db import init_pool, close_pool
from app.schema_loader import load_schema
from app.llm import load_model
from app.pipeline import process_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up...")
    await init_pool()
    await load_schema()
    load_model()
    logger.info("All systems ready")
    yield
    # Shutdown
    await close_pool()
    logger.info("Shut down complete")


app = FastAPI(title="NL-to-SQL Query Chatbot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


class QueryRequest(BaseModel):
    query: str


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.post("/api/query")
async def query_endpoint(request: QueryRequest):
    user_query = request.query.strip()
    if not user_query:
        return {"sql": None, "data": None, "explanation": None, "error": "Empty query"}
    result = await process_query(user_query)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
