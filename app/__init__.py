import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db.pool import init_pool, close_pool
from app.db.schema import load_schema
from app.llm.model import load_model
from app.metadata import load_metadata
from app.routes import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await init_pool()
    await load_schema()
    load_metadata()
    load_model()
    logger.info("All systems ready")
    yield
    await close_pool()
    logger.info("Shut down complete")


def create_app() -> FastAPI:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    app = FastAPI(title="NL-to-SQL Query Chatbot", lifespan=lifespan)

    # CORS — restrict origins from config
    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Request ID middleware
    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    app.mount("/static", StaticFiles(directory="static"), name="static")
    app.include_router(router)

    return app
