import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.db.pool import init_pool, close_pool
from app.db.schema import load_schema
from app.llm.model import load_model
from app.routes import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await init_pool()
    await load_schema()
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/static", StaticFiles(directory="static"), name="static")
    app.include_router(router)

    return app
