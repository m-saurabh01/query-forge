from pydantic import BaseModel


class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    request_id: str | None = None
    sql: str | None = None
    data: dict | None = None
    explanation: str | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    llm: str
    schema_tables: int


class MetricsResponse(BaseModel):
    total_queries: int = 0
    cache_hits: int = 0
    successes: int = 0
    validation_failures: int = 0
    execution_failures: int = 0
    generation_failures: int = 0
    total_retries: int = 0
    avg_latency_s: float = 0.0
    recent_queries: int = 0
