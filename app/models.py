from pydantic import BaseModel


class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    sql: str | None = None
    data: dict | None = None
    explanation: str | None = None
    error: str | None = None
