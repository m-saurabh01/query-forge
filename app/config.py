from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 3306
    db_name: str = "pulse_db"
    db_user: str = "root"
    db_password: str = ""

    model_path: str = "./Llama-3.2-3B.Q4_K_M.gguf"
    n_ctx: int = 2048
    n_gpu_layers: int = 0

    db_dialect: str = "mysql"

    # Metadata
    metadata_path: str = "./app/metadata/schema_metadata.json"

    # CORS — comma-separated origins
    cors_origins: str = "http://localhost:8000"

    # Query execution
    query_timeout_seconds: int = 30
    max_result_rows: int = 100
    max_result_size_bytes: int = 1_048_576  # 1 MB

    # LLM retry
    llm_max_retries: int = 3

    # Cache
    cache_max_size: int = 256
    cache_ttl_seconds: int = 300

    class Config:
        env_file = ".env"
        protected_namespaces = ("settings_",)


settings = Settings()
