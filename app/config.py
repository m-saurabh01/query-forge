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

    # LLM backend: "llamacpp" (load GGUF locally) or "ollama" (HTTP API)
    llm_backend: str = "llamacpp"

    # Ollama settings (only used when llm_backend=ollama)
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "mistral"

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

    # Input validation
    max_query_length: int = 500

    # Rate limiting (requests per IP per minute)
    rate_limit_rpm: int = 20

    # Debug endpoint
    debug_enabled: bool = True

    # Admin endpoint secret (empty = no auth on admin routes)
    admin_secret: str = ""

    class Config:
        env_file = ".env"
        protected_namespaces = ("settings_",)


settings = Settings()
