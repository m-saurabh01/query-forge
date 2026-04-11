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

    class Config:
        env_file = ".env"
        protected_namespaces = ("settings_",)


settings = Settings()
