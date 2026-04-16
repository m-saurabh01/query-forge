"""llama-cpp-python backend — loads a GGUF model file directly."""

import logging

from llama_cpp import Llama

from app.config import settings

logger = logging.getLogger(__name__)

_model: Llama | None = None


def load_model():
    global _model
    logger.info("Loading LLM from %s ...", settings.model_path)
    _model = Llama(
        model_path=settings.model_path,
        n_ctx=settings.n_ctx,
        n_gpu_layers=settings.n_gpu_layers,
        verbose=False,
    )
    logger.info("LLM loaded successfully")


def is_model_loaded() -> bool:
    return _model is not None


def count_tokens(text: str) -> int:
    """Count tokens using the loaded model's tokenizer."""
    if _model is None:
        return -1
    return len(_model.tokenize(text.encode("utf-8")))


def generate(prompt: str, max_tokens: int = 512, temperature: float = 0.1) -> str:
    if _model is None:
        raise RuntimeError("LLM not loaded. Call load_model() first.")
    output = _model(
        prompt,
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=0.9,
        repeat_penalty=1.1,
        stop=["```", "\n\n\n", "###", "Question:", "Question "],
    )
    return output["choices"][0]["text"].strip()
