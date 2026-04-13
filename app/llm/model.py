import asyncio
import logging
import time

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
    """Count tokens using the loaded model's tokenizer.
    Returns -1 if model is not loaded (graceful fallback).
    """
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


def generate_with_retry(
    prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.1,
    max_retries: int | None = None,
    error_feedback_fn=None,
) -> str:
    """Generate with retry loop. On failure, optionally append error feedback to prompt."""
    if max_retries is None:
        max_retries = settings.llm_max_retries

    last_error = None
    current_prompt = prompt

    for attempt in range(1, max_retries + 1):
        try:
            result = generate(current_prompt, max_tokens=max_tokens, temperature=temperature)
            return result
        except Exception as e:
            last_error = e
            logger.warning("LLM generation attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)
                if error_feedback_fn:
                    current_prompt = error_feedback_fn(prompt, str(e))
                temperature = min(temperature + 0.1, 0.5)

    raise RuntimeError(f"LLM generation failed after {max_retries} attempts: {last_error}")


async def generate_async(prompt: str, max_tokens: int = 512, temperature: float = 0.1) -> str:
    """Non-blocking wrapper — runs LLM inference in a thread pool so the
    async event loop stays responsive for health checks, static files, etc."""
    return await asyncio.to_thread(generate, prompt, max_tokens, temperature)
