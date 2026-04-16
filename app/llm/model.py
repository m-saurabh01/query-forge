"""LLM abstraction layer.

Dispatches to either llama-cpp-python (local GGUF) or Ollama (HTTP API)
based on ``settings.llm_backend``.  All other modules import from here —
the backend switch is transparent to the rest of the codebase.
"""

import asyncio
import importlib
import logging
import time

from app.config import settings

logger = logging.getLogger(__name__)

# ── Backend dispatch ────────────────────────────────────────────────────
# Each backend module must expose:
#   load_model(), is_model_loaded(), count_tokens(text), generate(prompt, max_tokens, temperature)

_backend = None  # module reference, set in load_model()


def _get_backend():
    global _backend
    if _backend is not None:
        return _backend

    name = settings.llm_backend.lower().strip()
    if name == "llamacpp":
        _backend = importlib.import_module("app.llm.llamacpp_backend")
    elif name == "ollama":
        _backend = importlib.import_module("app.llm.ollama_backend")
    else:
        raise ValueError(
            f"Unknown LLM_BACKEND '{settings.llm_backend}'. "
            "Use 'llamacpp' or 'ollama'."
        )
    logger.info("LLM backend: %s", name)
    return _backend


# ── Public API (unchanged signatures) ──────────────────────────────────

def load_model():
    _get_backend().load_model()


def is_model_loaded() -> bool:
    return _get_backend().is_model_loaded()


def count_tokens(text: str) -> int:
    return _get_backend().count_tokens(text)


def generate(prompt: str, max_tokens: int = 512, temperature: float = 0.1) -> str:
    return _get_backend().generate(prompt, max_tokens=max_tokens, temperature=temperature)


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
