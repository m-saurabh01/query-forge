"""Ollama HTTP API backend.

Talks to a running Ollama server (``ollama serve``) via its REST API.
No model file loading needed — Ollama manages models separately.
"""

import logging
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

from app.config import settings

logger = logging.getLogger(__name__)

_ready: bool = False


def load_model():
    """Verify Ollama is reachable and the configured model exists."""
    global _ready
    base = settings.ollama_base_url.rstrip("/")
    model = settings.ollama_model

    # Check connectivity
    try:
        resp = urlopen(f"{base}/api/tags", timeout=10)
        data = json.loads(resp.read())
    except (URLError, OSError) as e:
        raise RuntimeError(
            f"Cannot connect to Ollama at {base}: {e}"
        ) from e

    # Check model is available
    available = [m["name"].split(":")[0] for m in data.get("models", [])]
    if model not in available and f"{model}:latest" not in [m["name"] for m in data.get("models", [])]:
        logger.warning(
            "Model '%s' not found in Ollama. Available: %s. "
            "Ollama will attempt to pull it on first request.",
            model, available,
        )

    _ready = True
    logger.info("Ollama backend ready (server=%s, model=%s)", base, model)


def is_model_loaded() -> bool:
    return _ready


def count_tokens(text: str) -> int:
    """Ollama doesn't expose a tokenizer — return estimate (1 token ≈ 4 chars)."""
    return len(text) // 4


def generate(prompt: str, max_tokens: int = 512, temperature: float = 0.1) -> str:
    """Call Ollama /api/generate (synchronous, streaming disabled)."""
    if not _ready:
        raise RuntimeError("Ollama backend not initialized. Call load_model() first.")

    base = settings.ollama_base_url.rstrip("/")
    payload = json.dumps({
        "model": settings.ollama_model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
            "top_p": 0.9,
            "repeat_penalty": 1.1,
            "stop": ["```", "\n\n\n", "###", "Question:", "Question "],
        },
    }).encode()

    req = Request(
        f"{base}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    try:
        resp = urlopen(req, timeout=settings.query_timeout_seconds + 60)
        data = json.loads(resp.read())
    except (URLError, OSError) as e:
        raise RuntimeError(f"Ollama API call failed: {e}") from e

    return data.get("response", "").strip()
