import hashlib
import logging
import time
import uuid

from cachetools import TTLCache

from app.config import settings
from app.db.schema import get_schema, get_schema_text, filter_schema_for_query
from app.llm.prompts import get_prompt_template, build_few_shot_examples, build_error_feedback_prompt
from app.llm.model import generate_async, count_tokens
from app.metadata.loader import get_metadata, get_synonyms
from app.metadata.semantic import classify_intent, reorder_examples_by_intent, build_semantic_schema_text
from app.metrics import metrics
from app.query.generator import extract_sql
from app.query.validator import validate_sql
from app.query.executor import execute

logger = logging.getLogger(__name__)

# Simple in-memory cache for query results
_cache: TTLCache = TTLCache(
    maxsize=settings.cache_max_size,
    ttl=settings.cache_ttl_seconds,
)


def _cache_key(user_query: str) -> str:
    return hashlib.sha256(user_query.strip().lower().encode()).hexdigest()


async def process_query(user_query: str, request_id: str | None = None, debug: bool = False) -> dict:
    """
    NL-to-SQL pipeline with retry, schema filtering, and caching.

    1. Check cache
    2. Filter schema to relevant tables
    3. Build prompt with filtered schema + FK-based examples
    4. Generate SQL with retry loop (re-prompt with error feedback on failure)
    5. Validate SQL against full schema
    6. Execute query with timeout
    7. Cache and return result

    If debug=True, attaches a 'trace' dict with full pipeline internals.
    """
    if request_id is None:
        request_id = str(uuid.uuid4())[:8]

    t0 = time.time()
    log_ctx = f"[{request_id}]"
    metrics.record_query()

    trace = {} if debug else None

    def _t(key, value):
        if trace is not None:
            trace[key] = value

    _t("request_id", request_id)
    _t("user_query", user_query)

    # Check cache
    cache_key = _cache_key(user_query)
    cached = _cache.get(cache_key)
    if cached is not None:
        logger.info("%s Cache hit for query: %s", log_ctx, user_query)
        metrics.record_cache_hit()
        _t("cache_hit", True)
        result = {**cached, "request_id": request_id}
        if trace is not None:
            result["trace"] = trace
        return result

    _t("cache_hit", False)

    schema = get_schema()

    if not schema:
        result = {
            "request_id": request_id,
            "sql": None,
            "data": None,
            "explanation": None,
            "error": "No database schema loaded.",
        }
        if trace is not None:
            result["trace"] = trace
        return result

    # Filter schema to relevant tables (with synonym support)
    synonyms = get_synonyms()
    filtered_schema_typed, filtered_rels = filter_schema_for_query(user_query, synonyms=synonyms)

    _t("filtered_tables", {
        table: [col_name for col_name, _ in cols]
        for table, cols in filtered_schema_typed.items()
    })
    _t("filtered_relationships", filtered_rels)

    # Build semantic schema text (enriched with metadata if available)
    metadata = get_metadata()
    if metadata:
        filtered_schema_text = build_semantic_schema_text(
            filtered_schema_typed, filtered_rels, metadata=metadata,
        )
    else:
        filtered_schema_text = get_schema_text(filtered_schema_typed, filtered_rels)

    _t("schema_text", filtered_schema_text)

    # Classify intent
    intent = classify_intent(user_query, table_names=list(filtered_schema_typed.keys()))
    logger.info("%s Intent: %s", log_ctx, intent)
    _t("intent", intent)

    # Build filtered plain schema for few-shot examples (table -> column names)
    filtered_schema_plain = {
        table: [col_name for col_name, _ in cols]
        for table, cols in filtered_schema_typed.items()
    }

    # Build few-shot examples from filtered schema with actual FK relationships
    examples = build_few_shot_examples(
        filtered_schema_plain, filtered_rels, settings.db_dialect
    )
    examples = reorder_examples_by_intent(examples, intent)
    _t("few_shot_examples", examples)

    # Generate SQL with retry loop
    sql = None
    last_error = None
    attempts_trace = []

    for attempt in range(1, settings.llm_max_retries + 1):
        attempt_info = {"attempt": attempt}
        try:
            prompt_template = get_prompt_template(settings.db_dialect)
            prompt = prompt_template.format(
                schema=filtered_schema_text,
                examples=examples,
                user_query=user_query,
            )

            # On retry, append error feedback
            if last_error and attempt > 1:
                prompt = build_error_feedback_prompt(prompt, last_error)
                logger.info(
                    "%s Retry %d/%d with error feedback",
                    log_ctx, attempt, settings.llm_max_retries,
                )

            temperature = min(0.1 + (attempt - 1) * 0.1, 0.5)
            attempt_info["temperature"] = temperature
            attempt_info["prompt"] = prompt

            # Token counting — warn if prompt may be truncated
            prompt_tokens = count_tokens(prompt)
            attempt_info["prompt_tokens"] = prompt_tokens
            if prompt_tokens > 0:
                available = settings.n_ctx - 256  # reserve for generation
                if prompt_tokens > available:
                    logger.warning(
                        "%s Prompt (%d tokens) exceeds context window (%d). "
                        "Output may be truncated.",
                        log_ctx, prompt_tokens, settings.n_ctx,
                    )
                    _t("token_warning", f"Prompt {prompt_tokens} tokens exceeds {available} available")

            logger.info(
                "%s Generating SQL (attempt %d, %d tokens) for: %s",
                log_ctx, attempt, prompt_tokens, user_query,
            )
            raw = await generate_async(prompt, max_tokens=256, temperature=temperature)
            attempt_info["raw_llm_response"] = raw

            sql = extract_sql(raw)
            attempt_info["extracted_sql"] = sql
            logger.info("%s Generated SQL: %s", log_ctx, sql)

            # Validate SQL against full schema
            is_valid, error_msg, sql = validate_sql(sql, schema)
            attempt_info["validation_passed"] = is_valid
            attempt_info["validation_error"] = error_msg
            attempt_info["final_sql"] = sql

            if is_valid:
                if trace is not None:
                    attempts_trace.append(attempt_info)
                break
            else:
                last_error = error_msg
                metrics.record_retry()
                logger.warning(
                    "%s Validation failed (attempt %d): %s | SQL: %s",
                    log_ctx, attempt, error_msg, sql,
                )
                if trace is not None:
                    attempts_trace.append(attempt_info)
                if attempt == settings.llm_max_retries:
                    metrics.record_validation_failure()
                    _t("attempts", attempts_trace)
                    result = {
                        "request_id": request_id,
                        "sql": sql,
                        "data": None,
                        "explanation": None,
                        "error": f"SQL validation failed after {attempt} attempts: {error_msg}",
                    }
                    if trace is not None:
                        result["trace"] = trace
                    return result

        except Exception as e:
            last_error = str(e)
            attempt_info["exception"] = str(e)
            metrics.record_retry()
            logger.error("%s Generation attempt %d failed: %s", log_ctx, attempt, e)
            if trace is not None:
                attempts_trace.append(attempt_info)
            if attempt == settings.llm_max_retries:
                metrics.record_generation_failure()
                _t("attempts", attempts_trace)
                result = {
                    "request_id": request_id,
                    "sql": None,
                    "data": None,
                    "explanation": None,
                    "error": f"Failed to generate SQL after {attempt} attempts: {e}",
                }
                if trace is not None:
                    result["trace"] = trace
                return result

    _t("attempts", attempts_trace)

    # Execute
    try:
        logger.info("%s Executing SQL", log_ctx)
        data = await execute(sql)
        _t("execution_success", True)
    except RuntimeError as e:
        metrics.record_execution_failure()
        _t("execution_success", False)
        _t("execution_error", str(e))
        result = {
            "request_id": request_id,
            "sql": sql,
            "data": None,
            "explanation": None,
            "error": str(e),
        }
        if trace is not None:
            result["trace"] = trace
        return result

    elapsed = round(time.time() - t0, 2)
    metrics.record_success(elapsed)
    logger.info("%s Pipeline completed in %.2fs", log_ctx, elapsed)
    _t("elapsed_seconds", elapsed)

    # Build natural-language explanation
    row_count = len(data["rows"])
    explanation = await _build_explanation(sql, row_count, elapsed)

    result = {
        "request_id": request_id,
        "sql": sql,
        "data": data,
        "explanation": explanation,
        "error": None,
    }

    if trace is not None:
        result["trace"] = trace

    # Cache the result (without trace)
    cache_result = {k: v for k, v in result.items() if k != "trace"}
    _cache[cache_key] = cache_result

    return result


async def _build_explanation(sql: str, row_count: int, elapsed: float) -> str:
    """Generate a short NL explanation of what the SQL does.

    Runs a lightweight LLM call in the background thread. Falls back to a
    simple row-count string if the call fails or times out.
    """
    fallback = f"Returned {row_count} row(s) in {elapsed}s."
    try:
        prompt = (
            "Explain this SQL query in one plain-English sentence "
            "(no SQL, no code, no quotes):\n"
            f"{sql}\n"
            "Explanation:"
        )
        raw = await generate_async(prompt, max_tokens=80, temperature=0.1)
        explanation = raw.strip().split("\n")[0].strip()
        if explanation:
            return f"{explanation}  ({row_count} row(s), {elapsed}s)"
    except Exception as e:
        logger.debug("Explanation generation failed: %s", e)
    return fallback
