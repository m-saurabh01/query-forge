import threading
import time
from dataclasses import dataclass, field


@dataclass
class PipelineMetrics:
    total_queries: int = 0
    cache_hits: int = 0
    successes: int = 0
    validation_failures: int = 0
    execution_failures: int = 0
    generation_failures: int = 0
    total_retries: int = 0
    _latencies: list[float] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_query(self):
        with self._lock:
            self.total_queries += 1

    def record_cache_hit(self):
        with self._lock:
            self.cache_hits += 1

    def record_success(self, latency_s: float):
        with self._lock:
            self.successes += 1
            self._latencies.append(latency_s)
            # Keep only last 500 entries
            if len(self._latencies) > 500:
                self._latencies = self._latencies[-500:]

    def record_validation_failure(self):
        with self._lock:
            self.validation_failures += 1

    def record_execution_failure(self):
        with self._lock:
            self.execution_failures += 1

    def record_generation_failure(self):
        with self._lock:
            self.generation_failures += 1

    def record_retry(self):
        with self._lock:
            self.total_retries += 1

    def snapshot(self) -> dict:
        with self._lock:
            avg_latency = (
                round(sum(self._latencies) / len(self._latencies), 3)
                if self._latencies
                else 0.0
            )
            return {
                "total_queries": self.total_queries,
                "cache_hits": self.cache_hits,
                "successes": self.successes,
                "validation_failures": self.validation_failures,
                "execution_failures": self.execution_failures,
                "generation_failures": self.generation_failures,
                "total_retries": self.total_retries,
                "avg_latency_s": avg_latency,
                "recent_queries": len(self._latencies),
            }


# Module-level singleton
metrics = PipelineMetrics()
