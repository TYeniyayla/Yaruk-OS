from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class ProviderLatency:
    provider: str
    method: str
    elapsed_ms: float


class MetricsCollector:
    def __init__(self) -> None:
        self._records: list[ProviderLatency] = []

    @contextmanager
    def measure(self, provider: str, method: str) -> Generator[None, None, None]:
        start = time.monotonic()
        yield
        elapsed = (time.monotonic() - start) * 1000
        self._records.append(ProviderLatency(provider=provider, method=method, elapsed_ms=elapsed))

    @property
    def records(self) -> list[ProviderLatency]:
        return list(self._records)
