from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FallbackDecision:
    provider: str
    reason: str
    attempt: int


class FallbackExecutor:
    """Verilen provider zincirinde sirayla dener; ilk basariliyi dondurur."""

    async def execute_with_fallback(
        self,
        chain: list[str],
        execute_fn: Any,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Any, FallbackDecision]:
        last_error = ""
        for idx, provider_name in enumerate(chain):
            try:
                result = await execute_fn(provider_name, *args, **kwargs)
                return result, FallbackDecision(
                    provider=provider_name,
                    reason="success",
                    attempt=idx + 1,
                )
            except Exception as exc:
                last_error = f"{provider_name}: {exc}"
                log.warning("fallback triggered", extra={
                    "provider": provider_name, "error": str(exc), "attempt": idx + 1,
                })
                continue

        return None, FallbackDecision(
            provider=chain[-1] if chain else "none",
            reason=f"all-failed: {last_error}",
            attempt=len(chain),
        )

