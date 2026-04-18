from __future__ import annotations

import asyncio

from yaruk.core.fallback import FallbackExecutor


async def _success_fn(provider: str, *a: object, **kw: object) -> str:
    return f"result-from-{provider}"


async def _fail_first_fn(provider: str, *a: object, **kw: object) -> str:
    if provider == "first":
        raise RuntimeError("first failed")
    return f"result-from-{provider}"


async def _all_fail_fn(provider: str, *a: object, **kw: object) -> str:
    raise RuntimeError(f"{provider} failed")


def test_fallback_first_success() -> None:
    executor = FallbackExecutor()
    result, decision = asyncio.run(
        executor.execute_with_fallback(["marker", "docling"], _success_fn)
    )
    assert result == "result-from-marker"
    assert decision.provider == "marker"
    assert decision.attempt == 1


def test_fallback_second_success() -> None:
    executor = FallbackExecutor()
    result, decision = asyncio.run(
        executor.execute_with_fallback(["first", "second"], _fail_first_fn)
    )
    assert result == "result-from-second"
    assert decision.provider == "second"
    assert decision.attempt == 2


def test_fallback_all_fail() -> None:
    executor = FallbackExecutor()
    result, decision = asyncio.run(
        executor.execute_with_fallback(["a", "b"], _all_fail_fn)
    )
    assert result is None
    assert "all-failed" in decision.reason
