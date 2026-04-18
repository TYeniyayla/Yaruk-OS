from __future__ import annotations

import asyncio
from typing import Any

from yaruk.core.graph import DAG, Node


def _sync_node(ctx: dict[str, Any]) -> str:
    return "sync-result"


async def _async_node(ctx: dict[str, Any]) -> str:
    return "async-result"


def _conditional_node(ctx: dict[str, Any]) -> str:
    return "conditional-result"


def test_dag_async_execution() -> None:
    dag = DAG()
    dag.add(Node(name="step1", fn=_sync_node))
    dag.add(Node(name="step2", fn=_async_node, depends_on=["step1"]))

    ctx = asyncio.run(dag.execute())
    assert ctx["step1"] == "sync-result"
    assert ctx["step2"] == "async-result"


def test_dag_conditional_skip() -> None:
    dag = DAG()
    dag.add(Node(name="always", fn=_sync_node))
    dag.add(Node(
        name="skipped",
        fn=_conditional_node,
        depends_on=["always"],
        condition=lambda ctx: False,
    ))
    dag.add(Node(
        name="after",
        fn=_sync_node,
        depends_on=["skipped"],
    ))

    ctx = asyncio.run(dag.execute())
    assert ctx["always"] == "sync-result"
    assert ctx["skipped"] is None
    assert ctx["after"] == "sync-result"


def test_dag_parallel_execution() -> None:
    results: list[str] = []

    def fn_a(ctx: dict[str, Any]) -> str:
        results.append("a")
        return "a"

    def fn_b(ctx: dict[str, Any]) -> str:
        results.append("b")
        return "b"

    def fn_merge(ctx: dict[str, Any]) -> str:
        return f"{ctx.get('a')}-{ctx.get('b')}"

    dag = DAG()
    dag.add(Node(name="a", fn=fn_a))
    dag.add(Node(name="b", fn=fn_b))
    dag.add(Node(name="merge", fn=fn_merge, depends_on=["a", "b"]))

    ctx = asyncio.run(dag.execute())
    assert ctx["merge"] == "a-b"
    assert set(results) == {"a", "b"}
