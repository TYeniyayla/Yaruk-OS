from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class Node:
    name: str
    fn: Callable[..., Any]
    depends_on: list[str] = field(default_factory=list)
    condition: Callable[[dict[str, Any]], bool] | None = None


class DAG:
    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}

    def add(self, node: Node) -> None:
        self._nodes[node.name] = node

    def get(self, name: str) -> Node:
        return self._nodes[name]

    @property
    def nodes(self) -> dict[str, Node]:
        return dict(self._nodes)

    def topological_order(self) -> list[str]:
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            node = self._nodes[name]
            for dep in node.depends_on:
                visit(dep)
            order.append(name)

        for name in self._nodes:
            visit(name)
        return order

    def parallel_groups(self) -> list[list[str]]:
        """Nodes with all deps satisfied can run in parallel within the same group."""
        order = self.topological_order()
        done: set[str] = set()
        groups: list[list[str]] = []
        remaining = list(order)
        while remaining:
            group: list[str] = []
            still_remaining: list[str] = []
            for name in remaining:
                node = self._nodes[name]
                if all(d in done for d in node.depends_on):
                    group.append(name)
                else:
                    still_remaining.append(name)
            done.update(group)
            groups.append(group)
            remaining = still_remaining
        return groups

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        ctx: dict[str, Any] = dict(context) if context else {}
        groups = self.parallel_groups()

        for group in groups:
            runnable: list[str] = []
            for name in group:
                node = self._nodes[name]
                if node.condition and not node.condition(ctx):
                    log.debug("skipping node %s (condition false)", name)
                    ctx[name] = None
                    continue
                runnable.append(name)

            if not runnable:
                continue

            tasks = []
            for name in runnable:
                node = self._nodes[name]
                fn = node.fn
                if inspect.iscoroutinefunction(fn):
                    tasks.append((name, fn(ctx)))
                else:
                    tasks.append((name, asyncio.get_event_loop().run_in_executor(None, fn, ctx)))

            results = await asyncio.gather(*(t for _, t in tasks), return_exceptions=True)
            for (name, _), result in zip(tasks, results, strict=True):
                if isinstance(result, Exception):
                    log.error("node %s failed: %s", name, result)
                    ctx[name] = None
                else:
                    ctx[name] = result

        return ctx
