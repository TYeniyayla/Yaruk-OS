from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from yaruk.core.graph import DAG, Node


@dataclass
class PipelineStep:
    name: str
    handler: Callable[..., Any]
    depends_on: list[str] = field(default_factory=list)
    condition: Callable[[dict[str, Any]], bool] | None = None


class PipelineBuilder:
    def __init__(self) -> None:
        self._steps: list[PipelineStep] = []

    def add(
        self,
        name: str,
        handler: Callable[..., Any],
        depends_on: list[str] | None = None,
        condition: Callable[[dict[str, Any]], bool] | None = None,
    ) -> PipelineBuilder:
        self._steps.append(PipelineStep(name, handler, depends_on or [], condition))
        return self

    def build(self) -> DAG:
        dag = DAG()
        for step in self._steps:
            dag.add(
                Node(
                    name=step.name,
                    fn=step.handler,
                    depends_on=step.depends_on,
                    condition=step.condition,
                )
            )
        return dag

    async def execute(self, initial_data: dict[str, Any] | None = None) -> dict[str, Any]:
        dag = self.build()
        return await dag.execute(initial_data)
