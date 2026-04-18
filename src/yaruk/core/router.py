from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from yaruk.core.config import YarukSettings
from yaruk.core.memory_guard import DynamicMemoryGuard, MemoryDecision
from yaruk.models.canonical import AnalysisSignal
from yaruk.models.enums import BlockType

if TYPE_CHECKING:
    from yaruk.core.provider import BaseProvider
    from yaruk.core.segmenter import PageSegment

log = logging.getLogger(__name__)

GPU_ENGINES = frozenset({"mineru", "docling", "marker"})

DEFAULT_CHAIN: dict[str, list[str]] = {
    "paragraph": ["marker", "docling", "markitdown"],
    "table": ["docling", "mineru", "marker"],
    "equation": ["mineru", "marker", "docling"],
    "figure": ["marker", "docling", "mineru"],
    "heading": ["marker", "docling", "markitdown"],
    "list": ["marker", "docling", "markitdown"],
    "code": ["marker", "markitdown", "docling"],
    "caption": ["marker", "docling", "markitdown"],
    "footer": ["markitdown", "marker", "docling"],
    "header": ["markitdown", "marker", "docling"],
    "other": ["marker", "docling", "markitdown"],
}


@dataclass(frozen=True)
class RoutingDecision:
    provider_chain: list[str]
    gpu_allowed: bool
    max_batch_size: int
    reason: str


@dataclass(frozen=True)
class SegmentRoutingDecision:
    """Per-segment routing: which provider to use for a specific segment."""
    segment_block_type: str
    provider_chain: list[str]
    best_provider: str
    score: float
    gpu_allowed: bool
    reason: str


@dataclass
class PageRoutingPlan:
    """Complete routing plan for a single page: one decision per segment."""
    page_number: int
    segment_decisions: list[SegmentRoutingDecision] = field(default_factory=list)
    providers_needed: set[str] = field(default_factory=set)


class DynamicRouter:
    def __init__(
        self,
        settings: YarukSettings,
        available_providers: list[str] | None = None,
        provider_instances: dict[str, BaseProvider] | None = None,
    ) -> None:
        self._settings = settings
        self._guard = DynamicMemoryGuard(settings)
        self._available = set(available_providers) if available_providers else set()
        self._providers = provider_instances or {}

    def set_available_providers(self, names: list[str]) -> None:
        self._available = set(names)

    def set_provider_instances(self, instances: dict[str, BaseProvider]) -> None:
        self._providers = instances

    def _memory_decision(self) -> MemoryDecision:
        return self._guard.decide()

    def can_use_gpu_now(self) -> bool:
        return self._memory_decision().can_use_gpu

    # ------------------------------------------------------------------
    # Page-level routing (legacy, still used for full-doc fallback)
    # ------------------------------------------------------------------

    def route_page(self, signal: AnalysisSignal) -> RoutingDecision:
        mem = self._memory_decision()
        dominant_type = self._dominant_block_type(signal)
        raw_chain = self._chain_for(dominant_type)
        chain = self._filter_by_memory(raw_chain, mem)
        chain = self._filter_by_availability(chain)
        if not chain:
            chain = self._filter_by_availability(["marker", "markitdown"])
            if not chain:
                chain = ["marker"]

        reason_parts = [f"type={dominant_type.value}", mem.reason]
        if signal.has_equation_signals:
            reason_parts.append("has-equations")
        if signal.has_table_signals:
            reason_parts.append("has-tables")

        return RoutingDecision(
            provider_chain=chain,
            gpu_allowed=mem.can_use_gpu,
            max_batch_size=mem.max_batch_size,
            reason="; ".join(reason_parts),
        )

    # ------------------------------------------------------------------
    # Segment-level routing (MasterPlan 3.1 + 3.2)
    # ------------------------------------------------------------------

    def route_segments(self, segments: list[PageSegment], page_number: int) -> PageRoutingPlan:
        """For each segment on a page, pick the best provider using supports() scores."""
        from yaruk.core.provider import AnalysisContext

        mem = self._memory_decision()
        plan = PageRoutingPlan(page_number=page_number)

        for seg in segments:
            try:
                block_type = BlockType(seg.block_type)
            except ValueError:
                block_type = BlockType.PARAGRAPH

            raw_chain = self._chain_for(block_type)
            chain = self._filter_by_memory(raw_chain, mem)
            chain = self._filter_by_availability(chain)
            if not chain:
                chain = self._filter_by_availability(["marker", "markitdown"])
                if not chain:
                    chain = ["marker"]

            best_provider = chain[0]
            best_score = 0.0
            ctx = AnalysisContext(signals={"text_hint": seg.text_hint})

            for pname in chain:
                prov = self._providers.get(pname)
                if prov is not None:
                    try:
                        score = prov.supports(block_type, ctx)
                    except Exception:
                        score = 0.0
                    if score > best_score:
                        best_score = score
                        best_provider = pname

            if best_score == 0.0:
                best_score = 0.5

            plan.segment_decisions.append(SegmentRoutingDecision(
                segment_block_type=seg.block_type,
                provider_chain=chain,
                best_provider=best_provider,
                score=best_score,
                gpu_allowed=mem.can_use_gpu,
                reason=f"seg={seg.block_type} best={best_provider}({best_score:.2f})",
            ))
            plan.providers_needed.add(best_provider)
            for p in chain:
                plan.providers_needed.add(p)

        return plan

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _dominant_block_type(self, signal: AnalysisSignal) -> BlockType:
        if signal.has_equation_signals:
            return BlockType.EQUATION
        if signal.has_table_signals:
            return BlockType.TABLE
        if signal.text_density < 0.1 and not signal.has_text_layer:
            return BlockType.FIGURE
        return BlockType.PARAGRAPH

    def _chain_for(self, block_type: BlockType) -> list[str]:
        override = self._settings.fallback_chains.get(block_type.value)
        if override:
            return list(override)
        return list(DEFAULT_CHAIN.get(block_type.value, DEFAULT_CHAIN["other"]))

    def _filter_by_memory(self, chain: list[str], mem: MemoryDecision) -> list[str]:
        if mem.can_use_gpu:
            return chain
        return [p for p in chain if p not in GPU_ENGINES] or chain

    def _filter_by_availability(self, chain: list[str]) -> list[str]:
        if not self._available:
            return chain
        return [p for p in chain if p in self._available]

