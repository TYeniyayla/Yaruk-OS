from __future__ import annotations

from typing import ClassVar

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.engines.marker.worker import MARKER_AVAILABLE, MarkerWorkerHandler
from yaruk.engines.versions import dist_version
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

__all__ = ["MarkerProvider", "MarkerWorkerHandler"]


class MarkerProvider(BaseProvider):
    name = "marker"
    version = dist_version("marker-pdf", fallback="0.3.0")
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    _SCORES: ClassVar[dict[str, float]] = {
        "paragraph": 0.9,
        "heading": 0.95,
        "table": 0.7,
        "equation": 0.6,
        "figure": 0.5,
        "list": 0.8,
        "code": 0.7,
        "caption": 0.6,
        "other": 0.7,
    }

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        return self._SCORES.get(block_type.value, 0.5)

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        handler = MarkerWorkerHandler()
        result = handler.handle("get_page", {
            "page_number": segment.page,
        })
        from yaruk.engines.marker.adapter import worker_response_to_blocks
        return worker_response_to_blocks(result)

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="extract-0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="marker", source_version=self.version, reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        return ProviderHealth(ok=MARKER_AVAILABLE, detail=f"marker v{self.version}")
