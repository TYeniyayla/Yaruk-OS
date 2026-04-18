from __future__ import annotations

from typing import ClassVar

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.engines.docling.worker import DOCLING_AVAILABLE, DoclingWorkerHandler
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

__all__ = ["DoclingProvider", "DoclingWorkerHandler"]


class DoclingProvider(BaseProvider):
    name = "docling"
    version = "2.0.0"
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    _SCORES: ClassVar[dict[str, float]] = {
        "paragraph": 0.9,
        "heading": 0.95,
        "table": 0.98,
        "equation": 0.85,
        "figure": 0.9,
        "list": 0.85,
        "code": 0.8,
        "caption": 0.8,
        "other": 0.8,
    }

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        return self._SCORES.get(block_type.value, 0.5)

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        handler = DoclingWorkerHandler()
        result = handler.handle("get_page", {
            "page_number": segment.page,
        })
        from yaruk.engines.docling.adapter import worker_response_to_blocks
        return worker_response_to_blocks(result)

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="extract-0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="docling", source_version=self.version, reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        return ProviderHealth(ok=DOCLING_AVAILABLE, detail=f"docling v{self.version}")
