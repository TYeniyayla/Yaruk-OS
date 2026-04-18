from __future__ import annotations

from typing import ClassVar

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.engines.markitdown.worker import (
    MARKITDOWN_AVAILABLE,
    MarkItDownWorkerHandler,
)
from yaruk.engines.versions import dist_version
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

__all__ = ["MarkItDownProvider", "MarkItDownWorkerHandler"]


class MarkItDownProvider(BaseProvider):
    name = "markitdown"
    version = dist_version("markitdown", fallback="0.1.5")
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    _SCORES: ClassVar[dict[str, float]] = {
        "paragraph": 0.7,
        "heading": 0.8,
        "table": 0.5,
        "equation": 0.4,
        "figure": 0.4,
        "list": 0.75,
        "code": 0.9,
        "caption": 0.6,
        "footer": 0.7,
        "header": 0.7,
        "other": 0.6,
    }

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        return self._SCORES.get(block_type.value, 0.5)

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        handler = MarkItDownWorkerHandler()
        result = handler.handle("get_page", {
            "page_number": segment.page,
        })
        from yaruk.engines.markitdown.adapter import worker_response_to_blocks
        return worker_response_to_blocks(result)

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="extract-0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="markitdown", source_version=self.version, reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        return ProviderHealth(ok=MARKITDOWN_AVAILABLE, detail=f"markitdown v{self.version}")
