from __future__ import annotations

from typing import ClassVar

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.engines.mineru.worker import MINERU_AVAILABLE, MinerUWorkerHandler
from yaruk.engines.versions import dist_version
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

__all__ = ["MinerUProvider", "MinerUWorkerHandler"]


class MinerUProvider(BaseProvider):
    name = "mineru"
    version = dist_version("magic-pdf", fallback="0.1.0")
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    _SCORES: ClassVar[dict[str, float]] = {
        "paragraph": 0.85,
        "heading": 0.9,
        "table": 0.95,
        "equation": 0.98,
        "figure": 0.85,
        "list": 0.7,
        "code": 0.8,
        "caption": 0.7,
        "other": 0.75,
    }

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        return self._SCORES.get(block_type.value, 0.5)

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        handler = MinerUWorkerHandler()
        result = handler.handle("get_page", {
            "page_number": segment.page,
        })
        from yaruk.engines.mineru.adapter import worker_response_to_blocks
        return worker_response_to_blocks(result)

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="extract-0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="mineru", source_version=self.version, reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        return ProviderHealth(ok=MINERU_AVAILABLE, detail=f"mineru v{self.version}")
