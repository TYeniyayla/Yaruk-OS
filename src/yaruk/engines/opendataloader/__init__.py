from __future__ import annotations

from typing import ClassVar

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.engines.opendataloader.worker import (
    _ODL_VERSION,
    _USE_SDK,
    OPENDATALOADER_AVAILABLE,
    OpenDataLoaderWorkerHandler,
)
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

__all__ = ["OpenDataLoaderProvider", "OpenDataLoaderWorkerHandler"]


class OpenDataLoaderProvider(BaseProvider):
    name = "opendataloader"
    version = _ODL_VERSION
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    _SCORES_SDK: ClassVar[dict[str, float]] = {
        "paragraph": 0.88,
        "heading": 0.92,
        "table": 0.95,
        "equation": 0.80,
        "figure": 0.90,
        "list": 0.88,
        "code": 0.75,
        "caption": 0.82,
        "other": 0.75,
    }

    _SCORES_PLUMBER: ClassVar[dict[str, float]] = {
        "paragraph": 0.80,
        "heading": 0.85,
        "table": 0.88,
        "equation": 0.70,
        "figure": 0.85,
        "list": 0.80,
        "code": 0.70,
        "caption": 0.70,
        "other": 0.70,
    }

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        scores = self._SCORES_SDK if _USE_SDK else self._SCORES_PLUMBER
        return scores.get(block_type.value, 0.5)

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        handler = OpenDataLoaderWorkerHandler()
        result = handler.handle("get_page", {
            "page_number": segment.page,
        })
        from yaruk.engines.opendataloader.adapter import worker_response_to_blocks
        return worker_response_to_blocks(result)

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="extract-0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="opendataloader", source_version=self.version, reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        sdk_info = "SDK" if _USE_SDK else "pdfplumber-fallback"
        return ProviderHealth(
            ok=OPENDATALOADER_AVAILABLE,
            detail=f"opendataloader v{self.version} ({sdk_info})",
        )
