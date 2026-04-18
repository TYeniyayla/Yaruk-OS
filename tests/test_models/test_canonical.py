from __future__ import annotations

from pathlib import Path

from yaruk.models.canonical import (
    BoundingBox,
    DocumentBlock,
    DocumentResult,
    ProcessingInfo,
)
from yaruk.models.enums import BlockType


def test_document_block_validate() -> None:
    b = DocumentBlock(
        page=1,
        block_id="b1",
        type=BlockType.PARAGRAPH,
        text="hi",
        bbox=BoundingBox(x0=0, y0=0, x1=1, y1=1),
        confidence=0.9,
        source_provider="marker",
        source_version="poc",
        reading_order=0,
    )
    assert b.schema_version == "v1"


def test_document_result_validate() -> None:
    info = ProcessingInfo(trace_id="t1")
    r = DocumentResult(source_path=Path("x.pdf"), total_pages=0, pages=[], processing_info=info)
    assert r.processing_info.trace_id == "t1"

