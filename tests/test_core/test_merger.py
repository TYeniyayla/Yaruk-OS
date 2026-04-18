from __future__ import annotations

from yaruk.core.merger import _dedup, _iou, merge_blocks
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType


def _block(
    page: int = 1,
    x0: float = 0.0,
    y0: float = 0.0,
    x1: float = 1.0,
    y1: float = 1.0,
    conf: float = 0.9,
    provider: str = "marker",
    order: int = 0,
) -> DocumentBlock:
    return DocumentBlock(
        page=page,
        block_id=f"b-{provider}-{page}-{order}",
        type=BlockType.PARAGRAPH,
        text=f"text from {provider}",
        bbox=BoundingBox(x0=x0, y0=y0, x1=x1, y1=y1),
        confidence=conf,
        source_provider=provider,
        source_version="test",
        reading_order=order,
    )


def test_iou_identical() -> None:
    a = BoundingBox(x0=0, y0=0, x1=1, y1=1)
    assert _iou(a, a) == 1.0


def test_iou_no_overlap() -> None:
    a = BoundingBox(x0=0, y0=0, x1=1, y1=1)
    b = BoundingBox(x0=2, y0=2, x1=3, y1=3)
    assert _iou(a, b) == 0.0


def test_dedup_removes_overlapping() -> None:
    b1 = _block(conf=0.9, provider="marker")
    b2 = _block(conf=0.5, provider="docling")
    result = _dedup([b1, b2])
    assert len(result) == 1
    assert result[0].source_provider == "marker"


def test_dedup_keeps_non_overlapping() -> None:
    b1 = _block(y0=0, y1=1, conf=0.9)
    b2 = _block(y0=10, y1=11, conf=0.8, order=1)
    result = _dedup([b1, b2])
    assert len(result) == 2


def test_merge_reassigns_reading_order() -> None:
    b1 = _block(y0=100, y1=200, order=0)
    b2 = _block(y0=0, y1=50, order=1, provider="docling")
    merged = merge_blocks([[b1], [b2]])
    assert merged[0].bbox.y0 < merged[1].bbox.y0
    assert merged[0].reading_order == 0
    assert merged[1].reading_order == 1
