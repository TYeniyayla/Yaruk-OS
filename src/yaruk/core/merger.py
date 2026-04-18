from __future__ import annotations

from yaruk.models.canonical import BoundingBox, DocumentBlock


def _iou(a: BoundingBox, b: BoundingBox) -> float:
    """Intersection-over-Union for two bounding boxes."""
    ix0 = max(a.x0, b.x0)
    iy0 = max(a.y0, b.y0)
    ix1 = min(a.x1, b.x1)
    iy1 = min(a.y1, b.y1)
    inter = max(0.0, ix1 - ix0) * max(0.0, iy1 - iy0)
    area_a = max(0.0, a.x1 - a.x0) * max(0.0, a.y1 - a.y0)
    area_b = max(0.0, b.x1 - b.x0) * max(0.0, b.y1 - b.y0)
    union = area_a + area_b - inter
    if union <= 0:
        return 0.0
    return inter / union


def _dedup(blocks: list[DocumentBlock], iou_threshold: float = 0.8) -> list[DocumentBlock]:
    """Remove overlapping blocks from different providers, keeping highest confidence."""
    if not blocks:
        return blocks
    blocks_sorted = sorted(blocks, key=lambda b: -b.confidence)
    kept: list[DocumentBlock] = []
    for block in blocks_sorted:
        is_dup = False
        for existing in kept:
            if existing.page == block.page and _iou(existing.bbox, block.bbox) > iou_threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(block)
    return kept


def _reassign_reading_order(blocks: list[DocumentBlock]) -> list[DocumentBlock]:
    """Sort by page then top-to-bottom, left-to-right and reassign reading_order."""
    blocks_sorted = sorted(blocks, key=lambda b: (b.page, b.bbox.y0, b.bbox.x0))
    result: list[DocumentBlock] = []
    for idx, block in enumerate(blocks_sorted):
        result.append(block.model_copy(update={"reading_order": idx}))
    return result


def merge_blocks(
    block_lists: list[list[DocumentBlock]],
    iou_threshold: float = 0.8,
) -> list[DocumentBlock]:
    merged: list[DocumentBlock] = []
    for lst in block_lists:
        merged.extend(lst)
    merged = _dedup(merged, iou_threshold)
    merged = _reassign_reading_order(merged)
    return merged

