from __future__ import annotations

from typing import Any

from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType

_BLOCK_TYPE_MAP: dict[str, BlockType] = {
    "paragraph": BlockType.PARAGRAPH,
    "heading": BlockType.HEADING,
    "table": BlockType.TABLE,
    "equation": BlockType.EQUATION,
    "figure": BlockType.FIGURE,
    "list": BlockType.LIST,
    "code": BlockType.CODE,
    "caption": BlockType.CAPTION,
    "footer": BlockType.FOOTER,
    "header": BlockType.HEADER,
}


def _parse_bbox(raw: dict[str, Any]) -> BoundingBox:
    bbox_data = raw.get("bbox")
    if isinstance(bbox_data, dict):
        return BoundingBox(
            x0=bbox_data.get("x0", 0.0),
            y0=bbox_data.get("y0", 0.0),
            x1=bbox_data.get("x1", 0.0),
            y1=bbox_data.get("y1", 0.0),
        )
    return BoundingBox(x0=0.0, y0=0.0, x1=0.0, y1=0.0)


def worker_response_to_blocks(response: dict[str, Any]) -> list[DocumentBlock]:
    """Convert MarkItDown worker page response to canonical DocumentBlocks."""
    blocks: list[DocumentBlock] = []
    page = response.get("page_number", 1)
    for raw_block in response.get("blocks", []):
        raw_type = raw_block.get("type", "paragraph")
        block_type = _BLOCK_TYPE_MAP.get(raw_type, BlockType.OTHER)

        blocks.append(DocumentBlock(
            page=raw_block.get("page", page),
            block_id=raw_block.get("block_id", ""),
            type=block_type,
            text=raw_block.get("text", ""),
            bbox=_parse_bbox(raw_block),
            confidence=raw_block.get("confidence", 0.70),
            source_provider="markitdown",
            source_version=raw_block.get("source_version", "0.0.0"),
            reading_order=raw_block.get("reading_order", 0),
            language=raw_block.get("language"),
            style=raw_block.get("style"),
            raw_payload=raw_block.get("raw_payload"),
        ))
    return blocks
