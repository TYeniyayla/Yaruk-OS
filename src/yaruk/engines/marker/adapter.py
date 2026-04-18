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


def native_to_canonical(
    raw: dict[str, Any],
    page: int,
    source_version: str = "0.0.0",
) -> DocumentBlock:
    raw_type = raw.get("type", "paragraph")
    block_type = _BLOCK_TYPE_MAP.get(raw_type, BlockType.OTHER)

    return DocumentBlock(
        page=raw.get("page", page),
        block_id=raw.get("block_id", raw.get("id", "")),
        type=block_type,
        text=raw.get("text", ""),
        bbox=_parse_bbox(raw),
        confidence=raw.get("confidence", 0.5),
        source_provider="marker",
        source_version=raw.get("source_version", source_version),
        reading_order=raw.get("reading_order", 0),
        language=raw.get("language"),
        style=raw.get("style"),
        raw_payload=raw.get("raw_payload"),
    )


def worker_response_to_blocks(response: dict[str, Any]) -> list[DocumentBlock]:
    blocks: list[DocumentBlock] = []
    page = response.get("page_number", 1)
    for raw_block in response.get("blocks", []):
        blocks.append(native_to_canonical(raw_block, page=page))
    return blocks
