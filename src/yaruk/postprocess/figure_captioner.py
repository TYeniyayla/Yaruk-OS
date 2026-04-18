"""Context-based figure captioning for LLM-friendly output.

Extracts captions from surrounding text blocks so that an LLM can understand
figures without needing to parse the actual image bytes.  Each figure block
gets:
  - ``raw_payload.caption``   - verbatim caption text (if found nearby)
  - ``raw_payload.context``   - short context from preceding/following blocks
  - ``raw_payload.figure_id`` - stable reference id (e.g. "Figure 1.5")
  - ``text`` field updated to a human-readable summary string
"""
from __future__ import annotations

import re

from yaruk.models.canonical import DocumentBlock, PageResult
from yaruk.models.enums import BlockType

_FIG_REF_RE = re.compile(
    r"(?:Figure|Fig\.?|Diagram|Chart|Illustration|Schematic)\s*(\d+[\.\-]?\d*)",
    re.IGNORECASE,
)

_CAPTION_BLOCK_TYPES = frozenset({BlockType.CAPTION, BlockType.PARAGRAPH})


def _find_nearby_caption(
    blocks: list[DocumentBlock],
    fig_idx: int,
    search_range: int = 3,
) -> str | None:
    """Search nearby blocks for a caption-like text."""
    n = len(blocks)
    candidates: list[tuple[int, str]] = []

    for offset in range(1, search_range + 1):
        for idx in (fig_idx + offset, fig_idx - offset):
            if 0 <= idx < n:
                b = blocks[idx]
                if b.type not in _CAPTION_BLOCK_TYPES:
                    continue
                text = b.text.strip()
                if not text:
                    continue
                if _FIG_REF_RE.search(text):
                    return text
                if len(text) < 200 and offset == 1:
                    candidates.append((abs(offset), text))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return None


def _extract_figure_id(text: str) -> str | None:
    m = _FIG_REF_RE.search(text)
    return m.group(0).strip() if m else None


def _build_context_snippet(
    blocks: list[DocumentBlock],
    fig_idx: int,
    max_chars: int = 200,
) -> str:
    """Build a short context string from the block before and after the figure."""
    parts: list[str] = []
    for offset in (-1, 1):
        idx = fig_idx + offset
        if 0 <= idx < len(blocks):
            b = blocks[idx]
            if b.type in (BlockType.FOOTER, BlockType.HEADER):
                continue
            snippet = b.text.strip()[:max_chars]
            if snippet:
                parts.append(snippet)
    return " [...] ".join(parts) if parts else ""


def caption_figures_in_page(page: PageResult) -> int:
    """Enrich figure blocks in a page with caption/context metadata.

    Returns the number of figures enriched.
    """
    enriched = 0
    sorted_blocks = sorted(page.blocks, key=lambda b: b.reading_order)

    for fig_idx, block in enumerate(sorted_blocks):
        if block.type != BlockType.FIGURE:
            continue

        if block.raw_payload is None:
            block.raw_payload = {}

        caption = _find_nearby_caption(sorted_blocks, fig_idx)
        context = _build_context_snippet(sorted_blocks, fig_idx)
        figure_id = None

        if caption:
            figure_id = _extract_figure_id(caption)
            block.raw_payload["caption"] = caption

        if context:
            block.raw_payload["context"] = context

        if figure_id:
            block.raw_payload["figure_id"] = figure_id

        asset_path = block.raw_payload.get("asset_path", "")
        summary_parts: list[str] = []
        if figure_id:
            summary_parts.append(figure_id)
        if caption and caption != figure_id:
            summary_parts.append(caption[:150])
        if not summary_parts and context:
            summary_parts.append(f"[Figure near: {context[:100]}]")
        if asset_path:
            summary_parts.append(f"(see {asset_path})")

        if summary_parts:
            block.text = " -- ".join(summary_parts)
            enriched += 1

    return enriched


def caption_all_figures(pages: list[PageResult]) -> int:
    """Run figure captioning across all pages. Returns total enriched count."""
    total = 0
    for page in pages:
        total += caption_figures_in_page(page)
    return total
