from __future__ import annotations

import logging

from yaruk.models.canonical import DocumentBlock, DocumentResult, PageResult
from yaruk.models.enums import BlockType
from yaruk.models.output_contract import OutputLayout
from yaruk.postprocess.pipeline import beautify_markdown, clean_block_text

log = logging.getLogger(__name__)

_HEADING_LEVELS = {"h1": "#", "h2": "##", "h3": "###", "h4": "####", "h5": "#####", "h6": "######"}


def _render_block(block: DocumentBlock) -> str:
    btype = block.type
    text = clean_block_text(block.text or "")

    if btype == BlockType.HEADING:
        level = (block.style or {}).get("level", "h2")
        prefix = _HEADING_LEVELS.get(str(level), "##")
        return f"{prefix} {text}"

    if btype == BlockType.EQUATION:
        eq_text = text.strip()
        if eq_text.startswith("$$") and eq_text.endswith("$$"):
            return eq_text
        return f"$$\n{eq_text}\n$$"

    if btype == BlockType.CODE:
        lang = (block.style or {}).get("language", "")
        return f"```{lang}\n{block.text}\n```"

    if btype == BlockType.TABLE:
        return text

    if btype == BlockType.FIGURE:
        rp = block.raw_payload or {}
        asset_path = rp.get("asset_path")
        caption = clean_block_text(str(rp.get("caption", "")))
        figure_id = rp.get("figure_id", "")
        context = clean_block_text(str(rp.get("context", "")))

        alt = figure_id or text or "figure"
        parts: list[str] = []
        if asset_path:
            parts.append(f"![{alt}]({asset_path})")
        if caption:
            parts.append(f"*{caption}*")
        elif context:
            parts.append(f"*[Figure context: {context[:120]}]*")
        if not parts:
            parts.append(f"[Gorsel: {alt}]")
        return "\n\n".join(parts)

    if btype == BlockType.LIST:
        return text

    if btype == BlockType.CAPTION:
        return f"*{text}*"

    if btype in (BlockType.FOOTER, BlockType.HEADER):
        return ""

    return text


def sanitize_page_blocks(page: PageResult) -> PageResult:
    """Apply block-level text hygiene in place and return the same PageResult.

    Keeps IR on disk (``pages/page_XXX.json``) artefact-free, so downstream
    consumers (GUI/TUI/MT) don't have to re-sanitize.
    """
    for block in page.blocks:
        if block.text:
            cleaned = clean_block_text(block.text)
            if cleaned != block.text:
                block.text = cleaned
        if block.raw_payload:
            for key in ("caption", "context"):
                val = block.raw_payload.get(key)
                if isinstance(val, str) and val:
                    cleaned = clean_block_text(val)
                    if cleaned != val:
                        block.raw_payload[key] = cleaned
    return page


def render_page_markdown(page: PageResult) -> str:
    lines: list[str] = []
    for block in sorted(page.blocks, key=lambda b: b.reading_order):
        rendered = _render_block(block)
        if rendered:
            lines.append(rendered)
            lines.append("")
    return "\n".join(lines)


def export_result(
    result: DocumentResult,
    layout: OutputLayout,
    full_markdown: str | None = None,
) -> None:
    layout.ensure_dirs()
    layout.metadata_json.write_text(
        result.metadata.model_dump_json(indent=2), encoding="utf-8",
    )
    md_parts: list[str] = []
    for page in result.pages:
        page_path = layout.pages_dir / f"page_{page.page_number:03d}.json"
        page_path.write_text(page.model_dump_json(indent=2), encoding="utf-8")
        md_parts.append(render_page_markdown(page))

    if full_markdown:
        merged = beautify_markdown(full_markdown)
    else:
        merged = "\n---\n\n".join(md_parts) if md_parts else ""
        merged = beautify_markdown(merged)
    layout.merged_md.write_text(merged, encoding="utf-8")
    layout.merged_json.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    layout.asset_index_json.write_text(
        result.assets.model_dump_json(indent=2), encoding="utf-8",
    )
    log.info("exported job to %s", layout.job_dir)
