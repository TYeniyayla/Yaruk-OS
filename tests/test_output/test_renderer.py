from __future__ import annotations

from pathlib import Path

from yaruk.models.canonical import (
    BoundingBox,
    DocumentBlock,
    DocumentResult,
    PageResult,
    ProcessingInfo,
)
from yaruk.models.enums import BlockType
from yaruk.models.output_contract import OutputLayout
from yaruk.output.renderer import export_result, render_page_markdown


def _block(text: str, btype: BlockType = BlockType.PARAGRAPH, order: int = 0) -> DocumentBlock:
    return DocumentBlock(
        page=1,
        block_id=f"b{order}",
        type=btype,
        text=text,
        bbox=BoundingBox(x0=0, y0=0, x1=1, y1=1),
        confidence=0.9,
        source_provider="test",
        source_version="1.0",
        reading_order=order,
    )


def test_render_heading() -> None:
    page = PageResult(
        page_number=1, width=100, height=100,
        blocks=[_block("Baslik", BlockType.HEADING)],
    )
    md = render_page_markdown(page)
    assert "Baslik" in md
    assert "##" in md


def test_render_equation() -> None:
    page = PageResult(
        page_number=1, width=100, height=100,
        blocks=[_block("E=mc^2", BlockType.EQUATION)],
    )
    md = render_page_markdown(page)
    assert "$$" in md
    assert "E=mc^2" in md


def test_export_creates_files(tmp_path: Path) -> None:
    layout = OutputLayout.for_job(tmp_path, "test-job")
    result = DocumentResult(
        source_path=Path("test.pdf"),
        total_pages=1,
        pages=[
            PageResult(
                page_number=1, width=100, height=100,
                blocks=[_block("Hello world")],
            ),
        ],
        processing_info=ProcessingInfo(trace_id="test-trace"),
    )
    export_result(result, layout)
    assert layout.merged_md.exists()
    assert layout.merged_json.exists()
    assert layout.metadata_json.exists()
    assert (layout.pages_dir / "page_001.json").exists()
    md_content = layout.merged_md.read_text()
    assert "Hello world" in md_content
