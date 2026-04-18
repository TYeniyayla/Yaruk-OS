from __future__ import annotations

from pathlib import Path

from yaruk.engines.opendataloader.adapter import worker_response_to_blocks
from yaruk.engines.opendataloader.worker import (
    OPENDATALOADER_AVAILABLE,
    OpenDataLoaderWorkerHandler,
)


def test_opendataloader_health() -> None:
    handler = OpenDataLoaderWorkerHandler()
    result = handler.handle("health", {})
    assert result["name"] == "opendataloader"
    assert isinstance(result["ok"], bool)


def test_opendataloader_unknown_method() -> None:
    handler = OpenDataLoaderWorkerHandler()
    import pytest
    with pytest.raises(ValueError, match="unknown"):
        handler.handle("unknown_method", {})


def test_opendataloader_missing_file() -> None:
    handler = OpenDataLoaderWorkerHandler()
    result = handler.handle("convert_full", {"pdf_path": "/nonexistent/file.pdf"})
    assert result.get("error") or result.get("pages") == {}


def test_opendataloader_convert_fixture(tmp_path: Path) -> None:
    fixture = Path(__file__).parent.parent / "fixtures" / "pdfs" / "test_2page.pdf"
    if not fixture.exists() or not OPENDATALOADER_AVAILABLE:
        return

    handler = OpenDataLoaderWorkerHandler()
    result = handler.handle("convert_full", {"pdf_path": str(fixture)})
    assert result.get("pages")
    assert result["page_count"] >= 1

    page1 = result["pages"].get(1, {})
    assert page1.get("blocks")
    for block in page1["blocks"]:
        assert "bbox" in block
        assert block["source_provider"] == "opendataloader"


def test_opendataloader_adapter_response_to_blocks() -> None:
    response = {
        "page_number": 1,
        "blocks": [
            {
                "block_id": "p1-odl-b0",
                "type": "table",
                "text": "| A | B |\n|---|---|\n| 1 | 2 |",
                "bbox": {"x0": 50.0, "y0": 100.0, "x1": 400.0, "y1": 300.0},
                "confidence": 0.88,
                "source_provider": "opendataloader",
                "source_version": "0.11.0",
            }
        ],
    }
    blocks = worker_response_to_blocks(response)
    assert len(blocks) == 1
    assert blocks[0].type.value == "table"
    assert blocks[0].source_provider == "opendataloader"
    assert blocks[0].bbox.x0 == 50.0
