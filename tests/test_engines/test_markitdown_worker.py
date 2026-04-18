from __future__ import annotations

from yaruk.engines.markitdown.adapter import worker_response_to_blocks
from yaruk.engines.markitdown.worker import MarkItDownWorkerHandler


def test_markitdown_health() -> None:
    handler = MarkItDownWorkerHandler()
    result = handler.handle("health", {})
    assert result["name"] == "markitdown"
    assert isinstance(result["ok"], bool)


def test_markitdown_unknown_method() -> None:
    handler = MarkItDownWorkerHandler()
    import pytest
    with pytest.raises(ValueError, match="unknown"):
        handler.handle("unknown_method", {})


def test_markitdown_missing_file() -> None:
    handler = MarkItDownWorkerHandler()
    result = handler.handle("convert_full", {"pdf_path": "/nonexistent/file.pdf"})
    assert result.get("error") or result.get("pages") == {}


def test_markitdown_adapter_response_to_blocks() -> None:
    response = {
        "page_number": 1,
        "blocks": [
            {
                "block_id": "p1-markitdown-b0",
                "type": "heading",
                "text": "Chapter 1",
                "bbox": {"x0": 0.0, "y0": 0.0, "x1": 612.0, "y1": 50.0},
                "confidence": 0.70,
                "source_provider": "markitdown",
                "source_version": "0.1.5",
            }
        ],
    }
    blocks = worker_response_to_blocks(response)
    assert len(blocks) == 1
    assert blocks[0].text == "Chapter 1"
    assert blocks[0].type.value == "heading"
    assert blocks[0].source_provider == "markitdown"
