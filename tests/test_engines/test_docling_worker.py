from __future__ import annotations

from yaruk.engines.docling.adapter import worker_response_to_blocks
from yaruk.engines.docling.worker import DoclingWorkerHandler


def test_docling_health() -> None:
    handler = DoclingWorkerHandler()
    result = handler.handle("health", {})
    assert result["name"] == "docling"
    assert isinstance(result["ok"], bool)


def test_docling_unknown_method() -> None:
    handler = DoclingWorkerHandler()
    import pytest
    with pytest.raises(ValueError, match="unknown"):
        handler.handle("unknown_method", {})


def test_docling_missing_file() -> None:
    handler = DoclingWorkerHandler()
    result = handler.handle("convert_full", {"pdf_path": "/nonexistent/file.pdf"})
    assert result.get("error") or result.get("pages") == {}


def test_docling_adapter_response_to_blocks() -> None:
    response = {
        "page_number": 1,
        "blocks": [
            {
                "block_id": "p1-docling-b0",
                "type": "paragraph",
                "text": "Test paragraph",
                "bbox": {"x0": 0.0, "y0": 0.0, "x1": 100.0, "y1": 50.0},
                "confidence": 0.88,
                "source_provider": "docling",
                "source_version": "2.0.0",
            }
        ],
    }
    blocks = worker_response_to_blocks(response)
    assert len(blocks) == 1
    assert blocks[0].text == "Test paragraph"
    assert blocks[0].source_provider == "docling"
