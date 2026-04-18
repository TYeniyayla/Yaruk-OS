from __future__ import annotations

from yaruk.engines.marker.adapter import native_to_canonical, worker_response_to_blocks
from yaruk.engines.marker.worker import MarkerWorkerHandler


def test_marker_health() -> None:
    handler = MarkerWorkerHandler()
    result = handler.handle("health", {})
    assert result["name"] == "marker"
    assert result["ok"] is True


def test_marker_analyze_page_fallback() -> None:
    handler = MarkerWorkerHandler()
    result = handler.handle("analyze_page", {
        "page_number": 1,
        "text": "Hello World\n\nSecond paragraph",
    })
    assert result["page_number"] == 1
    assert len(result["blocks"]) >= 1


def test_marker_adapter_native_to_canonical() -> None:
    raw = {
        "block_id": "b1",
        "type": "paragraph",
        "text": "Hello",
        "bbox": {"x0": 0, "y0": 0, "x1": 100, "y1": 50},
        "confidence": 0.9,
    }
    block = native_to_canonical(raw, page=1)
    assert block.text == "Hello"
    assert block.source_provider == "marker"
    assert block.bbox.x1 == 100


def test_marker_adapter_worker_response() -> None:
    response = {
        "page_number": 2,
        "blocks": [
            {
                "block_id": "b0",
                "type": "heading",
                "text": "Title",
                "bbox": {"x0": 0, "y0": 0, "x1": 1, "y1": 1},
                "confidence": 0.85,
                "source_provider": "marker",
                "source_version": "test",
            }
        ],
    }
    blocks = worker_response_to_blocks(response)
    assert len(blocks) == 1
    assert blocks[0].type.value == "heading"
    assert blocks[0].page == 2


def test_marker_unknown_method() -> None:
    handler = MarkerWorkerHandler()
    import pytest
    with pytest.raises(ValueError, match="unknown"):
        handler.handle("unknown", {})
