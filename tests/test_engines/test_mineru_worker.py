from __future__ import annotations

from yaruk.engines.mineru.adapter import worker_response_to_blocks
from yaruk.engines.mineru.worker import MinerUWorkerHandler


def test_mineru_health() -> None:
    handler = MinerUWorkerHandler()
    result = handler.handle("health", {})
    assert result["name"] == "mineru"
    assert isinstance(result["ok"], bool)


def test_mineru_unknown_method() -> None:
    handler = MinerUWorkerHandler()
    import pytest
    with pytest.raises(ValueError, match="unknown"):
        handler.handle("unknown_method", {})


def test_mineru_missing_file() -> None:
    handler = MinerUWorkerHandler()
    result = handler.handle("convert_full", {"pdf_path": "/nonexistent/file.pdf"})
    assert result.get("error") or result.get("pages") == {}


def test_mineru_adapter_response_to_blocks() -> None:
    response = {
        "page_number": 1,
        "blocks": [
            {
                "block_id": "p1-mineru-b0",
                "type": "equation",
                "text": "E = mc^2",
                "bbox": {"x0": 10.0, "y0": 20.0, "x1": 200.0, "y1": 60.0},
                "confidence": 0.90,
                "source_provider": "mineru",
                "source_version": "1.3.12",
            }
        ],
    }
    blocks = worker_response_to_blocks(response)
    assert len(blocks) == 1
    assert blocks[0].text == "E = mc^2"
    assert blocks[0].type.value == "equation"
    assert blocks[0].source_provider == "mineru"
