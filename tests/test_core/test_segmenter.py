from __future__ import annotations

from yaruk.core.config import YarukSettings
from yaruk.core.segmenter import (
    PageLayout,
    PageSegment,
    page_layout_from_odl_json,
    score_layout_quality,
)


def test_score_layout_quality_empty() -> None:
    lo = PageLayout(page_number=1, width=100, height=100, segments=[])
    assert score_layout_quality(lo) == 0.0


def test_score_layout_quality_nonempty() -> None:
    lo = PageLayout(
        page_number=1,
        width=612,
        height=792,
        segments=[
            PageSegment(1, "paragraph", (10, 10, 100, 50)),
            PageSegment(1, "table", (10, 60, 400, 200)),
        ],
    )
    assert score_layout_quality(lo) > 0.2


def test_page_layout_from_odl_json() -> None:
    data = {
        "page_number": 3,
        "width": 600,
        "height": 800,
        "segments": [
            {
                "block_type": "table",
                "bbox": [0, 0, 100, 50],
                "text_hint": "h",
                "confidence": 0.9,
            },
        ],
    }
    lo = page_layout_from_odl_json(data, 1)
    assert lo is not None
    assert lo.page_number == 3
    assert len(lo.segments) == 1
    assert lo.segments[0].block_type == "table"


def test_segmenter_respects_settings_pdfplumber_only() -> None:
    from yaruk.core.segmenter import Segmenter

    s = Segmenter(YarukSettings(segmenter_backend="pdfplumber"))
    assert s._settings is not None
    assert s._settings.segmenter_backend == "pdfplumber"
