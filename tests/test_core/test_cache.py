from __future__ import annotations

from pathlib import Path

import pytest

from yaruk.core.cache import DiskCache, file_sha256


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def sample_pdf(tmp_path: Path) -> Path:
    p = tmp_path / "test.pdf"
    p.write_bytes(b"%PDF-1.4 dummy content for hash testing")
    return p


def test_file_sha256_deterministic(sample_pdf: Path) -> None:
    h1 = file_sha256(sample_pdf)
    h2 = file_sha256(sample_pdf)
    assert h1 == h2
    assert len(h1) == 64


def test_disk_cache_put_get(cache_dir: Path, sample_pdf: Path) -> None:
    dc = DiskCache(cache_dir)
    data = {"pages": {1: {"blocks": []}}, "markdown": "hello"}
    fhash = file_sha256(sample_pdf)
    dc.put(fhash, "marker", data, sample_pdf)
    result = dc.get(fhash, "marker")
    assert result is not None
    assert result["markdown"] == "hello"


def test_disk_cache_miss(cache_dir: Path) -> None:
    dc = DiskCache(cache_dir)
    assert dc.get("nonexistent", "marker") is None


def test_disk_cache_schema_mismatch(cache_dir: Path, sample_pdf: Path) -> None:
    dc = DiskCache(cache_dir)
    fhash = file_sha256(sample_pdf)
    dc.put(fhash, "marker", {"pages": {}}, schema_version="v1")
    assert dc.get(fhash, "marker", schema_version="v2") is None


def test_disk_cache_invalidate(cache_dir: Path, sample_pdf: Path) -> None:
    dc = DiskCache(cache_dir)
    fhash = file_sha256(sample_pdf)
    dc.put(fhash, "marker", {"pages": {}}, sample_pdf)
    dc.put(fhash, "docling", {"pages": {}}, sample_pdf)
    dc.invalidate(fhash, "marker")
    assert dc.get(fhash, "marker") is None
    assert dc.get(fhash, "docling") is not None
