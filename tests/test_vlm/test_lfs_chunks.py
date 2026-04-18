"""LFS parça bölme / birleştirme."""
from __future__ import annotations

from pathlib import Path

from yaruk.vlm.lfs_chunks import (
    reassemble_lfs_weight_shards,
    split_oversized_safetensors,
)


def test_split_reassemble_roundtrip(tmp_path: Path) -> None:
    chunk = 400
    f = tmp_path / "tiny.safetensors"
    data = b"x" * 2500
    f.write_bytes(data)

    split_oversized_safetensors(tmp_path, chunk_bytes=chunk)

    assert not f.exists()
    manifest = tmp_path / "tiny.safetensors.__lfs_manifest.json"
    assert manifest.is_file()

    reassemble_lfs_weight_shards(tmp_path)
    assert f.read_bytes() == data
