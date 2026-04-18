from __future__ import annotations

from pathlib import Path


def test_asset_manager_dedup(tmp_path: Path) -> None:
    from yaruk.output.asset_manager import AssetManager

    mgr = AssetManager(tmp_path / "assets")
    data = b"hello world image bytes"
    ref1 = mgr.store(data, "b1")
    ref2 = mgr.store(data, "b2")
    assert ref1.asset_id == ref2.asset_id
    assert len(mgr.index.assets) == 1
    assert "b1" in mgr.index.block_to_assets
    assert "b2" in mgr.index.block_to_assets
