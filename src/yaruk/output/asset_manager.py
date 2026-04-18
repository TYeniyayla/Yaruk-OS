from __future__ import annotations

import hashlib
from pathlib import Path

from yaruk.models.canonical import AssetIndex, AssetRef


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


class AssetManager:
    def __init__(self, assets_dir: Path) -> None:
        self._dir = assets_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._index = AssetIndex()

    def store(self, data: bytes, block_id: str, mime_type: str = "image/png", ext: str = ".png") -> AssetRef:
        h = _sha256_bytes(data)
        existing = self._index.assets.get(h)
        if existing:
            self._index.block_to_assets.setdefault(block_id, []).append(h)
            return existing
        filename = f"{h}{ext}"
        out_path = self._dir / filename
        out_path.write_bytes(data)
        ref = AssetRef(asset_id=h, rel_path=f"assets/{filename}", mime_type=mime_type, sha256=h)
        self._index.assets[h] = ref
        self._index.block_to_assets.setdefault(block_id, []).append(h)
        return ref

    @property
    def index(self) -> AssetIndex:
        return self._index
