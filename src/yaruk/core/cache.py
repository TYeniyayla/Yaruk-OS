"""Input hash cache: avoids reprocessing unchanged PDFs.

MasterPlan — Cache System:
- Input hash (PDF SHA-256) deduplication
- Page-level granular cache
- Cache invalidation strategy
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_BUF_SIZE = 256 * 1024  # 256 KB read chunks


def file_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file (streaming, memory-safe)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_BUF_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class DiskCache:
    """Flat-file JSON cache keyed by (file_hash, engine, schema_version).

    Directory layout::

        cache_dir/
          {sha256}/
            {engine}.json      # full conversion result
            meta.json           # {source_path, mtime, sha256, schema_version}
    """

    def __init__(self, cache_dir: Path) -> None:
        self._root = cache_dir

    def _entry_dir(self, file_hash: str, engine: str) -> Path:
        return self._root / file_hash

    def _entry_path(self, file_hash: str, engine: str) -> Path:
        return self._root / file_hash / f"{engine}.json"

    def get(self, file_hash: str, engine: str, schema_version: str = "v1") -> dict[str, Any] | None:
        p = self._entry_path(file_hash, engine)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if data.get("schema_version") != schema_version:
                log.info("cache schema mismatch for %s/%s — invalidating", file_hash[:12], engine)
                p.unlink(missing_ok=True)
                return None
            log.info("disk cache hit: %s/%s", file_hash[:12], engine)
            return data  # type: ignore[no-any-return]
        except Exception:
            log.debug("disk cache read error for %s/%s", file_hash[:12], engine, exc_info=True)
            return None

    def put(
        self,
        file_hash: str,
        engine: str,
        data: dict[str, Any],
        source_path: Path | None = None,
        schema_version: str = "v1",
    ) -> None:
        d = self._entry_dir(file_hash, engine)
        d.mkdir(parents=True, exist_ok=True)
        data["schema_version"] = schema_version
        self._entry_path(file_hash, engine).write_text(
            json.dumps(data, default=str), encoding="utf-8",
        )
        meta_path = d / "meta.json"
        if not meta_path.exists() and source_path:
            meta = {
                "sha256": file_hash,
                "source_path": str(source_path),
                "schema_version": schema_version,
            }
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
        log.info("disk cache write: %s/%s", file_hash[:12], engine)

    def invalidate(self, file_hash: str, engine: str | None = None) -> None:
        if engine:
            p = self._entry_path(file_hash, engine)
            p.unlink(missing_ok=True)
        else:
            d = self._root / file_hash
            if d.is_dir():
                for f in d.iterdir():
                    f.unlink(missing_ok=True)
                d.rmdir()
        log.info("disk cache invalidated: %s/%s", file_hash[:12], engine or "*")
